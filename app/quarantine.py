"""
质控隔离业务：标记隔离、解除隔离、隔离清单、按月可信取数。

关键正确性：
  * 标记在单事务内完成：先对观测行加行锁（SELECT ... FOR UPDATE）核对当前测值，
    再写隔离登记。若页面所见测值与当前记录不一致则拒绝（409），避免把一条
    已经被采集端更正过的观测用旧请求错误隔离；
  * 标记与更正交错时，行锁决定先后：
      - 更正先提交 → 标记拿到锁后读到新值，与页面旧值不符 → 拒绝；
      - 标记先拿到锁 → 更正阻塞到标记提交，更正随后由触发器自动解除该隔离。
    旧请求永远不会隔离一条已经更正的观测；
  * 解除/查询均为普通只读或单行删除，隔离状态持久化在 quarantines 表中，
    应用重启后依旧有效。
"""

from __future__ import annotations

from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row

from db import connect
from migrator import next_month, parse_month


class ObservationNotFound(Exception):
    """该站号 + UTC 时刻在父表中不存在对应观测。"""


class ValueMismatch(Exception):
    """页面所见测值与当前数据库记录不一致（可能已被采集端更正）。"""


class RowLocked(Exception):
    """观测正被其它事务（如采集端更正）锁定，等待超过 lock_timeout，可稍后重试。"""


# 标记事务等待行锁的上限：避免在长采集事务上无限挂起；超时返回 409 让人工重试
LOCK_TIMEOUT_MS = 5000


def _coerce_ts(ts: str) -> datetime:
    """接受 ISO 8601（含 Z）；不带时区的输入按 UTC 解释，统一返回带时区时刻。"""
    if not isinstance(ts, str) or not ts.strip():
        raise ValueError("ts 必须为 ISO 8601 时刻字符串")
    text = ts.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"时刻格式无法解析: {ts!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def mark_quarantined(station_id: str, ts: str, observed_value: int) -> dict:
    """标记一条观测为隔离。返回登记内容；已隔离且测值一致时幂等返回。"""
    if not station_id or not station_id.strip():
        raise ValueError("station_id 不能为空")
    if not isinstance(observed_value, int) or isinstance(observed_value, bool):
        raise ValueError("observed_value 必须为整数")
    moment = _coerce_ts(ts)

    with connect() as conn:
        with conn.transaction():
            # 行锁等待封顶：长采集事务持锁时不无限挂起，超时转为可重试错误
            conn.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}'")
            # 行锁：与采集端的 UPDATE/DELETE 互斥，保证"核对 + 登记"的原子性
            try:
                row = conn.execute(
                    "SELECT value FROM observations"
                    " WHERE station_id = %s AND ts = %s FOR UPDATE",
                    (station_id, moment),
                ).fetchone()
            except psycopg.errors.LockNotAvailable as exc:
                raise RowLocked(
                    f"观测正被其它事务锁定（可能采集端正在更正），{LOCK_TIMEOUT_MS}ms 内未取得行锁，"
                    "请稍后重新核对测值再标记"
                ) from exc
            if row is None:
                raise ObservationNotFound(
                    f"未找到观测：station_id={station_id!r}, ts={moment.isoformat()}"
                )
            current_value = row[0]
            if current_value != observed_value:
                raise ValueMismatch(
                    f"页面所见测值 {observed_value} 与当前记录 {current_value} 不一致，"
                    "该观测可能已被采集端更正，未执行隔离"
                )
            inserted = conn.execute(
                """
                INSERT INTO quarantines (station_id, ts, marked_value)
                VALUES (%s, %s, %s)
                ON CONFLICT (station_id, ts) DO UPDATE
                    SET marked_value = EXCLUDED.marked_value
                RETURNING station_id, ts, marked_value, marked_at
                """,
                (station_id, moment, observed_value),
            ).fetchone()
    return {
        "station_id": inserted[0],
        "ts": inserted[1].isoformat(),
        "marked_value": inserted[2],
        "marked_at": inserted[3].isoformat(),
        "quarantined": True,
    }


def release_quarantine(station_id: str, ts: str) -> dict:
    """解除隔离（人工解除）。观测是否存在不影响解除；返回是否实际删掉登记。"""
    moment = _coerce_ts(ts)
    with connect() as conn:
        cur = conn.execute(
            "DELETE FROM quarantines WHERE station_id = %s AND ts = %s",
            (station_id, moment),
        )
        removed = cur.rowcount > 0
    return {
        "station_id": station_id,
        "ts": moment.isoformat(),
        "released": removed,
        "quarantined": False,
    }


def list_quarantines(month: str | None = None, limit: int = 200) -> dict:
    """隔离清单，并关联父表当前测值（便于发现"登记值 vs 当前值"已不一致的情形）。"""
    limit = max(1, min(limit, 1000))
    with connect() as conn:
        conn.row_factory = dict_row
        if month:
            m = parse_month(month)
            rows = conn.execute(
                """
                SELECT q.station_id, q.ts, q.marked_value, q.marked_at,
                       o.value AS current_value,
                       (o.station_id IS NOT NULL) AS row_exists
                FROM quarantines q
                LEFT JOIN observations o
                       ON o.station_id = q.station_id AND o.ts = q.ts
                WHERE q.ts >= %s::date AND q.ts < %s::date
                ORDER BY q.marked_at DESC, q.ts DESC
                LIMIT %s
                """,
                (m.isoformat(), next_month(m).isoformat(), limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT q.station_id, q.ts, q.marked_value, q.marked_at,
                       o.value AS current_value,
                       (o.station_id IS NOT NULL) AS row_exists
                FROM quarantines q
                LEFT JOIN observations o
                       ON o.station_id = q.station_id AND o.ts = q.ts
                ORDER BY q.marked_at DESC, q.ts DESC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        total = conn.execute("SELECT count(*) FROM quarantines").fetchone()["count"]
    return {
        "month": month,
        "total": total,
        "items": [
            {
                "station_id": r["station_id"],
                "ts": r["ts"].isoformat(),
                "marked_value": r["marked_value"],
                "marked_at": r["marked_at"].isoformat(),
                "current_value": r["current_value"],
                "row_exists": r["row_exists"],
                "value_changed": r["row_exists"]
                and r["current_value"] != r["marked_value"],
            }
            for r in rows
        ],
    }


def trusted_query(month: str, limit: int = 50) -> dict:
    """按月查询未隔离（可信）观测；同时给出隔离数与父表总数对照。"""
    m = parse_month(month)
    lo, hi = m.isoformat(), next_month(m).isoformat()
    limit = max(1, min(limit, 500))
    with connect() as conn:
        parent_cnt, trusted_cnt, quarantined_cnt = conn.execute(
            """
            SELECT count(*) AS parent,
                   count(*) FILTER (WHERE NOT EXISTS (
                       SELECT 1 FROM quarantines q
                       WHERE q.station_id = o.station_id AND q.ts = o.ts)) AS trusted,
                   count(*) FILTER (WHERE EXISTS (
                       SELECT 1 FROM quarantines q
                       WHERE q.station_id = o.station_id AND q.ts = o.ts)) AS quar
            FROM observations o
            WHERE o.ts >= %s::date AND o.ts < %s::date
            """,
            (lo, hi),
        ).fetchone()
        rows = conn.execute(
            """
            SELECT o.station_id, o.ts, o.value
            FROM observations_trusted o
            WHERE o.ts >= %s::date AND o.ts < %s::date
            ORDER BY o.ts DESC, o.station_id
            LIMIT %s
            """,
            (lo, hi, limit),
        ).fetchall()
    return {
        "month": month,
        "parent_count": parent_cnt,
        "trusted_count": trusted_cnt,
        "quarantined_count": quarantined_cnt,
        "sample": [
            {"station_id": r[0], "ts": r[1].isoformat(), "value": r[2]}
            for r in rows
        ],
    }
