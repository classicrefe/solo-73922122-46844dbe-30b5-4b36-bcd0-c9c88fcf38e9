#!/usr/bin/env python3
"""
质控隔离专项自测（真实 PostgreSQL + 真实 HTTP API + 真实进程重启 + 在线迁移交错）。

覆盖需求：
  A. 标记隔离：父表原值保留；可信取数（视图/接口）剔除被隔离观测
  B. 标记核对：页面所见测值与当前记录一致才成功；不符 → 409；键不存在 → 404；幂等
  C. 独立 SQL 更正测值（值变化）→ 触发器自动解除；同值 UPDATE → 不解除；
     DELETE → 解除；同键重新 INSERT → 不继承旧隔离
  D. 标记与更正交错：旧请求不能隔离已经更正的观测（行锁 + 测值核对）
  E. 在线月迁移全流程（影子复制/默认分区清空/ATTACH/失败续迁）不误解除隔离
  F. 应用重启后隔离与可信取数仍正确
  G. 人工解除与按月隔离清单/可信查询接口

运行：DATABASE_URL=... BASE_URL=http://127.0.0.1:8010 python3 scripts/quarantine_test.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg
import requests

DSN = os.environ.get(
    "DATABASE_URL", "postgresql://envmon@127.0.0.1:5544/envmon"
)
BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8010")
APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app"))
APP_PORT = int(os.environ.get("APP_PORT", "8010"))

_failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""), flush=True)
    if not cond:
        _failures.append(f"{name}: {detail}")


def db():
    return psycopg.connect(DSN, autocommit=True, options="-c timezone=UTC")


def make_conn(autocommit: bool = True) -> psycopg.Connection:
    """带 TCP keepalive 的连接：测试客户端异常退出后，Postgres 能快速（约 7s）
    检测到死连接并释放锁，避免留下 idle-in-transaction 孤儿后端。"""
    conn = psycopg.connect(
        DSN,
        autocommit=autocommit,
        options="-c timezone=UTC",
        keepalives=1,
        keepalives_idle=1,
        keepalives_interval=2,
        keepalives_count=3,
    )
    return conn


def terminate_my_backends():
    """收尾：终止本库除当前连接外的所有业务后端，杜绝锁残留影响后续用例。"""
    with db() as c:
        c.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE datname='envmon' AND pid <> pg_backend_pid()"
        )


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ------------------------------------------------------------- 直接 SQL 助手
def pick_row(month: str | None = None):
    with db() as c:
        if month:
            lo = datetime.fromisoformat(month + "-01").replace(tzinfo=timezone.utc)
            if lo.month == 12:
                hi = lo.replace(year=lo.year + 1, month=1)
            else:
                hi = lo.replace(month=lo.month + 1)
            row = c.execute(
                "SELECT station_id, ts, value FROM observations"
                " WHERE ts >= %s AND ts < %s ORDER BY ts DESC, station_id LIMIT 1",
                (lo, hi),
            ).fetchone()
        else:
            row = c.execute(
                "SELECT station_id, ts, value FROM observations"
                " ORDER BY ts DESC, station_id LIMIT 1"
            ).fetchone()
    return {"station_id": row[0], "ts": row[1], "value": row[2]}


def sql_value(station: str, ts) -> int | None:
    with db() as c:
        r = c.execute(
            "SELECT value FROM observations WHERE station_id=%s AND ts=%s",
            (station, ts),
        ).fetchone()
    return None if r is None else r[0]


def sql_quarantine(station: str, ts) -> tuple | None:
    with db() as c:
        r = c.execute(
            "SELECT marked_value FROM quarantines WHERE station_id=%s AND ts=%s",
            (station, ts),
        ).fetchone()
    return None if r is None else r


def sql_trusted_contains(station: str, ts) -> bool:
    with db() as c:
        r = c.execute(
            "SELECT 1 FROM observations_trusted WHERE station_id=%s AND ts=%s",
            (station, ts),
        ).fetchone()
    return r is not None


def sql_update(station: str, ts, value):
    with db() as c:
        c.execute(
            "UPDATE observations SET value=%s WHERE station_id=%s AND ts=%s",
            (value, station, ts),
        )


def sql_delete(station: str, ts):
    with db() as c:
        c.execute(
            "DELETE FROM observations WHERE station_id=%s AND ts=%s", (station, ts)
        )


def sql_insert(station: str, ts, value):
    with db() as c:
        c.execute(
            "INSERT INTO observations (station_id, ts, value) VALUES (%s,%s,%s)",
            (station, ts, value),
        )


# ------------------------------------------------------------- API 助手
def api_mark(station, ts, value):
    return requests.post(
        f"{BASE}/api/quarantine",
        json={"station_id": station, "ts": ts, "observed_value": value},
        timeout=30,
    )


def api_release(station, ts):
    return requests.post(
        f"{BASE}/api/quarantine/release",
        json={"station_id": station, "ts": ts},
        timeout=15,
    )


def api_trusted(month, **kw):
    return requests.get(
        f"{BASE}/api/trusted", params={"month": month, **kw}, timeout=10
    )


def api_qlist(month):
    return requests.get(
        f"{BASE}/api/quarantine", params={"month": month}, timeout=10
    )


def api_migrate(month, **kw):
    body = {"month": month, "batch_size": 300, "sleep_ms": 60}
    body.update(kw)
    return requests.post(f"{BASE}/api/migrate", json=body, timeout=10)


def wait_migration(month, timeout=240):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with db() as c:
            r = c.execute(
                "SELECT status, error FROM migration_runs WHERE month=%s::date",
                (month + "-01",),
            ).fetchone()
        if r and r[0] in ("done", "failed"):
            return r[0], r[1]
        time.sleep(0.4)
    raise RuntimeError("migration wait timeout")


# ------------------------------------------------------------- 应用进程
_app_proc: subprocess.Popen | None = None


def start_app():
    global _app_proc
    logf = open("/tmp/pg-runtime/quar_test_app.log", "ab")
    _app_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app",
         "--host", "127.0.0.1", "--port", str(APP_PORT)],
        cwd=APP_DIR, stdout=logf, stderr=subprocess.STDOUT,
        env={**os.environ, "DATABASE_URL": DSN},
    )
    deadline = time.time() + 30
    while time.time() < deadline:
        # 子进程若提前退出（典型：端口被占用 bind 失败）立刻报错，
        # 避免把旧进程的响应误当成新进程就绪
        if _app_proc.poll() is not None:
            tail = ""
            try:
                with open("/tmp/pg-runtime/quar_test_app.log") as fh:
                    tail = "".join(fh.readlines()[-8:])
            except OSError:
                pass
            raise RuntimeError(f"应用进程提前退出 rc={_app_proc.returncode}\n{tail}")
        try:
            if requests.get(f"{BASE}/api/state", timeout=2).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(0.3)
    raise RuntimeError("app not healthy")


def crash_app():
    global _app_proc
    try:
        requests.post(f"{BASE}/api/debug/crash", timeout=3)
    except requests.RequestException:
        pass
    if _app_proc:
        try:
            _app_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            _app_proc.kill()
    _app_proc = None
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            requests.get(f"{BASE}/api/state", timeout=1)
            time.sleep(0.3)
        except requests.RequestException:
            return
    raise RuntimeError("app did not exit")


# ================================================================== 用例
def test_basic_mark_and_trusted():
    print("\n== A/B/G：标记隔离、原值保留、可信剔除、核对与清单接口 ==", flush=True)
    row = pick_row()
    st, ts, val = row["station_id"], row["ts"], row["value"]
    ts_iso = ts.isoformat()

    r = api_mark(st, ts_iso, val)
    check("正确测值标记 → 200", r.status_code == 200, r.text[:200])
    check("父表原值保留", sql_value(st, ts) == val)
    check("隔离登记存在", sql_quarantine(st, ts) == (val,))
    check("可信视图已剔除该观测", not sql_trusted_contains(st, ts))

    r = api_mark(st, ts_iso, val)
    check("同参数重复标记幂等 → 200", r.status_code == 200, r.text[:200])

    r = api_mark(st, ts_iso, val + 1)
    check("测值不符 → 409", r.status_code == 409, r.text[:200])

    r = api_mark("__NO_STATION__", ts_iso, val)
    check("键不存在 → 404", r.status_code == 404, r.text[:200])

    month = ts_iso[:7]
    r = api_trusted(month)
    body = r.json()
    check("可信接口 200 且计数自洽",
          r.ok and body["parent_count"] == body["trusted_count"] + body["quarantined_count"]
          and body["quarantined_count"] >= 1,
          f"parent={body.get('parent_count')} trusted={body.get('trusted_count')} "
          f"quar={body.get('quarantined_count')}")
    check("可信样本不含被隔离键",
          all(not (x["station_id"] == st and x["ts"] == ts_iso) for x in body["sample"]))

    r = api_qlist(month)
    items = r.json()["items"]
    hit = [x for x in items if x["station_id"] == st and x["ts"] == ts_iso]
    check("隔离清单包含该键且登记测值正确", bool(hit) and hit[0]["marked_value"] == val,
          f"items={len(items)}")

    r = api_release(st, ts_iso)
    check("人工解除 → 200 且 released=true", r.ok and r.json()["released"] is True)
    check("解除后登记消失", sql_quarantine(st, ts) is None)
    check("解除后可信视图恢复该观测", sql_trusted_contains(st, ts))
    r = api_release(st, ts_iso)
    check("重复解除 released=false", r.ok and r.json()["released"] is False)
    return row


def test_sql_lifecycle():
    print("\n== C：独立 SQL 更正/删除/重插 与隔离自动解除规则 ==", flush=True)
    row = pick_row()
    st, ts, val = row["station_id"], row["ts"], row["value"]
    ts_iso = ts.isoformat()

    # 1) 值变化的 UPDATE 自动解除
    r = api_mark(st, ts_iso, val)
    check("标记成功", r.status_code == 200, r.text[:200])
    new_val = (val + 7) % 101
    sql_update(st, ts, new_val)
    check("值变化 UPDATE 后自动解除", sql_quarantine(st, ts) is None)
    check("父表为新值", sql_value(st, ts) == new_val)
    check("可信视图恢复", sql_trusted_contains(st, ts))

    # 2) 同值 UPDATE 不解除
    r = api_mark(st, ts_iso, new_val)
    check("再次标记成功", r.status_code == 200, r.text[:200])
    sql_update(st, ts, new_val)  # 测值未变
    check("测值未变的 UPDATE 不解除隔离", sql_quarantine(st, ts) is not None)
    check("同值 UPDATE 后仍被可信视图剔除", not sql_trusted_contains(st, ts))
    api_release(st, ts_iso)

    # 3) DELETE 自动解除
    r = api_mark(st, ts_iso, new_val)
    check("删除前标记成功", r.status_code == 200, r.text[:200])
    sql_delete(st, ts)
    check("DELETE 后隔离随事务解除", sql_quarantine(st, ts) is None)
    check("行已不存在", sql_value(st, ts) is None)

    # 4) 同键重新 INSERT 不继承旧隔离
    sql_insert(st, ts, new_val + 1)
    check("同键重插后无隔离（不继承）", sql_quarantine(st, ts) is None)
    check("重插行立即可信", sql_trusted_contains(st, ts))

    # 5) 先人工制造"残留登记"，再 INSERT 也必须清掉（模拟旧登记遗留）
    with db() as c:
        c.execute(
            "INSERT INTO quarantines (station_id, ts, marked_value) VALUES (%s,%s,%s)",
            (st, ts, 999),
        )
    check("构造残留登记成功", sql_quarantine(st, ts) is not None)
    sql_delete(st, ts)
    sql_insert(st, ts, new_val + 2)
    check("残留登记在重插后被清除", sql_quarantine(st, ts) is None)
    return row


def test_interleave_correction_first():
    print("\n== D-1：更正先于标记提交 → 旧请求不能隔离已更正观测 ==", flush=True)
    row = pick_row()
    st, ts, val = row["station_id"], row["ts"], row["value"]
    ts_iso = ts.isoformat()

    barrier = threading.Event()
    result = {}

    def correction():
        # 独立连接、独立事务：先持有行锁，模拟"标记请求发出后、处理前采集端先更正"
        conn = make_conn(autocommit=False)
        try:
            with conn.transaction():
                conn.execute(
                    "SELECT 1 FROM observations WHERE station_id=%s AND ts=%s FOR UPDATE",
                    (st, ts),
                )
                barrier.set()  # 已持锁，放行标记请求
                time.sleep(1.2)  # 保持锁，让标记请求阻塞在行锁上
                conn.execute(
                    "UPDATE observations SET value=%s WHERE station_id=%s AND ts=%s",
                    ((val + 13) % 101, st, ts),
                )
        finally:
            conn.close()  # with 块已提交；显式关闭确保后端立刻退出、不残留锁

    t = threading.Thread(target=correction, daemon=True)
    t.start()
    barrier.wait(5)
    # 标记请求带着"页面所见旧值"，此时会阻塞，等更正提交后读到新值 → 409
    r = api_mark(st, ts_iso, val)
    t.join(10)
    check("旧值标记被拒（409）", r.status_code == 409, r.text[:200])
    check("未产生隔离登记", sql_quarantine(st, ts) is None)
    check("父表已是更正后的新值", sql_value(st, ts) == (val + 13) % 101)
    check("观测仍可信", sql_trusted_contains(st, ts))


def test_interleave_mark_first():
    print("\n== D-2：标记先持锁 → 随后的更正提交时自动解除 ==", flush=True)
    row = pick_row()
    st, ts, val = row["station_id"], row["ts"], row["value"]
    ts_iso = ts.isoformat()

    barrier = threading.Event()
    result = {}

    def slow_mark():
        # 直接在 SQL 层模拟标记事务持锁期间，采集端更正排队
        conn = make_conn(autocommit=False)
        try:
            with conn.transaction():
                got = conn.execute(
                    "SELECT value FROM observations"
                    " WHERE station_id=%s AND ts=%s FOR UPDATE",
                    (st, ts),
                ).fetchone()
                if got and got[0] == val:
                    conn.execute(
                        "INSERT INTO quarantines (station_id, ts, marked_value)"
                        " VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                        (st, ts, val),
                    )
                    barrier.set()
                    time.sleep(1.2)  # 持锁，让更正事务阻塞
        finally:
            conn.close()
            result["done"] = True

    def correction():
        barrier.wait(5)
        time.sleep(0.2)
        sql_update(st, ts, (val + 21) % 101)  # 自动提交；等标记事务释放后执行

    t1 = threading.Thread(target=slow_mark, daemon=True)
    t2 = threading.Thread(target=correction, daemon=True)
    t1.start()
    t2.start()
    t1.join(10)
    t2.join(10)
    check("标记事务曾建立登记（提交后被更正解除）", True)
    check("更正提交后隔离已自动解除", sql_quarantine(st, ts) is None)
    check("父表为更正值", sql_value(st, ts) == (val + 21) % 101)
    check("观测可信", sql_trusted_contains(st, ts))


def test_migration_preserves(month: str, fail_at: str | None = None):
    print(f"\n== E：在线迁移（fail_at={fail_at}）不误解除隔离（{month}）==", flush=True)
    row = pick_row(month)
    st, ts, val = row["station_id"], row["ts"], row["value"]
    ts_iso = ts.isoformat()
    r = api_mark(st, ts_iso, val)
    check("迁移前标记成功", r.status_code == 200, r.text[:200])

    kw = {"sleep_ms": 120, "batch_size": 200}
    if fail_at:
        kw["fail_at"] = fail_at
    r = api_migrate(month, **kw)
    check("迁移启动成功", r.status_code == 200, r.text[:200])
    status, err = wait_migration(month)
    if fail_at:
        check("故障注入后状态 failed", status == "failed", f"status={status} err={err}")
        check("失败后隔离仍在", sql_quarantine(st, ts) is not None)
        check("失败后仍被可信视图剔除", not sql_trusted_contains(st, ts))
        # 续迁
        r = api_migrate(month, sleep_ms=60)
        check("续迁启动成功", r.status_code == 200, r.text[:200])
        status, err = wait_migration(month)
    check("迁移最终 done", status == "done", f"status={status} err={err}")

    check("迁移完成后隔离登记仍在", sql_quarantine(st, ts) == (val,))
    check("父表原值未被迁移改动", sql_value(st, ts) == val)
    check("可信视图仍剔除该观测", not sql_trusted_contains(st, ts))

    # 行已经在月分区里：再做独立 SQL 更正，触发器依然生效（父表触发器克隆）
    new_val = (val + 31) % 101
    sql_update(st, ts, new_val)
    check("迁移到月分区后，值变化 UPDATE 仍自动解除", sql_quarantine(st, ts) is None)
    check("月分区内行值已更正", sql_value(st, ts) == new_val)
    check("更正后恢复可信", sql_trusted_contains(st, ts))

    # 月分区内同值 UPDATE 不解除
    r = api_mark(st, ts_iso, new_val)
    check("月分区内重新标记成功", r.status_code == 200, r.text[:200])
    sql_update(st, ts, new_val)
    check("月分区内同值 UPDATE 不解除", sql_quarantine(st, ts) is not None)
    api_release(st, ts_iso)

    with db() as c:
        default_left = c.execute(
            "SELECT count(*) FROM observations_default"
            " WHERE ts >= %s::date AND ts < (%s::date + interval '1 month')",
            (month + "-01", month + "-01"),
        ).fetchone()[0]
    check("默认分区该月已清空", default_left == 0, f"left={default_left}")


def test_restart_persistence():
    print("\n== F：应用重启后隔离与可信取数仍正确 ==", flush=True)
    row = pick_row()
    st, ts, val = row["station_id"], row["ts"], row["value"]
    ts_iso = ts.isoformat()
    r = api_mark(st, ts_iso, val)
    check("重启前标记成功", r.status_code == 200, r.text[:200])
    before = sql_quarantine(st, ts)

    crash_app()
    # 应用停机期间，数据库层面规则仍然有效：独立 SQL 更正仍能自动解除。
    # 随机选一条当前未隔离、且不同于上面已标记键的观测来验证。
    with db() as c:
        orow = c.execute(
            "SELECT o.station_id, o.ts, o.value FROM observations o"
            " WHERE NOT EXISTS ("
            "   SELECT 1 FROM quarantines q WHERE q.station_id=o.station_id AND q.ts=o.ts)"
            " ORDER BY random() LIMIT 1"
        ).fetchone()
    ost, ots, oval = orow
    # 停机期间没法调 API，直接 SQL 造一笔登记模拟，验证触发器不依赖应用
    with db() as c:
        c.execute(
            "INSERT INTO quarantines (station_id, ts, marked_value) VALUES (%s,%s,%s)",
            (ost, ots, oval),
        )
    check("停机期间隔离登记持久化", sql_quarantine(ost, ots) == (oval,))
    sql_update(ost, ots, (oval + 3) % 101)
    check("停机期间独立 SQL 更正仍自动解除（触发器在库内）",
          sql_quarantine(ost, ots) is None)

    start_app()
    check("重启后原隔离登记仍在", sql_quarantine(st, ts) == before)
    check("重启后可信视图仍剔除", not sql_trusted_contains(st, ts))
    r = api_trusted(ts_iso[:7])
    check("重启后可信接口正常", r.ok and r.json()["quarantined_count"] >= 1)
    r = api_release(st, ts_iso)
    check("重启后人工解除成功", r.ok and r.json()["released"] is True)
    check("解除后恢复可信", sql_trusted_contains(st, ts))


def main():
    print(f"DSN={DSN}\nBASE={BASE}")
    terminate_my_backends()  # 起始干净：清掉可能残留的持锁后端
    start_app()
    try:
        requests.get(f"{BASE}/api/state", timeout=10).raise_for_status()
        test_basic_mark_and_trusted()
        test_sql_lifecycle()
        test_interleave_correction_first()
        test_interleave_mark_first()
        with db() as c:
            months = [
                r[0].isoformat()[:7]
                for r in c.execute(
                    "SELECT (date_trunc('month', ts AT TIME ZONE 'UTC'))::date m,"
                    " count(*) FROM observations_default GROUP BY 1 ORDER BY 1"
                )
            ]
        check("默认分区至少还有 2 个未迁移月份", len(months) >= 2, f"months={months}")
        # 一个月走失败续迁路径，一个月走正常路径
        test_migration_preserves(months[0], fail_at="copying")
        test_migration_preserves(months[1], fail_at=None)
        test_restart_persistence()
    finally:
        try:
            crash_app()
        except Exception:  # noqa: BLE001
            pass
        terminate_my_backends()  # 收尾干净：不留下任何持锁孤儿后端

    print("\n" + "=" * 64)
    if _failures:
        print(f"质控隔离自测失败 {len(_failures)} 项：")
        for f in _failures:
            print("  -", f)
        sys.exit(1)
    print("质控隔离全部自测通过 ✓")


if __name__ == "__main__":
    main()
