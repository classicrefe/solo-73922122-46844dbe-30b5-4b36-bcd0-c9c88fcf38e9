"""数据库连接与进程内日志缓冲。"""

from __future__ import annotations

import os
import threading
from collections import deque
from datetime import datetime, timezone

import psycopg

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://envmon:envmon@localhost:5432/envmon"
)

_lock = threading.Lock()
_logs: deque[dict] = deque(maxlen=500)
_seq = 0


def connect() -> psycopg.Connection:
    """新建独立连接（采集写入演示也用它，模拟独立采集连接）。"""
    return psycopg.connect(
        DATABASE_URL, autocommit=True, options="-c timezone=UTC"
    )


def log(msg: str):
    global _seq
    with _lock:
        _seq += 1
        _logs.append(
            {
                "seq": _seq,
                "at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "msg": msg,
            }
        )
    print(f"[log] {msg}", flush=True)


def logs_since(seq: int) -> list[dict]:
    with _lock:
        return [e for e in _logs if e["seq"] > seq]


def ensure_meta_tables():
    """元数据表兜底创建（init.sql 已建则跳过）。"""
    with connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_runs (
                month           date        PRIMARY KEY,
                status          text        NOT NULL DEFAULT 'preparing',
                total_estimate  bigint      NOT NULL DEFAULT 0,
                copied_rows     bigint      NOT NULL DEFAULT 0,
                applied_delta   bigint      NOT NULL DEFAULT 0,
                last_station    text,
                last_ts         timestamptz,
                error           text,
                updated_at      timestamptz NOT NULL DEFAULT now(),
                created_at      timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_delta (
                seq         bigserial PRIMARY KEY,
                month       date        NOT NULL,
                op          text        NOT NULL,
                station_id  text        NOT NULL,
                ts          timestamptz NOT NULL,
                value       integer
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS migration_delta_month_seq "
            "ON migration_delta (month, seq)"
        )
        ensure_quarantine(conn)


def ensure_quarantine(conn):
    """质控隔离对象兜底创建：隔离登记表 + 自动解除触发器 + 可信取数视图。"""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quarantines (
            station_id    text          NOT NULL,
            ts            timestamptz   NOT NULL,
            marked_value  integer       NOT NULL,
            marked_at     timestamptz   NOT NULL DEFAULT now(),
            PRIMARY KEY (station_id, ts)
        )
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE FUNCTION obs_quarantine_fn() RETURNS trigger AS $fn$
        BEGIN
            IF current_setting('app.migration_in_progress', true) = 'on' THEN
                IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE' THEN
                DELETE FROM quarantines WHERE station_id = OLD.station_id AND ts = OLD.ts;
                RETURN OLD;
            ELSIF TG_OP = 'UPDATE' THEN
                IF OLD.value IS DISTINCT FROM NEW.value
                   OR OLD.ts IS DISTINCT FROM NEW.ts
                   OR OLD.station_id IS DISTINCT FROM NEW.station_id THEN
                    DELETE FROM quarantines
                    WHERE station_id = OLD.station_id AND ts = OLD.ts;
                END IF;
                IF OLD.station_id IS DISTINCT FROM NEW.station_id
                   OR OLD.ts IS DISTINCT FROM NEW.ts THEN
                    DELETE FROM quarantines
                    WHERE station_id = NEW.station_id AND ts = NEW.ts;
                END IF;
                RETURN NEW;
            ELSIF TG_OP = 'INSERT' THEN
                DELETE FROM quarantines WHERE station_id = NEW.station_id AND ts = NEW.ts;
                RETURN NEW;
            END IF;
            RETURN NULL;
        END
        $fn$ LANGUAGE plpgsql
        """
    )
    trg = conn.execute(
        "SELECT 1 FROM pg_trigger WHERE tgname = 'obs_quarantine_trg'"
    ).fetchone()
    if not trg:
        # 建在分区父表上，自动克隆到默认分区及未来 ATTACH 的月分区
        conn.execute(
            "CREATE TRIGGER obs_quarantine_trg AFTER INSERT OR UPDATE OR DELETE "
            "ON observations FOR EACH ROW EXECUTE FUNCTION obs_quarantine_fn()"
        )
    conn.execute(
        """
        CREATE OR REPLACE VIEW observations_trusted AS
        SELECT o.station_id, o.ts, o.value
        FROM observations o
        WHERE NOT EXISTS (
            SELECT 1 FROM quarantines q
            WHERE q.station_id = o.station_id AND q.ts = o.ts
        )
        """
    )
