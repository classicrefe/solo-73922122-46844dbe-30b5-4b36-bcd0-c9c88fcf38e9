-- ============================================================================
-- 环境监测数仓 · 观测表分区迁移演示 — 数据库初始化
-- ----------------------------------------------------------------------------
--  * observations  : 父表，按 UTC 时刻 RANGE 分区，主键 (station_id, ts)
--  * observations_default : 唯一的默认分区，历史观测最初全部落在这里
--  * migration_runs  : 每个月份迁移的状态机记录（幂等/断点续迁的依据）
--  * migration_delta : 迁移期间由触发器捕获的增量变更（新增/更正/删除）
--  * quarantines     : 质控隔离登记（独立于分区的元数据表，见下方详细注释）
--  * observations_trusted : 下游可信取数视图（自动剔除被隔离的观测）
-- ============================================================================

CREATE TABLE IF NOT EXISTS observations (
    station_id  text          NOT NULL,
    ts          timestamptz   NOT NULL,
    value       integer       NOT NULL,
    PRIMARY KEY (station_id, ts)
) PARTITION BY RANGE (ts);

CREATE TABLE IF NOT EXISTS observations_default
    PARTITION OF observations DEFAULT;

-- 迁移状态机：idle → preparing → copying → applying → finalizing → done
-- failed 状态仅作展示；重试时从断点继续（影子表/增量表均保留）。
CREATE TABLE IF NOT EXISTS migration_runs (
    month           date        PRIMARY KEY,   -- 月份首日（UTC）
    status          text        NOT NULL DEFAULT 'preparing',
    total_estimate  bigint      NOT NULL DEFAULT 0,
    copied_rows     bigint      NOT NULL DEFAULT 0,
    applied_delta   bigint      NOT NULL DEFAULT 0,
    last_station    text,
    last_ts         timestamptz,
    error           text,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- 增量变更日志：迁移期间默认分区上该月数据的 INSERT/UPDATE/DELETE
CREATE TABLE IF NOT EXISTS migration_delta (
    seq         bigserial PRIMARY KEY,
    month       date        NOT NULL,
    op          text        NOT NULL CHECK (op IN ('insert','update','delete')),
    station_id  text        NOT NULL,
    ts          timestamptz NOT NULL,
    value       integer
);
CREATE INDEX IF NOT EXISTS migration_delta_month_seq
    ON migration_delta (month, seq);

-- ============================================================================
-- 质控隔离（quarantine）
-- ----------------------------------------------------------------------------
-- 监测质控员发现某条观测测值可疑时，把该条观测暂时隔离：父表 observations
-- 仍原样保留该条（不删不改），只在元数据表 quarantines 里登记一笔。
-- 下游"可信取数"经 observations_trusted 视图（或反连接查询）自动跳过它。
--
-- 隔离登记是独立于分区的普通表：观测行在默认分区与月分区之间搬迁、月分区
-- 挂载/清空都不会触碰隔离登记，因此在线迁移不会误解除隔离，重启后依然有效。
--
-- 自动解除规则（由 observations 上的行级触发器保证，对任意独立 SQL 会话生效，
-- 且与应用是否在线无关）：
--   * 采集端 DELETE 该观测（事务提交）→ 隔离随事务解除；
--   * 采集端 UPDATE 且 value 真正发生变化（事务提交）→ 隔离随事务解除；
--   * 采集端 UPDATE 但 value 未变（例如只刷新标记列/同值重写）→ 不解除；
--   * 采集端 DELETE 后以同键重新 INSERT → 新行不继承旧隔离（INSERT 会清掉
--     该键上可能残留的旧登记）。
-- 迁移引擎在默认分区上做"漏网行搬迁 DELETE"时会把自定义 GUC
-- app.migration_in_progress 置为 on，触发器据此跳过，避免误解除。
-- ============================================================================
CREATE TABLE IF NOT EXISTS quarantines (
    station_id    text          NOT NULL,
    ts            timestamptz   NOT NULL,
    marked_value  integer       NOT NULL,   -- 标记当时页面所见测值（留痕核对）
    marked_at     timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, ts)
);

CREATE OR REPLACE FUNCTION obs_quarantine_fn() RETURNS trigger AS $fn$
BEGIN
    -- 迁移引擎内部的数据搬迁（默认分区→月分区）不得解除任何隔离
    IF current_setting('app.migration_in_progress', true) = 'on' THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN
        DELETE FROM quarantines WHERE station_id = OLD.station_id AND ts = OLD.ts;
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        -- 仅当测值真正变化（或主键迁移到别的时刻）才解除；同值 UPDATE 保留隔离
        IF OLD.value IS DISTINCT FROM NEW.value
           OR OLD.ts IS DISTINCT FROM NEW.ts
           OR OLD.station_id IS DISTINCT FROM NEW.station_id THEN
            DELETE FROM quarantines
            WHERE station_id = OLD.station_id AND ts = OLD.ts;
        END IF;
        -- 新键上若有历史残留登记（例如同键重插后的旧标记），同样不继承
        IF OLD.station_id IS DISTINCT FROM NEW.station_id
           OR OLD.ts IS DISTINCT FROM NEW.ts THEN
            DELETE FROM quarantines
            WHERE station_id = NEW.station_id AND ts = NEW.ts;
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        -- 同键重新插入：新行是一条全新观测，不能继承旧隔离
        DELETE FROM quarantines WHERE station_id = NEW.station_id AND ts = NEW.ts;
        RETURN NEW;
    END IF;
    RETURN NULL;
END
$fn$ LANGUAGE plpgsql;

-- 触发器建在分区父表上：PostgreSQL 会自动克隆到现有及未来 ATTACH 的每个分区，
-- 因此无论观测落在默认分区还是月分区、无论经父表还是直接写分区，规则都生效。
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'obs_quarantine_trg' AND tgrelid = 'observations'::regclass
    ) THEN
        CREATE TRIGGER obs_quarantine_trg
            AFTER INSERT OR UPDATE OR DELETE ON observations
            FOR EACH ROW EXECUTE FUNCTION obs_quarantine_fn();
    END IF;
END $$;

-- 下游可信取数视图：父表全量减去当前被隔离的观测。
-- 反连接（NOT EXISTS）可同时覆盖"行还在但被隔离"和"登记残留但行已不存在"。
CREATE OR REPLACE VIEW observations_trusted AS
SELECT o.station_id, o.ts, o.value
FROM observations o
WHERE NOT EXISTS (
    SELECT 1 FROM quarantines q
    WHERE q.station_id = o.station_id AND q.ts = o.ts
);

-- ============================================================================
-- 演示种子数据：最近 6 个自然月（UTC），8 个站点，每小时一条整数测值。
-- 全部写入默认分区 —— 即"历史观测 initially 都在默认分区"。
-- ============================================================================
INSERT INTO observations (station_id, ts, value)
SELECT 'ST' || lpad(s::text, 3, '0'),
       date_trunc('hour', t),
       (20 + 60 * random())::int
FROM generate_series(1, 8) AS s
CROSS JOIN generate_series(
        date_trunc('month', now()) - interval '5 months',
        now() - interval '1 hour',
        interval '1 hour'
    ) AS t
ON CONFLICT (station_id, ts) DO NOTHING;
