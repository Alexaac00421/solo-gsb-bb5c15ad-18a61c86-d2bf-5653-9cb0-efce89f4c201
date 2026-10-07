"""数据库引擎、会话工厂与建表初始化。"""
import time

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.config import settings


class Base(DeclarativeBase):
    pass


engine = create_engine(
    settings.database_url,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def init_db() -> None:
    """等待数据库就绪并创建全部表（幂等，重复执行无副作用）。

    由应用启动事件调用，因此 ``docker compose up`` 即自动初始化数据库，
    无需手工执行迁移脚本。
    """
    # 确保模型已注册到 metadata，再导入以避免循环导入。
    from app import models  # noqa: F401

    deadline = time.monotonic() + settings.db_wait_timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            Base.metadata.create_all(bind=engine)
            _upgrade_existing_tables(bind=engine)
            return
        except OperationalError as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"等待数据库就绪超时：{last_error}")


def _upgrade_existing_tables(bind) -> None:
    """对旧版本库做幂等补齐（无需手工迁移）。

    预留功能为 ``authorizations`` 增加了 ``reserved_count`` 列与
    ``ck_auth_reserved_invariant`` 约束；候补有效期功能为 ``waitlists``
    增加了 ``ttl_minutes`` / ``expires_at`` / ``expiry_reason`` /
    ``expired_at`` 列，并扩展状态约束以纳入 ``expired`` 终态。
    新建库由 ``create_all`` 直接建出，旧库则在此幂等补齐
    （列/约束存在时跳过）。
    """
    from sqlalchemy import text

    with bind.begin() as conn:
        has_col = conn.execute(
            text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name='authorizations' AND column_name='reserved_count'"
            )
        ).first()
        if not has_col:
            conn.execute(
                text(
                    "ALTER TABLE authorizations "
                    "ADD COLUMN reserved_count INTEGER NOT NULL DEFAULT 0"
                )
            )
        has_ck = conn.execute(
            text(
                "SELECT 1 FROM pg_constraint WHERE conname="
                "'ck_auth_reserved_invariant'"
            )
        ).first()
        if not has_ck:
            conn.execute(
                text(
                    "ALTER TABLE authorizations "
                    "ADD CONSTRAINT ck_auth_reserved_invariant "
                    "CHECK (reserved_count >= 0 "
                    "AND used_count + reserved_count <= max_count)"
                )
            )

        # 候补申请有效期（1～1440 分钟）相关列：可空以兼容旧记录——
        # 旧记录 ttl_minutes/expires_at 为空，表示永不按有效期失效，
        # 其既有状态/成交语义保持不变。
        for column, ddl in (
            ("ttl_minutes", "ADD COLUMN ttl_minutes INTEGER"),
            ("expires_at", "ADD COLUMN expires_at TIMESTAMPTZ"),
            ("expiry_reason", "ADD COLUMN expiry_reason VARCHAR(64)"),
            ("expired_at", "ADD COLUMN expired_at TIMESTAMPTZ"),
        ):
            exists = conn.execute(
                text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_name='waitlists' AND column_name=:col"
                ),
                {"col": column},
            ).first()
            if not exists:
                conn.execute(text(f"ALTER TABLE waitlists {ddl}"))

        # 扩展状态检查约束以纳入 expired：旧约束不含 expired，需替换。
        status_ck = conn.execute(
            text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname='ck_wait_status'"
            )
        ).scalar()
        if status_ck is not None and "'expired'" not in status_ck:
            conn.execute(
                text("ALTER TABLE waitlists DROP CONSTRAINT ck_wait_status")
            )
            status_ck = None
        if status_ck is None:
            conn.execute(
                text(
                    "ALTER TABLE waitlists ADD CONSTRAINT ck_wait_status "
                    "CHECK (status IN ('pending', 'fulfilled', 'failed', "
                    "'cancelled', 'expired'))"
                )
            )

        for name, ddl in (
            (
                "ck_wait_ttl_range",
                "CHECK (ttl_minutes IS NULL OR "
                "(ttl_minutes >= 1 AND ttl_minutes <= 1440))",
            ),
            (
                "ck_wait_ttl_consistency",
                "CHECK ((ttl_minutes IS NULL) = (expires_at IS NULL))",
            ),
            (
                "ck_wait_expiry_reason",
                "CHECK (status = 'expired' OR expiry_reason IS NULL)",
            ),
        ):
            exists = conn.execute(
                text(
                    "SELECT 1 FROM pg_constraint WHERE conname=:name"
                ),
                {"name": name},
            ).first()
            if not exists:
                conn.execute(
                    text(
                        f"ALTER TABLE waitlists ADD CONSTRAINT {name} {ddl}"
                    )
                )

        # 额度账本功能（新表由 create_all 直接建出；旧库同样由 create_all
        # 补建缺失表）。两步升级：
        # 1) 为功能启用时已存在的授权回填**期初余额**（以启用瞬间的
        #    used/reserved/max 现状为期初；ON CONFLICT 幂等，重复升级不
        #    覆盖，期初一旦写入不可变）。须在 reserved_count 列补齐之后执行。
        conn.execute(
            text(
                "INSERT INTO quota_ledger_openings "
                "(authorization_id, used_count, reserved_count, "
                "max_count, created_at) "
                "SELECT a.id, a.used_count, a.reserved_count, a.max_count, "
                "now() FROM authorizations a "
                "WHERE NOT EXISTS (SELECT 1 FROM quota_ledger_openings o "
                "WHERE o.authorization_id = a.id)"
            )
        )
        # 2) 流水表禁止 UPDATE / DELETE（TRUNCATE 受属主权限另行管控）：
        #    应用层只追加，数据库层兜底「不可改写」。触发器函数幂等创建。
        conn.execute(
            text(
                "CREATE OR REPLACE FUNCTION quota_ledger_entries_immutable() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN "
                "  RAISE EXCEPTION 'quota_ledger_entries 为不可改写流水，"
                "禁止 %（只允许追加）', TG_OP; "
                "END; $$"
            )
        )
        has_trg = conn.execute(
            text(
                "SELECT 1 FROM pg_trigger WHERE tgname="
                "'trg_quota_ledger_immutable'"
            )
        ).first()
        if not has_trg:
            conn.execute(
                text(
                    "CREATE TRIGGER trg_quota_ledger_immutable "
                    "BEFORE UPDATE OR DELETE ON quota_ledger_entries "
                    "FOR EACH ROW EXECUTE FUNCTION "
                    "quota_ledger_entries_immutable()"
                )
            )


def get_db():
    """FastAPI 依赖：每请求一个事务会话。"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
