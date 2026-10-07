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
    ``expired_at`` 列，并扩展状态约束以纳入 ``expired`` 终态；
    额度账本功能新增 ``quota_ledger_accounts`` / ``quota_ledger_entries``
    表（由 ``create_all`` 建出），并为启用前已存在的授权按当前三项计数
    幂等开立账本账户（期初余额）。新建库由 ``create_all`` 直接建出，
    旧库则在此幂等补齐（列/约束/表/账户行已存在则跳过）。
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

        # 额度账本：为功能启用前已存在的授权幂等开立账户——
        # 期初余额取启用时刻的 (used_count, reserved_count, max_count)，
        # 启用前的历史变化不追溯；已开立的授权跳过（幂等）。
        conn.execute(
            text(
                "INSERT INTO quota_ledger_accounts "
                "(authorization_id, opening_max_count, opening_used_count, "
                " opening_reserved_count, opened_at, entry_count) "
                "SELECT a.id, a.max_count, a.used_count, a.reserved_count, "
                "       now(), 0 "
                "FROM authorizations a "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM quota_ledger_accounts q "
                "  WHERE q.authorization_id = a.id)"
            )
        )


def get_db():
    """FastAPI 依赖：每请求一个事务会话。"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
