"""额度账本：期初余额与不可改写逐笔流水的写入 / 查询。

与原业务事务同成同败
====================
流水由各业务服务在**原事务内**调用 :func:`record_quota_change` 追加
（仅 INSERT），业务回滚则流水一并回滚；被拒绝的请求在写流水之前即
``rollback`` 退出，幂等重放分支直接返回、不触达计数更新，因此
**拒绝、回滚、幂等重放均不留流水**。

流水表上另有数据库触发器（见 ``database._upgrade_existing_tables``），
禁止任何 UPDATE / DELETE，保证「不可改写」。

取值口径
========
计数变化统一走 :func:`bump_counters`：以条件 ``UPDATE ... RETURNING``
原子推进并取回**变化后**的精确计数（行锁保护下不存在并发改写），
变化前值由调用点在行锁内读取的授权行给出；变化前/后值与业务时刻一并
落流水，因此每笔流水都能逐笔滚出余额。

期初余额
========
- 功能启用（建表）时已存在的授权：由建表升级逻辑以当时的
  ``used_count`` / ``reserved_count`` / ``max_count`` 回填一行；
- 新授权：随授权创建同事务写入期初行（``0/0/max_count``）。

翻页
====
每个授权内部 ``entry_seq`` 从 1 严格递增。首次查询把当次允许看到的
**固定上界**（查询时刻该授权的最大 seq）编入不透明游标；后续页只在
该上界内键集翻页，故首次查询之后的新写入不会混入该次翻页；页末余额
取本页最后一笔的变化后值（首页无上界则为期初），相邻页第一笔的
变化前值恰等于上一页页末值——**相邻页余额连续**。
"""
import base64
import binascii
import json
from datetime import datetime

from sqlalchemy import select, text, update
from sqlalchemy.orm import Session

from app import models
from app.errors import InvalidLedgerCursor, ResourceNotFound

CURSOR_VERSION = "q1"

# ---------------------------------------------------------------- 事件类型
EVENT_DIRECT_DISTRIBUTION = "direct_distribution"
EVENT_WAITLIST_FULFILLMENT = "waitlist_fulfillment"
EVENT_RESERVATION_PLACED = "reservation_placed"
EVENT_RESERVATION_CONFIRMED = "reservation_confirmed"
EVENT_RESERVATION_CANCELLED = "reservation_cancelled"
EVENT_RESERVATION_EXPIRED = "reservation_expired"
EVENT_REVOKE = "distribution_revoked"
EVENT_RESCHEDULE_OUT = "reschedule_out"
EVENT_RESCHEDULE_IN = "reschedule_in"
EVENT_MIGRATE_OUT = "migration_out"
EVENT_MIGRATE_IN = "migration_in"
EVENT_TRANSFER_OUT = "transfer_out"
EVENT_TRANSFER_IN = "transfer_in"


def ensure_opening(
    db: Session,
    authorization_id: int,
    *,
    used_count: int,
    reserved_count: int,
    max_count: int,
) -> None:
    """随授权创建写入期初余额行（每个授权至多一行，重复调用幂等跳过）。"""
    exists = db.scalar(
        select(models.QuotaLedgerOpening.id).where(
            models.QuotaLedgerOpening.authorization_id == authorization_id
        )
    )
    if exists is None:
        db.add(
            models.QuotaLedgerOpening(
                authorization_id=authorization_id,
                used_count=used_count,
                reserved_count=reserved_count,
                max_count=max_count,
            )
        )


def _next_seq(db: Session, authorization_id: int) -> int:
    """授权内下一流水序号（调用方须持有该通道咨询锁 + 授权行锁）。

    同一授权的全部计数变化都在同一把通道咨询锁内串行，序列分配无竞争。
    防御性地兼容期初行缺失（旧库回填竞态）：无流水且无期初时从 1 起。
    """
    current = db.scalar(
        select(models.QuotaLedgerEntry.entry_seq)
        .where(models.QuotaLedgerEntry.authorization_id == authorization_id)
        .order_by(models.QuotaLedgerEntry.entry_seq.desc())
        .limit(1)
    )
    return (current or 0) + 1


def record_quota_change(
    db: Session,
    *,
    authorization_id: int,
    event_type: str,
    ref_type: str,
    ref_id: object,
    occurred_at: datetime,
    before: tuple[int, int, int],
    after: tuple[int, int, int],
    counterparty_authorization_id: int | None = None,
    detail: dict | None = None,
) -> models.QuotaLedgerEntry:
    """在业务事务内追加一笔流水（不单独 commit；随业务事务同成同败）。

    ``before`` / ``after`` 为 ``(used_count, reserved_count, max_count)``。
    至少一项不同才允许落库（与表约束一致）；相同则视为空操作直接跳过，
    使调用点无需自行区分「本笔是否真的改了计数」。
    """
    if before == after:
        return None  # type: ignore[return-value]
    entry = models.QuotaLedgerEntry(
        authorization_id=authorization_id,
        entry_seq=_next_seq(db, authorization_id),
        event_type=event_type,
        ref_type=ref_type,
        ref_id=str(ref_id),
        counterparty_authorization_id=counterparty_authorization_id,
        before_used_count=before[0],
        before_reserved_count=before[1],
        before_max_count=before[2],
        after_used_count=after[0],
        after_reserved_count=after[1],
        after_max_count=after[2],
        occurred_at=occurred_at,
        detail=detail,
    )
    db.add(entry)
    db.flush()
    return entry


def bump_counters(
    db: Session,
    authorization_id: int,
    *,
    used_delta: int = 0,
    reserved_delta: int = 0,
    max_delta: int = 0,
    guard_sql: str | None = None,
) -> tuple[tuple[int, int, int], tuple[int, int, int]] | None:
    """条件推进单个授权的三项计数并返回 ``(变化前, 变化后)``。

    以单条 ``UPDATE ... RETURNING`` 原子完成：``guard_sql`` 为附加的
    纯列表达式护栏（如 ``used_count + reserved_count < max_count``），
    不满足（影响 0 行）时返回 ``None``，调用方按既有业务语义拒绝/报错。
    变化前值取自会话内同事务已 ``FOR UPDATE`` 锁定的授权行快照，
    RETURNING 得到变化后值，二者构成连续前后值。
    """
    auth = db.get(models.Authorization, authorization_id)
    before = (auth.used_count, auth.reserved_count, auth.max_count)
    stmt = (
        update(models.Authorization)
        .where(models.Authorization.id == authorization_id)
        .values(
            used_count=models.Authorization.used_count + used_delta,
            reserved_count=models.Authorization.reserved_count + reserved_delta,
            max_count=models.Authorization.max_count + max_delta,
        )
        .returning(
            models.Authorization.used_count,
            models.Authorization.reserved_count,
            models.Authorization.max_count,
        )
    )
    if guard_sql:
        stmt = stmt.where(text(guard_sql))
    row = db.execute(stmt).first()
    if row is None:
        return None
    # 使会话中 ORM 对象的过期属性随后读到 RETURNING 的同一值。
    db.expire(auth)
    return before, (row[0], row[1], row[2])


# ---------------------------------------------------------------- 游标
def encode_cursor(
    *, authorization_id: int, last_seq: int, high_water: int
) -> str:
    payload = json.dumps(
        [CURSOR_VERSION, authorization_id, last_seq, high_water],
        separators=(",", ":"),
    )
    # 去除 '=' 填充，避免其出现在 query string 中被网关/客户端转义。
    return base64.urlsafe_b64encode(payload.encode("utf-8")).rstrip(
        b"="
    ).decode("ascii")


def decode_cursor(token: str, authorization_id: int) -> tuple[int, int]:
    """校验不透明游标并返回 ``(last_seq, high_water)``。

    游标不可解析、版本不符、不属于当前授权或字段非法时明确报 422
    ``invalid_ledger_cursor``。
    """
    try:
        padded = token.encode("ascii") + b"=" * (-len(token) % 4)
        raw = base64.b64decode(padded, altchars=b"-_", validate=True)
        payload = json.loads(raw.decode("utf-8"))
        version, auth_id, last_seq, high_water = payload
    except (binascii.Error, ValueError, UnicodeDecodeError, TypeError) as exc:
        raise InvalidLedgerCursor("游标不可解析或已损坏") from exc
    if (
        version != CURSOR_VERSION
        or auth_id != authorization_id
        or not isinstance(last_seq, int)
        or not isinstance(high_water, int)
        or last_seq < 0
        or high_water < last_seq
    ):
        raise InvalidLedgerCursor("游标与该授权不匹配或字段非法")
    return last_seq, high_water


# ---------------------------------------------------------------- 查询
def get_authorization_ledger(
    db: Session,
    authorization_id: int,
    *,
    cursor: str | None,
    page_size: int,
) -> dict:
    """按授权编号 + 游标 + 页大小查询额度账本。

    返回期初余额、本页逐笔流水、页末余额与下一游标（无更多数据时为
    ``None``）。首次查询固定上界 = 查询瞬间该授权的最大 ``entry_seq``，
    后续写入不混入该次翻页。只读事务，不触发任何惰性结算/写入。
    """
    opening = db.scalar(
        select(models.QuotaLedgerOpening).where(
            models.QuotaLedgerOpening.authorization_id == authorization_id
        )
    )
    if opening is None:
        # 无期初行即未知授权（授权登记与期初行同事务落库）。
        raise ResourceNotFound("授权", authorization_id)

    if cursor is None:
        last_seq = 0
        high_water = (
            db.scalar(
                select(models.QuotaLedgerEntry.entry_seq)
                .where(
                    models.QuotaLedgerEntry.authorization_id
                    == authorization_id
                )
                .order_by(models.QuotaLedgerEntry.entry_seq.desc())
                .limit(1)
            )
            or 0
        )
    else:
        last_seq, high_water = decode_cursor(cursor, authorization_id)

    entries = list(
        db.scalars(
            select(models.QuotaLedgerEntry)
            .where(
                models.QuotaLedgerEntry.authorization_id == authorization_id,
                models.QuotaLedgerEntry.entry_seq > last_seq,
                models.QuotaLedgerEntry.entry_seq <= high_water,
            )
            .order_by(models.QuotaLedgerEntry.entry_seq.asc())
            .limit(page_size)
        )
    )

    if entries:
        page_end = {
            "used_count": entries[-1].after_used_count,
            "reserved_count": entries[-1].after_reserved_count,
            "max_count": entries[-1].after_max_count,
        }
        end_seq = entries[-1].entry_seq
    elif last_seq == 0:
        # 首页且无流水：页末即期初。
        page_end = {
            "used_count": opening.used_count,
            "reserved_count": opening.reserved_count,
            "max_count": opening.max_count,
        }
        end_seq = 0
    else:
        # 空页但游标已翻过若干笔（如上界内已到末页后再次请求）：页末
        # 余额取游标所指最后一笔的变化后值，保证余额语义始终连续。
        last_entry = db.scalar(
            select(models.QuotaLedgerEntry).where(
                models.QuotaLedgerEntry.authorization_id
                == authorization_id,
                models.QuotaLedgerEntry.entry_seq == last_seq,
            )
        )
        if last_entry is None:
            raise InvalidLedgerCursor("游标指向的流水序号不存在")
        page_end = {
            "used_count": last_entry.after_used_count,
            "reserved_count": last_entry.after_reserved_count,
            "max_count": last_entry.after_max_count,
        }
        end_seq = last_seq

    next_cursor = (
        None
        if end_seq >= high_water
        else encode_cursor(
            authorization_id=authorization_id,
            last_seq=end_seq,
            high_water=high_water,
        )
    )

    return {
        "authorization_id": authorization_id,
        "opening_balance": {
            "used_count": opening.used_count,
            "reserved_count": opening.reserved_count,
            "max_count": opening.max_count,
        },
        "entries": entries,
        "page_end_balance": page_end,
        "next_cursor": next_cursor,
        "page_size": page_size,
    }
