"""授权额度账本端到端与并发测试（真实 PostgreSQL + TestClient）。

覆盖需求：
- 期初：启用后新建授权以创建时计数（0/0/max）为期初；启用前已存在的
  授权由启动迁移以启用时三项计数幂等开立期初。
- 逐笔流水：直接发行与候补成交、预留及其确认/取消/到期、申请撤销/
  改期、授权改挂、额度转拨造成的三项计数变化按发生顺序逐笔记录，
  每笔含关联业务编号、变化前后值与时刻；与原业务同事务同成同败，
  拒绝/回滚/幂等重放不留流水。
- 查询：按授权编号 + 游标 + 页大小；返回期初、逐笔流水、页末余额与
  下一游标；首次查询固定上界（后续写入不混入本次翻页）；相邻页余额
  连续；未知授权 404、无效游标 422。

运行：
    DATABASE_URL=postgresql+psycopg://licensing@/licensing?host=/tmp&port=5439 \
        python3 -m tests.test_ledger
"""
import base64
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import func, text
from sqlalchemy import update as sa_update

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+psycopg://licensing@/licensing?host=/tmp&port=5439",
)

from app.database import Base, SessionLocal, engine, init_db  # noqa: E402
from app.main import app  # noqa: E402
from app import models  # noqa: E402

CST = timezone(timedelta(hours=8))
OCT_START = "2026-10-01T00:00:00+08:00"
OCT_END = "2026-11-01T00:00:00+08:00"
NOV_END = "2026-12-01T00:00:00+08:00"
OCCUR = "2026-10-15T12:00:00+08:00"
OCCUR2 = "2026-10-20T12:00:00+08:00"

passed = 0
failed = 0


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}  {detail}")


# ------------------------------------------------------------- 构造辅助
def make_material(c, code):
    return c.post(
        "/api/v1/materials", json={"code": code, "name": code}
    ).json()["id"]


def make_auth(c, mid, count, region="CN", channel="web",
              starts=OCT_START, ends=OCT_END):
    return c.post(
        "/api/v1/authorizations",
        json={
            "material_id": mid,
            "region": region,
            "channel": channel,
            "starts_at": starts,
            "ends_at": ends,
            "max_count": count,
        },
    ).json()["id"]


def apply(c, mids, region="CN", channel="web", occur=OCCUR):
    return c.post(
        "/api/v1/distributions",
        json={
            "material_ids": mids,
            "region": region,
            "channel": channel,
            "occur_at": occur,
        },
    )


def reserve(c, mids, ttl=15, region="CN", channel="web", occur=OCCUR):
    return c.post(
        "/api/v1/reservations",
        json={
            "material_ids": mids,
            "region": region,
            "channel": channel,
            "occur_at": occur,
            "ttl_minutes": ttl,
        },
    )


def waitlist(c, mids, ttl=120, region="CN", channel="web", occur=OCCUR):
    return c.post(
        "/api/v1/waitlists",
        json={
            "material_ids": mids,
            "region": region,
            "channel": channel,
            "occur_at": occur,
            "ttl_minutes": ttl,
        },
    )


def get_auth(c, aid):
    return c.get(f"/api/v1/authorizations/{aid}").json()


def ledger_page(c, aid, cursor=None, page_size=50):
    params = {"page_size": page_size}
    if cursor is not None:
        params["cursor"] = cursor
    return c.get(f"/api/v1/authorizations/{aid}/ledger", params=params)


def ledger_all(c, aid, page_size=200):
    """翻页取回某授权的全部流水，返回 (opening, entries, page_end)。"""
    entries = []
    opening = page_end = None
    cursor = None
    while True:
        r = ledger_page(c, aid, cursor=cursor, page_size=page_size)
        assert r.status_code == 200, r.text
        body = r.json()
        opening = body["opening"]
        page_end = body["page_end"]
        entries.extend(body["entries"])
        if not body["next_cursor"]:
            return opening, entries, page_end
        cursor = body["next_cursor"]


def entry_count(c, aid):
    return len(ledger_all(c, aid)[1])


def force_expire_reservation(reservation_id):
    """直接把库内到期时刻改为当前时刻（左闭右开：now == expires_at 即逾期）。"""
    db = SessionLocal()
    try:
        db.execute(
            sa_update(models.Reservation)
            .where(models.Reservation.id == reservation_id)
            .values(expires_at=func.now())
        )
        db.commit()
    finally:
        db.close()


def encode_cursor(last, upper, v=1):
    payload = json.dumps({"v": v, "last": last, "upper": upper})
    return base64.urlsafe_b64encode(payload.encode()).decode()


def ledger_books(c):
    """全量账本核对：每个授权 期初+逐笔流水 链式连续且终值与当前计数一致。"""
    db = SessionLocal()
    try:
        auths = db.query(models.Authorization).all()
    finally:
        db.close()
    bad = []
    for a in auths:
        opening, entries, page_end = ledger_all(c, a.id)
        used, reserved, maxc = (
            opening["used_count"],
            opening["reserved_count"],
            opening["max_count"],
        )
        ok = True
        for i, e in enumerate(entries, start=1):
            if (
                e["seq"] != i
                or e["used_before"] != used
                or e["reserved_before"] != reserved
                or e["max_before"] != maxc
            ):
                ok = False
                break
            used, reserved, maxc = (
                e["used_after"],
                e["reserved_after"],
                e["max_after"],
            )
        cur = get_auth(c, a.id)
        if (
            not ok
            or used != cur["used_count"]
            or reserved != cur["reserved_count"]
            or maxc != cur["max_count"]
            or page_end["used_count"] != cur["used_count"]
            or page_end["reserved_count"] != cur["reserved_count"]
            or page_end["max_count"] != cur["max_count"]
        ):
            bad.append((a.id, ok, (used, reserved, maxc),
                        (cur["used_count"], cur["reserved_count"],
                         cur["max_count"])))
    return bad


def main() -> int:
    global failed
    Base.metadata.drop_all(bind=engine)
    init_db()

    with TestClient(app) as c:
        print("== 1. 新授权期初：创建时计数起算、空流水 ==")
        m1 = make_material(c, "lg-m1")
        a1 = make_auth(c, m1, 5)
        r = ledger_page(c, a1)
        check("查询账本 200", r.status_code == 200, r.text)
        body = r.json()
        check(
            "期初 = (used 0, reserved 0, max 5)",
            body["opening"]["used_count"] == 0
            and body["opening"]["reserved_count"] == 0
            and body["opening"]["max_count"] == 5
            and body["opening"]["opened_at"] is not None,
            body,
        )
        check(
            "空流水 + 页末即期初 + 无下一游标",
            body["entries"] == []
            and body["page_end"]["used_count"] == 0
            and body["page_end"]["reserved_count"] == 0
            and body["page_end"]["max_count"] == 5
            and body["next_cursor"] is None,
            body,
        )

        print("== 2. 直接发行 / 撤销 / 重复撤销 ==")
        d1 = apply(c, [m1]).json()["id"]
        opening, entries, page_end = ledger_all(c, a1)
        check(
            "发行占用记一笔 distribution_commit",
            len(entries) == 1
            and entries[0]["entry_type"] == "distribution_commit"
            and entries[0]["biz_id"] == d1
            and entries[0]["seq"] == 1
            and entries[0]["used_before"] == 0
            and entries[0]["used_after"] == 1
            and entries[0]["reserved_before"] == 0
            and entries[0]["reserved_after"] == 0
            and entries[0]["max_before"] == 5
            and entries[0]["max_after"] == 5,
            entries,
        )
        check(
            "流水时刻与申请创建时刻一致",
            entries[0]["occurred_at"]
            == c.get(f"/api/v1/distributions/{d1}").json()["created_at"],
            entries[0],
        )
        check("页末余额 = (1,0,5)", page_end["used_count"] == 1
              and page_end["max_count"] == 5, page_end)

        c.post(f"/api/v1/distributions/{d1}/revoke")
        dist1 = c.get(f"/api/v1/distributions/{d1}").json()
        _, entries, page_end = ledger_all(c, a1)
        check(
            "撤销记一笔 distribution_revoke（used 1->0）",
            len(entries) == 2
            and entries[1]["entry_type"] == "distribution_revoke"
            and entries[1]["biz_id"] == d1
            and entries[1]["used_before"] == 1
            and entries[1]["used_after"] == 0,
            entries[-1],
        )
        check(
            "撤销流水时刻 = revoked_at",
            entries[1]["occurred_at"] == dist1["revoked_at"],
            (entries[1]["occurred_at"], dist1["revoked_at"]),
        )
        r = c.post(f"/api/v1/distributions/{d1}/revoke")
        check(
            "重复撤销 409 且不留流水",
            r.status_code == 409 and entry_count(c, a1) == 2,
            r.text,
        )

        print("== 3. 整体拒绝 / 冻结拒绝 不留流水 ==")
        m2 = make_material(c, "lg-m2")
        a2 = make_auth(c, m2, 1)
        apply(c, [m2])  # 占满
        n0 = entry_count(c, a2)
        r = apply(c, [m2])
        check(
            "额度耗尽拒绝 422 且不留流水",
            r.status_code == 422
            and r.json()["error"]["code"] == "distribution_rejected"
            and entry_count(c, a2) == n0,
            r.text,
        )
        r = reserve(c, [m2])
        check(
            "预留拒绝 422 且不留流水",
            r.status_code == 422
            and r.json()["error"]["code"] == "reservation_rejected"
            and entry_count(c, a2) == n0,
            r.text,
        )
        c.put("/api/v1/channel-freezes", json={
            "region": "CN", "channel": "web",
            "action": "freeze", "reason": "账本测试冻结",
        })
        r = apply(c, [m2])
        check(
            "冻结拒绝 422 且不留流水",
            r.status_code == 422
            and r.json()["error"]["code"] == "channel_frozen"
            and entry_count(c, a2) == n0,
            r.text,
        )
        c.put("/api/v1/channel-freezes", json={
            "region": "CN", "channel": "web", "action": "resume",
        })

        print("== 4. 预留生命周期：创建 / 确认 / 重复确认 ==")
        m3 = make_material(c, "lg-m3")
        a3 = make_auth(c, m3, 3)
        rv1 = reserve(c, [m3]).json()["id"]
        _, entries, _ = ledger_all(c, a3)
        check(
            "预留创建记一笔 reservation_create（reserved 0->1）",
            len(entries) == 1
            and entries[0]["entry_type"] == "reservation_create"
            and entries[0]["biz_id"] == rv1
            and entries[0]["reserved_before"] == 0
            and entries[0]["reserved_after"] == 1
            and entries[0]["used_before"] == 0
            and entries[0]["used_after"] == 0,
            entries,
        )
        r = c.post(f"/api/v1/reservations/{rv1}/confirm")
        d_conf = r.json()["distribution_id"]
        _, entries, page_end = ledger_all(c, a3)
        check(
            "确认为一笔 reservation_confirm（reserved-1 且 used+1）",
            len(entries) == 2
            and entries[1]["entry_type"] == "reservation_confirm"
            and entries[1]["biz_id"] == rv1
            and entries[1]["reserved_before"] == 1
            and entries[1]["reserved_after"] == 0
            and entries[1]["used_before"] == 0
            and entries[1]["used_after"] == 1,
            entries[-1],
        )
        check(
            "确认流水时刻 = confirmed_at",
            entries[1]["occurred_at"]
            == c.get(f"/api/v1/reservations/{rv1}").json()["confirmed_at"],
        )
        r = c.post(f"/api/v1/reservations/{rv1}/confirm")
        check(
            "重复确认幂等返回同一笔且不留流水",
            r.status_code == 200
            and r.json()["distribution_id"] == d_conf
            and entry_count(c, a3) == 2,
            r.text,
        )

        print("== 5. 预留取消 / 重复取消 / 到期 ==")
        rv2 = reserve(c, [m3]).json()["id"]
        c.post(f"/api/v1/reservations/{rv2}/cancel")
        _, entries, _ = ledger_all(c, a3)
        check(
            "取消记一笔 reservation_cancel（reserved 1->0）",
            len(entries) == 4
            and entries[3]["entry_type"] == "reservation_cancel"
            and entries[3]["biz_id"] == rv2
            and entries[3]["reserved_before"] == 1
            and entries[3]["reserved_after"] == 0,
            entries[-1],
        )
        r = c.post(f"/api/v1/reservations/{rv2}/cancel")
        check(
            "重复取消 409 且不再释放、不留流水",
            r.status_code == 409 and entry_count(c, a3) == 4,
            r.text,
        )
        rv3 = reserve(c, [m3]).json()["id"]
        force_expire_reservation(rv3)
        r = c.get(f"/api/v1/reservations/{rv3}")  # 触发惰性到期结算
        check("预留已过期", r.json()["status"] == "expired", r.text)
        _, entries, _ = ledger_all(c, a3)
        check(
            "到期记一笔 reservation_expire（reserved 1->0）",
            len(entries) == 6
            and entries[5]["entry_type"] == "reservation_expire"
            and entries[5]["biz_id"] == rv3
            and entries[5]["reserved_before"] == 1
            and entries[5]["reserved_after"] == 0,
            entries[-1],
        )
        check(
            "到期流水时刻 = expired_at",
            entries[5]["occurred_at"] == r.json()["expired_at"],
            (entries[5]["occurred_at"], r.json()["expired_at"]),
        )
        r = c.post(f"/api/v1/reservations/{rv3}/cancel")
        check(
            "已过期取消 409 且不留流水",
            r.status_code == 409 and entry_count(c, a3) == 6,
            r.text,
        )

        print("== 6. 改期：归属不变无流水 / 归属变化记两端 / 同瞬间幂等 ==")
        m4 = make_material(c, "lg-m4")
        a4 = make_auth(c, m4, 2, ends=NOV_END)  # 10+11 月档
        d4 = apply(c, [m4]).json()["id"]
        n0 = entry_count(c, a4)
        # 归属不变（同一授权自我承接）：次数不动、无流水
        r = c.post(f"/api/v1/distributions/{d4}/reschedule",
                   json={"occur_at": OCCUR2})
        check(
            "改期归属不变成功且无流水",
            r.status_code == 200 and entry_count(c, a4) == n0,
            r.text,
        )
        # 同一瞬间幂等：无流水
        r = c.post(f"/api/v1/distributions/{d4}/reschedule",
                   json={"occur_at": "2026-10-20T04:00:00Z"})
        check(
            "同一瞬间改期幂等且无流水",
            r.status_code == 200 and entry_count(c, a4) == n0,
            r.text,
        )
        # 归属变化：10 月档授权 a5 先命中；改到 11 月后只有 a4b 覆盖
        m5 = make_material(c, "lg-m5")
        a5 = make_auth(c, m5, 2)  # 10 月档
        a5b = make_auth(c, m5, 2, starts="2026-11-01T00:00:00+08:00",
                        ends=NOV_END)  # 11 月档
        d5 = apply(c, [m5]).json()["id"]  # 命中 a5
        r = c.post(f"/api/v1/distributions/{d5}/reschedule",
                   json={"occur_at": "2026-11-10T12:00:00+08:00"})
        check("改期跨授权成功", r.status_code == 200, r.text)
        _, ea, _ = ledger_all(c, a5)
        _, eb, _ = ledger_all(c, a5b)
        check(
            "原授权记 distribution_reschedule_out（used 1->0）",
            len(ea) == 2
            and ea[1]["entry_type"] == "distribution_reschedule_out"
            and ea[1]["biz_id"] == d5
            and ea[1]["used_before"] == 1
            and ea[1]["used_after"] == 0,
            ea,
        )
        check(
            "新授权记 distribution_reschedule_in（used 0->1）",
            len(eb) == 1
            and eb[0]["entry_type"] == "distribution_reschedule_in"
            and eb[0]["biz_id"] == d5
            and eb[0]["used_before"] == 0
            and eb[0]["used_after"] == 1,
            eb,
        )
        # 改期失败（目标无匹配授权）整体回滚：两端均无新流水
        r = c.post(f"/api/v1/distributions/{d5}/reschedule",
                   json={"occur_at": "2027-01-10T12:00:00+08:00"})
        check(
            "改期拒绝 422 且两端不留流水",
            r.status_code == 422
            and r.json()["error"]["code"] == "reschedule_rejected"
            and entry_count(c, a5) == 2
            and entry_count(c, a5b) == 1,
            r.text,
        )

        print("== 7. 候补：直接成交 / 入队无流水 / 释放后成交 ==")
        m6 = make_material(c, "lg-m6")
        a6 = make_auth(c, m6, 1)
        r = waitlist(c, [m6])
        w1 = r.json()["id"]
        check(
            "候补直接成交记 waitlist_fulfill",
            r.json()["status"] == "fulfilled",
            r.text,
        )
        _, entries, _ = ledger_all(c, a6)
        check(
            "waitlist_fulfill 关联候补编号与生成申请",
            len(entries) == 1
            and entries[0]["entry_type"] == "waitlist_fulfill"
            and entries[0]["biz_id"] == w1
            and entries[0]["biz_ref"]
            == f"distribution:{r.json()['distribution_id']}"
            and entries[0]["used_before"] == 0
            and entries[0]["used_after"] == 1,
            entries,
        )
        # 额度已满：候补入队不占额、无流水
        r = waitlist(c, [m6])
        w2 = r.json()["id"]
        check(
            "候补入队 pending 且无流水",
            r.json()["status"] == "pending" and entry_count(c, a6) == 1,
            r.text,
        )
        # 撤销占用 -> 队首成交：先记撤销、再记成交（顺序相邻）
        c.post(f"/api/v1/distributions/"
               f"{c.get(f'/api/v1/waitlists/{w1}').json()['distribution_id']}"
               "/revoke")
        _, entries, _ = ledger_all(c, a6)
        check(
            "释放后队首成交：revoke 与 fulfill 依序各记一笔",
            len(entries) == 3
            and entries[1]["entry_type"] == "distribution_revoke"
            and entries[2]["entry_type"] == "waitlist_fulfill"
            and entries[2]["biz_id"] == w2
            and entries[2]["used_before"] == 0
            and entries[2]["used_after"] == 1,
            entries,
        )
        # 候补拒绝（无匹配授权）不留流水
        m6x = make_material(c, "lg-m6x")
        r = waitlist(c, [m6x], region="ZZ", channel="nope")
        check(
            "候补拒绝 422 且不入队（无授权可记）",
            r.status_code == 422
            and r.json()["error"]["code"] == "waitlist_rejected",
            r.text,
        )

        print("== 8. 授权改挂：两端各记一笔 / 空迁移无流水 / 拒绝无流水 ==")
        m7 = make_material(c, "lg-m7")
        a7 = make_auth(c, m7, 3)
        a7b = make_auth(c, m7, 5)
        apply(c, [m7])
        apply(c, [m7])
        r = c.post("/api/v1/authorizations/migrate", json={
            "source_authorization_id": a7,
            "replacement_authorization_id": a7b,
        })
        check("改挂成功", r.status_code == 200
              and r.json()["migrated_count"] == 2, r.text)
        _, ea, _ = ledger_all(c, a7)
        _, eb, _ = ledger_all(c, a7b)
        check(
            "原授权记 migration_out（used 2->0，关联对端授权）",
            len(ea) == 3
            and ea[2]["entry_type"] == "migration_out"
            and ea[2]["biz_id"] == a7b
            and ea[2]["used_before"] == 2
            and ea[2]["used_after"] == 0,
            ea[-1],
        )
        check(
            "替代授权记 migration_in（used 0->2，关联对端授权）",
            len(eb) == 1
            and eb[0]["entry_type"] == "migration_in"
            and eb[0]["biz_id"] == a7
            and eb[0]["used_before"] == 0
            and eb[0]["used_after"] == 2,
            eb,
        )
        # 空迁移（原授权已无未撤销申请）：次数不变、无新流水
        r = c.post("/api/v1/authorizations/migrate", json={
            "source_authorization_id": a7,
            "replacement_authorization_id": a7b,
        })
        check(
            "空迁移幂等且无流水",
            r.status_code == 200
            and r.json()["migrated_count"] == 0
            and entry_count(c, a7) == 3
            and entry_count(c, a7b) == 1,
            r.text,
        )
        # 改挂拒绝（维度不符）：无流水
        m7x = make_material(c, "lg-m7x")
        a7x = make_auth(c, m7x, 3)
        r = c.post("/api/v1/authorizations/migrate", json={
            "source_authorization_id": a7b,
            "replacement_authorization_id": a7x,
        })
        check(
            "改挂拒绝 422 且两端不留流水",
            r.status_code == 422
            and r.json()["error"]["code"] == "migration_rejected"
            and entry_count(c, a7b) == 1
            and entry_count(c, a7x) == 0,
            r.text,
        )

        print("== 9. 额度转拨：max 变化 / 操作号 / 重放与拒绝无流水 ==")
        m8 = make_material(c, "lg-m8")
        a8 = make_auth(c, m8, 10)
        a8b = make_auth(c, m8, 5)
        r = c.post("/api/v1/authorizations/transfer", json={
            "source_authorization_id": a8, "target_authorization_id": a8b,
            "count": 3, "operation_no": "OP-LG-0001",
        })
        check("转拨成功", r.status_code == 200, r.text)
        _, ea, _ = ledger_all(c, a8)
        _, eb, _ = ledger_all(c, a8b)
        check(
            "源授权记 transfer_out（max 10->7，biz_ref=操作号）",
            len(ea) == 1
            and ea[0]["entry_type"] == "transfer_out"
            and ea[0]["biz_ref"] == "OP-LG-0001"
            and ea[0]["biz_id"] == a8b
            and ea[0]["max_before"] == 10
            and ea[0]["max_after"] == 7
            and ea[0]["used_before"] == 0
            and ea[0]["used_after"] == 0,
            ea,
        )
        check(
            "目标授权记 transfer_in（max 5->8）",
            len(eb) == 1
            and eb[0]["entry_type"] == "transfer_in"
            and eb[0]["biz_ref"] == "OP-LG-0001"
            and eb[0]["biz_id"] == a8
            and eb[0]["max_before"] == 5
            and eb[0]["max_after"] == 8,
            eb,
        )
        r = c.post("/api/v1/authorizations/transfer", json={
            "source_authorization_id": a8, "target_authorization_id": a8b,
            "count": 3, "operation_no": "OP-LG-0001",
        })
        check(
            "转拨幂等重放返回首次结果且不留流水",
            r.status_code == 200
            and entry_count(c, a8) == 1
            and entry_count(c, a8b) == 1,
            r.text,
        )
        r = c.post("/api/v1/authorizations/transfer", json={
            "source_authorization_id": a8, "target_authorization_id": a8b,
            "count": 99, "operation_no": "OP-LG-0002",
        })
        check(
            "转拨拒绝 422 且两端不留流水",
            r.status_code == 422
            and r.json()["error"]["code"] == "quota_transfer_rejected"
            and entry_count(c, a8) == 1
            and entry_count(c, a8b) == 1,
            r.text,
        )

        print("== 10. 分页：页大小 / 相邻页余额连续 / 首次固定上界 ==")
        m9 = make_material(c, "lg-m9")
        a9 = make_auth(c, m9, 20)
        for _ in range(5):
            apply(c, [m9])
        # 第一页（page_size=2）：固定上界 = 当前 5 笔
        r1 = ledger_page(c, a9, page_size=2)
        check("第一页 200", r1.status_code == 200, r1.text)
        p1 = r1.json()
        check(
            "第一页 2 条 + 有下一游标 + 页末 used=2",
            len(p1["entries"]) == 2
            and p1["next_cursor"]
            and p1["page_end"]["used_count"] == 2
            and p1["entries"][0]["seq"] == 1
            and p1["entries"][1]["seq"] == 2,
            p1,
        )
        # 翻页期间新写入 2 笔（seq 6、7）：不得混入本次翻页
        apply(c, [m9])
        apply(c, [m9])
        r2 = ledger_page(c, a9, cursor=p1["next_cursor"], page_size=2)
        p2 = r2.json()
        check(
            "第二页为 seq 3、4（新写入不混入）",
            [e["seq"] for e in p2["entries"]] == [3, 4]
            and p2["page_end"]["used_count"] == 4,
            p2,
        )
        check(
            "相邻页余额连续：上页页末 = 下页首笔变化前",
            p1["page_end"]["used_count"]
            == p2["entries"][0]["used_before"]
            and p1["page_end"]["reserved_count"]
            == p2["entries"][0]["reserved_before"]
            and p1["page_end"]["max_count"]
            == p2["entries"][0]["max_before"],
            (p1["page_end"], p2["entries"][0]),
        )
        r3 = ledger_page(c, a9, cursor=p2["next_cursor"], page_size=2)
        p3 = r3.json()
        check(
            "第三页仅 seq 5（固定上界）+ 游标结束 + 页末 used=5",
            [e["seq"] for e in p3["entries"]] == [5]
            and p3["next_cursor"] is None
            and p3["page_end"]["used_count"] == 5,
            p3,
        )
        # 新一轮查询：可见全部 7 笔，页末与授权当前计数一致
        opening, entries, page_end = ledger_all(c, a9, page_size=3)
        cur = get_auth(c, a9)
        check(
            "重新查询可见全部 7 笔且终值账实相符",
            len(entries) == 7
            and entries[-1]["seq"] == 7
            and page_end["used_count"] == cur["used_count"] == 7
            and page_end["max_count"] == cur["max_count"] == 20,
            (len(entries), page_end, cur),
        )
        check(
            "链式连续：每笔 before = 上笔 after，首笔 before = 期初",
            all(
                entries[i]["used_before"] == entries[i - 1]["used_after"]
                and entries[i]["reserved_before"]
                == entries[i - 1]["reserved_after"]
                and entries[i]["max_before"] == entries[i - 1]["max_after"]
                for i in range(1, len(entries))
            )
            and entries[0]["used_before"] == opening["used_count"]
            and entries[0]["max_before"] == opening["max_count"],
        )
        # 页大小校验
        for bad, name in [(0, "page_size=0"), (201, "page_size=201"),
                          ("abc", "page_size 非整数")]:
            r = ledger_page(c, a9, page_size=bad)
            check(f"{name} -> 422", r.status_code == 422, r.text)

        print("== 11. 未知授权 404 / 无效游标 422 ==")
        r = ledger_page(c, 999999)
        check(
            "未知授权 -> 404 not_found",
            r.status_code == 404
            and r.json()["error"]["code"] == "not_found",
            r.text,
        )
        for bad_cursor, name in [
            ("not-a-cursor", "乱码游标"),
            (base64.urlsafe_b64encode(b"not json").decode(), "非 JSON"),
            (base64.urlsafe_b64encode(b"{}").decode(), "缺字段"),
            (encode_cursor(5, 3), "last>upper"),
            (encode_cursor(-1, 3), "负数序号"),
            (encode_cursor(1, 3, v=99), "版本不符"),
            (encode_cursor(999, 999), "锚点流水不存在"),
        ]:
            r = ledger_page(c, a9, cursor=bad_cursor)
            check(
                f"无效游标（{name}）-> 422 invalid_ledger_cursor",
                r.status_code == 422
                and r.json()["error"]["code"] == "invalid_ledger_cursor",
                r.text,
            )

        print("== 12. 启用前存量授权：启动迁移按期初幂等开立 ==")
        # 绕过 API 直接落库一条授权（模拟账本功能启用前的存量数据），
        # 再执行启动迁移：应以当前三项计数开立期初，且重复执行幂等。
        m11 = make_material(c, "lg-m11")
        db = SessionLocal()
        try:
            db.execute(
                text(
                    "INSERT INTO authorizations "
                    "(material_id, region, channel, starts_at, ends_at, "
                    " max_count, used_count, reserved_count, status, "
                    " created_at) "
                    "VALUES (:mid, 'CN', 'web', now(), now() + interval '30 days', "
                    " 9, 4, 2, 'active', now())"
                ),
                {"mid": m11},
            )
            legacy_id = db.scalar(text("SELECT max(id) FROM authorizations"))
            db.commit()
        finally:
            db.close()
        db = SessionLocal()
        try:
            check(
                "迁移前存量授权无账本账户",
                db.get(models.QuotaLedgerAccount, legacy_id) is None,
            )
        finally:
            db.close()
        init_db()  # 启动迁移：幂等开立
        init_db()  # 重复执行仍幂等
        r = ledger_page(c, legacy_id)
        check(
            "存量授权期初 = 启用时计数 (used 4, reserved 2, max 9)，无流水",
            r.status_code == 200
            and r.json()["opening"]["used_count"] == 4
            and r.json()["opening"]["reserved_count"] == 2
            and r.json()["opening"]["max_count"] == 9
            and r.json()["entries"] == []
            and r.json()["page_end"]["used_count"] == 4,
            r.text,
        )
        # 存量授权后续业务照常记账
        c.post(f"/api/v1/distributions", json={
            "material_ids": [m11], "region": "CN", "channel": "web",
            "occur_at": datetime.now(CST).isoformat(),
        })
        _, entries, page_end = ledger_all(c, legacy_id)
        check(
            "存量授权新发行正常记账且承接期初",
            len(entries) == 1
            and entries[0]["used_before"] == 4
            and entries[0]["used_after"] == 5
            and page_end["used_count"] == 5,
            entries,
        )

        print("== 13. 并发：同授权并发申请，流水与成功数一致且序号连续 ==")
        m10 = make_material(c, "lg-m10")
        a10 = make_auth(c, m10, 5)
        with ThreadPoolExecutor(max_workers=20) as pool:
            rs = list(pool.map(lambda _: apply(c, [m10]), range(20)))
        ok = sum(1 for r in rs if r.status_code == 201)
        _, entries, page_end = ledger_all(c, a10)
        seqs = [e["seq"] for e in entries]
        check(
            "并发：成功数=流水数=5，seq 连续 1..5 无重复",
            ok == 5
            and len(entries) == 5
            and seqs == [1, 2, 3, 4, 5]
            and all(e["entry_type"] == "distribution_commit" for e in entries)
            and page_end["used_count"] == 5,
            (ok, seqs),
        )

        print("== 14. 全量账实核对（期初+流水链 == 当前计数） ==")
        bad = ledger_books(c)
        check("全部授权账本链式连续且账实相符", not bad, bad)

    print(f"\n结果：{passed} 通过，{failed} 失败")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
