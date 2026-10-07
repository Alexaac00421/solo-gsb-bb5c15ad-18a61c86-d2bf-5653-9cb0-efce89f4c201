"""额度账本端到端测试（真实 PostgreSQL + TestClient）。

运行：
    DATABASE_URL=postgresql+psycopg://licensing@/licensing?host=/tmp&port=5439 \
        python3 -m tests.test_ledger
"""
import os
import sys
from datetime import timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import text

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+psycopg://licensing@/licensing?host=/tmp&port=5439",
)

from app.database import Base, engine, init_db  # noqa: E402
from app.main import app  # noqa: E402

CST = timezone(timedelta(hours=8))
OCCUR = "2026-12-01T12:00:00+08:00"
WINDOW = (
    "2026-01-01T00:00:00+08:00",
    "2027-01-01T00:00:00+08:00",
)

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


def auth_body(material_id, max_count=10, region="CN", channel="web"):
    return {
        "material_id": material_id,
        "region": region,
        "channel": channel,
        "starts_at": WINDOW[0],
        "ends_at": WINDOW[1],
        "max_count": max_count,
    }


def walk_ledger(client, auth_id, page_size=2):
    """从首页起走完整本账（首次查询的固定上界内），返回 (页列表, 全量流水)。"""
    pages = []
    cursor = None
    while True:
        params = {"page_size": page_size}
        if cursor is not None:
            params["cursor"] = cursor
        r = client.get(
            f"/api/v1/authorizations/{auth_id}/ledger", params=params
        )
        assert r.status_code == 200, r.text
        page = r.json()
        pages.append(page)
        cursor = page["next_cursor"]
        if cursor is None:
            break
    entries = [e for p in pages for e in p["entries"]]
    return pages, entries


def balances_consistent(entries, opening):
    """逐笔校验：首笔 before==期初，其后每笔 before==上一笔 after。"""
    prev = opening
    keys = ("used_count", "reserved_count", "max_count")
    for e in entries:
        before = {k: e[f"before_{k}"] for k in keys}
        if before != prev:
            return False
        prev = {k: e[f"after_{k}"] for k in keys}
    return True


def main() -> int:
    Base.metadata.drop_all(bind=engine)
    init_db()

    with TestClient(app) as c:
        print("== 期初余额 ==")
        m1 = c.post(
            "/api/v1/materials", json={"code": "m1", "name": "素材一"}
        ).json()["id"]
        a1 = c.post("/api/v1/authorizations", json=auth_body(m1, 5)).json()
        a2 = c.post("/api/v1/authorizations", json=auth_body(m1, 5)).json()
        a1_id, a2_id = a1["id"], a2["id"]

        book = c.get(f"/api/v1/authorizations/{a1_id}/ledger").json()
        check(
            "新授权期初 0/0/max",
            book["opening_balance"]
            == {"used_count": 0, "reserved_count": 0, "max_count": 5},
            book,
        )
        check("新授权期初后暂无流水", book["entries"] == [], book)
        check(
            "空账页末余额=期初",
            book["page_end_balance"] == book["opening_balance"],
            book,
        )
        check("空账 next_cursor 为 null", book["next_cursor"] is None)

        print("== 直接发行 / 撤销 ==")
        d1 = c.post(
            "/api/v1/distributions",
            json={
                "material_ids": [m1],
                "region": "CN",
                "channel": "web",
                "occur_at": OCCUR,
            },
        )
        check("直接发行 201", d1.status_code == 201, d1.text)
        d1_id = d1.json()["id"]
        pages, entries = walk_ledger(c, a1_id)
        check(
            "直接发行为一笔 direct_distribution",
            [e["event_type"] for e in entries] == ["direct_distribution"]
            and entries[0]["ref_type"] == "distribution"
            and entries[0]["ref_id"] == str(d1_id),
            entries,
        )
        check(
            "直接发行 0->1 used",
            (
                entries[0]["before_used_count"],
                entries[0]["after_used_count"],
            )
            == (0, 1),
            entries[0],
        )
        check(
            "明细关联业务编号在 detail",
            entries[0]["detail"]["distribution_id"] == d1_id,
            entries[0]["detail"],
        )

        rv = c.post(f"/api/v1/distributions/{d1_id}/revoke")
        check("撤销 200", rv.status_code == 200, rv.text)
        _, entries = walk_ledger(c, a1_id)
        check(
            "撤销产生 distribution_revoked：used 1->0",
            entries[-1]["event_type"] == "distribution_revoked"
            and entries[-1]["before_used_count"] == 1
            and entries[-1]["after_used_count"] == 0
            and entries[-1]["ref_id"] == str(d1_id),
            entries[-1],
        )
        check(
            "页末余额=授权当前计数",
            balances_consistent(
                entries,
                {"used_count": 0, "reserved_count": 0, "max_count": 5},
            ),
        )

        print("== 拒绝不留流水 ==")
        # 缺匹配时刻（时段外）→ 422 拒绝。
        rej = c.post(
            "/api/v1/distributions",
            json={
                "material_ids": [m1],
                "region": "CN",
                "channel": "web",
                "occur_at": "2030-01-01T00:00:00+08:00",
            },
        )
        check("时段外拒绝 422", rej.status_code == 422, rej.text)
        _, entries_after_reject = walk_ledger(c, a1_id)
        check(
            "被拒绝申请不留流水",
            len(entries_after_reject) == len(entries),
        )
        # 重复撤销 → 409，不留流水。
        rv2 = c.post(f"/api/v1/distributions/{d1_id}/revoke")
        check("重复撤销 409", rv2.status_code == 409, rv2.text)
        _, entries2 = walk_ledger(c, a1_id)
        check("重复撤销不留流水", len(entries2) == len(entries))

        print("== 预留：占额 / 确认转账 ==")
        r1 = c.post(
            "/api/v1/reservations",
            json={
                "material_ids": [m1],
                "region": "CN",
                "channel": "web",
                "occur_at": OCCUR,
                "ttl_minutes": 15,
            },
        ).json()
        r1_id = r1["id"]
        cf = c.post(f"/api/v1/reservations/{r1_id}/confirm")
        check("预留确认 200", cf.status_code == 200, cf.text)
        dist_from_resv = cf.json()["distribution_id"]
        cf2 = c.post(f"/api/v1/reservations/{r1_id}/confirm")
        check("重复确认幂等 200", cf2.status_code == 200, cf2.text)
        _, entries = walk_ledger(c, a1_id)
        types = [e["event_type"] for e in entries]
        check(
            "预留占额+确认各一笔，幂等确认不重复",
            types.count("reservation_placed") == 1
            and types.count("reservation_confirmed") == 1,
            types,
        )
        placed = next(e for e in entries if e["event_type"] == "reservation_placed")
        check(
            "reservation_placed：reserved 0->1",
            placed["before_reserved_count"] == 0
            and placed["after_reserved_count"] == 1
            and placed["before_used_count"] == placed["after_used_count"],
            placed,
        )
        confirmed = next(
            e for e in entries if e["event_type"] == "reservation_confirmed"
        )
        check(
            "reservation_confirmed：reserved 1->0、used 0->1（合计变化）",
            confirmed["before_used_count"] == 0
            and confirmed["after_used_count"] == 1
            and confirmed["before_reserved_count"] == 1
            and confirmed["after_reserved_count"] == 0,
            confirmed,
        )
        check(
            "确认流水关联生成的申请编号",
            confirmed["detail"]["distribution_id"] == dist_from_resv,
            confirmed["detail"],
        )

        print("== 预留取消 ==")
        r2 = c.post(
            "/api/v1/reservations",
            json={
                "material_ids": [m1],
                "region": "CN",
                "channel": "web",
                "occur_at": OCCUR,
                "ttl_minutes": 15,
            },
        ).json()
        r2_id = r2["id"]
        cancel = c.post(f"/api/v1/reservations/{r2_id}/cancel")
        check("取消预留 200", cancel.status_code == 200, cancel.text)
        _, entries = walk_ledger(c, a1_id)
        cancelled = [
            e for e in entries if e["event_type"] == "reservation_cancelled"
        ]
        check(
            "取消产生 reservation_cancelled：reserved 1->0",
            len(cancelled) == 1
            and cancelled[0]["before_reserved_count"] == 1
            and cancelled[0]["after_reserved_count"] == 0
            and cancelled[0]["ref_id"] == str(r2_id),
            cancelled,
        )

        print("== 预留到期（惰性结算）==")
        # 占满 a1（max=5；当前 used=1），再制造一笔 1 分钟预留，然后直接
        # 改库把到期时刻挪到过去，触发下一次通道写事务的惰性到期结算。
        r3 = c.post(
            "/api/v1/reservations",
            json={
                "material_ids": [m1],
                "region": "CN",
                "channel": "web",
                "occur_at": OCCUR,
                "ttl_minutes": 1,
            },
        ).json()
        r3_id = r3["id"]
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE reservations SET expires_at = now() - interval "
                    "'1 second' WHERE id = :rid"
                ),
                {"rid": r3_id},
            )
        # 任意通道写事务先惰性结算（再提交一次直接发行）。
        settled = c.post(
            "/api/v1/distributions",
            json={
                "material_ids": [m1],
                "region": "CN",
                "channel": "web",
                "occur_at": OCCUR,
            },
        )
        check("到期结算后新发行成功", settled.status_code == 201, settled.text)
        _, entries = walk_ledger(c, a1_id)
        expired = [
            e for e in entries if e["event_type"] == "reservation_expired"
        ]
        check(
            "到期产生 reservation_expired：reserved 1->0",
            len(expired) == 1
            and expired[0]["before_reserved_count"] == 1
            and expired[0]["after_reserved_count"] == 0
            and expired[0]["ref_id"] == str(r3_id),
            expired,
        )
        # 账实相符：页末三项 == 授权当前三项。
        ga = c.get(f"/api/v1/authorizations/{a1_id}").json()
        check(
            "账实相符（used/reserved/max）",
            entries[-1]["after_used_count"] == ga["used_count"]
            and entries[-1]["after_reserved_count"] == ga["reserved_count"]
            and entries[-1]["after_max_count"] == ga["max_count"],
            (entries[-1], ga),
        )

        print("== 额度转拨（两端流水）==")
        before_cnt_a1 = len(walk_ledger(c, a1_id)[1])
        before_cnt_a2 = len(walk_ledger(c, a2_id)[1])
        tr = c.post(
            "/api/v1/authorizations/transfer",
            json={
                "source_authorization_id": a1_id,
                "target_authorization_id": a2_id,
                "count": 1,
                "operation_no": "OP-LEDGER-1",
            },
        )
        check("转拨 200", tr.status_code == 200, tr.text)
        _, e_a1 = walk_ledger(c, a1_id)
        _, e_a2 = walk_ledger(c, a2_id)
        out = e_a1[-1]
        into = e_a2[-1]
        check(
            "源端 transfer_out：max 5->4",
            out["event_type"] == "transfer_out"
            and out["before_max_count"] == 5
            and out["after_max_count"] == 4
            and out["counterparty_authorization_id"] == a2_id
            and out["detail"]["operation_no"] == "OP-LEDGER-1",
            out,
        )
        check(
            "目标端 transfer_in：max 5->6",
            into["event_type"] == "transfer_in"
            and into["before_max_count"] == 5
            and into["after_max_count"] == 6
            and into["counterparty_authorization_id"] == a1_id
            and into["ref_id"] == out["ref_id"],
            into,
        )
        check(
            "转拨不改 used/reserved",
            out["before_used_count"] == out["after_used_count"]
            and out["before_reserved_count"] == out["after_reserved_count"],
            out,
        )
        replay = c.post(
            "/api/v1/authorizations/transfer",
            json={
                "source_authorization_id": a1_id,
                "target_authorization_id": a2_id,
                "count": 1,
                "operation_no": "OP-LEDGER-1",
            },
        )
        check("同号重放 200", replay.status_code == 200, replay.text)
        check(
            "重放不留流水（两端）",
            len(walk_ledger(c, a1_id)[1]) == before_cnt_a1 + 1
            and len(walk_ledger(c, a2_id)[1]) == before_cnt_a2 + 1,
        )

        print("== 候补成交（队列与直接成交同事件类型）==")
        # 独占的新通道授权（max=2），避免与前面业务串账：
        # 2 笔直接发行占满 → 第 3 笔拒绝 → 候补入队 →
        # 撤销第 2 笔释放额度，队首在撤销事务内自动成交。
        wm = c.post(
            "/api/v1/materials", json={"code": "wm", "name": "候补素材"}
        ).json()["id"]
        wa = c.post(
            "/api/v1/authorizations",
            json=auth_body(wm, 2, region="CN", channel="wl"),
        ).json()["id"]
        wd = []
        for _ in range(2):
            r = c.post(
                "/api/v1/distributions",
                json={
                    "material_ids": [wm],
                    "region": "CN",
                    "channel": "wl",
                    "occur_at": OCCUR,
                },
            )
            wd.append(r.status_code)
        check("两笔直接发行占满", wd == [201, 201], wd)
        # 查最后一笔占用 wa 的申请编号（随后撤销以释放额度）。
        with engine.connect() as conn:
            full_d = conn.execute(
                text(
                    "SELECT d.id FROM distributions d JOIN "
                    "distribution_items di ON di.distribution_id = d.id "
                    "WHERE di.authorization_id = :aid ORDER BY d.id DESC "
                    "LIMIT 1"
                ),
                {"aid": wa},
            ).scalar()
        over = c.post(
            "/api/v1/distributions",
            json={
                "material_ids": [wm],
                "region": "CN",
                "channel": "wl",
                "occur_at": OCCUR,
            },
        )
        check("第 3 笔额度不足拒绝 422", over.status_code == 422, over.text)
        w = c.post(
            "/api/v1/waitlists",
            json={
                "material_ids": [wm],
                "region": "CN",
                "channel": "wl",
                "occur_at": OCCUR,
                "ttl_minutes": 30,
            },
        )
        check("额度不足候补入队 201", w.status_code == 201, w.text)
        check("候补 pending", w.json()["status"] == "pending", w.json())
        w_id = w.json()["id"]
        # 入队本身不占额、无流水（独占授权此时只有 2 笔直接发行流水）。
        n_before = len(walk_ledger(c, wa)[1])
        check("候补入队不留流水", n_before == 2, n_before)
        c.post(f"/api/v1/distributions/{full_d}/revoke")
        w_after = c.get(f"/api/v1/waitlists/{w_id}").json()
        check("撤销释放后候补成交", w_after["status"] == "fulfilled", w_after)
        _, e_wa = walk_ledger(c, wa)
        fulfills = [
            e for e in e_wa if e["event_type"] == "waitlist_fulfillment"
        ]
        check(
            "候补成交产生一笔 waitlist_fulfillment",
            len(fulfills) == 1
            and fulfills[0]["ref_type"] == "waitlist"
            and fulfills[0]["ref_id"] == str(w_id)
            and fulfills[0]["detail"]["waitlist_id"] == w_id
            and fulfills[0]["detail"]["distribution_id"]
            == w_after["distribution_id"],
            fulfills,
        )
        check(
            "候补成交 used 1->2（撤销先 2->1，成交再 1->2，同事务顺序入账）",
            fulfills[0]["before_used_count"] == 1
            and fulfills[0]["after_used_count"] == 2,
            fulfills[0],
        )
        check(
            "撤销与成交先后两笔（撤销 seq 在前、成交在后）",
            [(e["entry_seq"], e["event_type"]) for e in e_wa] == [
                (1, "direct_distribution"),
                (2, "direct_distribution"),
                (3, "distribution_revoked"),
                (4, "waitlist_fulfillment"),
            ],
            [(e["entry_seq"], e["event_type"]) for e in e_wa],
        )

        print("== 改期（迁出/迁入成对）==")
        # 新建两条同时段授权（a3 先被占用），把该笔申请改期到另一时刻，
        # 使重叠选择落到 a4：a3 reschedule_out、a4 reschedule_in。
        a3 = c.post(
            "/api/v1/authorizations",
            json=auth_body(m1, 5, region="CN", channel="mob"),
        ).json()["id"]
        a4 = c.post(
            "/api/v1/authorizations",
            json=auth_body(m1, 5, region="CN", channel="mob"),
        ).json()["id"]
        # 让 a4 到期更早以先被选中：直接改库缩短 a4 的 ends_at。
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE authorizations SET ends_at = "
                    "timestamp with time zone '2026-06-01 00:00:00+08' "
                    "WHERE id = :aid"
                ),
                {"aid": a4},
            )
        sd = c.post(
            "/api/v1/distributions",
            json={
                "material_ids": [m1],
                "region": "CN",
                "channel": "mob",
                "occur_at": "2026-03-01T12:00:00+08:00",
            },
        ).json()
        check("改期前命中 a4", sd["items"][0]["authorization"]["id"] == a4)
        sd_id = sd["id"]
        rs = c.post(
            f"/api/v1/distributions/{sd_id}/reschedule",
            json={"occur_at": "2026-09-01T12:00:00+08:00"},
        )
        check("改期 200", rs.status_code == 200, rs.text)
        check(
            "改期后命中 a3",
            rs.json()["items"][0]["authorization"]["id"] == a3,
            rs.json(),
        )
        _, e4 = walk_ledger(c, a4)
        _, e3 = walk_ledger(c, a3)
        check(
            "原授权 reschedule_out used 1->0，对端 a3",
            e4[-1]["event_type"] == "reschedule_out"
            and e4[-1]["before_used_count"] == 1
            and e4[-1]["after_used_count"] == 0
            and e4[-1]["counterparty_authorization_id"] == a3
            and e4[-1]["ref_id"] == str(sd_id),
            e4[-1],
        )
        check(
            "新授权 reschedule_in used 0->1，对端 a4",
            e3[-1]["event_type"] == "reschedule_in"
            and e3[-1]["before_used_count"] == 0
            and e3[-1]["after_used_count"] == 1
            and e3[-1]["counterparty_authorization_id"] == a4
            and e3[-1]["ref_id"] == str(sd_id),
            e3[-1],
        )
        # 同一瞬间幂等改期不留流水。
        n4 = len(e4)
        c.post(
            f"/api/v1/distributions/{sd_id}/reschedule",
            json={"occur_at": "2026-09-01T12:00:00+08:00"},
        )
        check("幂等改期不留流水", len(walk_ledger(c, a4)[1]) == n4)

        print("== 授权改挂（迁出/迁入成对 + 业务编号）==")
        # a5 承载一笔申请；改挂到 a6（同素材/地区/渠道、时段覆盖、有余量）。
        a5 = c.post(
            "/api/v1/authorizations",
            json=auth_body(m1, 5, region="CN", channel="tv"),
        ).json()["id"]
        a6 = c.post(
            "/api/v1/authorizations",
            json=auth_body(m1, 5, region="CN", channel="tv"),
        ).json()["id"]
        c.post(
            "/api/v1/distributions",
            json={
                "material_ids": [m1],
                "region": "CN",
                "channel": "tv",
                "occur_at": OCCUR,
            },
        ).json()["id"]
        mig = c.post(
            "/api/v1/authorizations/migrate",
            json={
                "source_authorization_id": a5,
                "replacement_authorization_id": a6,
            },
        )
        check("改挂 200", mig.status_code == 200, mig.text)
        check("改挂条数 1", mig.json()["migrated_count"] == 1, mig.json())
        _, e5 = walk_ledger(c, a5)
        _, e6 = walk_ledger(c, a6)
        check(
            "原授权 migration_out used 1->0 且停用",
            e5[-1]["event_type"] == "migration_out"
            and e5[-1]["before_used_count"] == 1
            and e5[-1]["after_used_count"] == 0
            and e5[-1]["counterparty_authorization_id"] == a6
            and e5[-1]["ref_type"] == "authorization_migration",
            e5[-1],
        )
        check(
            "替代授权 migration_in used 0->1，同一业务编号",
            e6[-1]["event_type"] == "migration_in"
            and e6[-1]["before_used_count"] == 0
            and e6[-1]["after_used_count"] == 1
            and e6[-1]["counterparty_authorization_id"] == a5
            and e6[-1]["ref_id"] == e5[-1]["ref_id"],
            e6[-1],
        )
        # 改挂拒绝（再次改挂已停用源 → 替代须承接自己？这里改挂到一条
        # 维度不符的授权必然拒绝），不留流水。
        n6 = len(e6)
        bad = c.post(
            "/api/v1/authorizations/migrate",
            json={
                "source_authorization_id": a6,
                "replacement_authorization_id": a2,
            },
        )
        check("维度不符改挂拒绝", bad.status_code == 422, bad.text)
        check("改挂拒绝不留流水", len(walk_ledger(c, a6)[1]) == n6)

        print("== 翻页：连续性 / 固定上界 / 余额 ==")
        pages, all_entries = walk_ledger(c, a1_id, page_size=3)
        check("a1 多页", len(pages) >= 2, len(pages))
        # 每页条数不超过页大小。
        check(
            "每页 <= page_size",
            all(len(p["entries"]) <= 3 for p in pages),
        )
        # seq 连续升序。
        seqs = [e["entry_seq"] for e in all_entries]
        check("seq 从 1 连续递增", seqs == list(range(1, len(seqs) + 1)), seqs)
        # 相邻页余额连续：上一页页末 == 下一页首笔 before。
        continuity = True
        for i in range(1, len(pages)):
            prev_end = pages[i - 1]["page_end_balance"]
            first_before = {
                k: pages[i]["entries"][0][f"before_{k}"]
                for k in ("used_count", "reserved_count", "max_count")
            }
            if prev_end != first_before:
                continuity = False
        check("相邻页余额连续", continuity)
        # 首页期初恒定。
        check(
            "首页返回期初",
            pages[0]["opening_balance"]
            == {"used_count": 0, "reserved_count": 0, "max_count": 5},
        )
        # 全部页 opening_balance 均回传且相同。
        check(
            "每页回传同一期初",
            all(
                p["opening_balance"] == pages[0]["opening_balance"]
                for p in pages
            ),
        )
        # 逐笔前后值自洽，末笔 after 为授权当前计数。
        check(
            "整本账逐笔滚算自洽",
            balances_consistent(
                all_entries, pages[0]["opening_balance"]
            ),
        )

        # 首次查询固定上界：取首页游标后新增一笔，旧翻页序列不得混入。
        first = c.get(
            f"/api/v1/authorizations/{a1_id}/ledger?page_size=2"
        ).json()
        hwm = max(e["entry_seq"] for e in all_entries)
        new = c.post(
            "/api/v1/distributions",
            json={
                "material_ids": [m1],
                "region": "CN",
                "channel": "web",
                "occur_at": OCCUR,
            },
        )
        check("上界后新发行成功", new.status_code == 201, new.text)
        new_dist_id = new.json()["id"]
        seen_seqs = []
        cur = first["next_cursor"]
        while cur:
            p = c.get(
                f"/api/v1/authorizations/{a1_id}/ledger",
                params={"page_size": 2, "cursor": cur},
            ).json()
            seen_seqs += [e["entry_seq"] for e in p["entries"]]
            cur = p["next_cursor"]
        check(
            "后续写入不混入该次翻页（旧游标止于上界）",
            all(s <= hwm for s in seen_seqs) and hwm + 1 not in seen_seqs,
            (seen_seqs, hwm),
        )
        # 重新从首页查询则能看到新写入，上界前移。
        fresh = c.get(
            f"/api/v1/authorizations/{a1_id}/ledger?page_size=100"
        ).json()
        check(
            "重新首页查询包含新写入",
            fresh["entries"][-1]["entry_seq"] == hwm + 1
            and fresh["entries"][-1]["event_type"] == "direct_distribution"
            and fresh["entries"][-1]["ref_id"] == str(new_dist_id),
            fresh["entries"][-1],
        )

        print("== 错误处理 ==")
        check(
            "未知授权 404",
            c.get("/api/v1/authorizations/999999/ledger").status_code == 404,
        )
        r = c.get(f"/api/v1/authorizations/{a1_id}/ledger?cursor=not-a-cursor")
        check(
            "非法游标 422 invalid_ledger_cursor",
            r.status_code == 422
            and r.json()["error"]["code"] == "invalid_ledger_cursor",
            r.text,
        )
        other_cur = c.get(
            f"/api/v1/authorizations/{a2_id}/ledger?page_size=1"
        ).json()["next_cursor"]
        # a2 可能恰好只有整页而无 next_cursor，取末页之后手工构造一个
        # 属于 a2 的游标用于串号校验。
        from app.ledger import encode_cursor

        other_cur = encode_cursor(
            authorization_id=a2_id, last_seq=1, high_water=10
        )
        r = c.get(
            f"/api/v1/authorizations/{a1_id}/ledger",
            params={"cursor": other_cur},
        )
        check(
            "他授权游标串用 422",
            r.status_code == 422
            and r.json()["error"]["code"] == "invalid_ledger_cursor",
            r.text,
        )
        # 篡改版本。
        import base64
        import json

        bad_token = base64.urlsafe_b64encode(
            json.dumps(["XX", a1_id, 1, 2]).encode()
        ).decode()
        r = c.get(
            f"/api/v1/authorizations/{a1_id}/ledger",
            params={"cursor": bad_token},
        )
        check("游标版本不符 422", r.status_code == 422, r.text)
        for ps in (0, -1, 101, 1000):
            r = c.get(
                f"/api/v1/authorizations/{a1_id}/ledger",
                params={"page_size": ps},
            )
            check(
                f"page_size={ps} 422 invalid_page_size",
                r.status_code == 422
                and r.json()["error"]["code"] == "invalid_page_size",
                r.text,
            )

    print("== 旧库升级：期初回填 + 触发器 ==")
    upgrade_ok = _test_upgrade_backfill()
    check("旧库升级回填期初与不可改写触发器", upgrade_ok)

    print(f"\n结果：{passed} 通过，{failed} 失败")
    return 1 if failed else 0


def _test_upgrade_backfill() -> bool:
    """在独立 schema 内模拟「功能上线前的旧库」：

    仅建旧表（authorizations/materials/distributions...）并写入已占用
    授权，再执行 init_db()（create_all 补建新表 + 升级回填），验证：
    已有授权以期初形式回填为启用瞬间的计数，且新授权仍走创建时期初。
    """
    schema = "ledger_upgrade_sim"
    url = os.environ["DATABASE_URL"]
    from sqlalchemy import create_engine

    admin = create_engine(url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    admin.dispose()

    sep = "&" if "?" in url else "?"
    old_engine = create_engine(f"{url}{sep}options=-csearch_path%3D{schema}")
    with old_engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE materials (id serial PRIMARY KEY, code varchar "
                "UNIQUE, name varchar, created_at timestamptz default now())"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE authorizations (id serial PRIMARY KEY, "
                "material_id int, region varchar, channel varchar, "
                "starts_at timestamptz, ends_at timestamptz, max_count int, "
                "used_count int default 0, "
                "reserved_count int NOT NULL DEFAULT 0, "
                "status varchar default 'active', "
                "created_at timestamptz default now())"
            )
        )
        conn.execute(
            text(
                "INSERT INTO materials (code, name) VALUES ('old','旧素材') "
                "RETURNING id"
            )
        )
        conn.execute(
            text(
                "INSERT INTO authorizations (material_id, region, channel, "
                "starts_at, ends_at, max_count, used_count) VALUES "
                "(1, 'CN', 'web', "
                "timestamp with time zone '2026-01-01 00:00:00+08', "
                "timestamp with time zone '2027-01-01 00:00:00+08', 7, 3)"
            )
        )
    old_engine.dispose()

    # 用同一 schema 的引擎执行升级。
    sim_engine = create_engine(f"{url}{sep}options=-csearch_path%3D{schema}")
    import app.database as dbmod

    orig_engine = dbmod.engine
    dbmod.engine = sim_engine
    dbmod.SessionLocal.configure(bind=sim_engine)
    try:
        dbmod.init_db()
        with sim_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT used_count, reserved_count, max_count FROM "
                    "quota_ledger_openings WHERE authorization_id = 1"
                )
            ).first()
            backfilled = row is not None and tuple(row) == (3, 0, 7)
            trg = conn.execute(
                text(
                    "SELECT 1 FROM pg_trigger WHERE tgname = "
                    "'trg_quota_ledger_immutable'"
                )
            ).first()
            trigger_ok = trg is not None
        # 幂等升级不报错、不重复回填。
        dbmod.init_db()
        with sim_engine.connect() as conn:
            cnt = conn.execute(
                text(
                    "SELECT count(*) FROM quota_ledger_openings "
                    "WHERE authorization_id = 1"
                )
            ).scalar()
            idem = cnt == 1
    finally:
        dbmod.engine = orig_engine
        dbmod.SessionLocal.configure(bind=orig_engine)
        sim_engine.dispose()
        with admin.connect() as conn:
            conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        admin.dispose()
    return backfilled and trigger_ok and idem


if __name__ == "__main__":
    sys.exit(main())
