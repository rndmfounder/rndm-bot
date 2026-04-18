"""DB-level checks for VPN admin flow (mirrors /vpn_grant, /vpn_setconf, /vpn_revoke)."""

import sqlite3

from vpn import db as vpn_db


def test_grant_setconf_revoke_flow():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    vpn_db.ensure_vpn_tables(cur, conn, False)

    uid = 424242
    vpn_db.grant_days(cur, conn, uid, 30, "manual", "2026-04-01T12:00:00+00:00")
    sub = vpn_db.get_subscription(cur, uid)
    assert sub["status"] == "active"
    assert sub["plan_code"] == "manual"

    conf = "[Interface]\nPrivateKey=test\nAddress=10.0.0.2/32\n"
    vpn_db.set_config_text(cur, conn, uid, conf, "2026-04-01T12:01:00+00:00")
    sub = vpn_db.get_subscription(cur, uid)
    assert conf in (sub["config_text"] or "")

    vpn_db.revoke_user(cur, conn, uid, "2026-04-01T12:02:00+00:00")
    sub = vpn_db.get_subscription(cur, uid)
    assert sub["status"] == "inactive"
    assert sub["config_text"] is None


def test_trial_only_if_absent():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    vpn_db.ensure_vpn_tables(cur, conn, False)
    uid = 99
    assert vpn_db.upsert_trial_if_absent(cur, conn, uid, 3, "2026-04-01T00:00:00+00:00") is True
    assert vpn_db.upsert_trial_if_absent(cur, conn, uid, 3, "2026-04-01T00:00:00+00:00") is False
    sub = vpn_db.get_subscription(cur, uid)
    assert sub["plan_code"] == "trial"
