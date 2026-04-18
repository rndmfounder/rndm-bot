"""Таблица и запросы для VPN-подписок (SQLite / PostgreSQL через DBCursor бота)."""

from __future__ import annotations

from typing import Any


def ensure_vpn_tables(cursor, conn, _use_postgres: bool) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS vpn_subscriptions (
            user_id BIGINT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'inactive',
            plan_code TEXT,
            started_at TEXT,
            expires_at TEXT,
            config_text TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def get_subscription(cursor, user_id: int) -> dict[str, Any] | None:
    cursor.execute(
        "SELECT user_id, status, plan_code, started_at, expires_at, config_text, updated_at "
        "FROM vpn_subscriptions WHERE user_id = ?",
        (user_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return {
        "user_id": int(row[0]),
        "status": row[1],
        "plan_code": row[2],
        "started_at": row[3],
        "expires_at": row[4],
        "config_text": row[5],
        "updated_at": row[6],
    }


def upsert_trial_if_absent(cursor, conn, user_id: int, trial_days: int, now_iso: str) -> bool:
    """Возвращает True, если создали/продлили пробный период."""
    if trial_days <= 0:
        return False
    cursor.execute("SELECT 1 FROM vpn_subscriptions WHERE user_id = ?", (user_id,))
    if cursor.fetchone():
        return False
    from datetime import datetime, timedelta, timezone

    start = datetime.now(timezone.utc)
    exp = start + timedelta(days=trial_days)
    exp_s = exp.replace(microsecond=0).isoformat()
    start_s = start.replace(microsecond=0).isoformat()
    cursor.execute(
        """
        INSERT INTO vpn_subscriptions (user_id, status, plan_code, started_at, expires_at, updated_at)
        VALUES (?, 'active', 'trial', ?, ?, ?)
        """,
        (user_id, start_s, exp_s, now_iso),
    )
    conn.commit()
    return True


def grant_days(cursor, conn, user_id: int, days: int, plan_code: str, now_iso: str) -> None:
    from datetime import datetime, timedelta, timezone

    row = get_subscription(cursor, user_id)
    now = datetime.now(timezone.utc)
    if row and row.get("expires_at"):
        try:
            cur_exp = datetime.fromisoformat(str(row["expires_at"]).replace("Z", "+00:00"))
            if cur_exp.tzinfo is None:
                cur_exp = cur_exp.replace(tzinfo=timezone.utc)
            base = max(now, cur_exp.astimezone(timezone.utc))
        except Exception:
            base = now
    else:
        base = now
    exp = base + timedelta(days=days)
    exp_s = exp.replace(microsecond=0).isoformat()
    start_s = now.replace(microsecond=0).isoformat()
    if row:
        cursor.execute(
            """
            UPDATE vpn_subscriptions
            SET status = 'active', plan_code = ?, expires_at = ?, updated_at = ?
            WHERE user_id = ?
            """,
            (plan_code, exp_s, now_iso, user_id),
        )
    else:
        cursor.execute(
            """
            INSERT INTO vpn_subscriptions (user_id, status, plan_code, started_at, expires_at, updated_at)
            VALUES (?, 'active', ?, ?, ?, ?)
            """,
            (user_id, plan_code, start_s, exp_s, now_iso),
        )
    conn.commit()


def revoke_user(cursor, conn, user_id: int, now_iso: str) -> None:
    cursor.execute(
        """
        UPDATE vpn_subscriptions
        SET status = 'inactive', expires_at = NULL, config_text = NULL, updated_at = ?
        WHERE user_id = ?
        """,
        (now_iso, user_id),
    )
    conn.commit()


def set_config_text(cursor, conn, user_id: int, text: str | None, now_iso: str) -> None:
    row = get_subscription(cursor, user_id)
    if row:
        cursor.execute(
            "UPDATE vpn_subscriptions SET config_text = ?, updated_at = ? WHERE user_id = ?",
            (text, now_iso, user_id),
        )
    else:
        cursor.execute(
            """
            INSERT INTO vpn_subscriptions (user_id, status, plan_code, started_at, expires_at, config_text, updated_at)
            VALUES (?, 'inactive', NULL, NULL, NULL, ?, ?)
            """,
            (user_id, text, now_iso),
        )
    conn.commit()
