"""Shared disk guard + email alerting for every pipeline writing to this Supabase.

    THIS FILE IS IDENTICAL IN EVERY PIPELINE REPO. If you change it, change it
    everywhere - or better, don't: everything configurable lives in the
    database (etl_disk_policy, etl_alert_config), so budgets, thresholds and
    recipients change with an UPDATE and no deploy.

WHAT IT DOES, IN ONE SENTENCE
    Before a pipeline writes, it asks the database "am I allowed?", and if the
    answer is no - or nearly no - you get an email.

WHY IT EXISTS
    One Supabase volume is shared by the BC sync, the GRN schedulers and the
    marketplace/ads loaders. Each used to watch TOTAL database size, so
    whichever pipeline happened to run next got blocked, even if it was using
    almost nothing. Now each pipeline is judged on its OWN footprint, and only
    the volume being genuinely full stops everybody.

HOW TO USE IT (two lines, near the top of your run)

    from etl_alerts import guard
    guard("marketplace")          # 'bc_sync' | 'marketplace' | 'grn'

    It raises DiskGuardStop if this pipeline must not write. Let it propagate:
    the run fails loudly, and the next run picks up where it left off.

WHAT YOU NEED IN THE ENVIRONMENT
    SUPABASE_DB_URL       the same connection string the pipeline already uses
    GMAIL_CLIENT_ID       ) all four are GitHub secrets; without them the guard
    GMAIL_CLIENT_SECRET   ) still works, it just cannot email - it logs instead
    GMAIL_REFRESH_TOKEN   )
    GMAIL_SENDER          birbal@thebakersdozen.in

WHERE THINGS LIVE (all in Postgres, none of it in code)
    etl_disk_policy    budgets per pipeline, which tables belong to whom
    etl_alert_config   who gets emailed, and how often at most
    etl_alert_state    what we last told you, so you are not mailed hourly
    etl_disk_check()   the decision: 'ok' | 'warn' | 'stop'
"""
from __future__ import annotations

import base64
import logging
import os
from email.message import EmailMessage

import requests

logger = logging.getLogger(__name__)

TOKEN_URL = "https://oauth2.googleapis.com/token"
SEND_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"


class DiskGuardStop(RuntimeError):
    """This pipeline must not write right now. Raised by guard()."""


# --------------------------------------------------------------- database --

def _connect(dsn: str | None = None):
    """Connect with whatever driver this repo actually has.

    The repos are not consistent, and one of them cannot use psycopg at all:
    marketplace-ads-pipeline uses pg8000 because psycopg-binary's bundled libpq
    verifies Supabase's private-CA pooler certificate and fails, and the only
    psycopg mode that connected (sslmode=allow) would negotiate plaintext
    first. So try psycopg, then psycopg2, then pg8000 with an SSL context -
    encrypted, unverified, the same posture that repo already uses.

    Credentials: SUPABASE_DB_URL if set, else the PG* variables (which is what
    the marketplace pipeline supplies).
    """
    dsn = dsn or os.environ.get("SUPABASE_DB_URL")
    if dsn:
        try:
            import psycopg                  # psycopg 3
            return psycopg.connect(dsn, autocommit=True)
        except ImportError:
            pass
        try:
            import psycopg2                 # psycopg 2
            conn = psycopg2.connect(dsn)
            conn.autocommit = True
            return conn
        except ImportError:
            pass

    host = os.environ.get("PGHOST")
    if not (dsn or host):
        raise RuntimeError(
            "No database credentials for the disk guard: set SUPABASE_DB_URL, "
            "or PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD.")

    import ssl
    import pg8000.dbapi
    ctx = ssl.create_default_context()
    ctx.check_hostname = False              # Supabase pooler uses a private CA
    ctx.verify_mode = ssl.CERT_NONE
    if dsn and not host:                    # pull the parts out of the DSN
        from urllib.parse import unquote, urlparse
        u = urlparse(dsn)
        host, port = u.hostname, u.port or 5432
        user, password = unquote(u.username or ""), unquote(u.password or "")
        database = (u.path or "/postgres").lstrip("/") or "postgres"
    else:
        port = int(os.environ.get("PGPORT", "5432"))
        user = os.environ.get("PGUSER")
        password = os.environ.get("PGPASSWORD")
        database = os.environ.get("PGDATABASE", "postgres")
    conn = pg8000.dbapi.connect(user=user, password=password, host=host,
                                port=port, database=database, ssl_context=ctx,
                                timeout=30)
    conn.autocommit = True
    return conn


# ------------------------------------------------------------------ email --

def _access_token() -> str | None:
    """Swap the long-lived refresh token for a short-lived access token.

    Gmail app passwords are disabled on this Workspace and a service account
    would need domain-wide delegation, so this uses an ordinary OAuth client
    with the gmail.send scope - permission to send, not to read.
    """
    cid = os.environ.get("GMAIL_CLIENT_ID")
    sec = os.environ.get("GMAIL_CLIENT_SECRET")
    rt = os.environ.get("GMAIL_REFRESH_TOKEN")
    if not (cid and sec and rt):
        return None
    r = requests.post(TOKEN_URL, timeout=30, data={
        "client_id": cid, "client_secret": sec,
        "refresh_token": rt, "grant_type": "refresh_token"})
    if r.status_code != 200:
        logger.error("Gmail token refresh failed (%s): %s", r.status_code, r.text[:200])
        return None
    return r.json().get("access_token")


def send_mail(subject: str, body: str, recipients: list[str]) -> bool:
    """Returns True if Gmail accepted the message. Never raises: a failed
    alert must not take down an otherwise working pipeline."""
    sender = os.environ.get("GMAIL_SENDER", "birbal@thebakersdozen.in")
    if not recipients:
        return False
    try:
        token = _access_token()
        if not token:
            logger.warning("No Gmail credentials in the environment - alert not emailed:\n%s", body)
            return False
        msg = EmailMessage()
        msg["To"] = ", ".join(recipients)
        msg["From"] = sender
        msg["Subject"] = subject
        msg.set_content(body)
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        r = requests.post(SEND_URL, timeout=30,
                          headers={"Authorization": "Bearer " + token},
                          json={"raw": raw})
        if r.status_code >= 300:
            logger.error("Gmail send failed (%s): %s", r.status_code, r.text[:300])
            return False
        return True
    except Exception:
        logger.exception("Could not send the disk alert email")
        return False


# ------------------------------------------------------------------ guard --

def guard(pipeline: str, dsn: str | None = None, *, raise_on_stop: bool = True) -> str:
    """Check this pipeline's disk budget. Returns 'ok' | 'warn' | 'stop'.

    Lets budgets grow into UNALLOCATED volume space first, so a pipeline that
    is legitimately growing is not blocked by a number somebody guessed months
    ago. It can never grow past the volume's stop threshold.

    FAILS OPEN. If the guard itself cannot run - no credentials in this step,
    database unreachable, policy tables not installed - it logs loudly and
    returns 'ok' rather than raising. A guard that breaks a working pipeline is
    worse than one that occasionally cannot check, and if the database is
    unreachable the pipeline's own writes are failing anyway. DiskGuardStop is
    the one exception: that is a real answer and it propagates.
    """
    try:
        return _guard(pipeline, dsn, raise_on_stop=raise_on_stop)
    except DiskGuardStop:
        raise
    except Exception:
        logger.exception(
            "Disk guard could not run for %s - continuing WITHOUT it. Fix this: "
            "the pipeline is now unguarded.", pipeline)
        return "ok"


def _guard(pipeline: str, dsn: str | None = None, *, raise_on_stop: bool = True) -> str:
    conn = _connect(dsn)
    try:
        cur = conn.cursor()

        grown = []
        try:
            cur.execute("SELECT pipeline_name, old_budget_gb, new_budget_gb, reason "
                        "FROM public.etl_disk_autobudget()")
            grown = cur.fetchall()
        except Exception:
            logger.debug("etl_disk_autobudget() unavailable - skipping auto-budget")

        cur.execute("SELECT action, reason FROM public.etl_disk_check(%s)", (pipeline,))
        row = cur.fetchone()
        if not row:
            return "ok"
        action, reason = row[0], row[1]

        cur.execute("SELECT recipients FROM public.etl_alert_config WHERE id")
        cfg = cur.fetchone()
        recipients = list(cfg[0]) if cfg else []

        cur.execute("SELECT public.etl_should_alert(%s, %s)", (pipeline, action))
        should = bool(cur.fetchone()[0])

        sent = False
        if should:
            cur.execute("SELECT table_name, gb FROM public.etl_unbudgeted_tables(0.5)")
            orphans = cur.fetchall()
            body = [reason, ""]
            if grown:
                body.append("Budgets grew automatically into free space:")
                body += ["  - " + g[3] for g in grown] + [""]
            if orphans:
                body.append("Large tables no budget claims (guarded by the volume ceiling only):")
                body += ["  - {}: {} GB".format(t[0], t[1]) for t in orphans]
                body.append("  Add a pattern in etl_disk_policy so these count against someone.")
                body.append("")
            body += [
                "What to do:",
                "  - raise a budget:  UPDATE etl_disk_policy SET budget_gb = <n> WHERE pipeline = '<name>';",
                "  - after a resize:  UPDATE etl_disk_policy SET budget_gb = <new size> WHERE pipeline = '_disk';",
                "  - change who gets this: UPDATE etl_alert_config SET recipients = ARRAY['a@b.com'];",
                "",
                "(pipeline: {}. At most one of these per pipeline per cooldown, but a "
                "change for the worse is sent immediately.)".format(pipeline),
            ]
            sent = send_mail(
                "[{}] Supabase disk - {}".format(action.upper(), pipeline),
                "\n".join(body), recipients)

        cur.execute("SELECT public.etl_record_alert(%s, %s, %s, %s)",
                    (pipeline, action, reason, sent))
    finally:
        conn.close()

    if action == "stop":
        logger.error("disk guard [%s]: %s", pipeline, reason)
        if raise_on_stop:
            raise DiskGuardStop("{}: {}".format(pipeline, reason))
    elif action == "warn":
        logger.warning("disk guard [%s]: %s", pipeline, reason)
    else:
        logger.info("disk guard [%s]: %s", pipeline, reason)
    return action


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print("action:", guard(sys.argv[1] if len(sys.argv) > 1 else "bc_sync", raise_on_stop=False))
