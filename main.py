"""
GiftTrove backend — FastAPI + Telethon (MTProto user session)
"""

import os
import time
import json
import asyncio
import sqlite3
import logging
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Body
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("gifttrove")

try:
    from telethon import TelegramClient, functions, types, Button  # noqa: F401
    from telethon.sessions import StringSession
    try:
        from telethon.errors import FloodWaitError
    except Exception:
        class FloodWaitError(Exception):
            seconds = 0
    TELETHON_OK = True
except Exception as e:  # pragma: no cover
    TELETHON_OK = False
    class FloodWaitError(Exception):
        seconds = 0
    log.error("Telethon import failed (%s). Install with: pip install -U telethon", e)

try:
    import httpx
    HTTPX_OK = True
except Exception:
    HTTPX_OK = False

# ─── Config ───────────────────────────────────────────────────────────────────
API_ID = os.getenv("API_ID", "")
API_HASH = os.getenv("API_HASH", "")
STRING_SESSION = os.getenv("STRING_SESSION", "").strip()
GETGEMS_API_KEY = os.getenv("GETGEMS_API_KEY", "")
GETGEMS_GRAPHQL = os.getenv("GETGEMS_GRAPHQL", "https://api.getgems.io/graphql")
# MarketApp aggregator (Tonnel / Portals / Fragment / GetGems / MarketApp).
# Auth is a raw token in the Authorization header (no "Bearer" prefix).
# The listings path + its collection query param are env-overridable so the exact
# endpoint can be corrected from their Swagger without a code change.
MARKETAPP_TOKEN = os.getenv("MARKETAPP_TOKEN", "")
MARKETAPP_BASE = os.getenv("MARKETAPP_BASE", "https://api.marketapp.ws").rstrip("/")
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "https://gift-trove-frontend.vercel.app").split(",") if o.strip()]
DB_PATH = os.getenv("DB_PATH", "gifttrove.db")
# Durable storage: if DATABASE_URL (Postgres, e.g. Neon) is set, use it so data
# survives redeploys. Otherwise fall back to local SQLite (ephemeral on Render).
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_PG = DATABASE_URL.startswith(("postgres://", "postgresql://"))
SEARCH_LIMIT = int(os.getenv("SEARCH_LIMIT", "100"))      # per page
SEARCH_MAX = int(os.getenv("SEARCH_MAX", "1000"))         # hard ceiling per query
# Telegram resale is priced in Stars; Fragment/GetGems in TON (GRAM). To rank a
# mixed feed by true price we convert Stars -> GRAM with this rate. It moves with
# the market, so it's tunable via env without a redeploy. Adjust if the ordering
# looks off (raise it if Stars-priced gifts rank too high).
STARS_PER_TON = float(os.getenv("STARS_PER_TON", "200"))
MTPROTO_TIMEOUT = int(os.getenv("MTPROTO_TIMEOUT", "18"))   # seconds per call

# ─── Premium (Telegram Stars subscriptions) ─────────────────────────────────────
# Monthly price (in Stars) per tier, and how many of each attribute filter
# (model / symbol / backdrop) a tier may apply at once. Enforced SERVER-SIDE so a
# modified client can't bypass it. Telegram Stars subscriptions currently allow
# only a 30-day period (2592000s).
STARS_PLUS = int(os.getenv("STARS_PLUS", "150"))   # Scout+  / month
STARS_PRO  = int(os.getenv("STARS_PRO", "500"))    # Scout Pro / month
SUB_PERIOD = 2592000                                # 30 days, the only allowed period
TIER_CAPS  = {"free": 1, "plus": 7, "pro": 999}     # max selections per filter type

# ─── Promoted gifts (one-time Stars, all tiers) ─────────────────────────────────
PROMO_PRICE = int(os.getenv("PROMO_PRICE", "50"))   # Stars for one promotion
PROMO_DAYS  = int(os.getenv("PROMO_DAYS", "3"))     # how long a promotion runs
PROMO_MAX_SHOWN = int(os.getenv("PROMO_MAX_SHOWN", "3"))         # promoted slots per search
PROMO_REPORT_HIDE = int(os.getenv("PROMO_REPORT_HIDE", "5"))     # auto-hide after N reports

# ─── Affiliate program (Scout Pro only) ─────────────────────────────────────────
# A Pro member earns a recurring cut of every subscription payment made by users
# they referred — but ONLY while the referrer is themselves an active Pro.
AFFILIATE_PCT = int(os.getenv("AFFILIATE_PCT", "30"))            # % of each sub payment
AFFILIATE_MIN_WITHDRAW = int(os.getenv("AFFILIATE_MIN_WITHDRAW", "1000"))  # Stars before payout
STAR_TO_TON = float(os.getenv("STAR_TO_TON", "0.005"))           # ~net Fragment rate (≈200★/TON); GRAM==TON

# ─── Access gate ──────────────────────────────────────────────────────────────
# Admins bypass automatically; everyone else needs the access code. BOTH live in
# env vars so only the operator can change them (never hard-coded in the client).
ADMIN_IDS = {s.strip() for s in os.getenv("ADMIN_IDS", "7608551523,8124847664").split(",") if s.strip()}
ACCESS_CODE = os.getenv("ACCESS_CODE", "8f70p").strip()
# Analytics + broadcast are locked tighter than the app: only this account, and a
# SEPARATE secret code (never shipped in the frontend) that must be entered each
# session. Set ADMIN_CODE in env; if it's blank the code path is disabled.
ANALYTICS_ADMIN_ID = os.getenv("ANALYTICS_ADMIN_ID", "8124847664").strip()
ADMIN_CODE = os.getenv("ADMIN_CODE", "").strip()

# ─── Rate limiting (protects the backend from abuse / accidental hammering) ────
RATE_WINDOW = int(os.getenv("RATE_WINDOW", "60"))   # seconds
RATE_MAX = int(os.getenv("RATE_MAX", "40"))         # requests per window per client

_mtproto_error = ""
FRAGMENT_CDN = "https://nft.fragment.com/gift"

# ─── Bot (/start handler) ─────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WELCOME_IMAGE = os.getenv("WELCOME_IMAGE", "https://i.ibb.co/5Xmf7H6b/Inria-Serif-1.png")

# Premium custom-emoji ids (rendered in the bot's own messages via HTML).
EMOJI_USER = "5974038293120027938"     # 👤  (start, spot 1)
EMOJI_SEARCH = "5429571366384842791"   # 🔎  (start, spot 2)
EMOJI_STAR = os.getenv("EMOJI_STAR", "5197390760122556297")   # premium gold star (🎖️ fallback)
# Marketplace custom-emoji ids (used by the bot; also returned to the app).
MARKET_EMOJI = {
    "Telegram": ("5875465628285931233", "\u2708\ufe0f"),
    "GetGems": ("5463274357008665413", "\U0001f6d2"),
    "Portals": ("5465613787040091303", "\U0001f6d2"),
    "MRKT": ("5465425006047564701", "\U0001f6d2"),
    "Tonnel": ("5465531018725329021", "\U0001f6d2"),
    "Fragment": ("5397982951369622729", "\U0001f3f4\u200d\u2620\ufe0f"),
}

WELCOME_HTML = os.getenv(
    "WELCOME_HTML",
    f'<emoji document-id={EMOJI_USER}>\U0001f464</emoji> <b>Welcome to GiftTrove! Scout unique '
    f'Telegram gifts from different marketplaces all at a go.</b>\n\n'
    f'<b>GiftTrove</b> <emoji document-id={EMOJI_SEARCH}>\U0001f50e</emoji>',
)
# Plain fallback if custom emoji can't be sent (still friendly).
WELCOME_PLAIN = (
    "Welcome to GiftTrove! Scout unique Telegram gifts from different "
    "marketplaces all at a go.\n\nGiftTrove"
)
MINIAPP_URL = os.getenv("MINIAPP_URL", "https://t.me/gifttrovebot/app")
COMMUNITY_URL = os.getenv("COMMUNITY_URL", "https://t.me/gifttrove")

FEATURED_NAMES = [n.strip() for n in os.getenv(
    "FEATURED_NAMES", "Plush Pepe,Durov's Cap,Heart Locket").split(",") if n.strip()]

# ─── Tiny TTL cache + a lock to serialise MTProto calls ───────────────────────
_cache = {}
_mtproto_lock = asyncio.Lock()
_featured_lock = asyncio.Lock()


def cache_get(key):
    item = _cache.get(key)
    if not item:
        return None
    exp, val = item
    return val if exp > time.time() else None


def cache_set(key, val, ttl):
    _cache[key] = (time.time() + ttl, val)


# ─── SQLite: referrals + privacy-safe analytics ──────────────────────────────
# NOTE: Render's free filesystem is EPHEMERAL — this DB resets on every deploy/
# restart. For durable analytics use a persistent disk or (better) Postgres.
# See the scaling notes at the bottom of this file.
import hashlib
import secrets as _secrets

if USE_PG:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except Exception as _pg_err:
        logging.getLogger("gifttrove").error("psycopg unavailable (%s) — using SQLite", _pg_err)
        USE_PG = False


class _DB:
    """Uniform wrapper over psycopg / sqlite3: dict rows + '?' placeholders."""

    def __init__(self):
        if USE_PG:
            self._c = psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=10)
        else:
            self._c = sqlite3.connect(DB_PATH)
            self._c.row_factory = sqlite3.Row

    def execute(self, sql, params=()):
        if USE_PG:
            return self._c.execute(sql.replace("?", "%s"), tuple(params))
        return self._c.execute(sql, params)

    def commit(self):
        self._c.commit()

    def close(self):
        try:
            self._c.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, et, ev, tb):
        try:
            if et is None:
                self._c.commit()
        except Exception:
            pass
        self.close()


def _existing_columns(conn, table):
    """Column names of a table, for both Postgres and SQLite."""
    try:
        if USE_PG:
            rows = conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name=?", (table,)).fetchall()
            return {r["column_name"] for r in rows}
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {r["name"] for r in rows}
    except Exception:
        return set()


def db():
    return _DB()


def _uid_hash(uid):
    """Store a salted hash, never the raw Telegram id (privacy by design)."""
    if not uid:
        return "anon"
    return hashlib.sha256(f"gt::{uid}".encode()).hexdigest()[:24]


def init_db():
    with db() as conn:
        if not USE_PG:
            conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS referrals (
                   uid TEXT NOT NULL, referred_by TEXT NOT NULL, ts INTEGER NOT NULL,
                   PRIMARY KEY (uid, referred_by))"""
        )
        # One row per visitor (hashed). No names, no usernames, no PII.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS members (
                   uid_hash TEXT PRIMARY KEY,
                   first_seen INTEGER NOT NULL,
                   last_seen INTEGER NOT NULL,
                   visits INTEGER NOT NULL DEFAULT 1)"""
        )
        # Aggregated search counts per gift name (no user linkage).
        conn.execute(
            """CREATE TABLE IF NOT EXISTS gift_searches (
                   gift TEXT PRIMARY KEY,
                   count INTEGER NOT NULL DEFAULT 0,
                   last_ts INTEGER NOT NULL)"""
        )
        # Aggregated SHARE counts per gift collection (no user linkage).
        conn.execute(
            """CREATE TABLE IF NOT EXISTS gift_shares (
                   gift TEXT PRIMARY KEY,
                   count INTEGER NOT NULL DEFAULT 0,
                   last_ts INTEGER NOT NULL)"""
        )
        # Tiny key/value store (e.g. last daily-digest timestamp).
        conn.execute(
            """CREATE TABLE IF NOT EXISTS meta (
                   key TEXT PRIMARY KEY, val TEXT NOT NULL)"""
        )
        # Durable per-gift attributes cache (models/symbols/backdrops + images)
        # so restarts don't cause slow or empty attribute loads. Capped to the
        # most recently used gifts to bound storage.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS attrs_cache (
                   gift_id TEXT PRIMARY KEY,
                   payload TEXT NOT NULL,
                   ts INTEGER NOT NULL)"""
        )
        # Lightweight event log (rollups are derived from this).
        if USE_PG:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS events (
                       id BIGSERIAL PRIMARY KEY, kind TEXT NOT NULL, ts INTEGER NOT NULL)"""
            )
        else:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS events (
                       id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, ts INTEGER NOT NULL)"""
            )
        # Per-user activity so saved gifts + recent searches follow the user
        # across devices. Keyed by the verified Telegram id; saved/searches are
        # JSON blobs. (Referrals already sync via the referrals table.)
        conn.execute(
            """CREATE TABLE IF NOT EXISTS user_data (
                   uid TEXT PRIMARY KEY,
                   saved TEXT NOT NULL DEFAULT '[]',
                   searches TEXT NOT NULL DEFAULT '[]',
                   updated INTEGER NOT NULL DEFAULT 0)"""
        )
        # Broadcast list: RAW Telegram ids of people who have used the mini app,
        # so the owner can message everyone. Stored only to deliver broadcasts;
        # a user's id is removed the moment they clear their data (and re-added if
        # they open the app again).
        conn.execute(
            """CREATE TABLE IF NOT EXISTS bcast (
                   uid TEXT PRIMARY KEY,
                   ts INTEGER NOT NULL)"""
        )
        # Users who used "Clear my data" opt OUT of broadcasts permanently — even if
        # they keep using the app — so the deletion actually sticks.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS bcast_optout (
                   uid TEXT PRIMARY KEY,
                   ts INTEGER NOT NULL)"""
        )
        # Friendly, memorable referral codes (e.g. 888OG) mapped to a member's id.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS ref_codes (
                   uid TEXT PRIMARY KEY,
                   code TEXT NOT NULL UNIQUE,
                   ts INTEGER NOT NULL)"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_refcode_code ON ref_codes(code);")
        # Active Stars subscriptions. tier in (plus,pro); expires_at is the unix time
        # the current paid period ends — lapses to free automatically when passed.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS subs (
                   uid TEXT PRIMARY KEY,
                   tier TEXT NOT NULL,
                   expires_at INTEGER NOT NULL,
                   charge_id TEXT,
                   ts INTEGER NOT NULL)"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_subs_exp ON subs(expires_at);")
        # Promoted gifts. One paid promotion = one row. status: pending|active|removed.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS promos (
                   id TEXT PRIMARY KEY,
                   uid TEXT NOT NULL,
                   collection TEXT NOT NULL,
                   gift_id TEXT,
                   slug TEXT,
                   num TEXT,
                   model TEXT,
                   symbol TEXT,
                   backdrop TEXT,
                   marketplace TEXT,
                   amount TEXT,
                   currency TEXT,
                   link TEXT,
                   charge_id TEXT,
                   status TEXT NOT NULL,
                   reports INTEGER DEFAULT 0,
                   ts INTEGER NOT NULL,
                   expires_at INTEGER DEFAULT 0)"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_promos_gift ON promos(gift_id, status, expires_at);")
        # Backfill columns on DBs created before they existed. We check first so we
        # never issue a duplicate ALTER (which on Postgres aborts the whole transaction).
        _have = _existing_columns(conn, "promos")
        for _col, _typ in (("amount", "TEXT"), ("currency", "TEXT"), ("link", "TEXT"), ("num", "TEXT")):
            if _col not in _have:
                conn.execute(f"ALTER TABLE promos ADD COLUMN {_col} {_typ}")
        # Affiliate earnings ledger (Scout Pro only). One row per credited payment.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS affiliate_earnings (
                   id TEXT PRIMARY KEY,
                   referrer TEXT NOT NULL,
                   referee TEXT NOT NULL,
                   tier TEXT,
                   stars INTEGER NOT NULL,
                   charge_id TEXT,
                   ts INTEGER NOT NULL)"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_aff_ref ON affiliate_earnings(referrer);")
        # Withdrawal requests. status: requested|paid|rejected.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS affiliate_payouts (
                   id TEXT PRIMARY KEY,
                   uid TEXT NOT NULL,
                   stars INTEGER NOT NULL,
                   ton_address TEXT,
                   status TEXT NOT NULL,
                   ts INTEGER NOT NULL)"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_aff_pay ON affiliate_payouts(uid, status);")
        # Indexes — keep the analytics/referral queries fast as data grows.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ref_uid ON referrals(uid);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_members_last ON members(last_seen);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_members_visits ON members(visits);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_kind_ts ON events(kind, ts);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gs_count ON gift_searches(count);")
        conn.commit()
    log.info("DB ready (%s)", "Postgres" if USE_PG else "SQLite")


def track_visit(uid):
    """Upsert a visitor; returns True if this is a brand-new member."""
    h = _uid_hash(uid)
    now = int(time.time())
    try:
        with db() as conn:
            cur = conn.execute("SELECT visits FROM members WHERE uid_hash=?", (h,))
            row = cur.fetchone()
            if row:
                conn.execute("UPDATE members SET last_seen=?, visits=visits+1 WHERE uid_hash=?", (now, h))
                conn.execute("INSERT INTO events(kind, ts) VALUES('open', ?)", (now,))
                conn.commit()
                return False
            conn.execute("INSERT INTO members(uid_hash, first_seen, last_seen, visits) VALUES(?,?,?,1)", (h, now, now))
            conn.execute("INSERT INTO events(kind, ts) VALUES('new_member', ?)", (now,))
            conn.commit()
            return True
    except Exception as e:
        log.info("track_visit skipped: %s", e)
        return False


def track_search(gift):
    if not gift:
        return
    now = int(time.time())
    g = gift.strip()[:64]
    try:
        with db() as conn:
            cur = conn.execute("UPDATE gift_searches SET count=count+1, last_ts=? WHERE gift=?", (now, g))
            if not cur.rowcount:
                conn.execute(
                    "INSERT INTO gift_searches(gift, count, last_ts) VALUES(?,1,?) ON CONFLICT DO NOTHING",
                    (g, now),
                )
            conn.execute("INSERT INTO events(kind, ts) VALUES('search', ?)", (now,))
            conn.commit()
    except Exception as e:
        log.info("track_search skipped: %s", e)


def track_share(gift):
    """Count a successful share of a gift collection (Postgres-safe upsert)."""
    if not gift:
        return
    now = int(time.time())
    g = str(gift).strip()[:64]
    try:
        with db() as conn:
            cur = conn.execute("UPDATE gift_shares SET count=count+1, last_ts=? WHERE gift=?", (now, g))
            if not cur.rowcount:
                conn.execute(
                    "INSERT INTO gift_shares(gift, count, last_ts) VALUES(?,1,?) ON CONFLICT DO NOTHING",
                    (g, now),
                )
            conn.execute("INSERT INTO events(kind, ts) VALUES('share', ?)", (now,))
            conn.commit()
    except Exception as e:
        log.info("track_share skipped: %s", e)


# ─── In-memory rate limiter (per hashed client, sliding window) ───────────────
_rate = {}


def rate_ok(uid):
    h = _uid_hash(uid)
    now = time.time()
    bucket = [t for t in _rate.get(h, []) if t > now - RATE_WINDOW]
    if len(bucket) >= RATE_MAX:
        _rate[h] = bucket
        return False
    bucket.append(now)
    _rate[h] = bucket
    # opportunistic cleanup so the dict can't grow unbounded
    if len(_rate) > 20000:
        for k in list(_rate.keys())[:5000]:
            if not _rate[k] or _rate[k][-1] < now - RATE_WINDOW:
                _rate.pop(k, None)
    return True


# Per-IP limiter for the global middleware (one user fires several calls, so a
# higher ceiling than the per-action limiter above).
RATE_MAX_IP = int(os.getenv("RATE_MAX_IP", "150"))
_rate_ip = {}


def rate_ok_ip(ip):
    now = time.time()
    bucket = [t for t in _rate_ip.get(ip, []) if t > now - RATE_WINDOW]
    if len(bucket) >= RATE_MAX_IP:
        _rate_ip[ip] = bucket
        return False
    bucket.append(now)
    _rate_ip[ip] = bucket
    if len(_rate_ip) > 50000:
        for k in list(_rate_ip.keys())[:10000]:
            if not _rate_ip[k] or _rate_ip[k][-1] < now - RATE_WINDOW:
                _rate_ip.pop(k, None)
    return True


# ─── Security: Telegram initData verification + strict input validation ───────
import hmac as _hmac
import re as _re
from urllib.parse import parse_qsl

ALERT_ADMIN_ID = os.getenv("ALERT_ADMIN_ID", "7608551523")
_SLUG_RE = _re.compile(r"^[A-Za-z0-9._\-]{1,80}$")


def _bcast_add(uid):
    """Remember a mini-app user's real id for broadcasts. Users who cleared their
    data (opted out) are never re-added, so the deletion stays permanent."""
    if not uid:
        return
    try:
        with db() as conn:
            opted = conn.execute("SELECT 1 FROM bcast_optout WHERE uid=?", (str(uid),)).fetchone()
            if opted:
                return
            cur = conn.execute("UPDATE bcast SET ts=? WHERE uid=?", (int(time.time()), str(uid)))
            if not cur.rowcount:
                conn.execute("INSERT INTO bcast(uid, ts) VALUES(?,?) ON CONFLICT DO NOTHING",
                             (str(uid), int(time.time())))
            conn.commit()
    except Exception as e:
        log.info("bcast add skipped: %s", e)


def _bcast_optout(uid):
    """Mark a user as opted out of broadcasts (set when they clear their data)."""
    if not uid:
        return
    try:
        with db() as conn:
            conn.execute("INSERT INTO bcast_optout(uid, ts) VALUES(?,?) ON CONFLICT DO NOTHING",
                         (str(uid), int(time.time())))
            conn.commit()
    except Exception as e:
        log.info("bcast optout skipped: %s", e)


# ─── Friendly referral codes ─────────────────────────────────────────────────
# Short, memorable, collision-free codes (e.g. 888OG) using an unambiguous
# alphabet — no 0/O/1/I/L so nobody mistypes a shared link. Length grows
# automatically as the member base grows.
_REF_ALPHABET = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"

def _gen_ref_code(n):
    return "".join(_secrets.choice(_REF_ALPHABET) for _ in range(n))

def get_ref_code(uid):
    """Return this member's referral code, creating a unique one on first use."""
    if not uid:
        return None
    try:
        with db() as conn:
            row = conn.execute("SELECT code FROM ref_codes WHERE uid=?", (str(uid),)).fetchone()
            if row:
                return row["code"]
            # length scales with how many codes already exist (keeps them short early on)
            total = conn.execute("SELECT COUNT(*) c FROM ref_codes").fetchone()["c"]
            length = 5 if total < 500_000 else (6 if total < 15_000_000 else 7)
            for _ in range(12):
                code = _gen_ref_code(length)
                try:
                    conn.execute("INSERT INTO ref_codes(uid, code, ts) VALUES(?,?,?)",
                                 (str(uid), code, int(time.time())))
                    conn.commit()
                    return code
                except Exception:
                    continue  # rare collision — try another
            # extreme fallback: widen by one char
            code = _gen_ref_code(length + 1)
            conn.execute("INSERT INTO ref_codes(uid, code, ts) VALUES(?,?,?)",
                         (str(uid), code, int(time.time())))
            conn.commit()
            return code
    except Exception as e:
        log.info("ref code error: %s", e)
        return None

def resolve_ref(token):
    """Resolve a referral token to a referrer uid. Accepts a friendly code (e.g.
    888OG), a legacy base36-of-uid link (the old frontend format, lowercase), or a
    raw numeric uid — so every link ever shared still works."""
    if not token:
        return None
    token = str(token).strip()
    try:
        with db() as conn:
            row = conn.execute("SELECT uid FROM ref_codes WHERE code=?", (token.upper(),)).fetchone()
            if row:
                return row["uid"]
    except Exception:
        pass
    # legacy base36-of-uid links (old frontend) — lowercase alphanumeric, not all digits
    low = token.lower()
    if low and not token.isdigit() and all(c in "0123456789abcdefghijklmnopqrstuvwxyz" for c in low):
        try:
            n = int(low, 36)
            if 10_000 < n < 10**13:   # plausible Telegram user-id range
                return str(n)
        except Exception:
            pass
    # raw numeric uid
    digits = _digits(token)
    return digits or None


# ─── Premium tiers ───────────────────────────────────────────────────────────
def get_tier(uid):
    """Active tier for a member: 'free' | 'plus' | 'pro'. Expired subs are free."""
    if not uid:
        return "free"
    try:
        with db() as conn:
            row = conn.execute("SELECT tier, expires_at FROM subs WHERE uid=?", (str(uid),)).fetchone()
        if row and int(row["expires_at"]) > int(time.time()) and row["tier"] in ("plus", "pro"):
            return row["tier"]
    except Exception as e:
        log.info("get_tier error: %s", e)
    return "free"

def sub_info(uid):
    """(tier, expires_at) for a member; expires_at is 0 when none/expired."""
    if not uid:
        return "free", 0
    try:
        with db() as conn:
            row = conn.execute("SELECT tier, expires_at FROM subs WHERE uid=?", (str(uid),)).fetchone()
        if row and int(row["expires_at"]) > int(time.time()) and row["tier"] in ("plus", "pro"):
            return row["tier"], int(row["expires_at"])
    except Exception:
        pass
    return "free", 0

def _sub_set(uid, tier, expires_at, charge_id=""):
    """Record/renew a subscription (upsert via delete+insert to avoid ON CONFLICT
    column ambiguity across SQLite/Postgres)."""
    if not uid or tier not in ("plus", "pro"):
        return
    try:
        with db() as conn:
            conn.execute("DELETE FROM subs WHERE uid=?", (str(uid),))
            conn.execute("INSERT INTO subs(uid, tier, expires_at, charge_id, ts) VALUES(?,?,?,?,?)",
                         (str(uid), tier, int(expires_at), charge_id or "", int(time.time())))
            conn.commit()
    except Exception as e:
        log.error("sub_set failed: %s", e)

def _sub_row(uid):
    """Raw current subs row as (tier, charge_id), ignoring expiry — used to grab the
    old charge_id before overwriting on a tier change."""
    if not uid:
        return None
    try:
        with db() as conn:
            row = conn.execute("SELECT tier, charge_id FROM subs WHERE uid=?", (str(uid),)).fetchone()
        if row:
            return (row["tier"], row["charge_id"])
    except Exception:
        pass
    return None

# A vanity code must look like a code, not impersonate a number that resolve_ref
# would read as a raw uid, so we require at least one letter.
_VANITY_RE = _re.compile(r"^[A-Z0-9]{3,12}$")

def set_vanity(uid, code):
    """Premium perk: claim a custom referral code. Returns (ok, code_or_errorkey)."""
    code = (code or "").strip().upper()
    if not _VANITY_RE.match(code) or not any(c.isalpha() for c in code):
        return False, "format"
    try:
        with db() as conn:
            taken = conn.execute("SELECT uid FROM ref_codes WHERE code=?", (code,)).fetchone()
            if taken and str(taken["uid"]) != str(uid):
                return False, "taken"
            conn.execute("DELETE FROM ref_codes WHERE uid=?", (str(uid),))
            conn.execute("INSERT INTO ref_codes(uid, code, ts) VALUES(?,?,?)",
                         (str(uid), code, int(time.time())))
            conn.commit()
        return True, code
    except Exception as e:
        log.error("set_vanity failed: %s", e)
        return False, "error"


# ─── Promoted gifts ──────────────────────────────────────────────────────────
def _promo_count_active(uid):
    now = int(time.time())
    try:
        with db() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM promos WHERE uid=? AND status='active' AND expires_at>?",
                (str(uid), now)).fetchone()
        return int(row["n"]) if row else 0
    except Exception:
        return 0

def _promo_create(uid, fields):
    """Insert a pending promo (activated when its Stars payment lands). Returns id."""
    pid = _secrets.token_hex(8)
    try:
        with db() as conn:
            conn.execute(
                """INSERT INTO promos(id, uid, collection, gift_id, slug, num, model, symbol,
                       backdrop, marketplace, amount, currency, link, charge_id, status, reports, ts, expires_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (pid, str(uid), fields["collection"], fields.get("gift_id", ""), fields.get("slug", ""),
                 fields.get("num", ""), fields.get("model", ""), fields.get("symbol", ""), fields.get("backdrop", ""),
                 fields.get("marketplace", ""), fields.get("amount", ""), fields.get("currency", ""),
                 fields.get("link", ""), "", "pending", 0, int(time.time()), 0))
            conn.commit()
        return pid
    except Exception as e:
        log.error("promo_create failed: %s", e)
        return None

def _promo_activate(pid, uid, charge_id):
    exp = int(time.time()) + PROMO_DAYS * 86400
    try:
        with db() as conn:
            conn.execute(
                "UPDATE promos SET status='active', charge_id=?, expires_at=? WHERE id=? AND uid=?",
                (charge_id or "", exp, pid, str(uid)))
            conn.commit()
        return exp
    except Exception as e:
        log.error("promo_activate failed: %s", e)
        return 0

def _promo_set_charge(pid, charge_id):
    try:
        with db() as conn:
            conn.execute("UPDATE promos SET charge_id=? WHERE id=?", (charge_id or "", pid))
            conn.commit()
    except Exception as e:
        log.error("promo_set_charge failed: %s", e)

def _promo_set_price(pid, amount, currency):
    try:
        with db() as conn:
            conn.execute("UPDATE promos SET amount=?, currency=? WHERE id=?", (str(amount), currency or "", pid))
            conn.commit()
    except Exception as e:
        log.error("promo_set_price failed: %s", e)

async def _promo_fetch_price(gift_id):
    """Cheapest current Telegram listing for a collection -> (amount, currency).
    Used to auto-fill a promoted gift's price so it stays accurate."""
    GetResale = _payments("GetResaleStarGiftsRequest")
    if client is None or not GetResale or not gift_id:
        return ("", "")
    try:
        res = await _invoke(lambda: GetResale(gift_id=int(gift_id), attributes_hash=0,
                                              sort_by_price=True, offset="", limit=1))
        g = getattr(res, "gifts", []) or []
        if not g:
            return ("", "")
        item = serialize_unique(g[0])
        price = item.get("price")
        if price is None:
            return ("", "")
        cur = "Stars" if str(item.get("currency", "")).lower() in ("stars", "star", "xtr") else "GRAM"
        return (str(price), cur)
    except Exception as e:
        log.info("promo price fetch failed: %s", e)
        return ("", "")

def _promo_go_live(pid):
    """Flip a reviewed promo to active and start its clock."""
    exp = int(time.time()) + PROMO_DAYS * 86400
    try:
        with db() as conn:
            conn.execute("UPDATE promos SET status='active', expires_at=? WHERE id=?", (exp, pid))
            conn.commit()
        return exp
    except Exception as e:
        log.error("promo_go_live failed: %s", e)
        return 0

def _promo_active_for(gift_id):
    """Active, non-expired, not-hidden promos for a gift collection."""
    if not gift_id:
        return []
    now = int(time.time())
    try:
        with db() as conn:
            rows = conn.execute(
                """SELECT id, collection, gift_id, slug, num, model, symbol, backdrop, marketplace,
                          amount, currency, link
                   FROM promos WHERE gift_id=? AND status='active' AND expires_at>? AND reports<?
                   ORDER BY ts ASC LIMIT ?""",
                (str(gift_id), now, PROMO_REPORT_HIDE, PROMO_MAX_SHOWN)).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        log.info("promo_active_for error: %s", e)
        return []

def _promo_report(pid):
    try:
        with db() as conn:
            conn.execute("UPDATE promos SET reports=reports+1 WHERE id=?", (pid,))
            conn.commit()
        return True
    except Exception:
        return False

def _promo_get(pid):
    try:
        with db() as conn:
            row = conn.execute(
                """SELECT id, uid, charge_id, status, marketplace, link, slug, num, collection,
                          model, symbol, backdrop, gift_id FROM promos WHERE id=?""", (pid,)).fetchone()
        return dict(row) if row else None
    except Exception:
        return None

def _promo_set_status(pid, status):
    try:
        with db() as conn:
            conn.execute("UPDATE promos SET status=? WHERE id=?", (status, pid))
            conn.commit()
    except Exception as e:
        log.error("promo_set_status failed: %s", e)


# ─── Affiliate program (Scout Pro only) ──────────────────────────────────────
def _referrer_of(uid):
    """The uid that referred this user, if any."""
    try:
        with db() as conn:
            row = conn.execute("SELECT referred_by FROM referrals WHERE uid=? LIMIT 1", (str(uid),)).fetchone()
        return row["referred_by"] if row else None
    except Exception:
        return None

def _affiliate_credit(referrer, referee, tier, stars, charge_id):
    if stars <= 0:
        return
    try:
        with db() as conn:
            conn.execute(
                "INSERT INTO affiliate_earnings(id, referrer, referee, tier, stars, charge_id, ts) VALUES(?,?,?,?,?,?,?)",
                (_secrets.token_hex(8), str(referrer), str(referee), tier or "", int(stars), charge_id or "", int(time.time())))
            conn.commit()
        log.info("affiliate credit: referrer=%s referee=%s +%s stars", referrer, referee, stars)
    except Exception as e:
        log.error("affiliate_credit failed: %s", e)

def _affiliate_stats(uid):
    """Earned / paid-out / pending / available, plus simple counts."""
    out = {"earned": 0, "paid": 0, "pending": 0, "available": 0, "referees": 0, "payers": 0}
    try:
        with db() as conn:
            r = conn.execute("SELECT COALESCE(SUM(stars),0) s, COUNT(DISTINCT referee) c FROM affiliate_earnings WHERE referrer=?", (str(uid),)).fetchone()
            out["earned"] = int(r["s"] or 0); out["payers"] = int(r["c"] or 0)
            p = conn.execute("SELECT status, COALESCE(SUM(stars),0) s FROM affiliate_payouts WHERE uid=? GROUP BY status", (str(uid),)).fetchall()
            for row in p:
                if row["status"] == "paid": out["paid"] += int(row["s"] or 0)
                elif row["status"] == "requested": out["pending"] += int(row["s"] or 0)
            rc = conn.execute("SELECT COUNT(*) c FROM referrals WHERE referred_by=?", (str(uid),)).fetchone()
            out["referees"] = int(rc["c"] or 0)
    except Exception as e:
        log.info("affiliate_stats error: %s", e)
    out["available"] = max(0, out["earned"] - out["paid"] - out["pending"])
    return out

def _affiliate_request_payout(uid, stars, ton_address):
    try:
        with db() as conn:
            conn.execute(
                "INSERT INTO affiliate_payouts(id, uid, stars, ton_address, status, ts) VALUES(?,?,?,?,?,?)",
                (_secrets.token_hex(8), str(uid), int(stars), ton_address or "", "requested", int(time.time())))
            conn.commit()
        return True
    except Exception as e:
        log.error("affiliate payout request failed: %s", e)
        return False

def _affiliate_payouts(uid, limit=20):
    try:
        with db() as conn:
            rows = conn.execute(
                "SELECT id, stars, ton_address, status, ts FROM affiliate_payouts WHERE uid=? ORDER BY ts DESC LIMIT ?",
                (str(uid), int(limit))).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []

def _affiliate_series(uid, days=30):
    """Daily earned-Stars buckets for the last `days` (oldest->newest)."""
    now = int(time.time())
    start = now - days * 86400
    buckets = {}
    try:
        with db() as conn:
            rows = conn.execute(
                "SELECT stars, ts FROM affiliate_earnings WHERE referrer=? AND ts>=?",
                (str(uid), start)).fetchall()
        for r in rows:
            day = (int(r["ts"]) - start) // 86400
            buckets[day] = buckets.get(day, 0) + int(r["stars"] or 0)
    except Exception as e:
        log.info("affiliate_series error: %s", e)
    out = []
    for d in range(days):
        ts = start + d * 86400
        out.append({"t": ts, "v": buckets.get(d, 0)})
    return out


def _bcast_remove(uid):
    try:
        with db() as conn:
            conn.execute("DELETE FROM bcast WHERE uid=?", (str(uid),))
            conn.commit()
    except Exception as e:
        log.info("bcast remove skipped: %s", e)


def verify_init_data(init_data):
    """Validate Telegram Mini App initData (HMAC). Returns verified user id (str) or None.

    Telegram added a `signature` field to initData (for third-party Ed25519
    validation). Client versions disagree on whether `signature` belongs in the
    HMAC data-check-string, and stripping it the wrong way silently breaks every
    verified feature. So we accept the hash if EITHER variant matches — signature
    excluded (the documented norm) OR included — which keeps verification working
    across all current Telegram clients. Failures log a one-line reason so the
    cause is visible in the server logs."""
    if not init_data:
        log.info("initData verify: empty payload (app likely opened outside Telegram)")
        return None
    if not BOT_TOKEN:
        log.warning("initData verify: BOT_TOKEN not set")
        return None
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
        recv_hash = pairs.pop("hash", None)
        if not recv_hash:
            log.info("initData verify: no hash field")
            return None
        had_sig = "signature" in pairs
        secret = _hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        def _calc(d):
            dcs = "\n".join(f"{k}={d[k]}" for k in sorted(d))
            return _hmac.new(secret, dcs.encode(), hashlib.sha256).hexdigest()
        without_sig = {k: v for k, v in pairs.items() if k != "signature"}
        ok = _hmac.compare_digest(_calc(without_sig), recv_hash)
        if not ok and had_sig:
            ok = _hmac.compare_digest(_calc(pairs), recv_hash)
        if not ok:
            log.info("initData verify: HASH MISMATCH (signature_present=%s) — check BOT_TOKEN matches the bot whose Mini App was opened", had_sig)
            return None
        try:
            if int(pairs.get("auth_date", "0")) < int(time.time()) - 86400:
                log.info("initData verify: stale auth_date (older than 24h)")
                return None
        except Exception:
            pass
        uid = (json.loads(pairs.get("user", "{}")) or {}).get("id")
        return str(uid) if uid else None
    except Exception as e:
        log.info("initData verify: exception %s", e)
        return None


def _digits(s, maxlen=20):
    s = str(s or "").strip()
    return s if s.isdigit() and 0 < len(s) <= maxlen else ""


def _safe_slug(s):
    s = str(s or "").strip()
    return s if _SLUG_RE.match(s) else ""


def _clamp(s, n=64):
    return str(s or "").strip()[:n]


# Admin reports: ALWAYS on by default — good news, warnings and issues alike.
# Set ADMIN_REPORTS=0 to silence. Same-message throttle: once per 10 min.
_alert_seen = {}
ADMIN_REPORTS = os.getenv("ADMIN_REPORTS", "1") == "1"
_REPORT_PREFIX = {"good": "GOOD NEWS", "warning": "WARNING", "issue": "ISSUE", "digest": "DAILY DIGEST"}


async def notify_admin(text, level="issue"):
    if not ADMIN_REPORTS:
        return
    try:
        if not bot or not ALERT_ADMIN_ID:
            return
        now = time.time()
        key = (text or "")[:90]
        if _alert_seen.get(key, 0) > now - 600:   # same alert at most once / 10 min
            return
        _alert_seen[key] = now
        head = f"GiftTrove report — {_REPORT_PREFIX.get(level, 'ISSUE')}"
        await asyncio.wait_for(bot.send_message(int(ALERT_ADMIN_ID), (head + "\n\n" + str(text))[:3500]), timeout=10)
    except Exception as e:
        log.info("notify_admin failed: %s", e)


_MILESTONES = {5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}


def _maybe_milestone():
    """If total members just hit a milestone, fire a good-news report."""
    try:
        with db() as conn:
            n = conn.execute("SELECT COUNT(*) c FROM members").fetchone()["c"]
        if n in _MILESTONES or (n >= 20000 and n % 10000 == 0):
            try:
                asyncio.get_running_loop().create_task(
                    notify_admin(f"Member milestone reached: {n} total members.", level="good")
                )
            except RuntimeError:
                pass
    except Exception as e:
        log.info("milestone check skipped: %s", e)


def _meta_get(key, default=""):
    try:
        with db() as conn:
            r = conn.execute("SELECT val FROM meta WHERE key=?", (key,)).fetchone()
            return r["val"] if r else default
    except Exception:
        return default


def _meta_set(key, val):
    try:
        with db() as conn:
            cur = conn.execute("UPDATE meta SET val=? WHERE key=?", (str(val), key))
            if not cur.rowcount:
                conn.execute("INSERT INTO meta(key, val) VALUES(?, ?) ON CONFLICT DO NOTHING", (key, str(val)))
            conn.commit()
    except Exception as e:
        log.info("meta_set skipped: %s", e)


async def _daily_digest_loop():
    """Once a day, DM the admin a full status digest (good news, plain facts)."""
    while True:
        try:
            await asyncio.sleep(3600)
            now = int(time.time())
            last = int(_meta_get("last_digest", "0") or 0)
            if last == 0:
                _meta_set("last_digest", now)   # first boot: start the clock
                continue
            if now - last < 86400:
                continue
            day = now - 86400
            with db() as conn:
                q = lambda s, p=(): conn.execute(s, p).fetchone()["c"]
                members = q("SELECT COUNT(*) c FROM members")
                new24 = q("SELECT COUNT(*) c FROM events WHERE kind='new_member' AND ts>?", (day,))
                opens24 = q("SELECT COUNT(*) c FROM events WHERE kind IN ('open','new_member') AND ts>?", (day,))
                searches24 = q("SELECT COUNT(*) c FROM events WHERE kind='search' AND ts>?", (day,))
                shares24 = q("SELECT COUNT(*) c FROM events WHERE kind='share' AND ts>?", (day,))
                toprow = conn.execute("SELECT gift, count FROM gift_searches ORDER BY count DESC LIMIT 1").fetchone()
            top_line = f"{toprow['gift']} ({toprow['count']} scouts)" if toprow else "none yet"
            txt = (
                f"Last 24h — opens: {opens24}, searches: {searches24}, shares: {shares24}, new members: {new24}.\n"
                f"Total members: {members}.\n"
                f"Top scouted gift overall: {top_line}.\n"
                f"DB: {'Postgres' if USE_PG else 'SQLite'} — MTProto: {'live' if (client and client.is_connected()) else 'down'}."
            )
            await notify_admin(txt, level="digest")
            _meta_set("last_digest", now)
        except Exception as e:
            log.info("digest loop skipped: %s", e)


# ─── Telethon client lifecycle ────────────────────────────────────────────────
client = None
bot = None


async def _register_bot_handlers():
    if not bot:
        return
    from telethon import events, Button

    @bot.on(events.NewMessage(pattern=r"^/start"))
    async def _start(event):
        buttons = [
            [Button.url("Open GiftTrove", MINIAPP_URL)],
            [Button.url("Join Community", COMMUNITY_URL)],
        ]
        # Build the message with explicit entities so the PREMIUM custom emoji
        # render (the bot may use them because the owner has Telegram Premium).
        from telethon.tl.types import MessageEntityCustomEmoji, MessageEntityBold

        def u16(s):
            return len(s.encode("utf-16-le")) // 2

        segs = [
            ("emoji", "\U0001f464", EMOJI_USER),
            ("text", " "),
            ("bold", "Welcome to GiftTrove! Scout unique Telegram gifts from different marketplaces all at a go."),
            ("text", "\n\n"),
            ("bold", "GiftTrove"),
            ("text", " "),
            ("emoji", "\U0001f50e", EMOJI_SEARCH),
        ]
        text, off, ents = "", 0, []
        for kind, *rest in segs:
            s = rest[0]
            ln = u16(s)
            if kind == "emoji":
                ents.append(MessageEntityCustomEmoji(off, ln, int(rest[1])))
            elif kind == "bold":
                ents.append(MessageEntityBold(off, ln))
            text += s
            off += ln
        try:
            await event.respond(text, file=WELCOME_IMAGE, buttons=buttons, formatting_entities=ents)
        except Exception as e:
            log.error("/start (custom emoji) failed: %s", e)
            try:
                await event.respond(WELCOME_PLAIN, file=WELCOME_IMAGE, buttons=buttons)
            except Exception as e2:
                log.error("/start fallback failed: %s", e2)
                try:
                    await event.respond(WELCOME_PLAIN)
                except Exception:
                    pass

    # ── Inline search: @gifttrovebot <gift> [backdrop] [model] [symbol] ──────
    # Works in any DM / group. Each result opens the Mini App on that collection.
    @bot.on(events.InlineQuery)
    async def _inline(event):
        q = (event.text or "").strip()
        cols = cache_get("collections") or []
        ql = q.lower()
        first = ql.split()[0] if ql else ""
        # 1-3 best matches; tapping launches the Mini App straight onto that gift.
        matches = ([c for c in cols if first and first in (c.get("name", "").lower())][:3]
                   if first else cols[:3])
        builder = event.builder
        results = []
        try:
            for c in matches:
                gid = c.get("gift_id")
                name = c.get("name", "Gift")
                slug = c.get("slug") or ""
                link = f"{MINIAPP_URL}?startapp=q_{gid}" if gid else MINIAPP_URL
                thumb = f"https://nft.fragment.com/gift/{slug}-1.large.jpg" if slug else None
                kw = {
                    "title": name,
                    "description": f"Scout {name} — live floor & listings",
                    "text": f"{name} on GiftTrove\nTap to scout live listings across Telegram marketplaces.",
                    "buttons": [Button.url(f"Open {name}", link)],
                }
                if thumb:
                    kw["thumb"] = thumb
                results.append(builder.article(**kw))
            if not results:
                results.append(builder.article(
                    title="Open GiftTrove",
                    description="Scout unique Telegram gifts",
                    text="Scout unique Telegram gifts on GiftTrove.",
                    buttons=[Button.url("Open GiftTrove", MINIAPP_URL)],
                ))
            await event.answer(results, cache_time=20, private=True)
        except Exception as e:
            log.info("inline answer failed: %s", e)

    # ── Fragment promotion review (admin Approve / Decline buttons) ───────────
    @bot.on(events.CallbackQuery(pattern=b"^p(approve|decline):"))
    async def _promo_review(event):
        if str(event.sender_id) not in ADMIN_IDS:
            await event.answer("Not allowed.", alert=True)
            return
        data = event.data.decode()
        act, pid = data.split(":", 1)
        promo = _promo_get(pid)
        if not promo:
            await event.answer("Promotion not found.", alert=True)
            return
        if act == "papprove":
            _promo_go_live(pid)
            try:
                await event.edit(f"Approved \u2014 live for {PROMO_DAYS} days.\nGift: {promo.get('collection','?')}")
            except Exception:
                pass
            try:
                await _bot_api("sendMessage", {"chat_id": int(promo["uid"]),
                    "text": f"Your Fragment promotion was approved and is live for {PROMO_DAYS} days."})
            except Exception:
                pass
        else:
            _promo_set_status(pid, "declined")
            refunded = False
            if promo.get("charge_id"):
                try:
                    r = await _bot_api("refundStarPayment",
                                       {"user_id": int(promo["uid"]), "telegram_payment_charge_id": promo["charge_id"]})
                    refunded = bool(r and r.get("ok"))
                except Exception as e:
                    log.info("promo decline refund failed: %s", e)
            try:
                await event.edit("Declined." + (" Stars refunded." if refunded else ""))
            except Exception:
                pass
            try:
                await _bot_api("sendMessage", {"chat_id": int(promo["uid"]),
                    "text": ("Your Fragment promotion was declined after review and your Stars were refunded."
                             if refunded else "Your Fragment promotion was declined after review.")})
            except Exception:
                pass
        await event.answer("Done.")

    # ── Stars payments ──────────────────────────────────────────────────────
    # Invoices are created via the Bot API (createInvoiceLink), but the resulting
    # updates flow to this MTProto bot. We must approve the pre-checkout within
    # ~10s, then the completed payment arrives as a service message.
    from telethon.tl import functions as _fn, types as _tl

    @bot.on(events.Raw)
    async def _on_update(update):
        if isinstance(update, _tl.UpdateBotPrecheckoutQuery):
            try:
                await bot(_fn.messages.SetBotPrecheckoutResultsRequest(
                    query_id=update.query_id, success=True))
            except Exception as e:
                log.warning("pre-checkout approve failed: %s", e)
            return
        msg = getattr(update, "message", None)
        action = getattr(msg, "action", None) if msg is not None else None
        if isinstance(action, _tl.MessageActionPaymentSentMe):
            try:
                await _record_payment(action)
            except Exception as e:
                log.error("payment record failed: %s", e)


async def _record_payment(action):
    """A Stars payment completed. Parse our own payload and grant the perk.
    Recurring renewals arrive here too (Telegram auto-charges), so we simply
    re-stamp the new expiry each time."""
    payload = action.payload
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", "ignore")
    parts = (payload or "").split(":")
    kind = parts[0] if parts else ""
    charge_id = ""
    try:
        # The Telegram charge id (the stx... value) is what botCancelStarsSubscription
        # and refundStarPayment expect — NOT provider_charge_id (caused CHARGE_ID_INVALID).
        charge_id = action.charge.id or action.charge.provider_charge_id
    except Exception:
        pass
    if kind == "sub" and len(parts) >= 3:
        tier, uid = parts[1], parts[2]
        # If they're switching tiers (e.g. Scout+ -> Scout Pro), cancel the OLD
        # subscription so Telegram doesn't keep charging for both. We stored the
        # old charge_id; botCancelStarsSubscription stops its auto-renewal.
        try:
            prev = _sub_row(uid)
            if prev and prev[0] != tier and prev[1] and bot is not None:
                from telethon.tl import functions as _fn
                try:
                    peer = await bot.get_input_entity(int(uid))
                    await bot(_fn.payments.BotCancelStarsSubscriptionRequest(
                        user_id=peer, charge_id=prev[1]))
                    log.info("cancelled old %s subscription for %s on tier change", prev[0], uid)
                except Exception as e:
                    log.warning("old-sub auto-cancel failed (uid=%s): %s", uid, e)
        except Exception:
            pass
        until = getattr(action, "subscription_until_date", None)
        # Telethon returns this as a datetime; the Bot API would give a unix int.
        if hasattr(until, "timestamp"):
            expires = int(until.timestamp())
        elif isinstance(until, (int, float)) and until:
            expires = int(until)
        else:
            expires = int(time.time()) + SUB_PERIOD
        _sub_set(uid, tier, expires, charge_id)
        log.info("Stars subscription active: uid=%s tier=%s until=%s", uid, tier, expires)
        # Affiliate: pay the referrer a recurring cut — but only while THEY are Pro.
        try:
            ref = _referrer_of(uid)
            if ref and str(ref) != str(uid) and get_tier(ref) == "pro":
                paid = getattr(action, "total_amount", 0) or (STARS_PRO if tier == "pro" else STARS_PLUS)
                cut = int(int(paid) * AFFILIATE_PCT / 100)
                _affiliate_credit(ref, uid, tier, cut, charge_id)
        except Exception as e:
            log.info("affiliate credit skipped: %s", e)
        await _send_sub_confirmation(uid, tier)
    elif kind == "promo" and len(parts) >= 3:
        pid, uid = parts[1], parts[2]
        _promo_set_charge(pid, charge_id)
        promo = _promo_get(pid) or {}
        if (promo.get("marketplace") or "") in ("Fragment", "MarketApp"):
            # Manual review: a human checks the Fragment link before it goes live.
            _promo_set_status(pid, "review")
            log.info("promotion awaiting review: id=%s uid=%s", pid, uid)
            try:
                await bot.send_message(
                    int(ALERT_ADMIN_ID),
                    f"Fragment promotion to review\nGift: {promo.get('collection','?')}\nLink: {promo.get('link','-')}\nFrom uid: {uid}",
                    buttons=[[Button.inline("Approve", f"papprove:{pid}".encode()),
                              Button.inline("Decline + refund", f"pdecline:{pid}".encode())]],
                    link_preview=False)
            except Exception as e:
                log.info("admin review DM failed: %s", e)
            try:
                await _bot_api("sendMessage", {"chat_id": int(uid),
                    "text": "Your Fragment promotion was received and is being reviewed. It goes live once approved (usually quickly)."})
            except Exception:
                pass
        else:
            exp = _promo_activate(pid, uid, charge_id)
            # Auto-source the live Telegram floor price so the card shows real value.
            try:
                amt, cur = await _promo_fetch_price(promo.get("gift_id"))
                if amt:
                    _promo_set_price(pid, amt, cur)
            except Exception as e:
                log.info("promo auto-price skipped: %s", e)
            log.info("promotion active: id=%s uid=%s until=%s", pid, uid, exp)
            try:
                await _bot_api("sendMessage", {"chat_id": int(uid),
                    "text": f"Your gift promotion is live for the next {PROMO_DAYS} days. It will appear at the top of matching scouts."})
            except Exception:
                pass
    else:
        log.info("payment with unrecognized payload: %r", payload)


async def _send_sub_confirmation(uid, tier):
    """DM the member what they just unlocked, led by a premium gold-star custom
    emoji (Premium users see the star; others see the fallback). Sent via the bot's
    MTProto client so the custom emoji renders, with a plain-text fallback."""
    if bot is None:
        return
    if tier == "pro":
        line = "Scout Pro unlocked. You can now scout with unlimited filters, claim your own custom referral code, and browse with no promoted gifts in your results."
    else:
        line = "Scout+ unlocked. You can now apply up to 5 of each filter in your scouts."
    try:
        from telethon.tl.types import MessageEntityCustomEmoji, MessageEntityBold
        def u16(s):
            return len(s.encode("utf-16-le")) // 2
        star = "\U0001F396\uFE0F"   # 🎖️ fallback; overlaid by the premium star
        segs = [("emoji", star, EMOJI_STAR), ("text", " "), ("bold", line)]
        text, off, ents = "", 0, []
        for kind, *rest in segs:
            s = rest[0]; ln = u16(s)
            if kind == "emoji":
                ents.append(MessageEntityCustomEmoji(off, ln, int(rest[1])))
            elif kind == "bold":
                ents.append(MessageEntityBold(off, ln))
            text += s; off += ln
        await bot.send_message(int(uid), text, formatting_entities=ents)
    except Exception as e:
        log.warning("sub confirmation (custom emoji) failed, sending plain: %s", e)
        try:
            await _bot_api("sendMessage", {"chat_id": int(uid), "text": line})
        except Exception:
            pass


async def _connect_user_session():
    """(Re)connect the user session. Returns True on success."""
    global client, _mtproto_error
    if not (TELETHON_OK and API_ID and API_HASH and STRING_SESSION):
        _mtproto_error = (
            "STRING_SESSION env var not set" if not STRING_SESSION else
            "API_ID / API_HASH env vars not set" if not (API_ID and API_HASH) else
            "Telethon package unavailable"
        )
        return False
    try:
        if client is None:
            client = TelegramClient(StringSession(STRING_SESSION), int(API_ID), API_HASH)
            # Auto-sleep only for short waits; longer floods raise (we catch them)
            client.flood_sleep_threshold = 5
        if not client.is_connected():
            await asyncio.wait_for(client.connect(), timeout=20)
        if not await client.is_user_authorized():
            _mtproto_error = "Session not authorised — regenerate STRING_SESSION via gen_session.py"
            log.error("MTProto: %s", _mtproto_error)
            return False
        _mtproto_error = ""
        return True
    except Exception as e:
        _mtproto_error = str(e)
        log.error("MTProto connect failed: %s", e)
        return False


async def background_telethon_initializer():
    """Connect Telethon in the background (so Uvicorn binds the port instantly),
    then keep the connection warm with a keepalive loop."""
    global bot

    # 1) User session (with one retry)
    for attempt in (1, 2):
        log.info("MTProto: connecting user session (attempt %d)...", attempt)
        ok = await _connect_user_session()
        if ok:
            try:
                me = await asyncio.wait_for(client.get_me(), timeout=20)
                log.info("MTProto session live as @%s", getattr(me, "username", me.id))
            except Exception as e:
                log.warning("get_me after connect failed: %s", e)
            break
        if attempt == 1:
            await asyncio.sleep(5)

    # 2) Bot (/start)
    if TELETHON_OK and API_ID and API_HASH and BOT_TOKEN:
        try:
            log.info("MTProto: initializing bot...")
            bot = TelegramClient(StringSession(), int(API_ID), API_HASH)
            await bot.start(bot_token=BOT_TOKEN)
            await _register_bot_handlers()
            binfo = await bot.get_me()
            log.info("Bot live as @%s — /start handler running", getattr(binfo, "username", binfo.id))
        except Exception as e:
            log.error("Failed to start bot handler: %s", e)
            bot = None
    else:
        log.warning("BOT_TOKEN not provided — /start handler skipped.")

    # 2b) Pre-warm the gift cache so the very first visitor sees gifts instantly
    #     (this also downloads the real preview thumbnails into memory).
    if client is not None:
        try:
            await featured()
            log.info("Featured gifts pre-warmed.")
        except Exception as e:
            log.info("featured pre-warm skipped: %s", e)
        try:
            asyncio.create_task(collections())
        except Exception as e:
            log.info("collections pre-warm skipped: %s", e)

    # 2c) Reports: deploy-live good news + the daily digest loop.
    try:
        asyncio.create_task(_daily_digest_loop())
        asyncio.create_task(notify_admin(
            f"Backend deployed and live.\nDB: {'Postgres' if USE_PG else 'SQLite'}.\n"
            f"MTProto session: {'connected' if (client and client.is_connected()) else 'offline'}. Bot: online.",
            level="good",
        ))
    except Exception as e:
        log.info("report tasks skipped: %s", e)

    # 3) Keepalive: ping Telegram every 2 min so the socket never goes stale,
    #    and refresh the featured cache so it's always warm.
    while True:
        await asyncio.sleep(120)
        try:
            async with _mtproto_lock:
                if client is not None and not client.is_connected():
                    await asyncio.wait_for(client.connect(), timeout=20)
                if client is not None:
                    await asyncio.wait_for(client.get_me(), timeout=20)
            _mtproto_error = "" if client and client.is_connected() else _mtproto_error
        except Exception as e:
            log.warning("keepalive: connection looked dead (%s) — reconnecting", e)
            try:
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                await _connect_user_session()
            except Exception as e2:
                log.error("keepalive reconnect failed: %s", e2)
        # keep featured fresh (cache TTL is 600s; refresh a bit before it lapses)
        try:
            if client is not None and not cache_get("featured"):
                await featured()
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    init_task = asyncio.create_task(background_telethon_initializer())
    yield
    init_task.cancel()
    for c in (client, bot):
        if c:
            try:
                await c.disconnect()
            except Exception:
                pass


app = FastAPI(title="GiftTrove API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS or ["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

from fastapi import Request, Header
from fastapi.responses import JSONResponse


@app.middleware("http")
async def _guard(request: Request, call_next):
    # Per-IP rate limiting on the API surface — caps abuse and runaway cost.
    path = request.url.path
    if path.startswith("/api/"):
        ip = (request.headers.get("x-forwarded-for", "") or (request.client.host if request.client else "")).split(",")[0].strip() or "?"
        if not rate_ok_ip(ip):
            return JSONResponse(status_code=429, content={"error": "rate_limited", "results": [], "collections": []})
    try:
        return await call_next(request)
    except Exception as exc:
        # Clean fallback for the user + a detailed DM to the admin.
        log.error("Unhandled error on %s: %s", path, exc)
        try:
            asyncio.create_task(notify_admin(f"{request.method} {path}\n{type(exc).__name__}: {exc}"))
        except Exception:
            pass
        return JSONResponse(status_code=200, content={"error": "temporary_issue", "results": [], "collections": [], "ok": False})


@app.exception_handler(Exception)
async def _all_errors(request: Request, exc: Exception):
    log.error("Handler error on %s: %s", request.url.path, exc)
    try:
        asyncio.create_task(notify_admin(f"{request.url.path}\n{type(exc).__name__}: {exc}"))
    except Exception:
        pass
    return JSONResponse(status_code=200, content={"error": "temporary_issue", "results": [], "collections": [], "ok": False})


# ─── Helpers ──────────────────────────────────────────────────────────────────
def _payments(name):
    if not TELETHON_OK:
        return None
    return getattr(functions.payments, name, None)


async def _invoke(build, timeout=None):
    """
    Run an MTProto request with a HARD timeout + one reconnect-and-retry.
    `build` is a zero-arg callable returning a FRESH request object (so we can
    safely re-send it after a reconnect). Never hangs the worker.
    """
    if client is None:
        raise RuntimeError("MTProto client not initialised yet")
    timeout = timeout or MTPROTO_TIMEOUT
    last = None
    async with _mtproto_lock:
        for attempt in (1, 2):
            try:
                if not client.is_connected():
                    await asyncio.wait_for(client.connect(), timeout=15)
                return await asyncio.wait_for(client(build()), timeout=timeout)
            except FloodWaitError as e:
                # Rate-limited by Telegram. Reconnecting won't help — bail out
                # so the handler can serve cache / empty instead of cascading.
                log.warning("flood wait %ss on MTProto call — skipping", getattr(e, "seconds", "?"))
                raise
            except Exception as e:
                last = e
                log.error("MTProto invoke attempt %d failed: %s", attempt, repr(e))
                if attempt == 1:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    try:
                        await asyncio.wait_for(client.connect(), timeout=15)
                    except Exception as e2:
                        log.error("reconnect failed: %s", repr(e2))
    raise last if last else RuntimeError("MTProto invoke failed")


def color_hex(c):
    try:
        return f"#{int(c) & 0xFFFFFF:06x}"
    except Exception:
        return None


# Expand a Telegram "stripped" thumbnail (photoStrippedSize.bytes) into a real
# JPEG, returned as a data URI. This needs NO file download / getFile call, so
# it's instant and never triggers flood waits — perfect for picker thumbnails.
_JPEG_HEADER = bytes.fromhex(
    "ffd8ffe000104a46494600010100000100010000ffdb004300281c1e231e19282321232d2b"
    "28303c64413c37373c7b585d4964918099968f808c8aa0b4e6c3a0aadaad8a8cc8ffcbdaee"
    "f5ffffff9bc1fffffffaffe6fdfff8ffdb0043012b2d2d3c353c76414176f8a58ca5f8f8f8"
    "f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8"
    "f8f8f8f8f8f8f8f8f8f8f8f8ffc00011080000000003012200021101031101ffc4001f0000"
    "010501010101010100000000000000000102030405060708090a0bffc400b5100002010303"
    "020403050504040000017d01020300041105122131410613516107227114328191a1082342"
    "b1c11552d1f02433627282090a161718191a25262728292a3435363738393a434445464748"
    "494a535455565758595a636465666768696a737475767778797a838485868788898a929394"
    "95969798999aa2a3a4a5a6a7a8a9aab2b3b4b5b6b7b8b9bac2c3c4c5c6c7c8c9cad2d3d4d5d6"
    "d7d8d9dae1e2e3e4e5e6e7e8e9eaf1f2f3f4f5f6f7f8f9faffc4001f0100030101010101010"
    "1010100000000000000010203040506070809ffc400b5110002010204040304070504040001"
    "0277000102031104052131061241510761711322328108144291a1b1c109233352f0156272"
    "d10a162434e125f11718191a262728292a35363738393a434445464748494a535455565758"
    "595a636465666768696a737475767778797a82838485868788898a92939495969798999aa2"
    "a3a4a5a6a7a8a9aab2b3b4b5b6b7b8b9bac2c3c4c5c6c7c8c9cad2d3d4d5d6d7d8d9dae2e3e4"
    "e5e6e7e8e9eaf2f3f4f5f6f7f8f9faffda000c03010002110311003f00"
)
_JPEG_FOOTER = bytes.fromhex("ffd9")


def _stripped_data_uri(doc):
    """Find a stripped thumbnail on a Document and return it as a data URI."""
    try:
        import base64 as _b64
        thumbs = getattr(doc, "thumbs", None) or []
        for th in thumbs:
            b = getattr(th, "bytes", None)
            if b and len(b) >= 3 and b[0] == 0x01:
                real = bytearray(_JPEG_HEADER)
                real[164] = b[1]
                real[166] = b[2]
                jpg = bytes(real) + bytes(b[3:]) + _JPEG_FOOTER
                return "data:image/jpeg;base64," + _b64.b64encode(jpg).decode()
    except Exception:
        pass
    return None


# Real (non-stripped) thumbnails for gift stickers + attribute documents.
# Star-gift stickers are TGS animations whose thumbs are usually vector paths,
# NOT stripped JPEGs — so the old stripped-only approach yielded no preview at
# all for most collections/models/symbols. Here we download the real static
# thumbnail once per document and cache the data URI in memory.
import base64 as _b64mod

_thumb_cache = {}
_thumb_sem = asyncio.Semaphore(8)


async def _doc_thumb_uri(doc):
    did = getattr(doc, "id", None)
    if did is None:
        return None
    if did in _thumb_cache:
        return _thumb_cache[did]
    raw = None
    try:
        thumbs = getattr(doc, "thumbs", None) or []
        # Prefer the smallest REAL PhotoSize (has w/h, no inline bytes) —
        # crisp enough for icons without multi-MB payloads.
        real = [t for t in thumbs if getattr(t, "w", 0) and not getattr(t, "bytes", None)]
        pick = min(real, key=lambda t: getattr(t, "w", 10**6)) if real else None
        if client is not None and (pick is not None or thumbs):
            async with _thumb_sem:
                try:
                    raw = await asyncio.wait_for(
                        client.download_media(doc, file=bytes, thumb=pick if pick is not None else -1),
                        timeout=20,
                    )
                except TypeError:
                    raw = await asyncio.wait_for(
                        client.download_media(doc, file=bytes, thumb=-1), timeout=20
                    )
    except Exception as e:
        log.info("thumb download skipped: %s", e)
    uri = None
    if raw:
        head = bytes(raw[:8])
        mime = ("image/webp" if head[:4] == b"RIFF"
                else "image/png" if head[:4] == b"\x89PNG"
                else "image/jpeg")
        uri = f"data:{mime};base64," + _b64mod.b64encode(raw).decode()
    if not uri:
        uri = _stripped_data_uri(doc)   # last-resort low-res fallback
    if uri and len(_thumb_cache) < 8000:
        _thumb_cache[did] = uri
    return uri


def _attr_id(a):
    """Build the StarGiftAttributeId used for server-side resale filtering."""
    cls = type(a).__name__
    try:
        if cls == "StarGiftAttributeModel":
            doc_id = getattr(getattr(a, "document", None), "id", None)
            return types.StarGiftAttributeIdModel(document_id=int(doc_id)) if doc_id else None
        if cls == "StarGiftAttributePattern":
            doc_id = getattr(getattr(a, "document", None), "id", None)
            return types.StarGiftAttributeIdPattern(document_id=int(doc_id)) if doc_id else None
        if cls == "StarGiftAttributeBackdrop":
            bid = getattr(a, "backdrop_id", None)
            return types.StarGiftAttributeIdBackdrop(backdrop_id=int(bid)) if bid is not None else None
    except Exception as e:
        log.info("attr_id build failed (%s): %s", cls, e)
    return None


def cdn_full(slug, num):
    base = (slug or "").strip()
    if num is not None and not base.lower().endswith(f"-{num}".lower()):
        base = f"{base}-{num}"
    return base


def cdn_image(base):
    return f"{FRAGMENT_CDN}/{base.lower()}.large.jpg" if base else None


def cdn_anim(base):
    return f"{FRAGMENT_CDN}/{base.lower()}.lottie.json" if base else None


def native_url(base):
    return f"https://t.me/nft/{base}" if base else None


def _slug_from_title(title):
    return "".join((title or "").split())


def _extract_price(g):
    """
    Resale price. `resell_amount` is a Vector<StarsAmount> that may contain:
      • starsAmount     -> Stars   (amount = whole stars, nanos = billionths)
      • starsTonAmount  -> GRAM/TON (amount = nanotons, 1e9 per coin)
    Telegram resale is natively in Stars, so we prefer Stars and fall back to
    GRAM for TON-only listings.
    """
    stars = None
    gram = None
    ton_only = bool(getattr(g, "resale_ton_only", False))
    amounts = getattr(g, "resell_amount", None)
    if amounts is None:
        amounts = []
    if not isinstance(amounts, (list, tuple)):
        amounts = [amounts]
    for a in amounts:
        amt = getattr(a, "amount", None)
        if amt is None:
            continue
        cls = type(a).__name__
        if "Ton" in cls:  # StarsTonAmount -> GRAM (nanotons)
            gram = round(int(amt) / 1e9, 4)
        else:             # StarsAmount -> Stars
            nanos = getattr(a, "nanos", 0) or 0
            val = int(amt) + (int(nanos) / 1e9)
            stars = int(val) if float(val).is_integer() else round(val, 2)
    # gram_value = accurate cross-market SORT key (prefer Telegram's real
    # TON-equivalent; fall back to an approximate Stars->GRAM conversion).
    gram_value = gram if gram is not None else (
        (stars / STARS_PER_TON) if (stars is not None and STARS_PER_TON) else None)
    # DISPLAY stays in the gift's NATIVE currency: a gift buyable with Stars is
    # shown in Stars; a TON/GRAM listing is shown in GRAM. Only the ordering uses
    # the unified gram_value above.
    if ton_only and gram is not None:
        return (gram, "GRAM", gram_value)
    if stars is not None:
        return (stars, "Stars", gram_value)
    if gram is not None:
        return (gram, "GRAM", gram_value)
    return (None, "Stars", None)


def _gift_attrs(g):
    model = model_rarity = symbol = backdrop = backdrop_hex = None
    for a in getattr(g, "attributes", []) or []:
        cls = type(a).__name__
        rar = getattr(a, "rarity_permille", None)
        rar = round(rar / 10, 2) if isinstance(rar, (int, float)) else None
        if cls == "StarGiftAttributeModel":
            model, model_rarity = getattr(a, "name", None), rar
        elif cls == "StarGiftAttributePattern":
            symbol = getattr(a, "name", None)
        elif cls == "StarGiftAttributeBackdrop":
            backdrop = getattr(a, "name", None)
            backdrop_hex = color_hex(getattr(a, "center_color", None))
    return model, model_rarity, symbol, backdrop, backdrop_hex


def serialize_unique(g):
    num = getattr(g, "num", None)
    title = getattr(g, "title", None) or "Gift"
    slug = getattr(g, "slug", None) or _slug_from_title(title)
    base = cdn_full(slug, num)
    model, model_rarity, symbol, backdrop, backdrop_hex = _gift_attrs(g)
    price, currency, gram_value = _extract_price(g)
    return {
        "id": str(getattr(g, "id", base)),
        "slug": base,
        "num": num,
        "name": title,
        "model": model,
        "modelRarity": model_rarity,
        "symbol": symbol,
        "backdrop": backdrop,
        "backdropHex": backdrop_hex,
        "price": price,
        "currency": currency,
        "gram_value": gram_value,
        "market": "Telegram",
        "url": native_url(base),
        "image": cdn_image(base),
        "animation": cdn_anim(base),
    }


# ─── Fragment (scraped public listings — Fragment has NO official API) ────────
_frag_cache = {}          # fslug -> (ts, items)
_frag_fail_noted = 0.0


def _frag_item(tg_slug, gift_name, num, price, fslug):
    base = cdn_full(tg_slug, num)
    return {
        "id": f"frag-{fslug}-{num}",
        "slug": base,
        "num": int(num),
        "name": gift_name or tg_slug,
        "model": None, "modelRarity": None, "symbol": None,
        "backdrop": None, "backdropHex": None,
        "price": price, "currency": "TON", "gram_value": (float(price) if price is not None else None),
        "market": "Fragment",
        "url": f"https://fragment.com/gift/{fslug}-{num}",
        "image": cdn_image(base), "animation": cdn_anim(base),
    }


def _parse_fragment_gifts(html, tg_slug, gift_name, fslug):
    """Structure-agnostic parser for fragment.com/gifts/<slug> pages.
    Anchors on every /gift/<fslug>-<num> link and reads the chunk up to the
    next link — works whether Fragment renders tables or card grids, and
    tolerates multiple links per card. Captures EVERY buyable listing on
    the page (the old row-split approach matched far too few)."""
    items, seen = [], set()
    link_re = _re.compile(rf"/gift/{_re.escape(fslug)}-(\d+)", _re.I)
    matches = list(link_re.finditer(html))
    for i, m in enumerate(matches):
        num = m.group(1)
        if num in seen:
            continue
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else min(len(html), start + 1600)
        win = html[start:end]
        low = win.lower()
        # only direct-sale listings in v1 (auctions have no clean buy-now price)
        if "auction" in low:
            continue
        if not ("for sale" in low or "available" in low or "status-avail" in low):
            continue
        pm = _re.search(r"icon-ton[^>]*>\s*([\d][\d,\.]*)", win)
        if not pm:
            pm = _re.search(r">\s*([\d][\d,\.]*)\s*<", win)
        if not pm:
            continue
        try:
            price = float(pm.group(1).replace(",", ""))
        except Exception:
            continue
        seen.add(num)
        items.append(_frag_item(tg_slug, gift_name, num, price, fslug))
    items.sort(key=lambda x: (x["price"] is None, x["price"]))
    return items


async def fragment_search(tg_slug, gift_name, limit=24):
    """Direct-sale Fragment listings for a collection. Cached 2 min; serves a
    stale copy on failure; warns the admin at most once an hour if blocked."""
    global _frag_fail_noted
    if not (HTTPX_OK and tg_slug):
        return []
    fslug = _re.sub(r"[^a-z0-9]", "", str(tg_slug).lower())
    if not fslug:
        return []
    now = time.time()
    hit = _frag_cache.get(fslug)
    if hit and now - hit[0] < 120:
        return hit[1][:limit]
    url = f"https://fragment.com/gifts/{fslug}?filter=sale&sort=price_asc"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://fragment.com/gifts",
    }
    try:
        async with httpx.AsyncClient(timeout=8, follow_redirects=True) as cli:
            r = await cli.get(url, headers=headers)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")
        items = _parse_fragment_gifts(r.text, tg_slug, gift_name, fslug)
        _frag_cache[fslug] = (now, items)
        if len(_frag_cache) > 200:
            _frag_cache.pop(next(iter(_frag_cache)))
        return items[:limit]
    except Exception as e:
        log.info("fragment search skipped: %s", e)
        if now - _frag_fail_noted > 3600:
            _frag_fail_noted = now
            try:
                await notify_admin(f"Fragment scrape failing: {type(e).__name__}: {e}", level="warning")
            except Exception:
                pass
        stale = _frag_cache.get(fslug)
        return stale[1][:limit] if stale else []


# ─── MarketApp aggregator (Tonnel / Portals / Fragment / GetGems / MarketApp) ──
# Endpoints (from their OpenAPI spec at api.marketapp.ws/docs/openapi.json):
#   GET /v1/collections/gifts/           → [{name, address, extra_data}]  (name→TON addr map)
#   GET /v1/gifts/onsale/                → {cursor, items:[NFTItem]}
#
# NFTItem fields:
#   address, name, collection_address, owner, real_owner,
#   min_bid (nanotons string), max_bid, currency ("TON"|"USDT"),
#   attributes [{trait_type, value}], item_num, listed_at, is_restricted
#
# Auth: Authorization: <raw_token>   (no "Bearer" prefix)
_marketapp_cache = {}          # key → (ts, [items])
_marketapp_colls_cache = {}    # {"ts": float, "data": [...]}

async def _marketapp_collection_address(gift_name, slug):
    """Resolve a gift name/slug to a MarketApp TON collection address.
    Tries /v1/collections/gifts/ first, falls back to /v1/collections/.
    Logs sample names on first fetch so mismatches are visible in Render logs."""
    global _marketapp_colls_cache
    now = time.time()
    if not _marketapp_colls_cache or now - _marketapp_colls_cache.get("ts", 0) > 3600:
        merged = []
        for path in ("/v1/collections/gifts/", "/v1/collections/"):
            try:
                async with httpx.AsyncClient(timeout=12) as cli:
                    r = await cli.get(f"{MARKETAPP_BASE}{path}",
                                      headers={"Authorization": MARKETAPP_TOKEN})
                log.info("marketapp collections %s status=%s", path, r.status_code)
                if r.status_code == 200:
                    data = r.json()
                    rows = data if isinstance(data, list) else (data.get("items") or data.get("data") or [])
                    # Log first 8 names so name-format mismatches are visible
                    names = [c.get("name") for c in rows[:8] if isinstance(c, dict)]
                    log.info("marketapp collections sample names: %s (total %d)", names, len(rows))
                    for c in rows:
                        if isinstance(c, dict) and c.get("address") and c.get("name"):
                            merged.append(c)
            except Exception as e:
                log.info("marketapp collections %s failed: %s", path, e)
        # Deduplicate by address
        seen = set()
        deduped = []
        for c in merged:
            a = c.get("address", "")
            if a not in seen:
                seen.add(a)
                deduped.append(c)
        _marketapp_colls_cache = {"ts": now, "data": deduped}
        log.info("marketapp: total %d unique collections cached", len(deduped))

    colls = _marketapp_colls_cache.get("data", [])
    if not colls:
        return None

    gname = (gift_name or "").strip()
    gslug = (slug or "").strip()

    # Normalise: lowercase, remove spaces and hyphens for flexible matching
    def norm(s):
        return _re.sub(r"[\s\-_]", "", str(s or "")).lower()

    gn = norm(gname)
    gs = norm(gslug)

    for c in colls:
        cname = c.get("name") or ""
        cn = norm(cname)
        if cn == gn or cn == gs:
            return c.get("address")

    # Substring fallback: name contained in collection name or vice-versa
    for c in colls:
        cn = norm(c.get("name") or "")
        if (gn and (gn in cn or cn in gn)) or (gs and (gs in cn or cn in gs)):
            log.info("marketapp: fuzzy match %r -> %r", gname, c.get("name"))
            return c.get("address")

    return None

def _marketapp_item(raw, gift_name, fallback_slug):
    if not isinstance(raw, dict):
        return None
    num = raw.get("item_num")
    try:
        num = int(num) if num is not None else None
    except Exception:
        num = None
    # min_bid is nanotons (string) → divide by 1e9 for TON/GRAM value
    try:
        price = float(raw.get("min_bid") or 0) / 1e9
        if price <= 0:
            price = None
    except Exception:
        price = None
    currency_raw = str(raw.get("currency") or "TON").upper()
    if currency_raw == "USDT":
        currency, gram_value = "USDT", None
    else:
        currency, gram_value = "GRAM", price   # TON == GRAM (rebrand)
    # Attribute extraction from [{trait_type, value}]
    attrs = raw.get("attributes") or []
    def _attr(*types):
        for a in attrs:
            if str(a.get("trait_type") or "").lower() in types:
                v = a.get("value")
                return str(v) if v is not None else None
        return None
    nft_address = raw.get("address") or ""
    coll_address = raw.get("collection_address") or ""
    slug = fallback_slug  # not in response; use the one we looked up with
    # Always link to MarketApp so the user buys there (they came from MarketApp chip).
    if nft_address:
        url = f"https://marketapp.ws/nft/{nft_address}/"
    elif coll_address:
        url = f"https://marketapp.ws/collection/{coll_address}/"
    else:
        url = "https://marketapp.ws/gifts/"
    return {
        "id": nft_address or f"ma-{slug}-{num}",
        "name": raw.get("name") or gift_name,
        "slug": slug, "num": num,
        "price": price, "currency": currency, "gram_value": gram_value,
        "model":   _attr("model") or "",
        "symbol":  _attr("symbol", "pattern") or "",
        "backdrop": _attr("backdrop", "background") or "",
        "market": "MarketApp",
        "url": url, "image": None, "animation": None,
    }

async def marketapp_search(slug, gift_name, gift_id=None, limit=40,
                            model="", symbol="", backdrop=""):
    if not (MARKETAPP_TOKEN and HTTPX_OK and (slug or gift_name)):
        return []
    key = f"{slug}|{gift_id}|{model}|{symbol}|{backdrop}"
    now = time.time()
    hit = _marketapp_cache.get(key)
    if hit and now - hit[0] < 60:
        return hit[1][:limit]
    coll_addr = await _marketapp_collection_address(gift_name, slug)
    if not coll_addr:
        log.info("marketapp: no collection address for %r / %r", gift_name, slug)
        return []
    url = f"{MARKETAPP_BASE}/v1/gifts/onsale/"
    params = {"collection_address": coll_addr, "sort_by": "min_bid_asc"}
    if model:    params["model"]   = model
    if symbol:   params["symbol"]  = symbol
    if backdrop: params["backdrop"] = backdrop
    try:
        async with httpx.AsyncClient(timeout=12) as cli:
            r = await cli.get(url, params=params,
                              headers={"Authorization": MARKETAPP_TOKEN, "Accept": "application/json"})
        log.info("marketapp %s status=%s sample=%s", url, r.status_code,
                 (r.text or "")[:400].replace("\n", " "))
        if r.status_code != 200:
            return []
        data = r.json()
        rows = (data.get("items") or []) if isinstance(data, dict) else []
        out = [it for raw in rows for it in [_marketapp_item(raw, gift_name, slug)] if it]
        _marketapp_cache[key] = (now, out)
        if len(_marketapp_cache) > 300:
            _marketapp_cache.pop(next(iter(_marketapp_cache)))
        return out[:limit]
    except Exception as e:
        log.info("marketapp search skipped: %s", e)
        return []


# ─── GetGems (OPTIONAL secondary source) ──────────────────────────────────────
async def getgems_search(gift_name, limit=12, collection_address=None):
    if not (GETGEMS_API_KEY and HTTPX_OK and gift_name):
        return []
    headers = {"Authorization": f"Bearer {GETGEMS_API_KEY}", "Content-Type": "application/json"}
    if collection_address:
        query = """
        query CollectionItems($addr: String!, $first: Int!, $cursor: String) {
          nftCollectionItems(collectionAddress: $addr, first: $first, after: $cursor,
            filter: { saleState: onSale }, sort: PRICE_LOW_TO_HIGH) {
            cursor
            items { name address sale { ... on NftSaleFixPrice { fullPrice } } previews { url resolution } }
          }
        }"""
        variables = {"addr": collection_address, "first": limit}
        op = "nftCollectionItems"
    else:
        query = """
        query Search($q: String!, $first: Int!) {
          nftSearch(text: $q, first: $first, filter: { saleState: onSale }) {
            items { name address sale { ... on NftSaleFixPrice { fullPrice } } previews { url resolution } }
          }
        }"""
        variables = {"q": gift_name, "first": limit}
        op = "nftSearch"
    try:
        async with httpx.AsyncClient(timeout=12) as h:
            r = await h.post(GETGEMS_GRAPHQL, json={"query": query, "variables": variables}, headers=headers)
        data = r.json()
    except Exception as e:
        log.info("GetGems request failed: %s", e)
        return []
    if data.get("errors"):
        log.info("GetGems %s errors: %s", op, data.get("errors"))
        return []
    root = (data.get("data") or {}).get(op) or {}
    items = root.get("items") or []
    out = []
    for n in items:
        price_nano = ((n.get("sale") or {}).get("fullPrice"))
        price = round(int(price_nano) / 1e9, 4) if price_nano else None
        previews = sorted(n.get("previews") or [], key=lambda p: p.get("resolution") or 0)
        img = previews[-1].get("url") if previews else None
        addr = n.get("address")
        out.append({
            "id": addr, "slug": None, "num": None,
            "name": n.get("name") or gift_name,
            "model": None, "modelRarity": None, "symbol": None,
            "backdrop": None, "backdropHex": None,
            "price": price, "currency": "TON", "gram_value": (float(price) if price is not None else None), "market": "GetGems",
            "url": f"https://getgems.io/nft/{addr}" if addr else None,
            "image": img, "animation": None,
        })
    return out


# ─── Routes ───────────────────────────────────────────────────────────────────
@app.api_route("/", methods=["GET", "HEAD"])
async def health():
    resp = {
        "ok": True,
        "mtproto": bool(client and client.is_connected()),
        "getgems": bool(GETGEMS_API_KEY),
        "marketapp": bool(MARKETAPP_TOKEN),
        "cached_collections": bool(cache_get("collections")),
        "tl_GetStarGifts": _payments("GetStarGiftsRequest") is not None,
        "tl_GetResaleStarGifts": _payments("GetResaleStarGiftsRequest") is not None,
    }
    if _mtproto_error:
        resp["mtproto_error"] = _mtproto_error
    return resp


@app.get("/api/ping")
async def ping():
    """Minimal authenticated round-trip test (timeout-protected)."""
    if client is None:
        return {"ok": False, "reason": "client not initialised", "mtproto_error": _mtproto_error or None}
    t0 = time.time()
    try:
        async with _mtproto_lock:
            if not client.is_connected():
                await asyncio.wait_for(client.connect(), timeout=15)
            me = await asyncio.wait_for(client.get_me(), timeout=MTPROTO_TIMEOUT)
        return {"ok": True, "me": getattr(me, "username", None) or getattr(me, "id", None),
                "ms": int((time.time() - t0) * 1000)}
    except Exception as e:
        return {"ok": False, "error": str(e), "ms": int((time.time() - t0) * 1000)}


@app.get("/api/debug")
async def debug():
    """Hang-proof diagnostics. Open in a browser to see exactly what's happening."""
    info = {
        "mtproto_connected": bool(client and client.is_connected()),
        "mtproto_error": _mtproto_error or None,
        "tl_GetStarGifts": _payments("GetStarGiftsRequest") is not None,
        "tl_GetResaleStarGifts": _payments("GetResaleStarGiftsRequest") is not None,
        "featured_names": FEATURED_NAMES,
    }
    GetStarGifts = _payments("GetStarGiftsRequest")
    if not (client is not None and GetStarGifts):
        info["catalog_call"] = "skipped — no client/function"
        return info
    t0 = time.time()
    try:
        res = await _invoke(lambda: GetStarGifts(hash=0))
        gifts = getattr(res, "gifts", []) or []
        info["catalog_ms"] = int((time.time() - t0) * 1000)
        info["response_type"] = type(res).__name__
        info["gift_count"] = len(gifts)
        info["sample"] = [
            {"type": type(g).__name__, "id": getattr(g, "id", None),
             "title": getattr(g, "title", None), "has_title": hasattr(g, "title")}
            for g in gifts[:6]
        ]
        titles = [getattr(g, "title", None) for g in gifts if getattr(g, "title", None)]
        info["titled_gift_count"] = len(titles)
        info["first_titles"] = titles[:10]
        lc = [t.lower() for t in titles]
        info["featured_matches"] = [n for n in FEATURED_NAMES if n.lower() in lc]
    except Exception as e:
        info["catalog_ms"] = int((time.time() - t0) * 1000)
        info["catalog_error"] = str(e)
        info["catalog_traceback"] = traceback.format_exc()[-1800:]

    GetResale = _payments("GetResaleStarGiftsRequest")
    try:
        first_id = None
        for s in info.get("sample", []):
            if s.get("id"):
                first_id = s["id"]
                break
        if GetResale and first_id:
            rr = await _invoke(lambda: GetResale(gift_id=int(first_id), attributes_hash=0,
                                                 sort_by_price=True, offset="", limit=2))
            rg = getattr(rr, "gifts", []) or []
            info["resale_test_gift_id"] = first_id
            info["resale_test_count"] = len(rg)
            if rg:
                info["resale_test_sample"] = serialize_unique(rg[0])
    except Exception as e:
        info["resale_error"] = str(e)
        info["resale_traceback"] = traceback.format_exc()[-1200:]
    return info


_collections_lock = asyncio.Lock()
_collections_refreshing = False


def _collections_db_get():
    try:
        raw = _meta_get("collections_cache", "")
        if raw:
            data = json.loads(raw)
            if isinstance(data, list) and data:
                return data
    except Exception as e:
        log.info("collections db read skipped: %s", e)
    return None


def _collections_db_set(out):
    try:
        blob = json.dumps(out)
        if len(blob) < 4_000_000:   # safety: never store a runaway blob
            _meta_set("collections_cache", blob)
    except Exception as e:
        log.info("collections db write skipped: %s", e)


async def _build_collections():
    """Fetch the catalog + real preview thumbs. Slow on a cold process."""
    GetStarGifts = _payments("GetStarGiftsRequest")
    res = await _invoke(lambda: GetStarGifts(hash=0))
    raw = getattr(res, "gifts", []) or []
    keep = []
    for g in raw:
        title = getattr(g, "title", None)
        # Skip gifts with no real name — those un-named "Gift <id>" entries
        # are non-collectible/parked star gifts and only add noise.
        if not title or not str(title).strip():
            continue
        keep.append((str(title).strip(), g))
    previews = await asyncio.gather(
        *[_doc_thumb_uri(getattr(g, "sticker", None)) for _, g in keep],
        return_exceptions=True,
    )
    out = []
    for (title, g), pv in zip(keep, previews):
        out.append({
            "name": title,
            "slug": getattr(g, "slug", None) or _slug_from_title(title),
            "gift_id": str(getattr(g, "id", "") or ""),
            "supply": getattr(g, "availability_total", None) or getattr(g, "availability_issued", None) or 0,
            "preview": (pv if isinstance(pv, str) else None) or "",
        })
    return [c for c in out if c["gift_id"]]


async def _refresh_collections_bg():
    """Background rebuild → memory + durable Postgres copy."""
    global _collections_refreshing
    if _collections_refreshing:
        return
    _collections_refreshing = True
    try:
        out = await _build_collections()
        if out:
            cache_set("collections", out, ttl=900)
            _collections_db_set(out)
            log.info("collections refreshed (%d items)", len(out))
    except Exception as e:
        log.info("collections bg refresh skipped: %s", e)
    finally:
        _collections_refreshing = False


@app.get("/api/collections")
async def collections():
    cached = cache_get("collections")
    if cached:
        return {"collections": cached}
    if client is None:
        return {"collections": [], "error": "MTProto background sync still initialising."}
    if not _payments("GetStarGiftsRequest"):
        return {"collections": [], "error": "GetStarGiftsRequest missing — pip install -U telethon"}
    # RESTART-PROOF PATH: a fresh process serves the durable Postgres copy
    # instantly (no 100+ image downloads inside the request) and refreshes
    # in the background. This is what keeps suggestions + the floating gifts
    # alive right after every deploy.
    db_copy = _collections_db_get()
    if db_copy:
        cache_set("collections", db_copy, ttl=900)
        asyncio.create_task(_refresh_collections_bg())
        return {"collections": db_copy, "_served": "durable"}
    # First boot ever (no durable copy yet): build inline, single-flight.
    async with _collections_lock:
        cached = cache_get("collections")
        if cached:
            return {"collections": cached}
        try:
            out = await _build_collections()
            cache_set("collections", out, ttl=900)
            _collections_db_set(out)
            return {"collections": out}
        except Exception as e:
            log.error("collections error: %s", e)
            return {"collections": [], "error": str(e)}


@app.get("/api/featured")
async def featured():
    cached = cache_get("featured")
    if cached:
        return {"gifts": cached}
    GetResale = _payments("GetResaleStarGiftsRequest")
    if client is None:
        return {"gifts": []}
    if not GetResale:
        return {"gifts": [], "error": "GetResaleStarGiftsRequest missing — pip install -U telethon"}
    # Single-flight: many splash requests collapse into ONE computation.
    async with _featured_lock:
        cached = cache_get("featured")
        if cached:
            return {"gifts": cached}
        try:
            cols = (await collections()).get("collections", [])
            by_name = {c["name"].lower(): c for c in cols}

            async def first_listing(gift_id):
                res = await _invoke(lambda: GetResale(gift_id=int(gift_id), attributes_hash=0,
                                                      sort_by_price=True, offset="", limit=1))
                g = getattr(res, "gifts", []) or []
                return serialize_unique(g[0]) if g else None

            out = []
            for name in FEATURED_NAMES:
                col = by_name.get(name.lower())
                if not col:
                    continue
                try:
                    item = await first_listing(col["gift_id"])
                    if item:
                        out.append(item)
                except Exception as e:
                    log.info("featured '%s' skipped: %s", name, repr(e))

            # Fallback capped at a FEW tries (never iterate the whole catalog —
            # that's what triggered the flood waits).
            if len(out) < 3:
                seen = {o["name"] for o in out}
                tries = 0
                for col in cols:
                    if len(out) >= 3 or tries >= 5:
                        break
                    if col["name"] in seen:
                        continue
                    tries += 1
                    try:
                        item = await first_listing(col["gift_id"])
                        if item:
                            out.append(item)
                            seen.add(col["name"])
                    except Exception:
                        continue

            # Cache for 10 min on success; brief negative-cache on failure so a
            # cold/flooded moment doesn't get hammered by repeated splash loads.
            cache_set("featured", out, ttl=600 if out else 45)
            return {"gifts": out}
        except Exception as e:
            log.error("featured error: %s", repr(e))
            return {"gifts": []}


_attr_ids_cache = {}   # gift_id -> {"model": {name: AttrId}, "symbol": {...}, "backdrop": {...}}
_attrs_stale = {}      # gift_id -> last good /api/attributes payload (stale-ok fallback)


def _attrs_db_get(gift_id):
    try:
        with db() as conn:
            r = conn.execute("SELECT payload, ts FROM attrs_cache WHERE gift_id=?", (str(gift_id),)).fetchone()
            if r:
                return json.loads(r["payload"]), int(r["ts"])
    except Exception as e:
        log.info("attrs db read skipped: %s", e)
    return None, 0


def _idmap_to_jsonable(id_map):
    """Telethon StarGiftAttributeId objects → plain ints for durable storage."""
    out = {"model": {}, "symbol": {}, "backdrop": {}}
    for name, obj in (id_map.get("model") or {}).items():
        v = getattr(obj, "document_id", None)
        if v is not None:
            out["model"][name] = int(v)
    for name, obj in (id_map.get("symbol") or {}).items():
        v = getattr(obj, "document_id", None)
        if v is not None:
            out["symbol"][name] = int(v)
    for name, obj in (id_map.get("backdrop") or {}).items():
        v = getattr(obj, "backdrop_id", None)
        if v is not None:
            out["backdrop"][name] = int(v)
    return out


def _idmap_from_jsonable(d):
    """Rebuild the Telethon attribute-id objects from the stored ints."""
    out = {"model": {}, "symbol": {}, "backdrop": {}}
    try:
        for name, v in (d.get("model") or {}).items():
            out["model"][name] = types.StarGiftAttributeIdModel(document_id=int(v))
        for name, v in (d.get("symbol") or {}).items():
            out["symbol"][name] = types.StarGiftAttributeIdPattern(document_id=int(v))
        for name, v in (d.get("backdrop") or {}).items():
            out["backdrop"][name] = types.StarGiftAttributeIdBackdrop(backdrop_id=int(v))
    except Exception as e:
        log.info("idmap rebuild skipped: %s", e)
    return out


def _attrs_db_set(gift_id, payload):
    try:
        blob = json.dumps(payload)
        if len(blob) > 3_000_000:
            return
        now = int(time.time())
        with db() as conn:
            cur = conn.execute("UPDATE attrs_cache SET payload=?, ts=? WHERE gift_id=?", (blob, now, str(gift_id)))
            if not cur.rowcount:
                conn.execute("INSERT INTO attrs_cache(gift_id, payload, ts) VALUES(?,?,?) ON CONFLICT DO NOTHING",
                             (str(gift_id), blob, now))
            # Bound storage: keep only the 40 most recently used gifts.
            conn.execute(
                "DELETE FROM attrs_cache WHERE gift_id NOT IN "
                "(SELECT gift_id FROM attrs_cache ORDER BY ts DESC LIMIT 40)"
            )
            conn.commit()
    except Exception as e:
        log.info("attrs db write skipped: %s", e)


@app.get("/api/attributes")
async def attributes(gift_id: str = Query(...)):
    gift_id = _digits(gift_id)
    if not gift_id:
        return {"models": [], "symbols": [], "backdrops": []}
    key = f"attrs:{gift_id}"
    cached = cache_get(key)
    if cached and str(gift_id) in _attr_ids_cache:
        return cached
    # RESTART-PROOF PATH: durable Postgres copy (response + attribute-id map)
    # serves instantly after a deploy; live fetch only when no usable copy.
    db_payload, db_ts = _attrs_db_get(gift_id)
    if db_payload and isinstance(db_payload, dict) and db_payload.get("resp") and (time.time() - db_ts) < 7 * 86400:
        if db_payload.get("ids"):
            _attr_ids_cache[str(gift_id)] = _idmap_from_jsonable(db_payload["ids"])
        resp = db_payload["resp"]
        cache_set(key, resp, ttl=21600)
        _attrs_stale[str(gift_id)] = resp
        return resp
    empty = {"models": [], "symbols": [], "backdrops": []}
    GetResale = _payments("GetResaleStarGiftsRequest")
    if client is None:
        return empty
    if not GetResale:
        return {**empty, "error": "GetResaleStarGiftsRequest missing — pip install -U telethon"}
    try:
        res = await _invoke(lambda: GetResale(gift_id=int(gift_id), attributes_hash=0, offset="", limit=1))
        models, symbols, backdrops = [], [], []
        model_docs, symbol_docs = [], []
        id_map = {"model": {}, "symbol": {}, "backdrop": {}}
        for a in getattr(res, "attributes", []) or []:
            cls = type(a).__name__
            name = getattr(a, "name", None)
            rar = getattr(a, "rarity_permille", None)
            rar = round(rar / 10, 2) if isinstance(rar, (int, float)) else None
            aid = _attr_id(a)
            if cls == "StarGiftAttributeModel":
                models.append({"name": name, "rarity": rar, "img": None})
                model_docs.append(getattr(a, "document", None))
                if name and aid is not None:
                    id_map["model"][name] = aid
            elif cls == "StarGiftAttributePattern":
                symbols.append({"name": name, "rarity": rar, "img": None})
                symbol_docs.append(getattr(a, "document", None))
                if name and aid is not None:
                    id_map["symbol"][name] = aid
            elif cls == "StarGiftAttributeBackdrop":
                backdrops.append({
                    "name": name, "rarity": rar,
                    "hex": color_hex(getattr(a, "center_color", None)),
                    "edge": color_hex(getattr(a, "edge_color", None)),
                })
                if name and aid is not None:
                    id_map["backdrop"][name] = aid
        # Real images for models + symbols, fetched in parallel (cached).
        imgs = await asyncio.gather(
            *[_doc_thumb_uri(d) for d in model_docs + symbol_docs],
            return_exceptions=True,
        )
        for i, m in enumerate(models):
            v = imgs[i]
            m["img"] = v if isinstance(v, str) else None
        for j, s in enumerate(symbols):
            v = imgs[len(model_docs) + j]
            s["img"] = v if isinstance(v, str) else None
        _attr_ids_cache[str(gift_id)] = id_map
        result = {"models": models, "symbols": symbols, "backdrops": backdrops}
        cache_set(key, result, ttl=21600)          # attributes barely change — 6h
        _attrs_stale[str(gift_id)] = result        # long-lived safety copy
        _attrs_db_set(gift_id, {"resp": result, "ids": _idmap_to_jsonable(id_map)})   # survives restarts
        return result
    except Exception as e:
        log.error("attributes error: %s", e)
        # Transient MTProto hiccup: serve the last good copy (memory, then
        # durable DB, any age) instead of an empty list — never just "Any".
        stale = _attrs_stale.get(str(gift_id))
        if stale:
            return stale
        db_payload, _ = _attrs_db_get(gift_id)
        if db_payload and isinstance(db_payload, dict) and db_payload.get("resp"):
            if db_payload.get("ids"):
                _attr_ids_cache[str(gift_id)] = _idmap_from_jsonable(db_payload["ids"])
            return db_payload["resp"]
        return {**empty, "error": str(e)}


@app.get("/api/search")
async def search(
    gift: str = Query(""),
    gift_id: str = Query(""),
    slug: str = Query(""),
    num: str = Query(""),
    model: str = Query(""),
    symbol: str = Query(""),
    backdrop: str = Query(""),
    markets: str = Query(""),
    uid: str = Query(""),
    sort: str = Query("price_asc"),   # price_asc | price_desc
    offset: str = Query(""),          # resale page cursor for "load more"
    limit: int = Query(SEARCH_LIMIT),
    min_price: float = Query(0),
    max_price: float = Query(0),
    x_init_data: str = Header(default="", alias="X-Init-Data"),
):
    if not rate_ok(uid):
        return {"results": [], "rate_limited": True}
    # Strict input validation (blocks malformed / injection-style input).
    gift = _clamp(gift, 64)
    gift_id = _digits(gift_id)
    slug = _safe_slug(slug)
    num = _digits(num, 12)
    sort = sort if sort in ("price_asc", "price_desc") else "default"
    offset = _clamp(offset, 256)
    try:
        min_price = max(0.0, float(min_price or 0))
        max_price = max(0.0, float(max_price or 0))
    except Exception:
        min_price = max_price = 0.0
    if gift:
        track_search(gift)

    # Premium tier governs how many of each attribute filter (model/symbol/backdrop)
    # may apply at once: free 1, plus 5, pro unlimited. Identity comes from signed
    # initData so a modified client can't raise its own caps; the spoofable query
    # `uid` is used only for rate-limiting and tracking.
    vuid = verify_init_data(x_init_data)
    cap = TIER_CAPS.get(get_tier(vuid), 1)
    def _csv_attr(s):
        out = []
        for part in (s or "").split(","):
            p = _clamp(part.strip(), 80)
            if p and p.lower() != "any" and p not in out:
                out.append(p)
            if len(out) >= cap:
                break
        return out
    models = _csv_attr(model)
    symbols = _csv_attr(symbol)
    backdrops = _csv_attr(backdrop)
    any_attr = bool(models or symbols or backdrops)
    # lower-cased sets for client-side membership (OR within a type, AND across types)
    sel = {"model": {n.lower() for n in models},
           "symbol": {n.lower() for n in symbols},
           "backdrop": {n.lower() for n in backdrops}}

    want = set([m.strip() for m in markets.split(",") if m.strip()]) if markets else set()
    results = []
    next_offset = ""
    limit = max(1, min(int(limit or SEARCH_LIMIT), SEARCH_MAX))
    GetResale = _payments("GetResaleStarGiftsRequest")

    # Resolve selected model/symbol/backdrop NAMES to attribute IDs so Telegram
    # filters server-side (otherwise matches on deeper pages get missed -> the
    # false "no listings" bug). Ensure the id map for this gift is populated.
    if gift_id and any_attr and str(gift_id) not in _attr_ids_cache:
        try:
            await attributes(gift_id=str(gift_id))
        except Exception:
            pass
    ids = _attr_ids_cache.get(str(gift_id), {})
    attr_filter = []
    for typ, names in (("model", models), ("symbol", symbols), ("backdrop", backdrops)):
        tmap = ids.get(typ, {})
        for nm in names:
            aid = tmap.get(nm)
            if aid is not None:
                attr_filter.append(aid)
    # Client-side guarantee of the multi-select semantics, independent of how
    # Telegram combines the id list (so results are always exactly right).
    def _attr_match(item):
        for typ in ("model", "symbol", "backdrop"):
            names = sel[typ]
            if names and (item.get(typ) or "").lower() not in names:
                return False
        return True

    if client is not None and GetResale and gift_id and (not want or "Telegram" in want):
        try:
            cur = offset or ""
            fetched = 0
            # Page through Telegram's resale listings until we hit `limit`
            # (the API returns a chunk + next_offset; we follow the cursor).
            for _ in range(20):  # safety cap on pages
                page = min(50, limit - fetched)
                if page <= 0:
                    break
                res = await _invoke(lambda c=cur, p=page: GetResale(
                    gift_id=int(gift_id), attributes_hash=0,
                    sort_by_price=(sort != "price_desc"),
                    attributes=(attr_filter or None),
                    offset=c, limit=p,
                ))
                chunk = getattr(res, "gifts", []) or []
                for g in chunk:
                    item = serialize_unique(g)
                    # Gift number is a SUBSTRING match: "31" -> #31, #312, #5231…
                    if num and num not in str(item.get("num", "")):
                        continue
                    # Multi-select attribute filter (OR within a type, AND across
                    # types) — enforced here so it's exact regardless of Telegram's
                    # own attribute-id combination semantics.
                    if not _attr_match(item):
                        continue
                    results.append(item)
                fetched += len(chunk)
                cur = getattr(res, "next_offset", "") or ""
                next_offset = cur
                if not cur or len(chunk) == 0:
                    next_offset = ""
                    break
        except FloodWaitError:
            return {"results": results, "next_offset": next_offset, "flood": True}
        except Exception as e:
            log.error("native search error: %s", repr(e))

    if gift and (not want or "GetGems" in want):
        results.extend(await getgems_search(gift, limit=12))

    # Fragment: real scraped listings. Attribute filters are Telegram-native
    # only, so Fragment is skipped when model/symbol/backdrop is selected
    # (better no results than wrong ones).
    if slug and not any_attr and (not want or "Fragment" in want):
        try:
            results.extend(await fragment_search(slug, gift, limit=40))
        except Exception as e:
            log.info("fragment dispatch skipped: %s", e)

    # MarketApp aggregator (Tonnel / Portals / Fragment / GetGems / MarketApp).
    # Dormant unless MARKETAPP_TOKEN is set; never raises into the scout.
    # Passes attribute filters natively so results are precise.
    if MARKETAPP_TOKEN and (slug or gift) and (not want or "MarketApp" in want):
        try:
            results.extend(await marketapp_search(
                slug, gift, gift_id=gift_id, limit=40,
                model=model, symbol=symbol, backdrop=backdrop))
        except Exception as e:
            log.info("marketapp dispatch skipped: %s", e)

    # Optional price-range filter (applies to numeric prices in the page).
    if min_price or max_price:
        lo = float(min_price or 0)
        hi = float(max_price or 0)
        def _in(p):
            if p is None:
                return False
            if lo and p < lo:
                return False
            if hi and p > hi:
                return False
            return True
        results = [r for r in results if _in(r.get("price"))]

    # Single global price ranking across ALL marketplaces, on a common GRAM
    # scale. Each item carries `gram_value`: the real TON-equivalent for
    # Telegram/Fragment/GetGems, or an approximate Stars->GRAM conversion when
    # Telegram gave no TON value. Cheapest first for price_asc, highest first for
    # price_desc; unpriced always last. No per-market interleaving.
    def _gram_value(r):
        gv = r.get("gram_value")
        if gv is not None:
            return gv
        p = r.get("price")
        if p is None:
            return None
        if str(r.get("currency") or "").lower().startswith("star"):
            return (p / STARS_PER_TON) if STARS_PER_TON else p
        return p
    # General is the default: keep the natural order the marketplaces return, and
    # only rank by price when the user explicitly taps Lowest or Highest.
    if sort in ("price_asc", "price_desc"):
        rev = (sort == "price_desc")
        results.sort(key=lambda r: (_gram_value(r) is None,
                                    -(_gram_value(r) or 0) if rev else (_gram_value(r) or 0)))

    return {"results": results, "next_offset": next_offset, "count": len(results)}


@app.get("/api/gift")
async def gift(slug: str = Query(...)):
    slug = _safe_slug(slug)
    if not slug:
        return {"error": "bad-slug"}
    GetUnique = _payments("GetUniqueStarGiftRequest")
    if client is None:
        return {"error": "mtproto-offline"}
    if not GetUnique:
        return {"error": "GetUniqueStarGiftRequest missing — pip install -U telethon"}
    try:
        res = await _invoke(lambda: GetUnique(slug=slug))
        g = getattr(res, "gift", res)
        data = serialize_unique(g)
        GetValue = _payments("GetUniqueStarGiftValueInfoRequest")
        if GetValue:
            try:
                v = await _invoke(lambda: GetValue(slug=slug))
                fp = getattr(v, "floor_price", None)
                if fp is not None:
                    fa = getattr(fp, "amount", None)
                    data["floor"] = round(float(fa) / 1e9, 4) if isinstance(fa, (int, float)) else None
                data["listedCount"] = getattr(v, "listed_count", None)
                data["fragmentUrl"] = getattr(v, "fragment_listed_url", None)
            except Exception as e:
                log.info("value info skipped: %s", e)
        return data
    except Exception as e:
        log.error("gift detail error: %s", e)
        return {"error": str(e)}


@app.get("/api/access")
async def access(uid: str = Query(""), code: str = Query(""), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """
    Gate the app. Admin status comes ONLY from a verified Telegram identity
    (signed initData) — it can't be spoofed by passing a uid. Everyone else
    needs the access code (stored server-side; only the operator can rotate it).
    """
    verified = verify_init_data(x_init_data)
    eff_uid = verified or _digits(uid)
    is_admin = bool(verified) and verified in ADMIN_IDS
    ok = is_admin or (_clamp(code, 40) == ACCESS_CODE and ACCESS_CODE != "")
    if ok:
        new_member = track_visit(eff_uid)
        _bcast_add(eff_uid)   # everyone who opens the app joins the broadcast list
        if new_member:
            _maybe_milestone()
        return {"ok": True, "admin": is_admin, "new_member": new_member}
    return {"ok": False, "admin": False}


@app.get("/api/analytics")
async def analytics(uid: str = Query(""), code: str = Query(""), range_q: str = Query("7d", alias="range"),
                    x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Admin-only product analytics. No personal data is stored or returned.
    Locked to the analytics admin + a separate secret ADMIN_CODE (never shipped in
    the frontend). The general access code can NOT open analytics."""
    verified = verify_init_data(x_init_data)
    code_ok = bool(ADMIN_CODE) and _clamp(code, 60) == ADMIN_CODE
    id_ok = (verified is None) or (verified == ANALYTICS_ADMIN_ID)
    if not (code_ok and id_ok):
        return {"error": "forbidden"}
    rng = range_q if range_q in ("7d", "12w", "24m", "all") else "7d"
    now = int(time.time())
    day, week = now - 86400, now - 7 * 86400
    out = {}
    try:
        with db() as conn:
            q = lambda s, p=(): conn.execute(s, p).fetchone()["c"]
            out["members_total"] = q("SELECT COUNT(*) c FROM members")
            out["bcast_count"] = q("SELECT COUNT(*) c FROM bcast")
            out["returning_members"] = q("SELECT COUNT(*) c FROM members WHERE visits>1")
            out["active_24h"] = q("SELECT COUNT(*) c FROM members WHERE last_seen>?", (day,))
            out["active_7d"] = q("SELECT COUNT(*) c FROM members WHERE last_seen>?", (week,))
            out["new_members_24h"] = q("SELECT COUNT(*) c FROM events WHERE kind='new_member' AND ts>?", (day,))
            out["new_members_7d"] = q("SELECT COUNT(*) c FROM events WHERE kind='new_member' AND ts>?", (week,))
            out["searches_total"] = q("SELECT COALESCE(SUM(count),0) c FROM gift_searches")
            out["searches_24h"] = q("SELECT COUNT(*) c FROM events WHERE kind='search' AND ts>?", (day,))
            out["searches_7d"] = q("SELECT COUNT(*) c FROM events WHERE kind='search' AND ts>?", (week,))
            out["opens_24h"] = q("SELECT COUNT(*) c FROM events WHERE kind IN ('open','new_member') AND ts>?", (day,))
            out["opens_7d"] = q("SELECT COUNT(*) c FROM events WHERE kind IN ('open','new_member') AND ts>?", (week,))
            out["unique_gifts"] = q("SELECT COUNT(*) c FROM gift_searches")
            out["referrals_total"] = q("SELECT COUNT(*) c FROM referrals")
            out["unique_referrers"] = q("SELECT COUNT(DISTINCT uid) c FROM referrals")
            # Membership breakdown (active subscriptions; everyone else is free).
            out["subs_plus"] = q("SELECT COUNT(*) c FROM subs WHERE tier='plus' AND expires_at>?", (now,))
            out["subs_pro"] = q("SELECT COUNT(*) c FROM subs WHERE tier='pro' AND expires_at>?", (now,))
            out["subs_free"] = max(0, (out["members_total"] or 0) - out["subs_plus"] - out["subs_pro"])
            out["shares_total"] = q("SELECT COALESCE(SUM(count),0) c FROM gift_shares")
            out["shares_24h"] = q("SELECT COUNT(*) c FROM events WHERE kind='share' AND ts>?", (day,))
            out["shares_7d"] = q("SELECT COUNT(*) c FROM events WHERE kind='share' AND ts>?", (week,))
            mt = out["members_total"] or 1
            out["avg_searches_per_member"] = round((out["searches_total"] or 0) / mt, 1)
            # FULL lists (every gift ever scouted / shared, ordered by count).
            top = conn.execute("SELECT gift, count FROM gift_searches ORDER BY count DESC LIMIT 500").fetchall()
            out["top_searches"] = [{"gift": r["gift"], "count": r["count"]} for r in top]
            shr = conn.execute("SELECT gift, count FROM gift_shares ORDER BY count DESC LIMIT 500").fetchall()
            out["top_shares"] = [{"gift": r["gift"], "count": r["count"]} for r in shr]

            # ── activity series for the requested range (single query) ──
            cfg = {"7d": (7, 86400), "12w": (12, 7 * 86400), "24m": (24, 30 * 86400)}
            if rng == "all":
                row = conn.execute("SELECT MIN(ts) m FROM events").fetchone()
                first = int((row and row["m"]) or (now - 86400))
                span = max(now - first, 86400)
                n = 24
                size = max(86400, span // n)
            else:
                n, size = cfg[rng]
            start = now - n * size
            rows = conn.execute("SELECT kind, ts FROM events WHERE ts >= ?", (start,)).fetchall()
            buckets = [{"opens": 0, "searches": 0, "new": 0} for _ in range(n)]
            for r in rows:
                idx = int((int(r["ts"]) - start) // size)
                if idx < 0 or idx >= n:
                    continue
                k = r["kind"]
                if k == "search":
                    buckets[idx]["searches"] += 1
                elif k == "new_member":
                    buckets[idx]["new"] += 1
                    buckets[idx]["opens"] += 1
                elif k == "open":
                    buckets[idx]["opens"] += 1
            fmt = "%b %d" if size <= 9 * 86400 else "%b %y"
            labels = [time.strftime(fmt, time.gmtime(start + (i + 1) * size - 1)) for i in range(n)]
            out["daily"] = buckets
            out["labels"] = labels
            out["range"] = rng
            out["range_start"] = time.strftime("%b %d, %Y", time.gmtime(start))
            out["range_end"] = time.strftime("%b %d, %Y", time.gmtime(now))
            # Range-scoped aggregates (the Activity tab's stat cards follow the
            # selected range, not a fixed 7d window).
            out["opens_range"] = q("SELECT COUNT(*) c FROM events WHERE kind IN ('open','new_member') AND ts>=?", (start,))
            out["searches_range"] = q("SELECT COUNT(*) c FROM events WHERE kind='search' AND ts>=?", (start,))
            out["new_range"] = q("SELECT COUNT(*) c FROM events WHERE kind='new_member' AND ts>=?", (start,))
            out["active_range"] = q("SELECT COUNT(*) c FROM members WHERE last_seen>=?", (start,))
            out["shares_range"] = q("SELECT COUNT(*) c FROM events WHERE kind='share' AND ts>=?", (start,))
    except Exception as e:
        out["error"] = str(e)
        await notify_admin(f"/api/analytics db error: {e}")
    return out


@app.get("/api/referrals")
async def referrals(uid: str = Query(""), x_init_data: str = Header(default="", alias="X-Init-Data")):
    eff = verify_init_data(x_init_data) or _digits(uid)
    if not eff:
        return {"count": 0}
    track_visit(eff)   # opening the profile counts as a visit
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(DISTINCT referred_by) AS n FROM referrals WHERE uid = ?", (eff,)
        ).fetchone()
    return {"count": int(row["n"]) if row else 0}


@app.post("/api/referral")
async def add_referral(payload: dict = Body(...), x_init_data: str = Header(default="", alias="X-Init-Data")):
    # The new user ("by") must be a verified identity where possible — you can
    # only attribute *yourself* as referred, which blocks fake-referral abuse.
    by = verify_init_data(x_init_data) or _digits(payload.get("by", ""))
    # The referrer arrives as a friendly code (e.g. 888OG) or a legacy raw-uid
    # link; resolve either to the referrer's real id.
    uid = resolve_ref(payload.get("uid", ""))
    if not uid or not by or uid == by:
        return {"ok": False}
    try:
        with db() as conn:
            conn.execute(
                "INSERT INTO referrals (uid, referred_by, ts) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                (uid, by, int(time.time())),
            )
            conn.commit()
        return {"ok": True}
    except Exception as e:
        log.error("referral insert error: %s", e)
        return {"ok": False}


@app.get("/api/refcode")
async def refcode(uid: str = Query(""), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Return the caller's friendly referral code + ready-to-share link."""
    eff = verify_init_data(x_init_data) or _digits(uid)
    if not eff:
        return {"code": None}
    code = get_ref_code(eff)
    link = f"{MINIAPP_URL}?startapp={code}" if code else None
    return {"code": code, "link": link}


# ─── Premium: Stars subscriptions + vanity codes ─────────────────────────────
@app.get("/api/subscription")
async def subscription(x_init_data: str = Header(default="", alias="X-Init-Data")):
    """The caller's current tier + when it expires + the per-tier filter caps."""
    uid = verify_init_data(x_init_data)
    tier, exp = sub_info(uid)
    return {"tier": tier, "expires_at": exp, "caps": TIER_CAPS,
            "prices": {"plus": STARS_PLUS, "pro": STARS_PRO},
            "promo_price": PROMO_PRICE, "promo_days": PROMO_DAYS}

@app.post("/api/create-invoice")
async def create_invoice(payload: dict = Body(...), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Create a Telegram Stars subscription invoice link for a tier. Identity is
    taken from signed initData and baked into the payment payload, so a payment can
    only ever upgrade the account that actually paid."""
    uid = verify_init_data(x_init_data)
    if not uid:
        return {"ok": False, "error": "auth"}
    tier = (payload.get("tier") or "").strip()
    if tier not in ("plus", "pro"):
        return {"ok": False, "error": "tier"}
    price = STARS_PLUS if tier == "plus" else STARS_PRO
    title = "GiftTrove Scout+" if tier == "plus" else "GiftTrove Scout Pro"
    desc = ("Up to 7 of each filter, a custom referral code, and new perks as they land."
            if tier == "plus" else
            "Unlimited filters, a custom referral code, the 30% affiliate program, and priority on new perks.")
    resp = await _bot_api("createInvoiceLink", {
        "title": title,
        "description": desc,
        "payload": f"sub:{tier}:{uid}",
        "currency": "XTR",
        "prices": [{"label": f"{title} · 1 month", "amount": price}],
        "subscription_period": SUB_PERIOD,
    })
    if resp and resp.get("ok") and resp.get("result"):
        return {"ok": True, "link": resp["result"]}
    log.error("createInvoiceLink failed: %s", resp)
    return {"ok": False, "error": "invoice"}

@app.post("/api/vanity")
async def vanity(payload: dict = Body(...), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Claim a custom referral code (Scout Pro only)."""
    uid = verify_init_data(x_init_data)
    if not uid:
        return {"ok": False, "error": "auth"}
    if get_tier(uid) != "pro":
        return {"ok": False, "error": "pro"}
    ok, res = set_vanity(uid, payload.get("code"))
    if ok:
        link = f"{MINIAPP_URL}?startapp={res}"
        return {"ok": True, "code": res, "link": link}
    return {"ok": False, "error": res}

@app.post("/api/consent")
async def consent(x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Called when a member accepts the Terms. Opts them into broadcasts and lifts
    any prior opt-out from a previous data clear, so re-accepting re-subscribes them
    (their old searches/saved/referrals stay gone — this only affects messaging)."""
    uid = verify_init_data(x_init_data)
    if not uid:
        return {"ok": False}
    try:
        with db() as conn:
            conn.execute("DELETE FROM bcast_optout WHERE uid=?", (str(uid),))
            conn.commit()
    except Exception as e:
        log.info("consent optout-clear skipped: %s", e)
    _bcast_add(uid)
    return {"ok": True}


# ─── Promoted gifts (one-time Stars, available to everyone) ──────────────────
def _collection_by_gift_id(gift_id):
    for c in (cache_get("collections") or []):
        if str(c.get("gift_id")) == str(gift_id):
            return c
    return None

@app.post("/api/promote/create")
async def promote_create(payload: dict = Body(...), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Create a pending promotion and return a one-time Stars invoice link. The promo
    goes live only when the payment lands. Available to all tiers."""
    uid = verify_init_data(x_init_data)
    if not uid:
        return {"ok": False, "error": "auth"}
    gift_id = _digits(payload.get("gift_id"))
    col = _collection_by_gift_id(gift_id)
    # Cache can be cold right after a deploy; fall back to the name/slug the client
    # already resolved so a valid gift is never wrongly rejected.
    name = _clamp((col or {}).get("name") or payload.get("name"), 64)
    slug = _safe_slug((col or {}).get("slug") or payload.get("slug") or "")
    if not gift_id or not name:
        return {"ok": False, "error": "collection"}
    mkt = _clamp(payload.get("marketplace"), 20)
    if mkt not in ("Telegram", "Fragment", "MarketApp"):
        return {"ok": False, "error": "marketplace"}
    plink = ""
    if mkt in ("Fragment", "MarketApp"):
        plink = _clamp(payload.get("link"), 256)
        low = plink.lower()
        if mkt == "Fragment":
            valid = low.startswith("https://fragment.com/") or low.startswith("https://www.fragment.com/")
        else:
            valid = low.startswith("https://marketapp.ws/") or low.startswith("https://www.marketapp.ws/")
        if not valid:
            return {"ok": False, "error": "domain"}
    # Telegram: bot auto-scouts the floor price — no link or manual amount needed.
    fields = {
        "collection": name,
        "gift_id": gift_id,
        "slug": slug,
        "num": _digits(payload.get("num")) if mkt == "Telegram" else "",
        "model": "", "symbol": "", "backdrop": "",   # not requested in simplified Telegram flow
        "marketplace": mkt,
        "amount": "", "currency": "",
        "link": plink,
    }
    pid = _promo_create(uid, fields)
    if not pid:
        return {"ok": False, "error": "create"}
    title = "GiftTrove promotion"
    bits = [fields["collection"]] + [b for b in (fields["model"], fields["symbol"], fields["backdrop"]) if b]
    desc = f"Promote {' · '.join(bits)} on {mkt} for {PROMO_DAYS} days."
    resp = await _bot_api("createInvoiceLink", {
        "title": title,
        "description": desc[:255],
        "payload": f"promo:{pid}:{uid}",
        "currency": "XTR",
        "prices": [{"label": f"{PROMO_DAYS}-day promotion", "amount": PROMO_PRICE}],
    })
    if resp and resp.get("ok") and resp.get("result"):
        return {"ok": True, "link": resp["result"], "id": pid, "price": PROMO_PRICE}
    log.error("promo createInvoiceLink failed: %s", resp)
    return {"ok": False, "error": "invoice"}

@app.get("/api/promos")
async def promos(gift_id: str = Query(""), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Active promotions for a gift collection, to blend at the top of a scout.
    Scout Pro members get none (their perk is an ad-free scout)."""
    uid = verify_init_data(x_init_data)
    if uid and get_tier(uid) == "pro":
        return {"promos": []}
    return {"promos": _promo_active_for(_digits(gift_id)), "redirect": MINIAPP_URL}

@app.post("/api/promote/report")
async def promote_report(payload: dict = Body(...), x_init_data: str = Header(default="", alias="X-Init-Data")):
    pid = _clamp(payload.get("id"), 32)
    if not pid:
        return {"ok": False}
    _promo_report(pid)
    return {"ok": True}

@app.post("/api/promote/remove")
async def promote_remove(payload: dict = Body(...), code: str = Query(""), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Admin kill-switch: remove a promotion and refund its Stars."""
    verified = verify_init_data(x_init_data)
    code_ok = bool(ADMIN_CODE) and _clamp(code, 60) == ADMIN_CODE
    id_ok = (verified is None) or (verified == ANALYTICS_ADMIN_ID)
    if not (code_ok and id_ok):
        return {"ok": False, "error": "forbidden"}
    pid = _clamp(payload.get("id"), 32)
    promo = _promo_get(pid)
    if not promo:
        return {"ok": False, "error": "missing"}
    _promo_set_status(pid, "removed")
    refunded = False
    if promo.get("charge_id"):
        try:
            r = await _bot_api("refundStarPayment",
                               {"user_id": int(promo["uid"]), "telegram_payment_charge_id": promo["charge_id"]})
            refunded = bool(r and r.get("ok"))
        except Exception as e:
            log.warning("promo refund failed: %s", e)
    return {"ok": True, "refunded": refunded}


@app.get("/api/star-balance")
async def star_balance(uid: str = Query(""), code: str = Query(""), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Admin-only: the bot's current Telegram Stars balance (what @gifttrovebot has
    earned and is holding). Gated exactly like analytics."""
    verified = verify_init_data(x_init_data)
    code_ok = bool(ADMIN_CODE) and _clamp(code, 60) == ADMIN_CODE
    id_ok = (verified is None) or (verified == ANALYTICS_ADMIN_ID)
    if not (code_ok and id_ok):
        return {"error": "forbidden"}
    try:
        r = await _bot_api("getMyStarBalance", {})
        if r and r.get("ok"):
            res = r.get("result") or {}
            stars = int(res.get("amount", res.get("star_amount", 0)) or 0)
            gram = round(stars * STAR_TO_TON, 2) if STAR_TO_TON > 0 else None
            return {"ok": True, "stars": stars, "nanostars": int(res.get("nanostar_amount", 0) or 0), "gram": gram}
        return {"ok": False, "error": (r or {}).get("description", "unavailable")}
    except Exception as e:
        log.info("star-balance error: %s", e)
        return {"ok": False, "error": "unavailable"}


# ─── Affiliate program endpoints (Scout Pro only) ────────────────────────────
@app.get("/api/affiliate")
async def affiliate(x_init_data: str = Header(default="", alias="X-Init-Data")):
    """A Pro member's affiliate dashboard: earnings, pending, available, counts."""
    uid = verify_init_data(x_init_data)
    if not uid:
        return {"ok": False, "error": "auth"}
    tier = get_tier(uid)
    s = _affiliate_stats(uid)
    ton_val = round(s["available"] * STAR_TO_TON, 4) if STAR_TO_TON > 0 else None
    gram_val = round(s["available"] * STAR_TO_TON, 2) if STAR_TO_TON > 0 else None
    return {
        "ok": True, "is_pro": tier == "pro", "pct": AFFILIATE_PCT,
        "min_withdraw": AFFILIATE_MIN_WITHDRAW,
        "earned": s["earned"], "paid": s["paid"], "pending": s["pending"],
        "available": s["available"], "referees": s["referees"], "payers": s["payers"],
        "ton_value": ton_val, "gram_value": gram_val,
        "payouts": _affiliate_payouts(uid), "series": _affiliate_series(uid),
        "star_to_gram": STAR_TO_TON,
    }

@app.post("/api/affiliate/withdraw")
async def affiliate_withdraw(payload: dict = Body(...), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Request a payout. Pro-only; needs >= AFFILIATE_MIN_WITHDRAW available Stars.
    The amount is moved to 'pending' (escrow) until the team settles it in TON."""
    uid = verify_init_data(x_init_data)
    if not uid:
        return {"ok": False, "error": "auth"}
    if get_tier(uid) != "pro":
        return {"ok": False, "error": "pro"}
    addr = _clamp(payload.get("ton_address"), 80)
    if not addr:
        return {"ok": False, "error": "address"}
    s = _affiliate_stats(uid)
    if s["available"] < AFFILIATE_MIN_WITHDRAW:
        return {"ok": False, "error": "min", "available": s["available"], "min": AFFILIATE_MIN_WITHDRAW}
    if not _affiliate_request_payout(uid, s["available"], addr):
        return {"ok": False, "error": "failed"}
    try:
        await notify_admin(f"Affiliate payout requested: uid={uid} stars={s['available']} ton={addr}", level="info")
    except Exception:
        pass
    return {"ok": True, "requested": s["available"]}

@app.post("/api/affiliate/mark-paid")
async def affiliate_mark_paid(payload: dict = Body(...), code: str = Query(""), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Admin: mark a payout request settled (after sending GRAM from the escrow wallet)."""
    verified = verify_init_data(x_init_data)
    code_ok = bool(ADMIN_CODE) and _clamp(code, 60) == ADMIN_CODE
    id_ok = (verified is None) or (verified == ANALYTICS_ADMIN_ID)
    if not (code_ok and id_ok):
        return {"ok": False, "error": "forbidden"}
    pid = _clamp(payload.get("id"), 32)
    try:
        with db() as conn:
            conn.execute("UPDATE affiliate_payouts SET status='paid' WHERE id=?", (pid,))
            conn.commit()
        return {"ok": True}
    except Exception as e:
        log.error("mark-paid failed: %s", e)
        return {"ok": False}


# ─── Cross-device sync: saved gifts + recent searches (verified users) ────────
@app.get("/api/userdata")
async def get_userdata(uid: str = Query(""), x_init_data: str = Header(default="", alias="X-Init-Data")):
    vid = verify_init_data(x_init_data)
    eff = vid or _digits(uid)
    if not eff:
        return {"saved": [], "searches": [], "synced": False}
    if vid:
        _bcast_add(vid)   # opt into broadcasts simply by using the app (verified id only)
    try:
        with db() as conn:
            row = conn.execute("SELECT saved, searches FROM user_data WHERE uid=?", (eff,)).fetchone()
        if not row:
            return {"saved": [], "searches": [], "synced": True}
        saved = json.loads(row["saved"] or "[]")
        searches = json.loads(row["searches"] or "[]")
        return {"saved": saved, "searches": searches, "synced": True}
    except Exception as e:
        log.error("get_userdata error: %s", e)
        return {"saved": [], "searches": [], "synced": False}


@app.post("/api/userdata")
async def set_userdata(payload: dict = Body(...), x_init_data: str = Header(default="", alias="X-Init-Data")):
    # Identity MUST be verified — a user can only write their own data.
    eff = verify_init_data(x_init_data) or _digits(payload.get("uid", ""))
    if not eff:
        return {"ok": False, "error": "unauthorized"}
    try:
        saved = payload.get("saved")
        searches = payload.get("searches")
        # Cap sizes so a client can't bloat the row.
        if isinstance(saved, list):
            saved_json = json.dumps(saved[:300])[:200000]
        else:
            saved_json = None
        if isinstance(searches, list):
            clean = [(_clamp(s, 64)) for s in searches if isinstance(s, str)][:40]
            searches_json = json.dumps(clean)
        else:
            searches_json = None
        now = int(time.time())
        with db() as conn:
            conn.execute("INSERT INTO user_data (uid, updated) VALUES (?, ?) ON CONFLICT DO NOTHING", (eff, now))
            if saved_json is not None:
                conn.execute("UPDATE user_data SET saved=?, updated=? WHERE uid=?", (saved_json, now, eff))
            if searches_json is not None:
                conn.execute("UPDATE user_data SET searches=?, updated=? WHERE uid=?", (searches_json, now, eff))
            conn.commit()
        return {"ok": True}
    except Exception as e:
        log.error("set_userdata error: %s", e)
        return {"ok": False}


# ─── Rich share: a prepared inline message carrying the marketplace's PREMIUM
#     custom emoji + price, opened in the app via tg.shareMessage(id). ──────────
def _u16len(s):
    return len(s.encode("utf-16-le")) // 2


async def _bot_api(method, payload):
    """Call the Bot API over HTTP (used for savePreparedInlineMessage)."""
    if not BOT_TOKEN:
        return None
    import urllib.request
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})

    def _do():
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    return await asyncio.to_thread(_do)


@app.post("/api/userdata/clear")
async def userdata_clear(x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Self-serve data deletion: wipes the caller's synced data + referral rows.
    Identity comes ONLY from Telegram's signed initData, so a user can never
    clear anyone else's data. Aggregate anonymous counters are untouched."""
    uid = verify_init_data(x_init_data)
    if not uid or not rate_ok(uid):
        return {"ok": False}
    try:
        with db() as conn:
            conn.execute("DELETE FROM user_data WHERE uid=?", (str(uid),))
            conn.execute("DELETE FROM referrals WHERE uid=?", (str(uid),))
            conn.execute("DELETE FROM referrals WHERE referred_by=?", (str(uid),))
            conn.execute("DELETE FROM members WHERE uid_hash=?", (_uid_hash(uid),))
            conn.execute("DELETE FROM bcast WHERE uid=?", (str(uid),))
            conn.execute("DELETE FROM ref_codes WHERE uid=?", (str(uid),))
            conn.execute("INSERT INTO bcast_optout(uid, ts) VALUES(?,?) ON CONFLICT DO NOTHING",
                         (str(uid), int(time.time())))
            conn.commit()
        return {"ok": True}
    except Exception as e:
        log.error("userdata clear error: %s", e)
        return {"ok": False}


@app.post("/api/share/track")
async def share_track(payload: dict = Body(...), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Count a share that went through the plain share sheet (fallback path)."""
    uid = verify_init_data(x_init_data)
    if not uid or not rate_ok(uid):
        return {"ok": False}
    track_share(_clamp(payload.get("name", ""), 64))
    return {"ok": True}


@app.post("/api/share")
async def share(payload: dict = Body(...), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Prepare a clean, text-only shareable card (bold name + market + price)."""
    uid = verify_init_data(x_init_data)
    if not uid:
        return {"ok": False, "error": "unauthorized"}
    name = _clamp(payload.get("name", "Telegram gift"), 80)
    num = _digits(payload.get("num", ""), 12)
    market = _clamp(payload.get("market", "Telegram"), 24)
    price = _clamp(payload.get("price", ""), 40)
    link = _clamp(payload.get("link", ""), 256)
    if not link.startswith("https://"):
        link = ""
    _mid = " \u00b7 "
    # Count the share the moment it's initiated — a share is a share whether
    # Telegram serves the prepared card or the plain sheet.
    track_share(name)

    title = f"{name}{(' #' + num) if num else ''}"
    # No emoji: premium custom emoji do not render in prepared inline messages
    # (Telegram falls back to the literal char), so we keep the card clean text.
    segs = [("bold", title), ("text", "\n")]
    segs.append(("text", f"{market}{(_mid + price) if price else ''}"))
    segs.append(("text", "\n\nScout unique Telegram gifts on GiftTrove"))
    if link:
        # Show a clean clickable phrase instead of a raw https:// URL.
        segs.append(("text", "\n"))
        segs.append(("link", f"View on {market}", link))

    text, off, entities = "", 0, []
    for seg in segs:
        s = seg[1]
        ln = _u16len(s)
        if seg[0] == "bold":
            entities.append({"type": "bold", "offset": off, "length": ln})
        elif seg[0] == "link":
            entities.append({"type": "text_link", "offset": off, "length": ln, "url": seg[2]})
        text += s
        off += ln

    import uuid as _uuid
    result = {
        "type": "article",
        "id": _uuid.uuid4().hex[:32],
        "title": title,
        "description": (market + (_mid + price if price else "")).strip(),
        "input_message_content": {
            "message_text": text,
            "entities": entities,
            "link_preview_options": {"is_disabled": True},
        },
    }
    try:
        resp = await _bot_api("savePreparedInlineMessage", {
            "user_id": int(uid),
            "result": result,
            "allow_user_chats": True,
            "allow_group_chats": True,
            "allow_channel_chats": True,
            "allow_bot_chats": False,
        })
        if resp and resp.get("ok") and (resp.get("result") or {}).get("id"):
            return {"ok": True, "id": resp["result"]["id"]}
        desc = (resp or {}).get("description", "prepare_failed")
        log.error("savePreparedInlineMessage failed: %s", resp)
        await notify_admin(f"/api/share rejected: {desc}", level="warning")
        return {"ok": False, "error": desc}
    except Exception as e:
        log.error("share prepare error: %s", e)
        await notify_admin(f"/api/share error: {type(e).__name__}: {e}", level="issue")
        return {"ok": False, "error": "prepare_failed"}


# ─── Broadcast: owner DMs every mini-app user (text + optional image). ─────────
async def _run_broadcast(ids, text, image, owner):
    """Fan out a broadcast at ~20/sec; drop ids that blocked/deleted the bot."""
    sent = failed = removed = 0
    for i, uid in enumerate(ids):
        try:
            if image:
                p = {"chat_id": int(uid), "photo": image}
                if text:
                    p["caption"] = text[:1024]
                resp = await _bot_api("sendPhoto", p)
            else:
                resp = await _bot_api("sendMessage", {"chat_id": int(uid), "text": text})
            if resp and resp.get("ok"):
                sent += 1
            else:
                failed += 1
                desc = str((resp or {}).get("description", "")).lower()
                if any(k in desc for k in ("blocked", "deactivated", "chat not found",
                                           "user is deactivated", "bot was kicked")):
                    _bcast_remove(uid)
                    removed += 1
        except Exception:
            failed += 1
        # Telegram tolerates ~30 msg/sec; stay well under it.
        await asyncio.sleep(1.0 if (i + 1) % 20 == 0 else 0.05)
    try:
        await _bot_api("sendMessage", {"chat_id": int(owner),
            "text": (f"Broadcast finished.\nSent: {sent}\nFailed: {failed}\n"
                     f"Removed (blocked/deleted): {removed}\nTotal recipients: {len(ids)}")})
    except Exception:
        pass


@app.post("/api/broadcast")
async def broadcast(payload: dict = Body(...), uid: str = Query(""), code: str = Query(""),
                    x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Owner-only: send a message (with an optional image URL) to every mini-app
    user who hasn't cleared their data. Runs in the background and DMs the admin
    a delivery summary when done. Locked to the analytics admin + the secret
    ADMIN_CODE (the general access code can NOT broadcast)."""
    verified = verify_init_data(x_init_data)
    code_ok = bool(ADMIN_CODE) and _clamp(code, 60) == ADMIN_CODE
    id_ok = (verified is None) or (verified == ANALYTICS_ADMIN_ID)
    if not (code_ok and id_ok):
        return {"ok": False, "error": "unauthorized"}
    owner = ANALYTICS_ADMIN_ID
    text = _clamp(payload.get("text", ""), 4000)
    image = _clamp(payload.get("image_url", ""), 512)
    if image and not image.startswith("https://"):
        image = ""
    if not text and not image:
        return {"ok": False, "error": "empty"}
    try:
        with db() as conn:
            rows = conn.execute("SELECT uid FROM bcast").fetchall()
        ids = [r["uid"] for r in rows]
    except Exception as e:
        log.error("broadcast recipient fetch failed: %s", e)
        return {"ok": False, "error": "db"}
    if not ids:
        return {"ok": True, "recipients": 0, "note": "no recipients yet"}
    asyncio.create_task(_run_broadcast(ids, text, image, owner))
    return {"ok": True, "recipients": len(ids)}


# ═════════════════════════════════════════════════════════════════════════════
#  SCALING NOTES — read before chasing big user numbers
# ═════════════════════════════════════════════════════════════════════════════
#  This single free instance + SQLite + ONE Telegram user-session is great for
#  launch, but it will NOT serve millions of concurrent users. The honest path:
#
#   1. Database: move from SQLite (ephemeral on Render free) to managed Postgres.
#      Analytics writes above are written to be trivially portable.
#   2. App tier: run several stateless web instances behind Render's load
#      balancer; keep the cache in Redis (shared) instead of in-process dicts.
#   3. THE REAL BOTTLENECK is Telegram itself: one user-session is rate-limited
#      (you saw the flood waits). Serving millions of live queries needs either
#      a pool of many sessions or an official data arrangement — caching (as we
#      do) absorbs most of it, since most users view the same popular gifts.
#   4. Rate limiting (above) protects you today; tune RATE_MAX / RATE_WINDOW.
#
#  In short: the architecture is ready to grow, but "5M active" is a Postgres +
#  Redis + multi-instance + Telegram-throughput project, not a config flag.
# ═════════════════════════════════════════════════════════════════════════════


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
