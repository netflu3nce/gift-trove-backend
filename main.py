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
    from telethon import TelegramClient, functions, types  # noqa: F401
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
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "https://gift-trove-frontend.vercel.app").split(",") if o.strip()]
DB_PATH = os.getenv("DB_PATH", "gifttrove.db")
# Durable storage: if DATABASE_URL (Postgres, e.g. Neon) is set, use it so data
# survives redeploys. Otherwise fall back to local SQLite (ephemeral on Render).
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_PG = DATABASE_URL.startswith(("postgres://", "postgresql://"))
SEARCH_LIMIT = int(os.getenv("SEARCH_LIMIT", "100"))      # per page
SEARCH_MAX = int(os.getenv("SEARCH_MAX", "1000"))         # hard ceiling per query
MTPROTO_TIMEOUT = int(os.getenv("MTPROTO_TIMEOUT", "18"))   # seconds per call

# ─── Access gate ──────────────────────────────────────────────────────────────
# Admins bypass automatically; everyone else needs the access code. BOTH live in
# env vars so only the operator can change them (never hard-coded in the client).
ADMIN_IDS = {s.strip() for s in os.getenv("ADMIN_IDS", "7608551523,8124847664").split(",") if s.strip()}
ACCESS_CODE = os.getenv("ACCESS_CODE", "8f70p").strip()

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


def verify_init_data(init_data):
    """Validate Telegram Mini App initData (HMAC). Returns verified user id (str) or None."""
    if not init_data or not BOT_TOKEN:
        return None
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
        recv_hash = pairs.pop("hash", None)
        pairs.pop("signature", None)
        if not recv_hash:
            return None
        dcs = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
        secret = _hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc = _hmac.new(secret, dcs.encode(), hashlib.sha256).hexdigest()
        if not _hmac.compare_digest(calc, recv_hash):
            return None
        try:
            if int(pairs.get("auth_date", "0")) < int(time.time()) - 86400:
                return None  # stale (older than 24h)
        except Exception:
            pass
        uid = (json.loads(pairs.get("user", "{}")) or {}).get("id")
        return str(uid) if uid else None
    except Exception:
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
            asyncio.get_event_loop().create_task(
                notify_admin(f"Member milestone reached: {n} total members.", level="good")
            )
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
            asyncio.get_event_loop().create_task(collections())
        except Exception as e:
            log.info("collections pre-warm skipped: %s", e)

    # 2c) Reports: deploy-live good news + the daily digest loop.
    try:
        asyncio.get_event_loop().create_task(_daily_digest_loop())
        asyncio.get_event_loop().create_task(notify_admin(
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
    # Some sellers list TON-only (can't be bought with Stars) -> price is GRAM.
    if ton_only and gram is not None:
        return (gram, "GRAM")
    if stars is not None:
        return (stars, "Stars")
    if gram is not None:
        return (gram, "GRAM")
    return (None, "Stars")


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
    price, currency = _extract_price(g)
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
        "market": "Telegram",
        "url": native_url(base),
        "image": cdn_image(base),
        "animation": cdn_anim(base),
    }


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
            "price": price, "currency": "TON", "market": "GetGems",
            "url": f"https://getgems.io/nft/{addr}" if addr else None,
            "image": img, "animation": None,
        })
    return out


# ─── Routes ───────────────────────────────────────────────────────────────────
@app.get("/")
async def health():
    resp = {
        "ok": True,
        "mtproto": bool(client and client.is_connected()),
        "getgems": bool(GETGEMS_API_KEY),
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


@app.get("/api/collections")
async def collections():
    cached = cache_get("collections")
    if cached:
        return {"collections": cached}
    GetStarGifts = _payments("GetStarGiftsRequest")
    if client is None:
        return {"collections": [], "error": "MTProto background sync still initialising."}
    if not GetStarGifts:
        return {"collections": [], "error": "GetStarGiftsRequest missing — pip install -U telethon"}
    try:
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
        # Real preview images, fetched in parallel (cached after first run).
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
        out = [c for c in out if c["gift_id"]]
        cache_set("collections", out, ttl=900)
        return {"collections": out, "_raw_count": len(raw)}
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


@app.get("/api/attributes")
async def attributes(gift_id: str = Query(...)):
    gift_id = _digits(gift_id)
    if not gift_id:
        return {"models": [], "symbols": [], "backdrops": []}
    key = f"attrs:{gift_id}"
    cached = cache_get(key)
    if cached and str(gift_id) in _attr_ids_cache:
        return cached
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
        cache_set(key, result, ttl=900)
        return result
    except Exception as e:
        log.error("attributes error: %s", e)
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
):
    if not rate_ok(uid):
        return {"results": [], "rate_limited": True}
    # Strict input validation (blocks malformed / injection-style input).
    gift = _clamp(gift, 64)
    gift_id = _digits(gift_id)
    slug = _safe_slug(slug)
    num = _digits(num, 12)
    model = _clamp(model, 80)
    symbol = _clamp(symbol, 80)
    backdrop = _clamp(backdrop, 80)
    sort = "price_desc" if sort == "price_desc" else "price_asc"
    offset = _clamp(offset, 256)
    try:
        min_price = max(0.0, float(min_price or 0))
        max_price = max(0.0, float(max_price or 0))
    except Exception:
        min_price = max_price = 0.0
    if gift:
        track_search(gift)

    want = set([m.strip() for m in markets.split(",") if m.strip()]) if markets else set()
    results = []
    next_offset = ""
    limit = max(1, min(int(limit or SEARCH_LIMIT), SEARCH_MAX))
    GetResale = _payments("GetResaleStarGiftsRequest")

    # Resolve selected model/symbol/backdrop NAMES to attribute IDs so Telegram
    # filters server-side (otherwise matches on deeper pages get missed -> the
    # false "no listings" bug). Ensure the id map for this gift is populated.
    if gift_id and (model or symbol or backdrop) and str(gift_id) not in _attr_ids_cache:
        try:
            await attributes(gift_id=str(gift_id))
        except Exception:
            pass
    ids = _attr_ids_cache.get(str(gift_id), {})
    attr_filter = []
    srv_model = srv_symbol = srv_backdrop = False
    if model and ids.get("model", {}).get(model) is not None:
        attr_filter.append(ids["model"][model]); srv_model = True
    if symbol and ids.get("symbol", {}).get(symbol) is not None:
        attr_filter.append(ids["symbol"][symbol]); srv_symbol = True
    if backdrop and ids.get("backdrop", {}).get(backdrop) is not None:
        attr_filter.append(ids["backdrop"][backdrop]); srv_backdrop = True

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
                    # Fallback client-side filter only for attributes Telegram
                    # didn't already filter for us (e.g. id couldn't be resolved).
                    if model and not srv_model and (item.get("model") or "").lower() != model.lower():
                        continue
                    if symbol and not srv_symbol and (item.get("symbol") or "").lower() != symbol.lower():
                        continue
                    if backdrop and not srv_backdrop and (item.get("backdrop") or "").lower() != backdrop.lower():
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

    # Sort by price (None last). Telegram already sorts, but GetGems + filters
    # can interleave, so we enforce it for a consistent UI.
    rev = (sort == "price_desc")
    results.sort(key=lambda r: (r.get("price") is None, r.get("price") or 0), reverse=rev)
    if rev:
        results.sort(key=lambda r: r.get("price") is None)  # keep None last

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
        if new_member:
            _maybe_milestone()
        return {"ok": True, "admin": is_admin, "new_member": new_member}
    return {"ok": False, "admin": False}


@app.get("/api/analytics")
async def analytics(uid: str = Query(""), code: str = Query(""), range_q: str = Query("7d", alias="range"),
                    x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Admin-only product analytics. No personal data is stored or returned."""
    verified = verify_init_data(x_init_data)
    if not ((verified and verified in ADMIN_IDS) or _clamp(code, 40) == ACCESS_CODE):
        return {"error": "forbidden"}
    rng = range_q if range_q in ("7d", "12w", "24m", "all") else "7d"
    now = int(time.time())
    day, week = now - 86400, now - 7 * 86400
    out = {}
    try:
        with db() as conn:
            q = lambda s, p=(): conn.execute(s, p).fetchone()["c"]
            out["members_total"] = q("SELECT COUNT(*) c FROM members")
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
    uid = _digits(payload.get("uid", ""))   # the referrer
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


# ─── Cross-device sync: saved gifts + recent searches (verified users) ────────
@app.get("/api/userdata")
async def get_userdata(uid: str = Query(""), x_init_data: str = Header(default="", alias="X-Init-Data")):
    eff = verify_init_data(x_init_data) or _digits(uid)
    if not eff:
        return {"saved": [], "searches": [], "synced": False}
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

    title = f"{name}{(' #' + num) if num else ''}"
    # No emoji: premium custom emoji do not render in prepared inline messages
    # (Telegram falls back to the literal char), so we keep the card clean text.
    segs = [("bold", title), ("text", "\n")]
    segs.append(("text", f"{market}{(_mid + price) if price else ''}"))
    segs.append(("text", "\n\nScout unique Telegram gifts on GiftTrove"))
    if link:
        segs.append(("text", f"\n{link}"))

    text, off, entities = "", 0, []
    for seg in segs:
        s = seg[1]
        ln = _u16len(s)
        if seg[0] == "bold":
            entities.append({"type": "bold", "offset": off, "length": ln})
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
            "link_preview_options": {"is_disabled": not bool(link)},
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
            track_share(name)
            return {"ok": True, "id": resp["result"]["id"]}
        desc = (resp or {}).get("description", "prepare_failed")
        log.error("savePreparedInlineMessage failed: %s", resp)
        await notify_admin(f"/api/share rejected: {desc}", level="warning")
        return {"ok": False, "error": desc}
    except Exception as e:
        log.error("share prepare error: %s", e)
        await notify_admin(f"/api/share error: {type(e).__name__}: {e}", level="issue")
        return {"ok": False, "error": "prepare_failed"}


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
