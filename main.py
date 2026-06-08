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
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
DB_PATH = os.getenv("DB_PATH", "gifttrove.db")
SEARCH_LIMIT = int(os.getenv("SEARCH_LIMIT", "100"))      # per page
SEARCH_MAX = int(os.getenv("SEARCH_MAX", "1000"))         # hard ceiling per query
MTPROTO_TIMEOUT = int(os.getenv("MTPROTO_TIMEOUT", "18"))   # seconds per call

# ─── Access gate ──────────────────────────────────────────────────────────────
# Admins bypass automatically; everyone else needs the access code. BOTH live in
# env vars so only the operator can change them (never hard-coded in the client).
ADMIN_IDS = {s.strip() for s in os.getenv("ADMIN_IDS", "7608551523").split(",") if s.strip()}
ACCESS_CODE = os.getenv("ACCESS_CODE", "8f70p").strip()

# ─── Rate limiting (protects the backend from abuse / accidental hammering) ────
RATE_WINDOW = int(os.getenv("RATE_WINDOW", "60"))   # seconds
RATE_MAX = int(os.getenv("RATE_MAX", "40"))         # requests per window per client

_mtproto_error = ""
FRAGMENT_CDN = "https://nft.fragment.com/gift"

# ─── Bot (/start handler) ─────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WELCOME_IMAGE = os.getenv("WELCOME_IMAGE", "https://i.ibb.co/5Xmf7H6b/Inria-Serif-1.png")
WELCOME_TEXT = os.getenv(
    "WELCOME_TEXT",
    "**Welcome to GiftTrove! Scout unique Telegram gifts from different "
    "marketplaces all at a go.**\n\n**GiftTrove**",
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


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _uid_hash(uid):
    """Store a salted hash, never the raw Telegram id (privacy by design)."""
    if not uid:
        return "anon"
    return hashlib.sha256(f"gt::{uid}".encode()).hexdigest()[:24]


def init_db():
    with db() as conn:
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
        # Lightweight event log (daily rollups are derived from this).
        conn.execute(
            """CREATE TABLE IF NOT EXISTS events (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   kind TEXT NOT NULL, ts INTEGER NOT NULL)"""
        )
        conn.commit()


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
    try:
        with db() as conn:
            conn.execute(
                """INSERT INTO gift_searches(gift, count, last_ts) VALUES(?,1,?)
                   ON CONFLICT(gift) DO UPDATE SET count=count+1, last_ts=excluded.last_ts""",
                (gift.strip()[:64], now),
            )
            conn.execute("INSERT INTO events(kind, ts) VALUES('search', ?)", (now,))
            conn.commit()
    except Exception as e:
        log.info("track_search skipped: %s", e)


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


# ─── Telethon client lifecycle ────────────────────────────────────────────────
client = None
bot = None


async def _register_bot_handlers():
    if not bot:
        return
    from telethon import events, Button

    @bot.on(events.NewMessage(pattern=r"^/start"))
    async def _start(event):
        try:
            buttons = [
                [Button.url(" Open GiftTrove", MINIAPP_URL)],
                [Button.url(" Join Community", COMMUNITY_URL)],
            ]
            await event.respond(WELCOME_TEXT, file=WELCOME_IMAGE, buttons=buttons)
        except Exception as e:
            log.error("/start failed: %s", e)
            try:
                await event.respond(WELCOME_TEXT)
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

    # 2b) Pre-warm the gift cache so the very first visitor sees gifts instantly.
    if client is not None:
        try:
            await featured()
            log.info("Featured gifts pre-warmed.")
        except Exception as e:
            log.info("featured pre-warm skipped: %s", e)

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
    allow_methods=["*"],
    allow_headers=["*"],
)


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
        out = []
        for g in raw:
            title = getattr(g, "title", None) or f"Gift {getattr(g, 'id', '')}"
            out.append({
                "name": title,
                "slug": getattr(g, "slug", None) or _slug_from_title(title),
                "gift_id": str(getattr(g, "id", "") or ""),
                "supply": getattr(g, "availability_total", None) or getattr(g, "availability_issued", None) or 0,
                "preview": "",
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


@app.get("/api/attributes")
async def attributes(gift_id: str = Query(...)):
    key = f"attrs:{gift_id}"
    cached = cache_get(key)
    if cached:
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
        for a in getattr(res, "attributes", []) or []:
            cls = type(a).__name__
            name = getattr(a, "name", None)
            rar = getattr(a, "rarity_permille", None)
            rar = round(rar / 10, 2) if isinstance(rar, (int, float)) else None
            if cls == "StarGiftAttributeModel":
                models.append({"name": name, "rarity": rar})
            elif cls == "StarGiftAttributePattern":
                symbols.append({"name": name, "rarity": rar})
            elif cls == "StarGiftAttributeBackdrop":
                backdrops.append({"name": name, "hex": color_hex(getattr(a, "center_color", None)), "rarity": rar})
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
    if gift:
        track_search(gift)

    want = set([m.strip() for m in markets.split(",") if m.strip()]) if markets else set()
    results = []
    next_offset = ""
    limit = max(1, min(int(limit or SEARCH_LIMIT), SEARCH_MAX))
    GetResale = _payments("GetResaleStarGiftsRequest")

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
                    offset=c, limit=p,
                ))
                chunk = getattr(res, "gifts", []) or []
                for g in chunk:
                    item = serialize_unique(g)
                    if num and str(item.get("num")) != str(num):
                        continue
                    if model and (item.get("model") or "").lower() != model.lower():
                        continue
                    if symbol and (item.get("symbol") or "").lower() != symbol.lower():
                        continue
                    if backdrop and (item.get("backdrop") or "").lower() != backdrop.lower():
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
async def access(uid: str = Query(""), code: str = Query("")):
    """
    Gate the app. Admins are allowed automatically; everyone else must supply
    the access code. The code + admin list live in env vars on the server, so
    they CANNOT be changed from the client — only the operator can rotate them.
    Also records the (anonymous) visit for analytics on success.
    """
    is_admin = str(uid).strip() in ADMIN_IDS
    ok = is_admin or (code.strip() == ACCESS_CODE and ACCESS_CODE != "")
    if ok:
        new_member = track_visit(uid)
        return {"ok": True, "admin": is_admin, "new_member": new_member}
    return {"ok": False, "admin": False}


@app.get("/api/analytics")
async def analytics(uid: str = Query(""), code: str = Query("")):
    """Admin-only product analytics. No personal data is stored or returned."""
    if str(uid).strip() not in ADMIN_IDS and code.strip() != ACCESS_CODE:
        return {"error": "forbidden"}
    now = int(time.time())
    day = now - 86400
    week = now - 7 * 86400
    out = {}
    try:
        with db() as conn:
            out["members_total"] = conn.execute("SELECT COUNT(*) c FROM members").fetchone()["c"]
            out["returning_members"] = conn.execute("SELECT COUNT(*) c FROM members WHERE visits>1").fetchone()["c"]
            out["active_24h"] = conn.execute("SELECT COUNT(*) c FROM members WHERE last_seen>?", (day,)).fetchone()["c"]
            out["active_7d"] = conn.execute("SELECT COUNT(*) c FROM members WHERE last_seen>?", (week,)).fetchone()["c"]
            out["new_members_24h"] = conn.execute("SELECT COUNT(*) c FROM events WHERE kind='new_member' AND ts>?", (day,)).fetchone()["c"]
            out["searches_total"] = conn.execute("SELECT COALESCE(SUM(count),0) c FROM gift_searches").fetchone()["c"]
            out["searches_24h"] = conn.execute("SELECT COUNT(*) c FROM events WHERE kind='search' AND ts>?", (day,)).fetchone()["c"]
            out["opens_24h"] = conn.execute("SELECT COUNT(*) c FROM events WHERE kind IN ('open','new_member') AND ts>?", (day,)).fetchone()["c"]
            top = conn.execute("SELECT gift, count FROM gift_searches ORDER BY count DESC LIMIT 15").fetchall()
            out["top_searches"] = [{"gift": r["gift"], "count": r["count"]} for r in top]
    except Exception as e:
        out["error"] = str(e)
    return out


@app.get("/api/referrals")
async def referrals(uid: str = Query(...)):
    track_visit(uid)   # opening the profile counts as a visit
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(DISTINCT referred_by) AS n FROM referrals WHERE uid = ?", (str(uid),)
        ).fetchone()
    return {"count": int(row["n"]) if row else 0}


@app.post("/api/referral")
async def add_referral(payload: dict = Body(...)):
    uid = str(payload.get("uid", "")).strip()
    by = str(payload.get("by", "")).strip()
    if not uid or not by or uid == by:
        return {"ok": False}
    try:
        with db() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO referrals (uid, referred_by, ts) VALUES (?, ?, ?)",
                (uid, by, int(time.time())),
            )
            conn.commit()
        return {"ok": True}
    except Exception as e:
        log.error("referral insert error: %s", e)
        return {"ok": False}


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
