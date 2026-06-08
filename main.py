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
SEARCH_LIMIT = int(os.getenv("SEARCH_LIMIT", "30"))
MTPROTO_TIMEOUT = int(os.getenv("MTPROTO_TIMEOUT", "18"))   # seconds per call

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


# ─── SQLite referrals ─────────────────────────────────────────────────────────
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS referrals (
                   uid TEXT NOT NULL,
                   referred_by TEXT NOT NULL,
                   ts INTEGER NOT NULL,
                   PRIMARY KEY (uid, referred_by)
               )"""
        )
        conn.commit()


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
                [Button.url("🎁 Open GiftTrove", MINIAPP_URL)],
                [Button.url("💬 Join Community", COMMUNITY_URL)],
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

    # 3) Keepalive: ping Telegram every 2 min so the socket never goes stale.
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
):
    want = set([m.strip() for m in markets.split(",") if m.strip()]) if markets else set()
    results = []
    GetResale = _payments("GetResaleStarGiftsRequest")
    if client is not None and GetResale and gift_id and (not want or "Telegram" in want):
        try:
            res = await _invoke(lambda: GetResale(gift_id=int(gift_id), attributes_hash=0,
                                                  sort_by_price=True, offset="", limit=SEARCH_LIMIT))
            for g in getattr(res, "gifts", []) or []:
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
        except Exception as e:
            log.error("native search error: %s", e)
    if gift and (not want or "GetGems" in want):
        results.extend(await getgems_search(gift, limit=12))
    return {"results": results}


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


@app.get("/api/referrals")
async def referrals(uid: str = Query(...)):
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
