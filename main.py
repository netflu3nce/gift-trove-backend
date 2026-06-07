"""
GiftTrove backend — FastAPI + Telethon (MTProto user session)
═════════════════════════════════════════════════════════════════════════════
Live Telegram-gift data via the official MTProto "gifts" API
(https://core.telegram.org/api/gifts) plus an OPTIONAL GetGems GraphQL source.

Endpoints (consumed verbatim by App.jsx):
    GET  /api/collections                 -> { collections:[{name,slug,gift_id,supply,preview}] }
    GET  /api/attributes?gift_id=...       -> { models:[{name,rarity}], symbols:[{name,rarity}],
                                                backdrops:[{name,hex,rarity}] }
    GET  /api/search?gift=&gift_id=&slug=&num=&model=&symbol=&backdrop=&markets=
                                          -> { results:[{...listing...}] }
    GET  /api/gift?slug=...                -> { ...unique gift detail... }
    GET  /api/referrals?uid=...            -> { count }
    POST /api/referral { uid, by }         -> { ok }
    GET  /                                 -> health

⚠️  IMPORTANT — these MTProto gift methods are very new. Telethon generates its
    request classes from the TL schema, so you need a RECENT Telethon
    (pip install -U telethon). If a request class below is missing on your
    version, that endpoint returns {"error": "..."} instead of crashing, and the
    log tells you to update. Field names on the returned objects are also new;
    every parser here is defensive and the spots most likely to drift are marked
    with  # VERIFY  so you can confirm against your installed schema.
"""

import os
import time
import json
import asyncio
import sqlite3
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Body
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("gifttrove")

# ─── Telethon (import guarded so the service still boots if it's missing) ─────
try:
    from telethon import TelegramClient, functions, types  # noqa: F401
    from telethon.sessions import StringSession
    TELETHON_OK = True
except Exception as e:  # pragma: no cover
    TELETHON_OK = False
    log.error("Telethon import failed (%s). Install with: pip install -U telethon", e)

try:
    import httpx
    HTTPX_OK = True
except Exception:
    HTTPX_OK = False

# ─── Config ───────────────────────────────────────────────────────────────────
API_ID = os.getenv("API_ID", "")
API_HASH = os.getenv("API_HASH", "")
STRING_SESSION = os.getenv("STRING_SESSION", "")
GETGEMS_API_KEY = os.getenv("GETGEMS_API_KEY", "")
GETGEMS_GRAPHQL = os.getenv("GETGEMS_GRAPHQL", "https://api.getgems.io/graphql")
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
DB_PATH = os.getenv("DB_PATH", "gifttrove.db")
SEARCH_LIMIT = int(os.getenv("SEARCH_LIMIT", "30"))

FRAGMENT_CDN = "https://nft.fragment.com/gift"

# ─── Bot (/start handler) — separate from the user session above ──────────────
# BOT_TOKEN comes from @BotFather. The user session (above) READS gift data;
# this bot only replies to /start. Both run in this one process.
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WELCOME_IMAGE = os.getenv("WELCOME_IMAGE", "https://i.ibb.co/5Xmf7H6b/Inria-Serif-1.png")
WELCOME_TEXT = os.getenv(
    "WELCOME_TEXT",
    "Welcome to GiftTrove! Scout unique Telegram gifts from different "
    "marketplaces all at a go.\n\nGiftTrove",
)
MINIAPP_URL = os.getenv("MINIAPP_URL", "https://t.me/gifttrovebot/app")
COMMUNITY_URL = os.getenv("COMMUNITY_URL", "https://t.me/gifttrove")

# Gifts shown on the app's launch screen (first one is the "hero"). Editable.
FEATURED_NAMES = [n.strip() for n in os.getenv(
    "FEATURED_NAMES", "Plush Pepe,Durov's Cap,Heart Locket").split(",") if n.strip()]

# ─── Tiny TTL cache + a lock to serialise MTProto calls ───────────────────────
_cache: dict[str, tuple[float, object]] = {}
_mtproto_lock = asyncio.Lock()


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
client = None   # user session (reads gift data)
bot = None      # bot (replies to /start)


async def _register_bot_handlers():
    """Wire up the /start command for the BotFather bot."""
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, bot
    init_db()
    if TELETHON_OK and API_ID and API_HASH and STRING_SESSION:
        try:
            client = TelegramClient(StringSession(STRING_SESSION), int(API_ID), API_HASH)
            await client.connect()
            if not await client.is_user_authorized():
                log.error("MTProto session is not authorised — regenerate STRING_SESSION (see gen_session.py).")
            else:
                me = await client.get_me()
                log.info("MTProto session live as @%s", getattr(me, "username", me.id))
        except Exception as e:
            log.error("Failed to start Telethon client: %s", e)
            client = None
    else:
        log.warning("MTProto not configured (need API_ID, API_HASH, STRING_SESSION). Live data disabled.")

    # Start the /start bot (optional — only if BOT_TOKEN is set)
    if TELETHON_OK and API_ID and API_HASH and BOT_TOKEN:
        try:
            bot = TelegramClient(StringSession(), int(API_ID), API_HASH)
            await bot.start(bot_token=BOT_TOKEN)
            await _register_bot_handlers()
            binfo = await bot.get_me()
            log.info("Bot live as @%s — /start is active", getattr(binfo, "username", binfo.id))
        except Exception as e:
            log.error("Failed to start bot: %s", e)
            bot = None
    else:
        log.warning("BOT_TOKEN not set — /start command disabled (the Mini App still works).")

    yield

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
    """Fetch a payments.* request class, or None if this Telethon lacks it."""
    if not TELETHON_OK:
        return None
    return getattr(functions.payments, name, None)


def color_hex(c):
    try:
        return f"#{int(c) & 0xFFFFFF:06x}"
    except Exception:
        return None


def cdn_full(slug, num):
    """Build the lowercase 'name-num' base used by Fragment's CDN + t.me/nft."""
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
    Resale price extraction. The resale amount lives in a NEW field whose exact
    name/shape can vary by schema version, so we probe a few shapes.   # VERIFY
    Returns (price_float|None, currency_str).
    """
    # 1) TON-style amount objects (amount + nanos) — common in newer schema
    for attr in ("resell_amount", "resale_amount", "value_amount"):
        v = getattr(g, attr, None)
        if v is not None:
            amount = getattr(v, "amount", None)
            nanos = getattr(v, "nanos", 0) or 0
            if amount is not None:
                try:
                    return (round(float(amount) + float(nanos) / 1e9, 4), "TON")
                except Exception:
                    pass
            if isinstance(v, (int, float)):
                # raw nanotons
                return (round(float(v) / 1e9, 4), "TON")
    # 2) Stars-priced resale
    for attr in ("resell_stars", "resale_stars"):
        v = getattr(g, attr, None)
        if isinstance(v, (int, float)) and v:
            return (int(v), "Stars")
    return (None, "TON")


def _gift_attrs(g):
    """Pull (model, model_rarity, symbol, backdrop, backdrop_hex) off a unique gift."""
    model = model_rarity = symbol = backdrop = backdrop_hex = None
    for a in getattr(g, "attributes", []) or []:
        cls = type(a).__name__
        rar = getattr(a, "rarity_permille", None)
        rar = round(rar / 10, 2) if isinstance(rar, (int, float)) else None
        if cls == "StarGiftAttributeModel":
            model, model_rarity = getattr(a, "name", None), rar
        elif cls == "StarGiftAttributePattern":   # pattern == "symbol" in the UI
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
        "slug": base,                 # full 'Name-Num' (frontend lowercases for CDN)
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
async def getgems_search(gift_name, limit=12):
    """
    Optional GetGems GraphQL listings. The exact query/fields depend on your
    key's schema — confirm them in GetGems' API explorer and adjust below.  # VERIFY
    Fails soft (returns []) so it never breaks /api/search.
    """
    if not (GETGEMS_API_KEY and HTTPX_OK and gift_name):
        return []
    # Placeholder query — REPLACE field names with those exposed to your key.
    query = """
    query Search($q: String!, $first: Int!) {
      nftSearch(query: $q, first: $first) {
        edges { node { name address content { ... on NftContentImage { image { sized(width:200,height:200) } } }
                        sale { ... on NftSaleFixPrice { fullPrice } } } }
      }
    }"""
    try:
        async with httpx.AsyncClient(timeout=12) as h:
            r = await h.post(
                GETGEMS_GRAPHQL,
                json={"query": query, "variables": {"q": gift_name, "first": limit}},
                headers={"Authorization": f"Bearer {GETGEMS_API_KEY}"},
            )
            data = r.json()
        out = []
        edges = (((data or {}).get("data") or {}).get("nftSearch") or {}).get("edges") or []
        for e in edges:
            n = e.get("node") or {}
            price_nano = (((n.get("sale") or {}).get("fullPrice")))
            price = round(int(price_nano) / 1e9, 4) if price_nano else None
            img = ((((n.get("content") or {}).get("image") or {}).get("sized")))
            out.append({
                "id": n.get("address"),
                "slug": None, "num": None,
                "name": n.get("name") or gift_name,
                "model": None, "modelRarity": None, "symbol": None,
                "backdrop": None, "backdropHex": None,
                "price": price, "currency": "TON",
                "market": "GetGems",
                "url": f"https://getgems.io/nft/{n.get('address')}" if n.get("address") else None,
                "image": img, "animation": None,
            })
        return out
    except Exception as e:
        log.info("GetGems search skipped: %s", e)
        return []


# ─── Routes ───────────────────────────────────────────────────────────────────
@app.get("/")
async def health():
    return {
        "ok": True,
        "mtproto": bool(client),
        "getgems": bool(GETGEMS_API_KEY),
        "cached_collections": bool(cache_get("collections")),
    }


@app.get("/api/collections")
async def collections():
    cached = cache_get("collections")
    if cached:
        return {"collections": cached}
    if not client:
        return {"collections": []}  # frontend falls back to its tiny offline list
    GetStarGifts = _payments("GetStarGiftsRequest")
    if not GetStarGifts:
        return {"collections": [], "error": "GetStarGiftsRequest missing — pip install -U telethon"}
    try:
        async with _mtproto_lock:
            res = await client(GetStarGifts(hash=0))
        out = []
        for g in getattr(res, "gifts", []) or []:
            title = getattr(g, "title", None) or f"Gift {getattr(g, 'id', '')}"
            out.append({
                "name": title,
                "slug": getattr(g, "slug", None) or _slug_from_title(title),
                "gift_id": str(getattr(g, "id", "")),
                "supply": getattr(g, "availability_total", None) or getattr(g, "availability_issued", None) or 0,
                "preview": "",  # collection-level preview omitted (needs a specific item num)
            })
        out = [c for c in out if c["gift_id"]]
        cache_set("collections", out, ttl=600)
        return {"collections": out}
    except Exception as e:
        log.error("collections error: %s", e)
        return {"collections": [], "error": str(e)}


@app.get("/api/featured")
async def featured():
    """Real animated gifts for the app's launch screen (Plush Pepe + 2 others)."""
    cached = cache_get("featured")
    if cached:
        return {"gifts": cached}
    if not client:
        return {"gifts": []}
    GetResale = _payments("GetResaleStarGiftsRequest")
    if not GetResale:
        return {"gifts": [], "error": "GetResaleStarGiftsRequest missing — pip install -U telethon"}
    try:
        cols = (await collections()).get("collections", [])
        by_name = {c["name"].lower(): c for c in cols}
        out = []
        for name in FEATURED_NAMES:
            col = by_name.get(name.lower())
            if not col:
                continue
            try:
                async with _mtproto_lock:
                    res = await client(GetResale(
                        gift_id=int(col["gift_id"]), attributes_hash=0,
                        sort_by_price=True, offset="", limit=1,
                    ))
                gifts = getattr(res, "gifts", []) or []
                if gifts:
                    out.append(serialize_unique(gifts[0]))
            except Exception as e:
                log.info("featured '%s' skipped: %s", name, e)
        if out:
            cache_set("featured", out, ttl=300)
        return {"gifts": out}
    except Exception as e:
        log.error("featured error: %s", e)
        return {"gifts": []}


@app.get("/api/attributes")
async def attributes(gift_id: str = Query(...)):
    key = f"attrs:{gift_id}"
    cached = cache_get(key)
    if cached:
        return cached
    empty = {"models": [], "symbols": [], "backdrops": []}
    if not client:
        return empty
    GetResale = _payments("GetResaleStarGiftsRequest")
    if not GetResale:
        return {**empty, "error": "GetResaleStarGiftsRequest missing — pip install -U telethon"}
    try:
        # attributes_hash=0 returns the FULL attribute list (with rarity_permille). # VERIFY arg names
        async with _mtproto_lock:
            res = await client(GetResale(gift_id=int(gift_id), attributes_hash=0, offset="", limit=1))
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
        cache_set(key, result, ttl=300)
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

    # ── Telegram-native resale (primary) ──
    if client and gift_id and (not want or "Telegram" in want):
        GetResale = _payments("GetResaleStarGiftsRequest")
        if GetResale:
            try:
                async with _mtproto_lock:
                    res = await client(GetResale(
                        gift_id=int(gift_id), attributes_hash=0,
                        sort_by_price=True, offset="", limit=SEARCH_LIMIT,
                    ))
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

    # ── GetGems (optional secondary) ──
    if gift and (not want or "GetGems" in want):
        results.extend(await getgems_search(gift, limit=12))

    return {"results": results}


@app.get("/api/gift")
async def gift(slug: str = Query(...)):
    if not client:
        return {"error": "mtproto-offline"}
    GetUnique = _payments("GetUniqueStarGiftRequest")
    if not GetUnique:
        return {"error": "GetUniqueStarGiftRequest missing — pip install -U telethon"}
    try:
        async with _mtproto_lock:
            res = await client(GetUnique(slug=slug))
        g = getattr(res, "gift", res)
        data = serialize_unique(g)

        # Optional richer value info (floor / avg / last sale).  # VERIFY class name
        GetValue = _payments("GetUniqueStarGiftValueInfoRequest")
        if GetValue:
            try:
                async with _mtproto_lock:
                    v = await client(GetValue(slug=slug))
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
