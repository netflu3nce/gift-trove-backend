"""
GiftTrove backend — FastAPI + Telethon (MTProto user session)
"""

import os
import re
import time
import json
import gzip
import asyncio
import sqlite3
import logging
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Body
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("gifttrove")

# ─── Secret redaction in logs ─────────────────────────────────────────────────
# Credentials must never reach the logs in clear text. We snapshot the values of
# known-sensitive env vars at startup and mask any occurrence of them in every
# log record with ****, plus a defensive catch-all for bot-token-shaped strings
# (e.g. 1234567890:AA...). Env vars are present in os.environ from process start,
# so this snapshot is complete even though the typed config constants below are
# assigned a few lines later.
_SENSITIVE_ENV_KEYS = (
    "BOT_TOKEN", "API_HASH", "API_ID", "STRING_SESSION", "DATABASE_URL",
    "MARKETAPP_TOKEN", "ACCESS_CODE", "ADMIN_CODE",
    "STRING_SESSION_2", "STRING_SESSION_3", "STRING_SESSION_4", "STRING_SESSION_5",
    "API_ID_2", "API_ID_3", "API_ID_4", "API_ID_5",
    "API_HASH_2", "API_HASH_3", "API_HASH_4", "API_HASH_5",
)
_BOT_TOKEN_RE = re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{30,}\b")

class _RedactSecrets(logging.Filter):
    def __init__(self):
        super().__init__()
        self._secrets = set()
        for k in _SENSITIVE_ENV_KEYS:
            v = (os.getenv(k, "") or "").strip()
            if v and len(v) >= 4:
                self._secrets.add(v)

    def _mask(self, s):
        if not isinstance(s, str):
            return s
        for sec in self._secrets:
            if sec in s:
                s = s.replace(sec, "****")
        return _BOT_TOKEN_RE.sub("****", s)

    def filter(self, record):
        try:
            if isinstance(record.msg, str):
                record.msg = self._mask(record.msg)
            if record.args:
                if isinstance(record.args, dict):
                    record.args = {k: self._mask(v) for k, v in record.args.items()}
                else:
                    record.args = tuple(self._mask(a) for a in record.args)
        except Exception:
            pass
        return True

# Attach to every root handler so all loggers that propagate to root (ours, and
# uvicorn's) get redacted. Re-applied if handlers are added later.
def _install_redaction():
    flt = _RedactSecrets()
    root = logging.getLogger()
    for h in root.handlers:
        if not any(isinstance(f, _RedactSecrets) for f in h.filters):
            h.addFilter(flt)
_install_redaction()

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
# This backend's own public URL — Render sets RENDER_EXTERNAL_URL automatically.
# Needed anywhere we hand the frontend a URL pointing back at OURSELVES (like
# /api/model-anim/<id> below): the frontend's api() helper prefixes its own
# backend base URL for its calls, but LottieGift does a plain fetch(src) on
# whatever string is in `animation`/`image` — a bare relative path there would
# resolve against the FRONTEND's own domain (Vercel), not this backend.
BACKEND_PUBLIC_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
if not BACKEND_PUBLIC_URL:
    log.warning(
        "RENDER_EXTERNAL_URL not set — this backend doesn't know its own public "
        "URL. The Telegram-sourced image/animation fallback (model-anim URLs) "
        "will be skipped rather than guess at a domain that might belong to a "
        "DIFFERENT deployment (beta vs. production)."
    )
GETGEMS_API_KEY = os.getenv("GETGEMS_API_KEY", "")
GETGEMS_GRAPHQL = os.getenv("GETGEMS_GRAPHQL", "https://api.getgems.io/graphql")
# MarketApp aggregator (Tonnel / Portals / Fragment / GetGems / MarketApp).
# Auth is a raw token in the Authorization header (no "Bearer" prefix).
# The listings path + its collection query param are env-overridable so the exact
# endpoint can be corrected from their Swagger without a code change.
MARKETAPP_TOKEN = os.getenv("MARKETAPP_TOKEN", "")
MARKETAPP_BASE = os.getenv("MARKETAPP_BASE", "https://api.marketapp.org").rstrip("/")
# Thermos — their documented Proxy API (proxy.thermos.gifts) is a public,
# read-only aggregator across TONNEL/PORTALS/MRKT with no auth shown in their
# docs, unlike their separate authenticated Market API (for buy/sell/withdraw
# actions, which GiftTrove never needs — it only ever displays listings).
# THERMOS_API_TOKEN is kept as a defined env var but currently unused in the
# actual request (see thermos_search) — sending it as a bearer token to a
# documented-public endpoint looked like the likely cause of a real 400 seen
# in production.
THERMOS_API_TOKEN = os.getenv("THERMOS_API_TOKEN", "").strip()
THERMOS_PROXY_BASE = os.getenv("THERMOS_PROXY_BASE", "https://proxy.thermos.gifts/api/v1").rstrip("/")
# The referral id from YOUR OWN Thermos share link (the part between "ref_"
# and "_g_" in a link like https://t.me/thermos/thermos/?startapp=ref_<THIS>_g_...)
# — every Thermos listing link this backend builds includes it, so referral
# credit accrues on purchases made through GiftTrove.
THERMOS_REF_ID = os.getenv("THERMOS_REF_ID", "").strip()
_KNOWN_FRONTEND_ORIGINS = [
    "https://gift-trove-frontend.vercel.app",  # main/production
    "https://trovebeta.vercel.app",            # beta
]
_env_origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
# Union, not replace: an ALLOWED_ORIGINS env var (e.g. set before a new domain
# existed) can only ADD origins here, never silently drop one of the two
# known frontends. That silent-drop is exactly what breaks "beta still works,
# production doesn't" — both domains are always safe regardless of what's
# actually configured in the env var.
ALLOWED_ORIGINS = list(dict.fromkeys(_KNOWN_FRONTEND_ORIGINS + _env_origins))
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

# ─── Affiliate program ───────────────────────────────────────────────────────
# Any referrer earns a recurring 30% cut of every subscription PAYMENT made by
# users they referred — Plus or Pro, same rate, every renewal, for as long as
# that referral keeps paying — PROVIDED the referrer is a CURRENT Pro at the
# moment of that payment (see the credit call site for the full rationale).
# If a referrer's Pro lapses, new earning simply stops; it resumes
# automatically the instant they resubscribe, since the check runs live on
# every payment rather than needing a separate trigger. Existing balance is
# never touched by a lapse — only future accrual pauses. Dashboard *access*
# is a further, separate check (see is_pro in /api/affiliate), applied at
# view-time with the same current-Pro requirement.
AFFILIATE_PCT = int(os.getenv("AFFILIATE_PCT", "30"))            # % of each sub payment
AFFILIATE_MIN_WITHDRAW = int(os.getenv("AFFILIATE_MIN_WITHDRAW", "1000"))  # Stars before payout
STAR_TO_TON = float(os.getenv("STAR_TO_TON", "0.005"))           # fallback only; live rate preferred
# Telegram fixes the USD price of a Star — it's the TON *amount* that floats with
# TON's market price, not the other way around. So the right fallback is a stable
# USD-per-Star constant, not "a fixed TON ratio × live TON/USD" (that compounds
# incorrectly as TON's price moves and was the actual bug behind cross-currency
# ranking looking wrong). $0.015 sits in the documented range for Telegram Stars
# (~$0.013 creator payout floor to ~$0.017-0.02 consumer purchase price). Override
# via env if a more precise figure becomes available.
STARS_USD_RATE = float(os.getenv("STARS_USD_RATE", "0.015"))
USDT_PER_TON = float(os.getenv("USDT_PER_TON", "3.2"))           # fallback only; live rate preferred

# ── Live TON/USD rate (Binance primary, OKX + CoinGecko fallbacks) ──────────────
# Used for USDT→GRAM ranking and Stars→USD conversion. 2-minute cache.
_ton_usd_cache = {"ts": 0.0, "rate": 0.0}
_TON_USD_TTL = 120

async def _ton_usd_rate():
    now = time.time()
    if _ton_usd_cache["rate"] > 0 and (now - _ton_usd_cache["ts"]) < _TON_USD_TTL:
        return _ton_usd_cache["rate"]
    rate, source = 0.0, None
    # Primary: TonAPI — the TON ecosystem's own public indexer, built to be
    # called from app/dApp backends, so it doesn't apply the cloud/datacenter-IP
    # geofencing that exchange APIs (Binance especially) commonly enforce.
    try:
        async with httpx.AsyncClient(timeout=6) as cli:
            r = await cli.get("https://tonapi.io/v2/rates", params={"tokens": "ton", "currencies": "usd"})
        if r.status_code == 200:
            rate = float((((r.json() or {}).get("rates") or {}).get("TON") or {}).get("prices", {}).get("USD") or 0)
            if rate > 0:
                source = "tonapi"
    except Exception as e:
        log.info("tonapi ton rate failed: %s", e)
    # Binance TONUSDT spot.
    if rate <= 0:
        try:
            async with httpx.AsyncClient(timeout=6) as cli:
                r = await cli.get("https://api.binance.com/api/v3/ticker/price",
                                  params={"symbol": "TONUSDT"})
            if r.status_code == 200:
                rate = float((r.json() or {}).get("price") or 0)
                if rate > 0:
                    source = "binance"
        except Exception as e:
            log.info("binance ton rate failed: %s", e)
    # OKX.
    if rate <= 0:
        try:
            async with httpx.AsyncClient(timeout=6) as cli:
                r = await cli.get("https://www.okx.com/api/v5/market/ticker",
                                  params={"instId": "TON-USDT"})
            if r.status_code == 200:
                rate = float((((r.json() or {}).get("data") or [{}])[0]).get("last") or 0)
                if rate > 0:
                    source = "okx"
        except Exception as e:
            log.info("okx ton rate failed: %s", e)
    # Kraken — different cloud-IP policy than Binance/OKX, useful diversity.
    if rate <= 0:
        try:
            async with httpx.AsyncClient(timeout=6) as cli:
                r = await cli.get("https://api.kraken.com/0/public/Ticker", params={"pair": "TONUSD"})
            if r.status_code == 200:
                res = ((r.json() or {}).get("result") or {})
                pair = next(iter(res.values()), {}) if res else {}
                rate = float((pair.get("c") or [0])[0] or 0)
                if rate > 0:
                    source = "kraken"
        except Exception as e:
            log.info("kraken ton rate failed: %s", e)
    # CoinGecko.
    if rate <= 0:
        try:
            async with httpx.AsyncClient(timeout=6) as cli:
                r = await cli.get("https://api.coingecko.com/api/v3/simple/price",
                                  params={"ids": "the-open-network", "vs_currencies": "usd"})
            if r.status_code == 200:
                rate = float(((r.json() or {}).get("the-open-network") or {}).get("usd") or 0)
                if rate > 0:
                    source = "coingecko"
        except Exception as e:
            log.info("coingecko ton rate failed: %s", e)
    if rate > 0:
        _ton_usd_cache.update(ts=now, rate=rate)
        log.info("ton/usd rate refreshed via %s: %.4f", source, rate)
        return rate
    # Every source failed — this should be rare and is never allowed to fail
    # silently again, since it feeds both search ranking AND affiliate GRAM
    # payout math. notify_admin already de-dupes identical alerts within 10 min.
    try:
        asyncio.create_task(notify_admin(
            f"TON/USD live rate fetch failed on every source (tonapi, binance, okx, kraken, "
            f"coingecko) — falling back to the static default ({USDT_PER_TON}). GRAM-based "
            f"ranking and affiliate GRAM conversion are inaccurate until this recovers.",
            level="warning"))
    except Exception:
        pass
    return _ton_usd_cache["rate"] or USDT_PER_TON

# ── Live Stars/USD rate via Telegram's official MTProto endpoint ─────────────────
# payments.getStarsRevenueStats returns usd_rate — the official Telegram conversion
# rate for Stars ↔ USD. This is the number that drives affiliate payouts (what
# Telegram itself pays out per Star). Cache 4 hours — the Stars price rarely moves.
# Falls back to the stable STARS_USD_RATE constant if the MTProto call fails.
_stars_usd_cache = {"ts": 0.0, "rate": 0.0}
_STARS_USD_TTL = 14400
_stars_refresh_running = False

async def _refresh_stars_usd_bg():
    """Background task: refresh Stars/USD rate via Telegram MTProto without blocking callers.
    Must be called via the BOT's own session asking about ITSELF (InputPeerSelf) —
    payments.getStarsRevenueStats reports the revenue of whoever you're authenticated
    as, so calling it via our separate user session (`client`) while passing the bot
    as a third-party peer has no permission to succeed and silently errors every time."""
    global _stars_refresh_running, _stars_usd_cache
    if _stars_refresh_running:
        return
    _stars_refresh_running = True
    try:
        if bot is not None:
            from telethon.tl.functions.payments import GetStarsRevenueStatsRequest
            from telethon.tl.types import InputPeerSelf
            res = await bot(GetStarsRevenueStatsRequest(peer=InputPeerSelf()))
            if hasattr(res, "usd_rate") and res.usd_rate:
                rate = float(res.usd_rate)
                _stars_usd_cache.update(ts=time.time(), rate=rate)
                log.info("stars/usd rate refreshed (official Telegram): %.6f", rate)
    except Exception as e:
        log.info("stars_usd MTProto bg refresh failed: %s", e)
    finally:
        _stars_refresh_running = False

async def _stars_usd_rate():
    """Return the best available Stars→USD rate immediately, never blocking.
    If the cache is fresh, returns it. If stale or empty, returns the stable
    USD-anchored fallback (STARS_USD_RATE) and kicks off a background MTProto
    refresh so future calls get the official Telegram rate. This keeps the sort
    path fast even during Telegram MTProto flood waits."""
    now = time.time()
    # Fresh cache: return immediately.
    if _stars_usd_cache["rate"] > 0 and (now - _stars_usd_cache["ts"]) < _STARS_USD_TTL:
        return _stars_usd_cache["rate"]
    # Stale or empty: schedule background refresh, return best available now.
    try:
        asyncio.create_task(_refresh_stars_usd_bg())
    except Exception:
        pass
    return _stars_usd_cache["rate"] or STARS_USD_RATE

# ── Share banner: pre-upload once to get a stable Telegram file_id ──────────────
# Using cached_photo (file already on Telegram's CDN) instead of photo_url
# eliminates the half-loaded banner caused by Telegram re-fetching from ibb.co
# on every single share. Re-uploads on cold start (once per Render deploy).
_share_photo_fid = None

async def _ensure_share_photo_fid():
    """Upload SHARE_IMAGE via Bot API once per cold start, return the file_id."""
    global _share_photo_fid
    if _share_photo_fid:
        return _share_photo_fid
    last_err = None
    for attempt in range(2):
        try:
            resp = await _bot_api("sendPhoto", {
                "chat_id": int(ANALYTICS_ADMIN_ID),
                "photo": SHARE_IMAGE,
                "disable_notification": True,
                "caption": "share banner (internal cache)",
            })
            if resp and resp.get("ok"):
                photos = (resp.get("result") or {}).get("photo") or []
                fid = photos[-1]["file_id"] if photos else None
                if fid:
                    _share_photo_fid = fid
                    log.info("share banner uploaded, file_id cached: %s…", fid[:24])
                    return fid
            last_err = resp
        except Exception as e:
            last_err = e
        if attempt == 0:
            await asyncio.sleep(1.0)
    # Both attempts failed — shares will still go out via the article fallback
    # (clean text + links, no image), but this should never be silent: it's
    # the difference between a share looking right and looking half-finished.
    log.info("share banner upload failed: %s", last_err)
    try:
        await notify_admin(
            f"Share banner upload failed twice — shares are falling back to the "
            f"no-image article format until this recovers. Last error: {last_err}",
            level="warning")
    except Exception:
        pass
    return None

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
WELCOME_IMAGE = os.getenv("WELCOME_IMAGE", "https://i.ibb.co/ksyP8tjh/Gift-Trove-Telegram-Gifts-Landing-1.png")
# Banner attached to every shared-gift inline message.
SHARE_IMAGE = os.getenv("SHARE_IMAGE", "https://i.ibb.co/4g4s6vxj/6-EC6-BD64-4686-4115-A8-A5-CE9-E12-F65-C8-B.png")
# Bot DM illustrations for three notification moments:
IMG_CREDIT_SUCCESS = os.getenv("IMG_CREDIT_SUCCESS", "https://i.ibb.co/RpV8YBHH/Gift-Trove-Telegram-Gifts-Landing.png")   # payout credited
IMG_DECLINED_ENDED = os.getenv("IMG_DECLINED_ENDED", "https://i.ibb.co/Q7VyKBK0/Gift-Trove-Telegram-Gifts-Landing.png")   # payout declined / promo ended unsold
IMG_PURCHASE_UPGRADE = os.getenv("IMG_PURCHASE_UPGRADE", "https://i.ibb.co/5gtDxJ66/Gift-Trove-Telegram-Gifts-Landing.png")  # promo bought / premium upgrade

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
    "<b>Welcome to GiftTrove! Scout unique Telegram gifts from different "
    "marketplaces all at a go. \u2726</b>",
)
# Plain fallback if formatting can't be sent (still friendly).
WELCOME_PLAIN = (
    "Welcome to GiftTrove! Scout unique Telegram gifts from different "
    "marketplaces all at a go. \u2726"
)
MINIAPP_URL = os.getenv("MINIAPP_URL", "https://t.me/gifttrovebot/app")
COMMUNITY_URL = os.getenv("COMMUNITY_URL", "https://t.me/gifttrove")
# Official GiftTrove announcement channel — checked on every app launch to
# decide whether to show the "Join our channel" bottom sheet. Channel ID is
# stored as an env var so it can be changed without a redeploy.
CHANNEL_ID = os.getenv("CHANNEL_ID", "-1003990905901")
CHANNEL_INVITE_URL = os.getenv("CHANNEL_INVITE_URL", "https://t.me/gifttrove")
# Hoton (cheaper Stars) referral — used in the "insufficient balance" DM and the
# in-app "Need Stars?" CTA. Keep this in sync with the frontend link.
HOTON_URL = os.getenv(
    "HOTON_URL",
    "https://t.me/hotontgbot/app?startapp=UQBRBt4DBvWYNqP9_p9-nysR55R2PXz9BEZjvrxiPWvVBcJO")

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
        # Durable model-animation cache (decompressed Lottie JSON, keyed by the
        # model document's own id) — thumbnails survive a Render restart via
        # attrs_cache above, but animations were previously ONLY held in an
        # in-memory dict, wiped clean on every restart. On a free tier prone
        # to cold starts, that meant "correct static image, but the animation
        # never comes back" until the warm loop happened to re-fetch it —
        # exactly the intermittent stagnant-PNG pattern reported. Keyed
        # separately from attrs_cache (per-model, not per-collection) since a
        # collection's full animation set can be large.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS anim_cache (
                   doc_id TEXT PRIMARY KEY,
                   data TEXT NOT NULL,
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
        # cancelled=1 once the member cancels in-app (so a later lapse isn't mistaken
        # for a failed renewal). end_notified=1 once we've sent the lapse DM.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS subs (
                   uid TEXT PRIMARY KEY,
                   tier TEXT NOT NULL,
                   expires_at INTEGER NOT NULL,
                   charge_id TEXT,
                   ts INTEGER NOT NULL,
                   cancelled INTEGER DEFAULT 0,
                   end_notified INTEGER DEFAULT 0)"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_subs_exp ON subs(expires_at);")
        _have_subs = _existing_columns(conn, "subs")
        for _col, _typ in (("cancelled", "INTEGER DEFAULT 0"), ("end_notified", "INTEGER DEFAULT 0"),
                           ("tag_cleared", "INTEGER DEFAULT 0")):
            if _col not in _have_subs:
                conn.execute(f"ALTER TABLE subs ADD COLUMN {_col} {_typ}")

        # Per-user language for bot DMs ('en' | 'ru' | 'zh'). Driven by the in-app
        # language switcher (synced via /api/lang) and seeded from the user's
        # Telegram language on /start. Defaults to English when unknown.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS user_lang (
                   uid TEXT PRIMARY KEY,
                   lang TEXT NOT NULL DEFAULT 'en',
                   ts INTEGER NOT NULL)"""
        )
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


def _bump_event(kind):
    """Record a lightweight analytics event (e.g. 'inline', 'open'). Safe no-op
    on any DB hiccup — analytics must never break a user-facing path."""
    k = (kind or "").strip()[:24]
    if not k:
        return
    try:
        with db() as conn:
            conn.execute("INSERT INTO events(kind, ts) VALUES(?, ?)", (k, int(time.time())))
            conn.commit()
    except Exception as e:
        log.info("bump_event(%s) skipped: %s", k, e)


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

def _sub_full(uid):
    """Raw current subs row as (tier, expires_at, charge_id), ignoring expiry.
    Used to tell a first activation from an auto-renewal or a tier switch."""
    if not uid:
        return None
    try:
        with db() as conn:
            row = conn.execute("SELECT tier, expires_at, charge_id FROM subs WHERE uid=?", (str(uid),)).fetchone()
        if row:
            return (row["tier"], int(row["expires_at"] or 0), row["charge_id"])
    except Exception:
        pass
    return None

# A vanity code must look like a code, not impersonate a number that resolve_ref
# would read as a raw uid, so we require at least one letter.
_VANITY_RE = _re.compile(r"^[A-Z0-9]{5,12}$")

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


async def _promo_auto_fetch(marketplace, slug, gift_id, num, collection_name=""):
    """Fetch real listing data for a specific gift (by num) from the given marketplace.
    Returns dict {price, currency, model, symbol, backdrop, url} when found, or {}
    when the lookup genuinely succeeded but the gift isn't listed.
    Raises on network/API errors — callers that use a {} result to mean "sold" or
    "not listed" should catch exceptions separately and treat them as inconclusive
    (don't mark anything sold), since a transient error is not the same as a
    confirmed miss."""
    num_int = int(num) if str(num or "").isdigit() else None

    # ── Telegram (direct unique-gift lookup — reliable regardless of collection
    # size, unlike scanning paginated resale listings which can miss items in
    # large collections) ─────────────────────────────────────────────────────
    if marketplace == "Telegram" and slug and num_int is not None:
        GetUnique = _payments("GetUniqueStarGiftRequest")
        if client and GetUnique:
            full_slug = f"{slug}-{num_int}"
            res = await _invoke(lambda: GetUnique(slug=full_slug))
            g = getattr(res, "gift", res)
            item = serialize_unique(g)
            if item.get("price") is not None:
                log.info("promo_auto_fetch Telegram found %s (direct lookup)", full_slug)
                return {
                    "price":    str(item.get("price", "")) if item.get("price") is not None else "",
                    "currency": item.get("currency", "Stars"),
                    "model":    item.get("model")    or "",
                    "symbol":   item.get("symbol")   or "",
                    "backdrop": item.get("backdrop") or "",
                    "url":      item.get("url")      or f"https://t.me/nft/{full_slug}",
                }
            log.info("promo_auto_fetch Telegram %s exists but isn't currently listed for sale", full_slug)
        return {}

    # ── Fragment (scraper — num captured per item) ─────────────────────────────
    if marketplace == "Fragment" and slug:
        fslug = slug.lower()
        listings = await fragment_search(fslug, collection_name or slug, limit=200)
        for item in listings:
            if item.get("num") == num_int:
                log.info("promo_auto_fetch Fragment found #%s", num_int)
                return {
                    "price":    str(item.get("price", "")) if item.get("price") is not None else "",
                    "currency": item.get("currency", "TON"),
                    "model":    item.get("model")    or "",
                    "symbol":   item.get("symbol")   or "",
                    "backdrop": item.get("backdrop") or "",
                    "url":      item.get("url")      or f"https://fragment.com/gift/{fslug}-{num}",
                }
        log.info("promo_auto_fetch Fragment #%s not found", num_int)
        return {}

    # ── Thermos (aggregator search — num captured per item, same
    #    fetch-broadly-then-filter-exact approach as Fragment above) ──────────
    if marketplace == "Thermos" and slug:
        listings = await thermos_search(collection_name or slug, slug, gift_id=gift_id, limit=100)
        for item in listings:
            if item.get("num") == num_int:
                log.info("promo_auto_fetch Thermos found #%s", num_int)
                return {
                    "price":    str(item.get("price", "")) if item.get("price") is not None else "",
                    "currency": item.get("currency", "GRAM"),
                    "model":    item.get("model")    or "",
                    "symbol":   item.get("symbol")   or "",
                    "backdrop": item.get("backdrop") or "",
                    "url":      item.get("url")      or _thermos_gift_url(collection_name or slug, num_int),
                }
        log.info("promo_auto_fetch Thermos #%s not found", num_int)
        return {}

    # ── MarketApp (item_num_from / item_num_to exact filter) ──────────────────
    if marketplace == "MarketApp" and MARKETAPP_TOKEN and num_int is not None:
        coll_addr = await _marketapp_collection_address(collection_name, slug)
        if not coll_addr:
            log.info("promo_auto_fetch MarketApp: no address for %r", slug)
            raise RuntimeError(f"no collection address for {slug!r}")
        async with httpx.AsyncClient(timeout=12) as cli:
            r = await cli.get(f"{MARKETAPP_BASE}/v1/gifts/onsale/",
                              params={"collection_address": coll_addr,
                                      "item_num_from": num_int, "item_num_to": num_int},
                              headers={"Authorization": MARKETAPP_TOKEN})
        if r.status_code != 200:
            raise RuntimeError(f"marketapp status={r.status_code}")
        data = r.json()
        items = (data.get("items") or []) if isinstance(data, dict) else []
        if items:
            mapped = _marketapp_item(items[0], collection_name or slug, slug, gift_id)
            if mapped:
                log.info("promo_auto_fetch MarketApp found #%s", num_int)
                return {
                    "price":    str(mapped.get("price", "")) if mapped.get("price") is not None else "",
                    "currency": mapped.get("currency", "GRAM"),
                    "model":    mapped.get("model")    or "",
                    "symbol":   mapped.get("symbol")   or "",
                    "backdrop": mapped.get("backdrop") or "",
                    "url":      mapped.get("url")      or "",
                }
        log.info("promo_auto_fetch MarketApp #%s not found", num_int)
        return {}

    return {}


# Telegram lookups (GetUniqueStarGiftRequest) share ONE MTProto connection —
# Telethon serializes RPCs on it, so firing many at once doesn't actually run
# them in parallel, it just queues them and starves every other feature
# (Results, Scout, anything else using `client`) for however long the queue
# takes to drain. Every caller that does a Telegram listing check (saved-gift
# verification, promo background verification) shares this cap so they can
# never compound into a multi-second connection traffic jam together.
_telegram_check_sema = asyncio.Semaphore(2)
# Cache listing-check results briefly so re-opening the Saved tab or a promo's
# 5-min re-verify cycle doesn't redundantly re-hit Telegram/Fragment/MarketApp
# for a gift that was just confirmed seconds ago.
_listing_check_cache: dict = {}   # "{mkt}:{slug}:{num}" -> (unix_ts, sold_bool)
_LISTING_CHECK_TTL = 1200         # 20 min


def _promo_set_attrs(pid, model, symbol, backdrop, url):
    """Update attribute + link fields on a promo after auto-fetch."""
    try:
        with db() as conn:
            conn.execute(
                "UPDATE promos SET model=?, symbol=?, backdrop=?, link=? WHERE id=?",
                (model or "", symbol or "", backdrop or "", url or "", pid))
            conn.commit()
    except Exception as e:
        log.error("promo_set_attrs failed: %s", e)

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
            # Idempotency: a duplicate webhook delivery for the SAME charge
            # must never double-credit the referrer. charge_id is the only
            # thing tying this back to one real payment event.
            if charge_id:
                existing = conn.execute(
                    "SELECT 1 FROM affiliate_earnings WHERE charge_id=? AND referrer=? LIMIT 1",
                    (str(charge_id), str(referrer))
                ).fetchone()
                if existing:
                    log.info("affiliate credit skipped: charge_id=%s already credited to referrer=%s", charge_id, referrer)
                    return
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
    """Create a payout request and return its id (or None on failure)."""
    pid = _secrets.token_hex(8)
    try:
        with db() as conn:
            conn.execute(
                "INSERT INTO affiliate_payouts(id, uid, stars, ton_address, status, ts) VALUES(?,?,?,?,?,?)",
                (pid, str(uid), int(stars), ton_address or "", "requested", int(time.time())))
            conn.commit()
        return pid
    except Exception as e:
        log.error("affiliate payout request failed: %s", e)
        return None

def _affiliate_payout_get(pid):
    """Fetch a single payout request row as a dict (or None)."""
    try:
        with db() as conn:
            r = conn.execute(
                "SELECT id, uid, stars, ton_address, status, ts FROM affiliate_payouts WHERE id=?",
                (str(pid),)).fetchone()
        return dict(r) if r else None
    except Exception:
        return None

def _affiliate_payout_set_status(pid, status):
    try:
        with db() as conn:
            conn.execute("UPDATE affiliate_payouts SET status=? WHERE id=?", (status, str(pid)))
            conn.commit()
        return True
    except Exception as e:
        log.error("payout status update failed: %s", e)
        return False

async def _gram_from_stars(stars):
    """Convert a Stars amount to GRAM using the SAME live rate as search ranking
    (Stars->USD via _stars_usd_rate, USD->GRAM via the live TON/USD rate) — not
    the old static STAR_TO_TON placeholder, which drifted from real market value
    and disagreed with what listings actually rank at. Logged so the rate behind
    any given payout is always visible after the fact, not just inferred."""
    try:
        stars = int(stars)
        if stars <= 0:
            return 0
        usd_rate = await _stars_usd_rate()
        ton_usd = await _ton_usd_rate()
        gram = round((stars * usd_rate) / ton_usd, 2) if ton_usd else 0
        log.info("gram_from_stars: stars=%d stars_usd=%.6f ton_usd=%.4f -> gram=%.2f",
                  stars, usd_rate, ton_usd, gram)
        return gram
    except Exception as e:
        log.info("_gram_from_stars error: %s", e)
        return 0

async def _fmt_gram(stars):
    """Pretty GRAM string: drops a trailing .0 (e.g. 5 not 5.0; 5.25 stays)."""
    g = await _gram_from_stars(stars)
    return str(int(g)) if float(g).is_integer() else f"{g:g}"


def _user_authenticity(uid):
    """Heuristic authenticity report for a member, used by the admin scanner before
    approving a payout. These are SIGNALS, not proof — the verdict is advisory and
    should be sanity-checked, not blindly trusted."""
    uid = str(uid)
    now = int(time.time())
    s = _affiliate_stats(uid)
    out = {
        "uid": uid, "stats": s, "tier": get_tier(uid),
        "referrer": _referrer_of(uid), "member": {},
        "referrals_total": s.get("referees", 0), "paid_referrals": s.get("payers", 0),
        "payouts": _affiliate_payouts(uid, limit=20), "flags": [], "authentic": True,
    }
    # Member activity (the members table is hashed, so look up by hash).
    try:
        h = _uid_hash(uid)
        with db() as conn:
            m = conn.execute("SELECT first_seen, last_seen, visits FROM members WHERE uid_hash=?", (h,)).fetchone()
        if m:
            fs = int(m["first_seen"]); ls = int(m["last_seen"]); v = int(m["visits"])
            out["member"] = {"first_seen": fs, "last_seen": ls, "visits": v,
                             "age_days": max(0, (now - fs) // 86400), "seen_app": True}
        else:
            out["member"] = {"seen_app": False}
    except Exception:
        out["member"] = {}
    # Referral timestamps for velocity / burst detection.
    ref_ts = []
    try:
        with db() as conn:
            rows = conn.execute("SELECT ts FROM referrals WHERE referred_by=? ORDER BY ts ASC", (uid,)).fetchall()
        ref_ts = [int(r["ts"]) for r in rows]
    except Exception:
        ref_ts = []
    burst, j = 0, 0
    for i in range(len(ref_ts)):              # max referrals in any rolling 1-hour window
        while ref_ts[i] - ref_ts[j] > 3600:
            j += 1
        burst = max(burst, i - j + 1)
    out["max_referrals_per_hour"] = burst
    referees = s.get("referees", 0); payers = s.get("payers", 0)
    conv = (payers / referees) if referees else 0.0
    out["conversion"] = round(conv, 3)
    flags = []
    if out["referrer"] and str(out["referrer"]) == uid:
        flags.append({"code": "self_referral", "label": "Self-referral",
                      "detail": "This account is recorded as its own referrer."})
    if not out["member"].get("seen_app", False) and s.get("earned", 0) > 0:
        flags.append({"code": "no_app_activity", "label": "No app activity",
                      "detail": "Has commission earnings but never opened the mini app."})
    if burst >= 8:
        flags.append({"code": "rapid_referrals", "label": "Rapid referrals",
                      "detail": f"{burst} referrals arrived within a single hour."})
    if referees >= 10 and conv < 0.05:
        flags.append({"code": "low_conversion", "label": "Very low conversion",
                      "detail": f"{payers}/{referees} referred members ever paid ({out['conversion']*100:.0f}%)."})
    elif referees >= 5 and payers == 0:
        flags.append({"code": "no_conversions", "label": "No conversions",
                      "detail": f"{referees} referrals but none have paid for a plan."})
    out["flags"] = flags
    out["authentic"] = len(flags) == 0
    high_risk = any(f["code"] in ("self_referral", "no_app_activity") for f in flags)
    out["verdict"] = ("Looks authentic" if out["authentic"]
                      else ("High risk" if high_risk else "Needs review"))
    return out

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


# ─── Anti-scraping guard for the high-value data endpoints ────────────────────
# No captchas. The gate is Telegram's own cryptography: every Mini App session
# carries HMAC-signed initData (verified against BOT_TOKEN, max 24h old). A
# scraper can rotate IPs and spoof uids all day — it cannot mint a valid
# signature without actually opening the Mini App from a real Telegram account,
# and each such identity is then individually rate-limited. Off-switch via env
# (REQUIRE_SIGNED_API=0) for local dev outside Telegram.
REQUIRE_SIGNED_API = os.getenv("REQUIRE_SIGNED_API", "1") == "1"
_BOT_UA_RX = _re.compile(
    r"(python-requests|python-httpx|python-urllib|aiohttp|curl/|wget/|scrapy|"
    r"go-http-client|node-fetch|axios/|okhttp|libwww|java/|httpclient|postman)", _re.I)


def _client_ip(request):
    try:
        return (request.headers.get("x-forwarded-for", "") or
                (request.client.host if request.client else "")).split(",")[0].strip() or "?"
    except Exception:
        return "?"


def _data_guard(request, x_init_data):
    """Gate for /api/search, /api/attributes and /api/gift. Returns
    (verified_uid, None) for a legitimate Telegram Mini App session, or
    (None, JSONResponse) to reject. The empty-shape body keeps every client
    code path safe regardless of which endpoint rejected."""
    _reject_body = {"error": "telegram_only", "results": [],
                    "models": [], "symbols": [], "backdrops": []}
    try:
        ua = (request.headers.get("user-agent") or "") if request is not None else ""
    except Exception:
        ua = ""
    if ua and _BOT_UA_RX.search(ua):
        return None, JSONResponse(status_code=403, content=dict(_reject_body, error="automated_client"))
    vuid = verify_init_data(x_init_data)
    if REQUIRE_SIGNED_API and not vuid:
        return None, JSONResponse(status_code=401, content=_reject_body)
    return vuid, None


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


def _gift_label(name, num):
    """Consistent 'Gift Name #1234' label used across all user-facing DMs."""
    nm = (name or "your gift").strip()
    n = str(num or "").strip()
    return f"{nm} #{n}" if n else nm


# Leading glyph for every informational DM (a circled "i" — a typographic symbol,
# not a colour emoji, so it renders identically everywhere).
INFO = "\u24d8 "


def _gift_url_for(slug="", num="", link="", mkt=""):
    """Best public link for a promoted gift. Telegram-native gifts get the exact
    t.me/nft link; Thermos gets a referral deep-link; otherwise we fall back to
    whatever marketplace link we stored."""
    slug = (slug or "").strip()
    num = str(num or "").strip()
    if mkt == "Thermos" and slug and num:
        # Reconstruct the readable collection name from the slug for the
        # referral deep-link format, then delegate to the same helper used
        # everywhere else — consistent referral tracking on all Thermos links.
        coll_name = slug  # best we have here; _thermos_gift_url handles it
        try:
            num_int = int(num)
        except Exception:
            num_int = None
        return _thermos_gift_url(coll_name, num_int)
    if slug and num:
        return f"https://t.me/nft/{slug}-{num}"
    return (link or "").strip()


def _promo_label_link(coll, num, slug="", link="", mkt=""):
    """Returns (label, url) where label is the readable 'Name #id' shown to the
    user and url is the exact t.me gift link it should point to."""
    return _gift_label(coll, num), _gift_url_for(slug, num, link, mkt)


# ─── Localization ────────────────────────────────────────────────────────────
# Every user-facing bot DM is available in English, Russian and Chinese. The
# language is chosen per user (in-app switcher synced via /api/lang, seeded from
# the user's Telegram language on /start). Brand terms — "scout+", "scout pro",
# "hoton", "GRAM", @handles — stay as-is in all languages. Templates DO NOT carry
# the leading "ⓘ " (the sender prepends INFO) so the gift-label hyperlink always
# starts at offset len(INFO).
def _norm_lang(code):
    c = (code or "").strip().lower()
    if c.startswith("ru"):
        return "ru"
    if c.startswith("zh"):
        return "zh"
    if c in ("en", "ru", "zh"):
        return c
    return "en"

def _user_lang(uid):
    try:
        with db() as conn:
            r = conn.execute("SELECT lang FROM user_lang WHERE uid=?", (str(uid),)).fetchone()
        if r and r["lang"] in ("en", "ru", "zh"):
            return r["lang"]
    except Exception:
        pass
    return "en"

def _set_user_lang(uid, lang):
    lang = _norm_lang(lang)
    try:
        with db() as conn:
            conn.execute("DELETE FROM user_lang WHERE uid=?", (str(uid),))
            conn.execute("INSERT INTO user_lang(uid, lang, ts) VALUES(?,?,?)",
                         (str(uid), lang, int(time.time())))
            conn.commit()
        return True
    except Exception as e:
        log.info("set user lang failed: %s", e)
        return False

def _t(key, lang):
    d = TR.get(key, {})
    return d.get(lang) or d.get("en") or ""

TR = {
    "sub_plus": {
        "en": "your GiftTrove scout+ subscription is now active. you can now \n\n\u203a apply up to 7 filters each ",
        "ru": "ваша подписка GiftTrove scout+ теперь активна. теперь вы можете \n\n\u203a применять до 7 фильтров каждого вида ",
        "zh": "您的 GiftTrove scout+ 订阅现已激活。现在您可以 \n\n\u203a 每种最多使用 7 个筛选条件 ",
    },
    "sub_pro": {
        "en": "your GiftTrove scout pro subscription is now active. you can now \n\n\u203a scout with unlimited filters \n\u203a get customized invite code\n\u203a access to affiliate program\n\u203a no promoted gifts in your scouts",
        "ru": "ваша подписка GiftTrove scout pro теперь активна. теперь вы можете \n\n\u203a искать без ограничений по фильтрам \n\u203a получить персональный пригласительный код\n\u203a доступ к партнёрской программе\n\u203a без рекламируемых подарков в результатах",
        "zh": "您的 GiftTrove scout pro 订阅现已激活。现在您可以 \n\n\u203a 使用无限筛选条件侦测 \n\u203a 获取专属邀请码\n\u203a 使用推广联盟计划\n\u203a 侦测结果中不含推广礼物",
    },
    "sub_renewed": {
        "en": "your {plan} subscription has been renewed automatically\n\nkeep scouting.",
        "ru": "ваша подписка {plan} была автоматически продлена\n\nпродолжайте искать.",
        "zh": "您的 {plan} 订阅已自动续订\n\n继续侦测吧。",
    },
    "sub_insufficient": {
        "en": "your {plan} subscription has been ended due to insufficient star balance\u2026 \n\ndeposit stars with hoton to keep your premium running",
        "ru": "ваша подписка {plan} завершена из-за недостатка звёзд\u2026 \n\nпополните звёзды через hoton, чтобы сохранить премиум",
        "zh": "由于星星余额不足，您的 {plan} 订阅已结束\u2026 \n\n通过 hoton 充值星星以保持会员有效",
    },
    "sub_cancel": {
        "en": "your {plan} plan has been cancelled. if this wasn't a mistake ensure you confirm in telegram to verify it's been truly cancelled to avoid extra billings \n\ntelegram \u2192 settings \u2192 my stars \u2192 confirm",
        "ru": "ваш план {plan} был отменён. если это не ошибка, подтвердите отмену в telegram, чтобы избежать повторных списаний \n\ntelegram \u2192 настройки \u2192 мои звёзды \u2192 подтвердить",
        "zh": "您的 {plan} 套餐已取消。如果这不是误操作，请在 telegram 中确认取消，以避免额外扣费 \n\ntelegram \u2192 设置 \u2192 我的星星 \u2192 确认",
    },
    "promo_live": {
        "en": "{label} is live for {days} days. \n\nyou'd be notified if your gift promotion has been sold or when the promotion ends.",
        "ru": "{label} рекламируется {days} дн. \n\nвы получите уведомление, если ваш подарок будет продан или когда продвижение завершится.",
        "zh": "{label} 已上线推广 {days} 天。\n\n当您推广的礼物被售出或推广结束时，您会收到通知。",
    },
    "promo_notfound": {
        "en": "{label} couldn't be found. \n\nkindly cross check if you inputted the wrong ID or gift isn't listed for sale.",
        "ru": "{label} не найден. \n\nпожалуйста, проверьте, не ошиблись ли вы в ID или подарок не выставлен на продажу.",
        "zh": "找不到 {label}。\n\n请检查您是否输入了错误的 ID，或该礼物未上架出售。",
    },
    "promo_bought": {
        "en": "{label} has been purchased. \n\ncongratulations\u2026 you're always welcome to promote with GiftTrove.",
        "ru": "{label} был куплен. \n\nпоздравляем\u2026 будем рады снова видеть вас в продвижении с GiftTrove.",
        "zh": "{label} 已被购买。\n\n恭喜\u2026 欢迎随时再次通过 GiftTrove 推广。",
    },
    "promo_ended": {
        "en": "{label} promotion has ended and not purchased. \n\nyou can consider making some changes with your listing and try again.",
        "ru": "продвижение {label} завершилось без покупки. \n\nвы можете изменить параметры объявления и попробовать снова.",
        "zh": "{label} 的推广已结束且未被购买。\n\n您可以考虑调整挂单后再试一次。",
    },
    "payout_review": {
        "en": "your payout request of {gram} GRAM is on review by an admin\n\nwe are working towards automatic payout soon\u2026 feel free to reach out to @asktrove if the payout is taking too long",
        "ru": "ваш запрос на выплату {gram} GRAM проверяется администратором\n\nмы скоро добавим автоматические выплаты\u2026 если выплата задерживается, напишите @asktrove",
        "zh": "您的 {gram} GRAM 提现申请正在由管理员审核\n\n我们即将推出自动提现\u2026 如果提现耗时过长，请联系 @asktrove",
    },
    "payout_credited": {
        "en": "congratulations, {gram} GRAM has been credited to {addr} \n\nkeep earning with @gifttrove {when}",
        "ru": "поздравляем, {gram} GRAM зачислено на {addr} \n\nпродолжайте зарабатывать с @gifttrove {when}",
        "zh": "恭喜，{gram} GRAM 已发放至 {addr} \n\n继续通过 @gifttrove 赚取收益 {when}",
    },
    "payout_declined": {
        "en": "your payout request has been declined for possible reasons which might include fake or gaming the system\n\nfeel free to reach out to @asktrove to request a review",
        "ru": "ваш запрос на выплату отклонён по возможным причинам, включая мошенничество или попытки обмануть систему\n\nнапишите @asktrove, чтобы запросить пересмотр",
        "zh": "您的提现申请已被拒绝，可能原因包括作弊或滥用系统\n\n如需复核，请联系 @asktrove",
    },
    "welcome": {
        "en": "Welcome to GiftTrove! Scout unique Telegram gifts from different marketplaces all at a go. \u2726",
        "ru": "Добро пожаловать в GiftTrove! Ищите уникальные подарки Telegram сразу с нескольких маркетплейсов. \u2726",
        "zh": "欢迎使用 GiftTrove！一次性从多个市场侦测独特的 Telegram 礼物。\u2726",
    },
    "share_hey": {
        "en": "Hey! Check out ",
        "ru": "Привет! Зацени ",
        "zh": "嘿！来看看 ",
    },
    "share_scout": {
        "en": "Scout unique Telegram gifts on GiftTrove",
        "ru": "Ищите уникальные подарки Telegram в GiftTrove",
        "zh": "在 GiftTrove 上侦测独特的 Telegram 礼物",
    },
    "share_open": {
        "en": "Open in GiftTrove",
        "ru": "Открыть в GiftTrove",
        "zh": "在 GiftTrove 中打开",
    },
}


async def _dm(uid, text, bold_ranges=None, code_ranges=None, link_ranges=None):
    """Send a plain DM to a user with optional rich entities:
      - bold_ranges:  list of (start, len)         -> bold
      - code_ranges:  list of (start, len)         -> monospace (code)
      - link_ranges:  list of (start, len, url)    -> tappable text link
    All offsets are PYTHON string indices; we convert to the UTF-16 units that
    Telegram entities require. Falls back to a plain Bot API message if entity
    sending isn't possible."""
    if not uid:
        return False
    txt = str(text or "")
    has_rich = bool(bold_ranges or code_ranges or link_ranges)
    if bot is not None and has_rich:
        try:
            from telethon.tl.types import (
                MessageEntityBold, MessageEntityCode, MessageEntityTextUrl)
            def _u16(s):
                return len(s.encode("utf-16-le")) // 2
            ents = []
            for start, length in (bold_ranges or []):
                if start < 0 or length <= 0 or start + length > len(txt):
                    continue
                ents.append(MessageEntityBold(_u16(txt[:start]), _u16(txt[start:start + length])))
            for start, length in (code_ranges or []):
                if start < 0 or length <= 0 or start + length > len(txt):
                    continue
                ents.append(MessageEntityCode(_u16(txt[:start]), _u16(txt[start:start + length])))
            for start, length, url in (link_ranges or []):
                if start < 0 or length <= 0 or start + length > len(txt) or not url:
                    continue
                ents.append(MessageEntityTextUrl(_u16(txt[:start]), _u16(txt[start:start + length]), url))
            if ents:
                await bot.send_message(int(uid), txt, formatting_entities=ents)
                return True
        except Exception as e:
            log.info("_dm rich send failed, falling back to plain: %s", e)
    try:
        await _bot_api("sendMessage", {"chat_id": int(uid), "text": txt})
        return True
    except Exception as e:
        log.info("_dm plain send failed: %s", e)
        return False


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
                inline24 = q("SELECT COUNT(*) c FROM events WHERE kind='inline' AND ts>?", (day,))
                toprow = conn.execute("SELECT gift, count FROM gift_searches ORDER BY count DESC LIMIT 1").fetchone()
            top_line = f"{toprow['gift']} ({toprow['count']} scouts)" if toprow else "none yet"
            txt = (
                f"Last 24h — opens: {opens24}, searches: {searches24}, shares: {shares24}, inline: {inline24}, new members: {new24}.\n"
                f"Total members: {members}.\n"
                f"Top scouted gift overall: {top_line}.\n"
                f"DB: {'Postgres' if USE_PG else 'SQLite'} — MTProto: {'live' if (client and client.is_connected()) else 'down'}."
            )
            await notify_admin(txt, level="digest")
            _meta_set("last_digest", now)
        except Exception as e:
            log.info("digest loop skipped: %s", e)


# ─── Telethon client lifecycle ────────────────────────────────────────────────
client = None      # the one MTProto user session
bot = None

# ─── Single MTProto session ────────────────────────────────────────────────
# The multi-session pool experiment (5 accounts sharing load) is removed.
# In practice 4 of 5 accounts kept getting rejected by Telegram at the
# transport level (TCP connects fine, then Telegram's server returns a
# malformed response instead of completing auth) no matter what retry or
# pooling logic wrapped around them — that's an external rejection, not
# something client-side code can fix. Worse, round-robin picking one of
# those dead sessions on every other call was actively making searches
# SLOWER and less reliable than just using the one session that works.
# Back to a single, simple, reliable session.
class _SessionStatus:
    def __init__(self):
        self.error = ""
        self.verified = False   # true only after an ACTUAL successful call —
                                 # is_connected() alone isn't a reliable signal
        self.last_flood_at = 0.0   # epoch seconds of the most recent flood wait

    @property
    def under_pressure(self):
        """True if we hit a flood wait recently. Optional/enhancement work
        (per-search image & animation fetching) should back off entirely
        while this is true — piling more MTProto calls onto an already
        rate-limited session just makes the core search/attribute fetches
        that actually matter fail more too."""
        return (time.time() - self.last_flood_at) < 45


_session = _SessionStatus()


# ─── Inline mode: warm floor cache + fast lookups ────────────────────────────
# Inline has a tight latency budget (Telegram wants an answer in ~1-2s). We never
# block an inline query on a slow live scrape: collection floors come from a warm
# cache refreshed in the background, and an exact-gift lookup is a single direct
# MTProto call wrapped in a short timeout with a graceful fall-back to the floor.
_INLINE_FLOOR_TTL = 180          # seconds a cached collection floor stays fresh
_inline_floor_cache: dict = {}   # gift_id(str) -> (unix_ts, serialized_listing | None)
_INLINE_WARM_TOP = 60            # how many top collections the warmer keeps hot


async def _cheapest_listing(gift_id):
    """The single cheapest live Telegram listing for a collection, or None.
    One MTProto call, price-sorted, limit=1 — the fast primitive inline needs."""
    GetResale = _payments("GetResaleStarGiftsRequest")
    if client is None or not GetResale or not gift_id:
        return None
    res = await _invoke(lambda: GetResale(gift_id=int(gift_id), attributes_hash=0,
                                          sort_by_price=True, offset="", limit=1))
    g = getattr(res, "gifts", []) or []
    return serialize_unique(g[0]) if g else None


async def _inline_floor(gift_id, timeout=1.6):
    """Cached cheapest listing for a collection. Serves a warm cache instantly;
    on a miss does ONE short live lookup (never hangs inline). Returns the
    serialized listing dict or None."""
    if not gift_id:
        return None
    key = str(gift_id)
    hit = _inline_floor_cache.get(key)
    now = time.time()
    if hit and (now - hit[0]) < _INLINE_FLOOR_TTL:
        return hit[1]
    try:
        item = await asyncio.wait_for(_cheapest_listing(key), timeout=timeout)
    except Exception:
        # On any error/timeout fall back to a stale cached value if we have one,
        # else None — inline must always answer fast.
        return hit[1] if hit else None
    _inline_floor_cache[key] = (now, item)
    return item


async def _inline_exact_gift(gift_id, slug, num, timeout=1.6):
    """Live data for an EXACT gift number via direct unique-gift lookup (reliable,
    O(1)). Returns the serialized listing dict or None. Short timeout so a slow
    lookup never makes inline feel broken — the caller falls back to the floor."""
    GetUnique = _payments("GetUniqueStarGiftRequest")
    if client is None or not GetUnique or not slug or not num:
        return None
    full = f"{slug}-{num}"
    try:
        res = await asyncio.wait_for(
            _invoke(lambda: GetUnique(slug=full)), timeout=timeout)
    except Exception:
        return None
    g = getattr(res, "gift", res)
    try:
        return serialize_unique(g)
    except Exception:
        return None


async def _warm_inline_floors():
    """Background: keep the top collections' floor prices hot so inline answers
    are sub-100ms cache reads. Runs gently, well within MTProto limits."""
    while True:
        try:
            await asyncio.sleep(150)   # refresh cycle; slightly under the TTL
            cols = cache_get("collections") or []
            for c in cols[:_INLINE_WARM_TOP]:
                gid = c.get("gift_id")
                if not gid:
                    continue
                try:
                    item = await asyncio.wait_for(_cheapest_listing(str(gid)), timeout=8)
                    _inline_floor_cache[str(gid)] = (time.time(), item)
                except Exception:
                    pass
                await asyncio.sleep(0.4)   # gentle pacing between collections
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.info("_warm_inline_floors error: %s", e)


def _fmt_price(item):
    """'380 GRAM' / '12.5 TON' / 'Stars' — a compact price label for inline cards."""
    if not item:
        return ""
    price = item.get("price")
    if price is None:
        return ""
    cur = item.get("currency") or "GRAM"
    try:
        n = float(price)
        amt = f"{int(n):,}" if n == int(n) else f"{n:,.2f}"
    except Exception:
        amt = str(price)
    return f"{amt} {cur}"


def _inline_deep_link(slug, sender_uid):
    """A GiftTrove deep-link to a specific gift carrying the SENDER's current
    referral code (resolved from our own records) so inline sharing drives
    referrals just like the in-app share does."""
    code = get_ref_code(sender_uid) if sender_uid else ""
    if slug:
        return f"{MINIAPP_URL}?startapp=g_{slug}_{code}" if code else f"{MINIAPP_URL}?startapp=g_{slug}"
    return f"{MINIAPP_URL}?startapp={code}" if code else MINIAPP_URL


def _parse_inline_query(q, cols):
    """Parse inline text into an intent.
    Returns (kind, collection, num) where kind is:
      'empty'      -> show featured/trending entry points
      'collection' -> floor + cheapest few for a matched collection
      'gift'       -> an exact gift number within a matched collection
      'cheap'      -> cheapest-first list for a matched collection
    collection is the matched cached collection dict (or None)."""
    q = (q or "").strip()
    if not q:
        return ("empty", None, "")
    toks = q.split()
    # Trailing explicit number (e.g. "chill flame 50090" or "... #50090")
    num = ""
    name_toks = toks[:]
    if toks and (toks[-1].lstrip("#").isdigit()):
        num = toks[-1].lstrip("#")
        name_toks = toks[:-1]
    # "cheap"/"low" verb anywhere -> sorted floor list
    verb_cheap = any(t.lower() in ("cheap", "cheapest", "low", "lowest", "floor") for t in name_toks)
    name_toks = [t for t in name_toks if t.lower() not in ("cheap", "cheapest", "low", "lowest", "floor")]
    name = " ".join(name_toks).strip().lower()

    col = None
    if name:
        # exact name match first, then prefix, then substring
        col = next((c for c in cols if (c.get("name", "").lower() == name)), None)
        if not col:
            col = next((c for c in cols if c.get("name", "").lower().startswith(name)), None)
        if not col:
            col = next((c for c in cols if name in c.get("name", "").lower()), None)
    elif num:
        # number only, no name — can't resolve a collection
        col = None

    if col and num:
        return ("gift", col, num)
    if col and verb_cheap:
        return ("cheap", col, "")
    if col:
        return ("collection", col, "")
    return ("empty", None, "")


async def _register_bot_handlers():
    if not bot:
        return
    from telethon import events, Button

    @bot.on(events.ChatAction(chats=COMMUNITY_GROUP_IDS))
    async def _on_community_join(event):
        # Someone joined one of the community chats — if they're an active
        # plus/pro subscriber (e.g. they upgraded before ever joining), tag
        # them right away instead of waiting for their next renewal event.
        if not event.user_joined and not event.user_added:
            return
        try:
            uid = str(event.user_id)
        except Exception:
            return
        tier = get_tier(uid)
        if tier in ("plus", "pro"):
            await _set_member_tag(uid, tier)

    @bot.on(events.NewMessage(pattern=r"^/start"))
    async def _start(event):
        # Seed the user's language from their Telegram client (the in-app switcher
        # can override it later via /api/lang). Then reply in that language.
        lang = "en"
        try:
            sender = await event.get_sender()
            lang = _norm_lang(getattr(sender, "lang_code", None))
            if event.sender_id:
                _set_user_lang(event.sender_id, lang)
        except Exception:
            lang = "en"
        labels = {
            "open": {"en": "Open GiftTrove", "ru": "Открыть GiftTrove", "zh": "打开 GiftTrove"},
            "community": {"en": "Join Community", "ru": "Сообщество", "zh": "加入社区"},
        }
        buttons = [
            [Button.url(labels["open"].get(lang, labels["open"]["en"]), MINIAPP_URL)],
            [Button.url(labels["community"].get(lang, labels["community"]["en"]), COMMUNITY_URL)],
        ]
        # Plain text (no bold), localized, with the landing image.
        text = _t("welcome", lang)
        try:
            await event.respond(text, file=WELCOME_IMAGE, buttons=buttons)
        except Exception as e:
            log.error("/start failed: %s", e)
            try:
                await event.respond(text, buttons=buttons)
            except Exception as e2:
                log.error("/start fallback failed: %s", e2)

    # ── Inline scouting: @gifttrovebot <query> — works in any DM / group ─────
    # Grammar (see _parse_inline_query):
    #   <empty>              -> trending collections with live floors
    #   <collection>         -> that collection's live floor (+ open card)
    #   <collection> <num>   -> an exact gift's live price/model/backdrop/symbol
    #   <collection> cheap   -> cheapest-first individual listings
    # Every card carries the sender's referral code and links to both GiftTrove
    # and the live marketplace listing. Answers come from a warm floor cache so
    # they stay within Telegram's tight inline latency budget.
    @bot.on(events.InlineQuery)
    async def _inline(event):
        # Inline mode (the "@gifttrovebot …" search in groups & DMs) is disabled
        # for now — planned for a later update. We return an empty result set so
        # the picker shows nothing instead of erroring. Also turn Inline Mode OFF
        # in @BotFather (/mybots → Bot Settings → Inline Mode → Turn off) so the
        # bot stops advertising inline search in the first place.
        try:
            await event.answer([])
        except Exception:
            pass
        return
        # ── legacy inline implementation kept below for the future re-enable ──
        q = (event.text or "").strip()
        sender_uid = str(getattr(event, "sender_id", "") or "")
        cols = cache_get("collections") or []
        builder = event.builder
        kind, col, num = _parse_inline_query(q, cols)

        def _gift_thumb(slug):
            # Reuse the same Fragment CDN image the app uses (correctly lowercased).
            return cdn_image(slug) if slug else None

        def _card(title, desc, body_segs, slug, market, market_url):
            """Build one inline article result. body_segs is a list of plain
            strings already joined; we attach two honest buttons + a thumbnail."""
            deep = _inline_deep_link(slug, sender_uid)
            btns = [[Button.url("Open in GiftTrove", deep)]]
            if market_url:
                btns.append([Button.url(f"View on {market}", market_url)])
            kw = {"title": title, "description": desc, "text": body_segs, "buttons": btns,
                  "link_preview": False}
            thumb = _gift_thumb(slug)
            if thumb:
                kw["thumb"] = thumb
            return builder.article(**kw)

        results = []
        try:
            if kind == "gift":
                # Exact gift number: one precise live card.
                gid = col.get("gift_id"); name = col.get("name", "Gift")
                cslug = col.get("slug") or ""
                item = await _inline_exact_gift(gid, cslug, num)
                if item:
                    slug = item.get("slug") or f"{cslug}-{num}"
                    price = _fmt_price(item)
                    mkt = item.get("market") or "Telegram"
                    attrs = " · ".join([a for a in (
                        (f"Model: {item.get('model')}" if item.get("model") else ""),
                        (f"Backdrop: {item.get('backdrop')}" if item.get("backdrop") else ""),
                        (f"Symbol: {item.get('symbol')}" if item.get("symbol") else "")) if a])
                    title = f"{name} #{num}"
                    body = f"{title}\n{('Floor: ' + price + ' · ') if price else ''}{mkt}"
                    if attrs:
                        body += f"\n{attrs}"
                    body += "\n\nScout live listings on GiftTrove"
                    results.append(_card(title, (price + " · " + mkt) if price else mkt,
                                         body, slug, mkt, item.get("url") or ""))
                else:
                    # Couldn't confirm that exact number — fall back to the floor.
                    floor = await _inline_floor(col.get("gift_id"))
                    name = col.get("name", "Gift")
                    if floor:
                        slug = floor.get("slug") or ""
                        price = _fmt_price(floor); mkt = floor.get("market") or "Telegram"
                        title = f"{name} — floor {price}" if price else name
                        body = (f"{name}\nCouldn't find #{num} listed right now. "
                                f"Cheapest available: {price} on {mkt}." if price else
                                f"{name}\nCouldn't find #{num} listed right now.")
                        body += "\n\nScout live listings on GiftTrove"
                        results.append(_card(title, f"#{num} not listed · see floor",
                                             body, slug, mkt, floor.get("url") or ""))

            elif kind in ("collection", "cheap"):
                gid = col.get("gift_id"); name = col.get("name", "Gift")
                cslug = col.get("slug") or ""
                floor = await _inline_floor(gid)
                # Lead card: the collection's live floor.
                if floor:
                    slug = floor.get("slug") or ""
                    price = _fmt_price(floor); mkt = floor.get("market") or "Telegram"
                    title = f"{name} — floor {price}" if price else f"{name}"
                    body = (f"{name}\nFloor: {price} · {mkt}" if price else f"{name}")
                    body += "\n\nScout live listings across marketplaces on GiftTrove"
                    results.append(_card(title, (f"Floor {price} · {mkt}") if price else "Live listings",
                                         body, slug, mkt, floor.get("url") or ""))
                else:
                    # No floor available — still offer to open the collection.
                    slug = f"{cslug}-1" if cslug else ""
                    body = f"{name}\nTap to scout live listings on GiftTrove."
                    results.append(_card(name, "Scout live listings", body, slug, "Telegram", ""))

                # For "cheap": add a few more individual cheapest listings.
                if kind == "cheap":
                    GetResale = _payments("GetResaleStarGiftsRequest")
                    if GetResale and gid:
                        try:
                            res = await asyncio.wait_for(
                                _invoke(lambda: GetResale(gift_id=int(gid), attributes_hash=0,
                                                          sort_by_price=True, offset="", limit=6)),
                                timeout=1.8)
                            gifts = getattr(res, "gifts", []) or []
                            for g in gifts:
                                it = serialize_unique(g)
                                slug = it.get("slug") or ""
                                price = _fmt_price(it); mkt = it.get("market") or "Telegram"
                                gn = it.get("num")
                                title = f"{name} #{gn} — {price}" if price else f"{name} #{gn}"
                                body = f"{name}{(' #' + str(gn)) if gn is not None else ''}\n{price} · {mkt}\n\nScout it on GiftTrove"
                                results.append(_card(title, f"{price} · {mkt}" if price else mkt,
                                                     body, slug, mkt, it.get("url") or ""))
                        except Exception:
                            pass

            # 'empty' or nothing matched: a few trending entry points. These read
            # ONLY the warm floor cache (never a live lookup) so the discovery
            # view is always instant even when the cache is cold right after a
            # deploy — a missing floor just shows the collection without a price.
            if not results:
                for c in cols[:6]:
                    gid = c.get("gift_id"); name = c.get("name", "Gift")
                    cslug = c.get("slug") or ""
                    hit = _inline_floor_cache.get(str(gid)) if gid else None
                    floor = hit[1] if hit else None
                    price = _fmt_price(floor) if floor else ""
                    slug = (floor.get("slug") if floor else (f"{cslug}-1" if cslug else "")) or ""
                    mkt = (floor.get("market") if floor else "Telegram") or "Telegram"
                    murl = floor.get("url") if floor else ""
                    title = f"{name} — floor {price}" if price else name
                    body = (f"{name}\nFloor: {price} · {mkt}" if price else
                            f"{name}\nTap to scout live listings on GiftTrove.")
                    body += "\n\nScout unique Telegram gifts on GiftTrove"
                    results.append(_card(title, (f"Floor {price}") if price else "Scout live listings",
                                         body, slug, mkt, murl))

            # Always-present escape hatch.
            results.append(builder.article(
                title="Open GiftTrove",
                description="Scout unique Telegram gifts",
                text="Scout unique Telegram gifts on GiftTrove.",
                buttons=[[Button.url("Open GiftTrove", _inline_deep_link("", sender_uid))]],
            ))

            await event.answer(results[:10], cache_time=30, private=True)
            try:
                _bump_event("inline")
            except Exception:
                pass
        except Exception as e:
            log.info("inline answer failed: %s", e)
            try:
                await event.answer([builder.article(
                    title="Open GiftTrove",
                    description="Scout unique Telegram gifts",
                    text="Scout unique Telegram gifts on GiftTrove.",
                    buttons=[[Button.url("Open GiftTrove", MINIAPP_URL)]],
                )], cache_time=10, private=True)
            except Exception:
                pass

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

    # ── Affiliate payout review (admin Approve / Decline buttons) ─────────────
    @bot.on(events.CallbackQuery(pattern=b"^aff(approve|decline):"))
    async def _payout_review(event):
        if str(event.sender_id) not in ADMIN_IDS:
            await event.answer("Not allowed.", alert=True)
            return
        data = event.data.decode()
        act, pid = data.split(":", 1)
        payout = _affiliate_payout_get(pid)
        if not payout:
            await event.answer("Request not found.", alert=True)
            return
        if (payout.get("status") or "") != "requested":
            await event.answer("Already handled.", alert=True)
            try:
                await event.edit(f"Already {payout.get('status')}.")
            except Exception:
                pass
            return
        uid = payout["uid"]
        stars = int(payout.get("stars") or 0)
        addr = payout.get("ton_address") or ""
        gram = await _fmt_gram(stars)
        if act == "affapprove":
            _affiliate_payout_set_status(pid, "paid")
            try:
                await _send_payout_credited(uid, stars, addr)
            except Exception as e:
                log.info("payout credited DM failed: %s", e)
            try:
                await event.edit(f"Approved \u2014 {gram} GRAM ({stars}\u2605) marked paid to\n{addr}")
            except Exception:
                pass
        else:
            # Decline releases the held amount back to 'available' (it's no longer
            # counted as pending), so the member keeps their balance.
            _affiliate_payout_set_status(pid, "rejected")
            try:
                await _send_payout_declined(uid)
            except Exception as e:
                log.info("payout declined DM failed: %s", e)
            try:
                await event.edit(f"Declined \u2014 {gram} GRAM returned to the member's balance.")
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
        now0 = int(time.time())
        # Snapshot the prior subscription BEFORE we overwrite it, so we can tell a
        # first activation from an auto-renewal from a tier switch.
        old = _sub_full(uid)            # (old_tier, old_expires_at, old_charge) or None
        # If they're switching tiers (e.g. Scout+ -> Scout Pro), cancel the OLD
        # subscription so Telegram doesn't keep charging for both.
        try:
            if old and old[0] != tier and old[2] and bot is not None:
                from telethon.tl import functions as _fn
                try:
                    peer = await bot.get_input_entity(int(uid))
                    await bot(_fn.payments.BotCancelStarsSubscriptionRequest(
                        user_id=peer, charge_id=old[2]))
                    log.info("cancelled old %s subscription for %s on tier change", old[0], uid)
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
        # Reflect the new tier as a community-chat member tag (Scout+ / Scout Pro).
        # Fire-and-forget: this is a courtesy badge, never something a payment
        # confirmation should wait on or fail over.
        try:
            asyncio.create_task(_set_member_tag(uid, tier))
        except Exception:
            pass
        # Affiliate: pay the referrer a recurring cut of EVERY payment their
        # referral makes, PROVIDED the referrer is a CURRENT Pro at the moment
        # of that payment. This is a deliberate correction of an earlier
        # design: earning is now gated on being an active Pro right now, not
        # on the historical referral relationship alone. If a referrer's Pro
        # lapses, earning simply stops accruing (their existing balance isn't
        # touched, but no NEW credit comes in) — and the moment they
        # resubscribe, earning resumes automatically, since this check runs
        # live on every payment event rather than needing a separate
        # "resume" trigger. Dashboard *access* is a further, separate check
        # (see is_pro in /api/affiliate) — same current-Pro requirement,
        # applied at view-time instead of earn-time.
        try:
            ref = _referrer_of(uid)
            if ref and str(ref) != str(uid) and get_tier(ref) == "pro":
                paid = getattr(action, "total_amount", 0) or (STARS_PRO if tier == "pro" else STARS_PLUS)
                cut = int(int(paid) * AFFILIATE_PCT / 100)
                _affiliate_credit(ref, uid, tier, cut, charge_id)
        except Exception as e:
            log.info("affiliate credit skipped: %s", e)
        # Renewal = the SAME tier was active (or only just lapsed within ~2 days)
        # and Telegram auto-charged it again. Otherwise it's a first activation or
        # a switch — both get the "now active" welcome.
        is_renewal = bool(old and old[0] == tier and int(old[1] or 0) > (now0 - 2 * 86400))
        switched_from = old[0] if (old and old[0] and old[0] != tier and int(old[1] or 0) > now0) else ""
        if is_renewal:
            await _send_sub_renewed(uid, tier)
        else:
            await _send_sub_confirmation(uid, tier, switched_from=switched_from)
    elif kind == "promo" and len(parts) >= 3:
        pid, uid = parts[1], parts[2]
        _promo_set_charge(pid, charge_id)
        promo = _promo_get(pid) or {}
        mkt  = promo.get("marketplace") or "Telegram"
        num  = promo.get("num") or ""
        slug = promo.get("slug") or ""
        gift_id = promo.get("gift_id") or ""
        coll_name = promo.get("collection") or ""
        # Auto-fetch listing data: price, model, symbol, backdrop from the marketplace.
        # No manual admin review — every marketplace is handled programmatically.
        # fetch=None  → inconclusive (API error, network hiccup) — park as pending
        # fetch={}    → confirmed not found (API succeeded, listing absent)
        # fetch={...} → listing confirmed live — activate
        fetch = None
        try:
            result = await _promo_auto_fetch(mkt, slug, gift_id, num, coll_name)
            # _promo_auto_fetch returns {} for "not found" and a real dict for "found"
            fetch = result  # dict (possibly empty) means we got a real answer
        except Exception as e:
            log.info("promo activation fetch error (inconclusive — parking as pending): %s", e)
            fetch = None   # explicitly None = we don't know; don't burn Stars
        if fetch:
            # Listing confirmed live — activate and show at the top of matching scouts.
            _promo_set_price(pid, fetch.get("price", ""), fetch.get("currency", ""))
            _promo_set_attrs(pid, fetch.get("model"), fetch.get("symbol"),
                             fetch.get("backdrop"), fetch.get("url"))
            exp = _promo_activate(pid, uid, charge_id)
            log.info("promotion active: id=%s uid=%s mkt=%s num=%s until=%s", pid, uid, mkt, num, exp)
            try:
                label, gift_url = _promo_label_link(coll_name, num, slug, fetch.get("url") or promo.get("link"), mkt)
                await _send_promo_live(uid, label, gift_url, PROMO_DAYS)
            except Exception:
                pass
        elif fetch is None:
            # Inconclusive — API was unreachable or returned an error. Park as
            # 'pending' so the background loop can retry. Stars are NOT consumed
            # yet — the promo stays in a holding state and activates automatically
            # once the check succeeds. If it never resolves, a human review of
            # 'pending' promos is needed (see admin endpoint).
            _promo_set_status(pid, "pending")
            log.warning("promotion PENDING (inconclusive fetch — will retry): id=%s uid=%s mkt=%s num=%s",
                        pid, uid, mkt, num)
        else:
            # fetch == {} → listing NOT found on a successful check. Do NOT activate.
            # Stars are kept (entering a gift that isn't listed is the member's
            # mistake). The promo is parked as 'unlisted' and is spent — no retry.
            _promo_set_status(pid, "unlisted")
            log.info("promotion unlisted (not activated, no refund): id=%s uid=%s mkt=%s num=%s",
                     pid, uid, mkt, num)
            try:
                label, gift_url = _promo_label_link(coll_name, num, slug, promo.get("link"), mkt)
                await _send_promo_notfound(uid, label, gift_url)
            except Exception:
                pass
    else:
        log.info("payment with unrecognized payload: %r", payload)


async def _send_sub_confirmation(uid, tier, switched_from=""):
    """DM the member what their new subscription unlocks, in their language.
    switched_from is accepted for signature compatibility but no longer appended —
    switching plans auto-cancels the previous tier, so there's nothing to do."""
    lang = _user_lang(uid)
    msg = INFO + _t("sub_pro" if tier == "pro" else "sub_plus", lang)
    await _dm_photo(uid, IMG_PURCHASE_UPGRADE, msg)


async def _send_sub_renewed(uid, tier):
    """DM sent when Telegram auto-charges an existing subscription (a renewal)."""
    plan = "scout pro" if tier == "pro" else "scout+"
    msg = INFO + _t("sub_renewed", _user_lang(uid)).format(plan=plan)
    await _dm(uid, msg)


async def _send_sub_insufficient(uid, tier):
    """DM sent when a subscription lapses without renewal (e.g. low Star balance).
    The word 'hoton' links to the current top-up referral."""
    plan = "scout pro" if tier == "pro" else "scout+"
    msg = INFO + _t("sub_insufficient", _user_lang(uid)).format(plan=plan)
    start = msg.rfind("hoton")
    links = [(start, len("hoton"), HOTON_URL)] if start >= 0 else None
    await _dm(uid, msg, link_ranges=links)


async def _send_sub_cancel_confirmation(uid, tier):
    """DM sent after a member cancels in-app — reminds them to also confirm the
    cancellation in Telegram so the recurring Stars charge actually stops."""
    plan = "scout pro" if tier == "pro" else "scout+"
    msg = INFO + _t("sub_cancel", _user_lang(uid)).format(plan=plan)
    await _dm(uid, msg)


# ─── Promoted-gift DMs (label hyperlinks to the exact t.me gift link) ─────────
async def _send_promo_live(uid, label, url, days):
    msg = INFO + _t("promo_live", _user_lang(uid)).format(label=label, days=days)
    links = [(len(INFO), len(label), url)] if url else None
    await _dm(uid, msg, link_ranges=links)


async def _send_promo_notfound(uid, label, url):
    msg = INFO + _t("promo_notfound", _user_lang(uid)).format(label=label)
    links = [(len(INFO), len(label), url)] if url else None
    await _dm_photo(uid, IMG_DECLINED_ENDED, msg, link_ranges=links)


async def _send_promo_bought(uid, label, url):
    msg = INFO + _t("promo_bought", _user_lang(uid)).format(label=label)
    links = [(len(INFO), len(label), url)] if url else None
    await _dm_photo(uid, IMG_PURCHASE_UPGRADE, msg, link_ranges=links)


async def _send_promo_ended(uid, label, url):
    msg = INFO + _t("promo_ended", _user_lang(uid)).format(label=label)
    links = [(len(INFO), len(label), url)] if url else None
    await _dm_photo(uid, IMG_DECLINED_ENDED, msg, link_ranges=links)


# ─── Affiliate payout DMs + admin review ─────────────────────────────────────
async def _send_payout_under_review(uid, stars):
    """DM sent the moment a member submits a payout request (balance held pending)."""
    gram = await _fmt_gram(stars)
    msg = INFO + _t("payout_review", _user_lang(uid)).format(gram=gram)
    await _dm(uid, msg)


async def _send_payout_credited(uid, stars, addr):
    """DM sent when an admin approves a payout (GRAM sent to the member's wallet)."""
    gram = await _fmt_gram(stars)
    when = time.strftime("%b %d, %Y \u00b7 %H:%M UTC", time.gmtime())
    msg = INFO + _t("payout_credited", _user_lang(uid)).format(gram=gram, addr=addr, when=when)
    await _dm_photo(uid, IMG_CREDIT_SUCCESS, msg)


async def _send_payout_declined(uid):
    """DM sent when an admin declines a payout (held balance returns to available)."""
    msg = INFO + _t("payout_declined", _user_lang(uid))
    await _dm_photo(uid, IMG_DECLINED_ENDED, msg)


async def _notify_admin_payout(payout, stats=None):
    """Send the owner an interactive payout request (user id, amount in Stars and
    GRAM, wallet address) with Approve / Decline buttons. Goes to every admin so
    the request can be actioned from the owner's account."""
    if bot is None:
        return
    try:
        from telethon import Button
    except Exception:
        return
    pid = payout.get("id"); uid = payout.get("uid")
    stars = int(payout.get("stars") or 0); addr = payout.get("ton_address") or "—"
    gram = await _fmt_gram(stars)
    s = stats or _affiliate_stats(uid)
    text = (
        "**Affiliate payout request**\n\n"
        f"User ID: `{uid}`\n"
        f"Amount: {stars}\u2605  \u2248 {gram} GRAM\n"
        f"Address: `{addr}`\n\n"
        f"Referrals: {s.get('referees', 0)} \u00b7 Paid referees: {s.get('payers', 0)} "
        f"\u00b7 Lifetime earned: {s.get('earned', 0)}\u2605\n"
        f"Run an authenticity scan on `{uid}` in Analytics before approving."
    )
    buttons = [[Button.inline("Approve", f"affapprove:{pid}".encode()),
                Button.inline("Decline", f"affdecline:{pid}".encode())]]
    for admin in ADMIN_IDS:
        try:
            await bot.send_message(int(admin), text, buttons=buttons, parse_mode="md")
        except Exception as e:
            log.info("payout admin notify failed (admin=%s): %s", admin, e)


async def _connect_user_session():
    """(Re)connect the single user session. Returns True on success."""
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
            # Auto-sleep only for short waits; longer floods raise (we catch them).
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
        _mtproto_error = f"{type(e).__name__}: {e}"
        log.error("MTProto connect failed: %s: %r", type(e).__name__, e)
        return False


_consecutive_mtproto_failures = 0

async def _hard_reset_client():
    """Full teardown + recreation of the MTProto client OBJECT itself — not
    just disconnect()/connect() on the same object.

    Observed in production: Telethon's own internal 'am I connected'
    bookkeeping can get stuck desynced from the actual socket state — its own
    logs showed "Not disconnecting (already have no connection)" immediately
    followed by "User is already connected!" on the very next attempt, over
    and over, for many minutes straight, with every single real RPC call
    timing out the whole time. is_connected() kept reporting a state that
    didn't reflect reality, so disconnect()+connect() on that SAME object just
    repeated the same broken cycle — it was asking the confused object to fix
    itself using the same confused bookkeeping.

    A brand new TelegramClient has none of that stale internal state to be
    stuck in. This is deliberately NOT the first thing tried on every failure
    (that would be wasteful for an ordinary one-off hiccup) — see the
    consecutive-failure counter in _invoke, which only escalates to this
    after the cheap same-object retry has clearly stopped working."""
    global client, _consecutive_mtproto_failures
    log.warning("MTProto: hard-resetting client after repeated consecutive failures")
    old = client
    client = None
    try:
        if old is not None:
            await asyncio.wait_for(old.disconnect(), timeout=10)
    except Exception:
        pass
    _consecutive_mtproto_failures = 0
    await _connect_user_session()


async def background_telethon_initializer():
    """Connect Telethon in the background (so Uvicorn binds the port instantly),
    then keep the connection warm with a keepalive loop."""
    global bot, _mtproto_error

    # 1) User session (with one retry)
    for attempt in (1, 2):
        log.info("MTProto: connecting user session (attempt %d)...", attempt)
        ok = await _connect_user_session()
        if ok:
            try:
                me = await asyncio.wait_for(client.get_me(), timeout=20)
                log.info("MTProto session live as @%s", getattr(me, "username", me.id))
                _session.verified = True
            except Exception as e:
                log.warning("get_me after connect failed: %r", e)
                _session.verified = False
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
        asyncio.create_task(_bg_expire_promos())
        asyncio.create_task(_bg_expire_subs())
        asyncio.create_task(_warm_inline_floors())
        asyncio.create_task(_bg_warm_attributes())
        asyncio.create_task(_bg_refresh_collections_loop())
        # One-time backfill: members who were already on an active plus/pro
        # subscription BEFORE the member-tag feature shipped only get tagged at
        # their next payment event otherwise (up to 30 days away). Catch them now.
        asyncio.create_task(_backfill_member_tags())

    # 2c) Reports: deploy-live good news + the daily digest loop.
    try:
        asyncio.create_task(_daily_digest_loop())
        asyncio.create_task(notify_admin(
            f"Backend deployed and live.\nDB: {'Postgres' if USE_PG else 'SQLite'}.\n"
            f"MTProto session: {'connected and verified' if (client and client.is_connected() and _session.verified) else 'offline'}. "
            f"Bot: online.",
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
                    _session.verified = True
            _mtproto_error = "" if client and client.is_connected() else _mtproto_error
        except Exception as e:
            log.warning("keepalive: connection looked dead (%s) — reconnecting", e)
            _session.verified = False
            try:
                await _hard_reset_client()
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
    for c in [client, bot]:
        if c:
            try:
                await c.disconnect()
            except Exception:
                pass


app = FastAPI(title="GiftTrove API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS or ["*"],
    # Vercel serves the same project under several valid URLs (the custom
    # domain, and auto-generated per-deployment/preview URLs) — an exact-match
    # allow_origins list breaks the instant a request comes from any of those
    # OTHER valid URLs, which is exactly what "beta works, production doesn't"
    # looks like. This regex covers any vercel.app subdomain as a safety net
    # on top of the explicit list above.
    allow_origin_regex=r"https://.*\.vercel\.app",
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
    global _consecutive_mtproto_failures
    if client is None:
        raise RuntimeError("MTProto client not initialised yet")
    timeout = timeout or MTPROTO_TIMEOUT
    last = None
    async with _mtproto_lock:
        for attempt in (1, 2):
            try:
                if not client.is_connected():
                    await asyncio.wait_for(client.connect(), timeout=15)
                result = await asyncio.wait_for(client(build()), timeout=timeout)
                _session.verified = True
                _consecutive_mtproto_failures = 0
                return result
            except FloodWaitError as e:
                # Rate-limited by Telegram. Reconnecting won't help — bail out
                # so the handler can serve cache / empty instead of cascading.
                _session.last_flood_at = time.time()
                log.warning("flood wait %ss on MTProto call — skipping", getattr(e, "seconds", "?"))
                raise
            except Exception as e:
                last = e
                _session.verified = False
                _consecutive_mtproto_failures += 1
                log.error("MTProto invoke attempt %d failed: %s: %r (consecutive failures: %d)",
                          attempt, type(e).__name__, e, _consecutive_mtproto_failures)
                # Escalate to a full client rebuild once the cheap same-object
                # retry has clearly stopped working (see _hard_reset_client for
                # why this is necessary rather than just trying harder with
                # the same object). Threshold of 4 is roughly "two full
                # _invoke calls' worth of attempts", so a single unlucky call
                # doesn't trigger a rebuild, but a genuinely stuck connection
                # gets caught within well under a minute.
                if _consecutive_mtproto_failures >= 4:
                    await _hard_reset_client()
                elif attempt == 1:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    try:
                        await asyncio.wait_for(client.connect(), timeout=15)
                    except Exception as e2:
                        log.error("reconnect failed: %s: %r", type(e2).__name__, e2)
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


_thumb_sem = asyncio.Semaphore(3)

async def _doc_thumb_uri(doc):
    did = getattr(doc, "id", None)
    if did is None:
        return None
    if did in _thumb_cache:
        return _thumb_cache[did]
    raw = None
    thumbs = getattr(doc, "thumbs", None) or []
    # Prefer the smallest REAL PhotoSize (has w/h, no inline bytes) —
    # crisp enough for icons without multi-MB payloads.
    real = [t for t in thumbs if getattr(t, "w", 0) and not getattr(t, "bytes", None)]
    pick = min(real, key=lambda t: getattr(t, "w", 10**6)) if real else None
    if pick is None and not thumbs:
        return None
    try:
        if client is not None:
            if not client.is_connected():
                await asyncio.wait_for(client.connect(), timeout=10)
            async with _thumb_sem:
                try:
                    raw = await asyncio.wait_for(
                        client.download_media(doc, file=bytes, thumb=pick if pick is not None else -1),
                        timeout=15,
                    )
                except TypeError:
                    raw = await asyncio.wait_for(
                        client.download_media(doc, file=bytes, thumb=-1), timeout=15
                    )
    except Exception as e:
        if isinstance(e, FloodWaitError):
            _session.last_flood_at = time.time()
        log.info("thumb download skipped: %s", e)
        raw = None
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


_anim_cache = {}   # doc_id -> decompressed Lottie JSON bytes, or False if confirmed unavailable

async def _doc_anim_json(doc):
    """
    Fetch a model attribute's FULL document (not just its thumbnail) and, if
    it's a TGS file, decompress it to raw Lottie JSON.

    TGS is Telegram's own animated-sticker format, and by Telegram's own
    published spec a .tgs file IS a gzip-compressed Lottie animation — the
    exact same JSON format LottieGift already plays everywhere else in this
    app. So when a gift's model document turns out to be TGS, we get a real
    Telegram-sourced ANIMATION, not just a static thumbnail — available the
    instant the gift exists, with zero dependency on Fragment ever crawling
    the collection.

    Detection is by gzip's own magic bytes (0x1f 0x8b), not by trusting a
    mime_type string, since that's a format guarantee rather than metadata
    that could be missing or wrong.

    Returns decompressed JSON bytes, or None if this document isn't TGS (a
    plain static-image model — nothing wrong, just nothing to animate).
    """
    did = getattr(doc, "id", None)
    if did is None:
        return None
    cached = _anim_cache.get(did)
    if cached is not None:
        return cached if cached is not False else None
    # Durable check BEFORE a live fetch — this is what actually survives a
    # Render restart. Previously only the in-memory dict was checked, so
    # every cold start meant every animation had to be re-fetched from
    # Telegram from scratch before it would show again.
    db_data, _db_ts = _anim_db_get(did)
    if db_data:
        if len(_anim_cache) < 2000:
            _anim_cache[did] = db_data
        return db_data
    raw = None
    transient_failure = False
    try:
        if client is not None:
            if not client.is_connected():
                await asyncio.wait_for(client.connect(), timeout=10)
            async with _thumb_sem:
                raw = await asyncio.wait_for(client.download_media(doc, file=bytes), timeout=15)
    except Exception as e:
        if isinstance(e, FloodWaitError):
            _session.last_flood_at = time.time()
        log.info("anim download skipped: %s", e)
        raw = None
        transient_failure = True
    if not raw or raw[:2] != b"\x1f\x8b":
        if not transient_failure:
            _anim_cache[did] = False   # confirmed not TGS — don't retry every request
        return None
    try:
        data = gzip.decompress(bytes(raw))
        json.loads(data)   # validate it's real JSON before trusting/caching it
    except Exception as e:
        log.info("anim decompress failed for doc %s: %s", did, e)
        _anim_cache[did] = False
        return None
    if len(_anim_cache) < 2000:
        _anim_cache[did] = data
    _anim_db_set(did, data)
    return data


@app.get("/api/model-anim/{doc_id}")
async def model_anim(doc_id: int):
    """Serves the decompressed Lottie JSON cached by _doc_anim_json above.
    This is what item["animation"] points to for gifts whose model turned out
    to be a real TGS animation — the frontend's LottieGift already just does
    a plain fetch(src) on whatever URL is in `animation`, so no frontend
    change was needed to consume this."""
    data = _anim_cache.get(doc_id)
    if not data or data is False:
        db_data, _ts = _anim_db_get(doc_id)
        if db_data:
            data = db_data
            if len(_anim_cache) < 2000:
                _anim_cache[doc_id] = db_data
    if not data or data is False:
        return JSONResponse(status_code=404, content={"error": "not_available"})
    from fastapi.responses import Response
    return Response(content=data, media_type="application/json",
                     headers={"Cache-Control": "public, max-age=604800, immutable"})


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
    model = model_rarity = symbol = symbol_rarity = backdrop = backdrop_rarity = backdrop_hex = model_doc = None
    for a in getattr(g, "attributes", []) or []:
        cls = type(a).__name__
        rar = getattr(a, "rarity_permille", None)
        rar = round(rar / 10, 2) if isinstance(rar, (int, float)) else None
        if cls == "StarGiftAttributeModel":
            model, model_rarity = getattr(a, "name", None), rar
            model_doc = getattr(a, "document", None)
        elif cls == "StarGiftAttributePattern":
            symbol, symbol_rarity = getattr(a, "name", None), rar
        elif cls == "StarGiftAttributeBackdrop":
            backdrop, backdrop_rarity = getattr(a, "name", None), rar
            backdrop_hex = color_hex(getattr(a, "center_color", None))
    return model, model_rarity, symbol, symbol_rarity, backdrop, backdrop_rarity, backdrop_hex, model_doc


def _model_doc_of(g):
    """The exact per-item model document Telegram embeds directly on this
    specific unique gift — NOT the collection-wide attribute list. This is
    what lets us show the correct model artwork straight from Telegram,
    available the instant a gift exists (no dependency on Fragment having
    crawled/indexed the collection yet)."""
    for a in getattr(g, "attributes", []) or []:
        if type(a).__name__ == "StarGiftAttributeModel":
            return getattr(a, "document", None)
    return None


def serialize_unique(g):
    num = getattr(g, "num", None)
    title = getattr(g, "title", None) or "Gift"
    slug = getattr(g, "slug", None) or _slug_from_title(title)
    base = cdn_full(slug, num)
    model, model_rarity, symbol, symbol_rarity, backdrop, backdrop_rarity, backdrop_hex, _model_doc = _gift_attrs(g)
    price, currency, gram_value = _extract_price(g)
    return {
        "id": str(getattr(g, "id", base)),
        "slug": base,
        "num": num,
        "name": title,
        "model": model,
        "modelRarity": model_rarity,
        "symbol": symbol,
        "symbolRarity": symbol_rarity,
        "backdrop": backdrop,
        "backdropRarity": backdrop_rarity,
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
        "model": None, "modelRarity": None, "symbol": None, "symbolRarity": None,
        "backdrop": None, "backdropRarity": None, "backdropHex": None,
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
    # Fragment is an unofficial scrape target behind Cloudflare, so an occasional
    # transient 500/429 is expected, not a real outage — retry once after a short
    # delay before treating it as a genuine failure worth alerting on.
    for attempt in range(2):
        try:
            async with httpx.AsyncClient(timeout=6, follow_redirects=True) as cli:
                r = await cli.get(url, headers=headers)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            items = _parse_fragment_gifts(r.text, tg_slug, gift_name, fslug)
            _frag_cache[fslug] = (now, items)
            if len(_frag_cache) > 200:
                _frag_cache.pop(next(iter(_frag_cache)))
            return items[:limit]
        except Exception as e:
            if attempt == 0:
                await asyncio.sleep(0.7)
                continue
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
# Endpoints (from their OpenAPI spec at api.marketapp.org/docs/openapi.json):
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

_BACKDROP_HEX = {}   # {backdrop_name_lower: hex} — backdrop colours are universal
                     # across gifts, so this fills from any /api/attributes call and
                     # lets MarketApp/Fragment listings resolve a colour dot even
                     # when no per-gift attribute data is cached for them yet.

def _backdrop_hex_lookup(gift_id, backdrop_name):
    """Resolve a backdrop colour hex by name. Tries the per-gift attribute cache
    first (from /api/attributes), then falls back to the global name->hex map,
    so Fragment/MarketApp listings get the same colour dot as Telegram ones."""
    if not backdrop_name:
        return None
    name_l = backdrop_name.strip().lower()
    gift_id = _digits(gift_id) if gift_id else None
    if gift_id:
        payload = cache_get(f"attrs:{gift_id}")
        if not (payload and isinstance(payload, dict)):
            db_payload, _ts = _attrs_db_get(gift_id)
            payload = (db_payload or {}).get("resp") if isinstance(db_payload, dict) else None
        if payload and isinstance(payload, dict):
            for b in payload.get("backdrops") or []:
                if str(b.get("name") or "").strip().lower() == name_l:
                    return b.get("hex") or _BACKDROP_HEX.get(name_l)
    return _BACKDROP_HEX.get(name_l)


def _marketapp_item(raw, gift_name, fallback_slug, gift_id=None):
    if not isinstance(raw, dict):
        return None
    num = raw.get("item_num")
    try:
        num = int(num) if num is not None else None
    except Exception:
        num = None
    if num is None:
        # MarketApp doesn't actually return a separate item-number field — the
        # number only exists as the "#NNNN" suffix on "name" (e.g. "Plush Pepe
        # #476"). Without this, num stayed None for every MarketApp item, which
        # meant no CDN image/animation URL could ever be built for them — the
        # empty-box placeholder the user was seeing was 100% of MarketApp results.
        _m = _re.search(r"#(\d+)\s*$", str(raw.get("name") or ""))
        if _m:
            try:
                num = int(_m.group(1))
            except Exception:
                num = None
    # min_bid / max_bid are integer-string amounts in the listing's OWN currency,
    # scaled by that currency's decimals: TON/GRAM = 9 decimals (nanotons),
    # USDT = 6 decimals. For a fixed-price sale min_bid == max_bid; for an auction
    # min_bid is the opening bid and max_bid the buy-now — take the higher so a low
    # opening bid can't masquerade as the floor in the "lowest" sort.
    currency_raw = str(raw.get("currency") or "TON").upper()
    # Launching with Stars / GRAM(TON) / USDT only. Some aggregated listings (e.g.
    # via GetGems) are priced in other coins — skip them entirely rather than
    # mislabel the amount and confuse buyers.
    if currency_raw not in ("TON", "GRAM", "USDT"):
        return None
    _scale = 1e6 if currency_raw == "USDT" else 1e9
    try:
        _minb = float(raw.get("min_bid") or 0)
        _maxb = float(raw.get("max_bid") or 0)
        bid = _maxb if _maxb > _minb else _minb
        price = bid / _scale if bid > 0 else None
    except Exception:
        price = None
    if currency_raw == "USDT":
        # Priced in USDT — show the real USDT amount; it is NOT a GRAM/TON value,
        # so don't expose a gram_value (the frontend won't mislabel it).
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
        url = f"https://marketapp.org/nft/{nft_address}/"
    elif coll_address:
        url = f"https://marketapp.org/collection/{coll_address}/"
    else:
        url = "https://marketapp.org/gifts/"
    backdrop_name = _attr("backdrop", "background") or ""
    # MarketApp's own "name" field already includes the item number, e.g.
    # "Plush Pepe #476" — strip that suffix so the frontend (which appends
    # " #{num}" itself for every marketplace) doesn't show it twice.
    raw_name = str(raw.get("name") or "").strip()
    clean_name = _re.sub(r"\s*#\d+\s*$", "", raw_name).strip() or gift_name
    return {
        "id": nft_address or f"ma-{slug}-{num}",
        "name": clean_name,
        "slug": slug, "num": num,
        "price": price, "currency": currency, "gram_value": gram_value,
        "model":   _attr("model") or "",
        "symbol":  _attr("symbol", "pattern") or "",
        "backdrop": backdrop_name,
        "backdropHex": _backdrop_hex_lookup(gift_id, backdrop_name),
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
        async with httpx.AsyncClient(timeout=8) as cli:
            r = await cli.get(url, params=params,
                              headers={"Authorization": MARKETAPP_TOKEN, "Accept": "application/json"})
        log.info("marketapp %s status=%s sample=%s", url, r.status_code,
                 (r.text or "")[:400].replace("\n", " "))
        if r.status_code != 200:
            return []
        data = r.json()
        rows = (data.get("items") or []) if isinstance(data, dict) else []
        out = [it for raw in rows for it in [_marketapp_item(raw, gift_name, slug, gift_id)] if it]
        _marketapp_cache[key] = (now, out)
        if len(_marketapp_cache) > 300:
            _marketapp_cache.pop(next(iter(_marketapp_cache)))
        return out[:limit]
    except Exception as e:
        log.info("marketapp search skipped: %s", e)
        return []


# ─── Thermos (Proxy API — public aggregator across TONNEL/PORTALS/MRKT) ──────
_thermos_cache = {}

def _thermos_item(raw, gift_name, slug, gift_id=None):
    """Normalize one item from Thermos's /v1/gifts search response into
    GiftTrove's shared item shape. Their response already gives model/symbol/
    backdrop as {name, rarity_per_mille} — same permille-of-1000 scale
    Telegram itself uses, so /10 gives the same percent format as every other
    source. Price is a plain nanoton string (no per-listing currency field —
    their docs confirm everything here is TON)."""
    if not isinstance(raw, dict):
        return None
    try:
        price = float(raw.get("price") or 0) / 1e9
    except Exception:
        price = None
    def _attr(key):
        d = raw.get(key) or {}
        name = d.get("name") or ""
        rar = d.get("rarity_per_mille")
        rar = round(rar / 10, 2) if isinstance(rar, (int, float)) else None
        return name, rar
    model, model_rarity = _attr("model")
    symbol, symbol_rarity = _attr("symbol")
    backdrop, backdrop_rarity = _attr("backdrop")
    num = raw.get("number")
    try:
        num = int(num) if num is not None else None
    except Exception:
        num = None
    ext_id = str(raw.get("external_id") or "")
    return {
        "id": f"thermos-{ext_id}" if ext_id else f"thermos-{slug}-{num}",
        "name": gift_name,
        "slug": slug, "num": num,
        "price": price, "currency": "GRAM", "gram_value": price,
        "model": model, "modelRarity": model_rarity,
        "symbol": symbol, "symbolRarity": symbol_rarity,
        "backdrop": backdrop, "backdropRarity": backdrop_rarity,
        "backdropHex": _backdrop_hex_lookup(gift_id, backdrop),
        "market": "Thermos",
        "url": _thermos_gift_url(gift_name, num, ext_id),
        "image": raw.get("image_url"), "animation": raw.get("lottie_url"),
    }


def _thermos_gift_url(gift_name, num, ext_id=""):
    """Deep link to a specific gift through YOUR OWN referral link, so
    purchases made via GiftTrove credit back to the referrer account.
    Format confirmed directly from a real share link:
      https://t.me/thermos/thermos/?startapp=ref_<REF_ID>_g_<Collection_Name>-<number>
    Falls back to a plain (non-deep) link if the referral id or the specific
    gift number isn't available, rather than building a malformed deep link."""
    if THERMOS_REF_ID and num is not None and gift_name:
        collection_slug = "_".join(str(gift_name).split())
        return f"https://t.me/thermos/thermos/?startapp=ref_{THERMOS_REF_ID}_g_{collection_slug}-{num}"
    if THERMOS_REF_ID:
        return f"https://t.me/thermos/thermos/?startapp=ref_{THERMOS_REF_ID}"
    return "https://t.me/thermos"


async def thermos_search(gift_name, slug, gift_id=None, limit=100, model="", symbol="", backdrop=""):
    if not (HTTPX_OK and gift_name):
        return []
    key = f"{gift_name}|{model}|{symbol}|{backdrop}"
    now = time.time()
    hit = _thermos_cache.get(key)
    if hit and now - hit[0] < 60:
        return hit[1][:limit]
    # Confirmed via live fetch: GET /api/v1/collections returns data with no
    # auth, no special headers, no User-Agent spoofing. The POST to /api/v1/gifts
    # returns a consistent 400. All body fields have been verified correct per
    # docs. Most likely remaining cause: per_page exceeded an undocumented server
    # maximum (we were sending 100; reducing to 20 which is what their own docs
    # show as an example value).
    body: dict = {"collections": [gift_name], "per_page": 20, "page": 1}
    if model:
        body["models"] = [model]
    if symbol:
        body["symbols"] = [symbol]
    if backdrop:
        body["backdrops"] = [backdrop]
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    out: list = []
    try:
        page = 1
        max_pages = 5   # up to 100 items (5 pages × 20); stops early when total_pages is lower
        while page <= max_pages:
            body["page"] = page
            async with httpx.AsyncClient(timeout=9) as cli:
                if page == 1:
                    log.info("thermos request body=%s", json.dumps(body))
                r = await cli.post(f"{THERMOS_PROXY_BASE}/gifts", json=body, headers=headers)
            if page == 1:
                log.info("thermos search status=%s full_response=%s", r.status_code, (r.text or "")[:800].replace("\n", " "))
            if r.status_code != 200:
                break
            data = r.json()
            rows = (data.get("items") or []) if isinstance(data, dict) else []
            if not rows:
                break
            for raw in rows:
                item = _thermos_item(raw, gift_name, slug, gift_id)
                if item:
                    out.append(item)
            total_pages = int((data.get("pages") or 1))
            if page >= total_pages:
                break
            page += 1
        _thermos_cache[key] = (now, out)
        if len(_thermos_cache) > 300:
            _thermos_cache.pop(next(iter(_thermos_cache)))
        return out[:limit]
    except Exception as e:
        log.info("thermos search skipped: %s", e)
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
            "model": None, "modelRarity": None, "symbol": None, "symbolRarity": None,
            "backdrop": None, "backdropRarity": None, "backdropHex": None,
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
        "mtproto": bool(client and client.is_connected() and _session.verified),
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
        "mtproto_verified": _session.verified,
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


async def _warm_new_collections(gift_ids):
    """Fire-and-forget: fetch attributes for collections that just appeared,
    right away, instead of leaving them to wait for the next scheduled
    _bg_warm_attributes sweep (which can be hours away). Same gentle pacing
    philosophy as that loop — never burst MTProto."""
    for gid in gift_ids:
        try:
            await _fetch_attributes_live(gid)
            log.info("attributes warmed immediately for new collection (gift_id=%s)", gid)
        except Exception as e:
            log.info("immediate attr warm failed for new collection (gift_id=%s): %s", gid, e)
        await asyncio.sleep(4)


async def _refresh_collections_bg():
    """Background rebuild → memory + durable Postgres copy."""
    global _collections_refreshing
    if _collections_refreshing:
        return
    _collections_refreshing = True
    try:
        prev = _collections_db_get() or []
        prev_ids = {str(c.get("gift_id")) for c in prev if c.get("gift_id")}
        out = await _build_collections()
        if out:
            new_ones = [c for c in out if str(c.get("gift_id")) not in prev_ids]
            gone_ones = [c["name"] for c in prev if str(c.get("gift_id")) not in {str(x.get("gift_id")) for x in out}]
            cache_set("collections", out, ttl=900)
            _collections_db_set(out)
            if new_ones or gone_ones:
                log.info("collections refreshed (%d items) — NEW: %s | REMOVED: %s",
                          len(out), [c["name"] for c in new_ones] or "none", gone_ones or "none")
                if new_ones:
                    new_gift_ids = [str(c.get("gift_id")) for c in new_ones if c.get("gift_id")]
                    if new_gift_ids:
                        asyncio.create_task(_warm_new_collections(new_gift_ids))
            else:
                log.info("collections refreshed (%d items) — no changes", len(out))
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


def _anim_db_get(doc_id):
    try:
        with db() as conn:
            r = conn.execute("SELECT data, ts FROM anim_cache WHERE doc_id=?", (str(doc_id),)).fetchone()
            if r:
                return _b64mod.b64decode(r["data"]), int(r["ts"])
    except Exception as e:
        log.info("anim db read skipped: %s", e)
    return None, 0


def _anim_db_set(doc_id, data_bytes):
    try:
        blob = _b64mod.b64encode(data_bytes).decode()
        if len(blob) > 2_000_000:   # a couple MB of base64 is already a very large single animation
            return
        now = int(time.time())
        with db() as conn:
            cur = conn.execute("UPDATE anim_cache SET data=?, ts=? WHERE doc_id=?", (blob, now, str(doc_id)))
            if not cur.rowcount:
                conn.execute("INSERT INTO anim_cache(doc_id, data, ts) VALUES(?,?,?) ON CONFLICT DO NOTHING",
                             (str(doc_id), blob, now))
            # Bound storage: keep only the 300 most recently used models —
            # this is per-MODEL, not per-collection, so the cap is higher
            # than attrs_cache's 40.
            conn.execute(
                "DELETE FROM anim_cache WHERE doc_id NOT IN "
                "(SELECT doc_id FROM anim_cache ORDER BY ts DESC LIMIT 300)"
            )
            conn.commit()
    except Exception as e:
        log.info("anim db write skipped: %s", e)


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


async def _fetch_attributes_live(gift_id):
    """Live MTProto fetch of a collection's models/symbols/backdrops + thumbnail
    images, persisted to the durable Postgres copy (survives restarts) and the
    in-memory cache. Raises on failure — callers decide how to handle that
    (the endpoint falls back to stale data; the background warmer just skips
    and retries next pass)."""
    GetResale = _payments("GetResaleStarGiftsRequest")
    if client is None:
        raise RuntimeError("MTProto client not initialised yet")
    if not GetResale:
        raise RuntimeError("GetResaleStarGiftsRequest missing — pip install -U telethon")
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
            _bd_hex = color_hex(getattr(a, "center_color", None))
            backdrops.append({
                "name": name, "rarity": rar,
                "hex": _bd_hex,
                "edge": color_hex(getattr(a, "edge_color", None)),
            })
            if name and _bd_hex:
                _BACKDROP_HEX[name.strip().lower()] = _bd_hex   # universal colour
            if name and aid is not None:
                id_map["backdrop"][name] = aid
    log.info(
        "attributes RARITY sample for gift_id=%s — models: %s | symbols: %s | backdrops: %s",
        gift_id,
        [(m["name"], m["rarity"]) for m in models[:5]],
        [(s["name"], s["rarity"]) for s in symbols[:5]],
        [(b["name"], b["rarity"]) for b in backdrops[:5]],
    )
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
    # Also warm the ANIMATION cache for each model here, in the background —
    # this is what lets live search results become a pure cache-read (never a
    # fresh MTProto call at search time, see the search loop below). Gentle:
    # reuses the same pacing as the rest of this warm pass, one collection at
    # a time, so it doesn't add a new burst source.
    await asyncio.gather(*[_doc_anim_json(d) for d in model_docs if d is not None], return_exceptions=True)
    _attr_ids_cache[str(gift_id)] = id_map
    result = {"models": models, "symbols": symbols, "backdrops": backdrops}
    key = f"attrs:{gift_id}"
    cache_set(key, result, ttl=21600)          # attributes barely change — 6h
    _attrs_stale[str(gift_id)] = result        # long-lived safety copy
    # The persisted "resp" already embeds each thumbnail as a base64 data URI
    # (see _doc_thumb_uri above), so this durable copy carries the images too —
    # a warm DB hit never needs to touch Telegram for icons, only for the
    # attribute LIST itself when it's missing or stale.
    _attrs_db_set(gift_id, {"resp": result, "ids": _idmap_to_jsonable(id_map)})   # survives restarts
    return result


# How long a durable attributes copy is trusted before the endpoint will do a
# live re-fetch on a real user's request (worst case, cold path).
_ATTRS_DB_TTL = 7 * 86400
# How long before that same expiry the background warmer proactively refreshes
# it — well ahead of the hard cutoff, so in practice a real user almost never
# hits the cold path at all; the warmer keeps every collection topped up first.
_ATTRS_WARM_MARGIN = 2 * 86400


@app.get("/api/attributes")
async def attributes(request: Request, gift_id: str = Query(...),
                     x_init_data: str = Header(default="", alias="X-Init-Data")):
    _vuid, _blocked = _data_guard(request, x_init_data)
    if _blocked is not None:
        return _blocked
    gift_id = _digits(gift_id)
    if not gift_id:
        return {"models": [], "symbols": [], "backdrops": []}
    key = f"attrs:{gift_id}"
    cached = cache_get(key)
    if cached and str(gift_id) in _attr_ids_cache:
        return cached
    # RESTART-PROOF PATH: durable Postgres copy (response + attribute-id map,
    # thumbnails included) serves instantly after a deploy; live fetch only
    # when no usable copy exists yet.
    db_payload, db_ts = _attrs_db_get(gift_id)
    if db_payload and isinstance(db_payload, dict) and db_payload.get("resp") and (time.time() - db_ts) < _ATTRS_DB_TTL:
        if db_payload.get("ids"):
            _attr_ids_cache[str(gift_id)] = _idmap_from_jsonable(db_payload["ids"])
        resp = db_payload["resp"]
        cache_set(key, resp, ttl=21600)
        _attrs_stale[str(gift_id)] = resp
        return resp
    empty = {"models": [], "symbols": [], "backdrops": []}
    try:
        return await _fetch_attributes_live(gift_id)
    except FloodWaitError as e:
        wait_s = float(getattr(e, "seconds", 999) or 999)
        if wait_s <= 8:
            # Short wait — worth eating the delay once and self-healing rather
            # than permanently serving stale/empty for something that would've
            # worked moments later. Longer waits fall through to the stale path
            # below rather than making a real user sit through it.
            try:
                await asyncio.sleep(wait_s + 0.3)
                return await _fetch_attributes_live(gift_id)
            except Exception as e2:
                log.error("attributes retry-after-flood-wait failed: %s", e2)
        else:
            log.error("attributes flood wait too long to retry live (%.0fs): %s", wait_s, e)
        stale = _attrs_stale.get(str(gift_id))
        if stale:
            return stale
        db_payload, _ = _attrs_db_get(gift_id)
        if db_payload and isinstance(db_payload, dict) and db_payload.get("resp"):
            if db_payload.get("ids"):
                _attr_ids_cache[str(gift_id)] = _idmap_from_jsonable(db_payload["ids"])
            return db_payload["resp"]
        return {**empty, "error": str(e)}
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


async def _bg_refresh_collections_loop():
    """Recurring, traffic-INDEPENDENT check for new/changed collections.
    Before this, a refresh only ever happened when a real request happened to
    land right as the 15-minute in-memory cache expired — during a quiet
    period (e.g. overnight) nothing would re-trigger a check for a long time,
    which is exactly why a brand-new Telegram collection needed a manual
    redeploy to show up. This loop calls the same guarded refresh on a fixed
    clock instead, so new collections surface within one cycle of Telegram
    actually publishing them — no deploy required."""
    while True:
        try:
            await asyncio.sleep(900)   # every 15 minutes
            await _refresh_collections_bg()   # already no-ops if a refresh is mid-flight
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.info("_bg_refresh_collections_loop error: %s", e)
            await asyncio.sleep(300)


async def _bg_warm_attributes():
    """Recurring background pass: keep every known collection's attributes
    (+ thumbnails) fresh in the durable cache BEFORE a real user's request would
    ever hit the slow live-MTProto path. Paced gently (one collection every few
    seconds) so it never competes with real traffic for the single MTProto
    connection, and skips anything that's already fresh. A flood-wait during a
    warm pass just fails that one collection silently — _invoke already handles
    that — and it gets picked up again on the next pass a few hours later."""
    while True:
        try:
            await asyncio.sleep(30)   # let startup settle before the first pass
            try:
                colls = _collections_db_get() or []
            except Exception:
                colls = []
            warmed = 0
            for c in colls:
                gid = str(c.get("gift_id") or "")
                if not gid:
                    continue
                try:
                    old_payload, db_ts = _attrs_db_get(gid)
                    fresh_until = db_ts + _ATTRS_DB_TTL - _ATTRS_WARM_MARGIN
                    if db_ts and time.time() < fresh_until:
                        continue   # already fresh enough, skip
                    old_resp = (old_payload or {}).get("resp") or {}
                    old_names = {
                        k: {m.get("name") for m in (old_resp.get(k) or []) if m.get("name")}
                        for k in ("models", "symbols", "backdrops")
                    }
                    new_resp = await _fetch_attributes_live(gid)
                    new_names = {
                        k: {m.get("name") for m in (new_resp.get(k) or []) if m.get("name")}
                        for k in ("models", "symbols", "backdrops")
                    }
                    if old_resp:
                        added = {k: sorted(new_names[k] - old_names[k]) for k in new_names}
                        removed = {k: sorted(old_names[k] - new_names[k]) for k in new_names}
                        if any(added.values()) or any(removed.values()):
                            coll_name = c.get("name") or gid
                            log.info(
                                "attributes CHANGED for %s (gift_id=%s) — new models: %s | new symbols: %s | "
                                "new backdrops: %s | removed models: %s | removed symbols: %s | removed backdrops: %s",
                                coll_name, gid,
                                added["models"] or "none", added["symbols"] or "none", added["backdrops"] or "none",
                                removed["models"] or "none", removed["symbols"] or "none", removed["backdrops"] or "none",
                            )
                    warmed += 1
                    await asyncio.sleep(4)   # gentle pacing — never burst MTProto
                except FloodWaitError as e:
                    # Telegram is genuinely rate-limiting us right now. The old
                    # behaviour moved on after only 4s regardless — walking
                    # straight into the SAME still-active flood window on the
                    # very next collection, which cascaded into several
                    # collections in a row failing within seconds of each other
                    # (exactly what showed up in the logs). Actually wait out
                    # the real remaining duration (capped, so one huge wait
                    # can't stall the whole pass for minutes) before continuing.
                    wait_s = min(float(getattr(e, "seconds", 20) or 20), 45)
                    log.info("attr warm backing off %.0fs after flood wait (gift_id=%s)", wait_s, gid)
                    await asyncio.sleep(wait_s)
                except Exception as e:
                    log.info("attr warm skipped for gift_id=%s: %s", gid, e)
                    await asyncio.sleep(4)
            if warmed:
                log.info("attribute warm pass: refreshed %d/%d collection(s)", warmed, len(colls))
            await asyncio.sleep(6 * 3600)   # next pass in 6 hours
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.info("_bg_warm_attributes error: %s", e)
            await asyncio.sleep(3600)


async def _src_budget(coro, seconds, label):
    """Run one marketplace fetch under a hard wall-clock budget. A slow or hung
    source returns [] instead of stalling the whole search — the response goes
    out with whatever the fast sources found."""
    try:
        return await asyncio.wait_for(coro, timeout=seconds) or []
    except Exception as e:
        log.info("%s source skipped (budget/err): %s", label, e)
        return []


@app.get("/api/search")
async def search(
    request: Request,
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
    # Anti-scraping gate: a valid, fresh Telegram signature is the ticket in.
    vuid, _blocked = _data_guard(request, x_init_data)
    if _blocked is not None:
        return _blocked
    # Rate-limit the VERIFIED identity — rotating the spoofable `uid` query
    # param no longer buys a fresh bucket. Unsigned dev-mode calls fall back
    # to a per-IP identity.
    if not rate_ok(vuid or f"ip:{_client_ip(request)}"):
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
    # may apply at once: free 1, plus 5, pro unlimited. Identity comes from the
    # signed initData already verified by the guard above; the spoofable query
    # `uid` is used only for tracking.
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

    # Short-lived result cache (~25s): when many people search the same gift in
    # a tight window — exactly what happens on a popular collection — this
    # serves every one of them from one cached response instead of repeating
    # the same MTProto/HTTP calls per person. This is what actually protects
    # the single Telegram session from flood-waits under real concurrent load;
    # it keeps working unchanged whether there's 1 session behind it or several.
    # Only applied to fresh searches (no offset) — a "load more" page is always
    # fetched live so pagination cursors stay correct.
    _cache_key = None
    if not offset:
        _cache_key = "search:" + "|".join([
            gift, gift_id, slug, num, sort,
            f"{min_price:g}", f"{max_price:g}", str(limit),
            ",".join(sorted(want)), ",".join(sorted(models)),
            ",".join(sorted(symbols)), ",".join(sorted(backdrops)),
        ])
        _cached = cache_get(_cache_key)
        if _cached is not None:
            return _cached

    results = []
    next_offset = ""
    limit = max(1, min(int(limit or SEARCH_LIMIT), SEARCH_MAX))
    GetResale = _payments("GetResaleStarGiftsRequest")

    # Resolve selected model/symbol/backdrop NAMES to attribute IDs so Telegram
    # filters server-side (otherwise matches on deeper pages get missed -> the
    # false "no listings" bug). Populate the id map directly (durable copy first,
    # live fetch as fallback) — NOT via the HTTP endpoint, which is now guarded.
    if gift_id and any_attr and str(gift_id) not in _attr_ids_cache:
        try:
            db_payload, _ts = _attrs_db_get(gift_id)
            if db_payload and isinstance(db_payload, dict) and db_payload.get("ids"):
                _attr_ids_cache[str(gift_id)] = _idmap_from_jsonable(db_payload["ids"])
            else:
                await _fetch_attributes_live(gift_id)
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

    flood = False
    # ── PARALLEL FAN-OUT ─────────────────────────────────────────────────────
    # Kick the secondary marketplaces off as background tasks NOW, before the
    # Telegram fetch starts, so their network time overlaps with Telegram's
    # instead of stacking after it. This is the difference between a search
    # taking max(telegram, fragment, marketapp) and taking the SUM of all
    # three — the single biggest wall-clock win available. Each task carries a
    # hard time budget so one slow source can never hold the response hostage.
    # Same gating rules as before: first page only, market selection respected,
    # Fragment skipped when attribute filters are set.
    sec_tasks = []
    if gift and not offset and (not want or "GetGems" in want):
        sec_tasks.append(asyncio.create_task(
            _src_budget(getgems_search(gift, limit=12), 6, "getgems")))
    if slug and not any_attr and not offset and (not want or "Fragment" in want):
        sec_tasks.append(asyncio.create_task(
            _src_budget(fragment_search(slug, gift, limit=40), 9, "fragment")))
    if MARKETAPP_TOKEN and (slug or gift) and not offset and (not want or "MarketApp" in want):
        sec_tasks.append(asyncio.create_task(
            _src_budget(marketapp_search(
                slug, gift, gift_id=gift_id, limit=40,
                model=model, symbol=symbol, backdrop=backdrop), 9, "marketapp")))
    if gift and not offset and (not want or "Thermos" in want):
        sec_tasks.append(asyncio.create_task(
            _src_budget(thermos_search(
                gift, slug, gift_id=gift_id, limit=100,
                model=model, symbol=symbol, backdrop=backdrop), 9, "thermos")))

    if client is not None and GetResale and gift_id and (not want or "Telegram" in want):
        try:
            cur = offset or ""
            fetched = 0
            native_docs = {}   # item id -> raw model document (for the batch fetch below)
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
                    doc = _model_doc_of(g)
                    if doc is not None:
                        native_docs[item["id"]] = doc
                fetched += len(chunk)
                cur = getattr(res, "next_offset", "") or ""
                next_offset = cur
                if not cur or len(chunk) == 0:
                    next_offset = ""
                    break
            # Swap in the EXACT per-item model artwork, sourced directly from
            # Telegram, in place of the Fragment-CDN guess — this is what fixes
            # a brand-new collection showing the wrong/generic image (Fragment
            # hasn't crawled it yet) or a broken-image placeholder. Deduplicated
            # by document id first: many listings share the same model, so a
            # collection with, say, 30 results across 8 distinct models only
            # costs 8 fetches, not 30 — and _thumb_cache makes every fetch after
            # the very first search of this collection free for everyone.
            if native_docs:
                uniq = {}
                for doc in native_docs.values():
                    did = getattr(doc, "id", None)
                    if did is not None and did not in uniq:
                        uniq[did] = doc
                # Two tiers: pure cache reads for everything (always safe, no
                # network call, no flood risk), PLUS a small bounded live-fetch
                # attempt for whatever's still missing — capped at 4 distinct
                # models and skipped entirely under flood pressure. Pure-cache-
                # only was too conservative: a brand-new collection with
                # nothing warmed yet would show NOTHING at all (no poster, no
                # animation, just the plain backdrop) until the background
                # loop happened to reach it, which is exactly the regression
                # reported. This gives a new collection a real shot on its
                # very first search while keeping the worst case bounded.
                thumb_by_did = {}
                anim_by_did = {}
                live_slots = 0 if _session.under_pressure else 4
                for did, doc in uniq.items():
                    t = _thumb_cache.get(did)
                    a = _anim_cache.get(did)
                    if a is None:
                        db_data, _ts = _anim_db_get(did)
                        if db_data:
                            a = db_data
                            if len(_anim_cache) < 2000:
                                _anim_cache[did] = db_data
                    need_live = (t is None or a is None) and live_slots > 0
                    if need_live:
                        live_slots -= 1
                        if t is None:
                            t = await _doc_thumb_uri(doc)
                        if a is None:
                            a = await _doc_anim_json(doc)
                    if isinstance(t, str):
                        thumb_by_did[did] = t
                    if isinstance(a, bytes):
                        anim_by_did[did] = a
                for item in results:
                    doc = native_docs.get(item["id"])
                    if doc is None:
                        continue
                    did = getattr(doc, "id", None)
                    if did is None:
                        continue
                    # Fragment-first, ours as fallback: item["image"]/["animation"]
                    # (the Fragment-CDN guess from serialize_unique) stay as the
                    # PRIMARY source, since once Fragment has properly crawled
                    # and composited a collection (backdrop + symbol + model all
                    # baked in), that's a better result than our bare model-only
                    # render. The frontend tries Fragment first and only falls
                    # back to these fields if that 404s — which also means the
                    # moment Fragment catches up, results switch over
                    # automatically on the next load, with no extra code needed.
                    if did in thumb_by_did:
                        item["imageFallback"] = thumb_by_did[did]
                    if did in anim_by_did and BACKEND_PUBLIC_URL:
                        item["animationFallback"] = f"{BACKEND_PUBLIC_URL}/api/model-anim/{did}"
            # Symbol pattern overlay: Telegram's bare model document is only the
            # character shape — no backdrop colour, no symbol pattern, unlike
            # Fragment's pre-composited image. We already have the backdrop
            # colour (backdropHex). This adds the symbol layer too, reusing the
            # collection's ALREADY-cached attribute thumbnails (no new MTProto
            # calls, safe even under flood pressure) — every listing just looks
            # up its own symbol name against that cached list.
            if results:
                try:
                    attrs_payload, _ts = _attrs_db_get(gift_id)
                    symbol_img_by_name = {}
                    if attrs_payload and isinstance(attrs_payload, dict):
                        for s in (attrs_payload.get("resp") or {}).get("symbols") or []:
                            if s.get("name") and s.get("img"):
                                symbol_img_by_name[s["name"]] = s["img"]
                    if symbol_img_by_name:
                        for item in results:
                            sym_name = item.get("symbol")
                            if sym_name and sym_name in symbol_img_by_name:
                                item["symbolImage"] = symbol_img_by_name[sym_name]
                except Exception as e:
                    log.info("symbol overlay lookup skipped: %s", e)
        except FloodWaitError:
            # Telegram is rate-limiting us right now. Don't abort the whole
            # search — Fragment/MarketApp tasks are already in flight and their
            # results still go out below; only the Telegram slice is degraded.
            flood = True
        except Exception as e:
            log.error("native search error: %s", repr(e))

    # Collect the parallel marketplace tasks (started before the Telegram
    # block). Task order — GetGems, Fragment, MarketApp, Thermos — matches
    # the old sequential order, so the dedup pass below keeps the same
    # source priority.
    # By now they've been running the whole time Telegram was fetching, so in
    # the common case these awaits return instantly.
    for _t in sec_tasks:
        try:
            chunk = await _t
            if chunk:
                results.extend(chunk)
        except Exception as e:
            log.info("secondary source task failed: %s", e)

    # Cross-source de-dup: MarketApp's own aggregator already pulls in Fragment/
    # GetGems/Portals/Tonnel listings, so the SAME physical NFT can legitimately
    # come back twice — once from our direct Fragment/GetGems call, once again
    # via MarketApp's passthrough — each with a different per-source id string,
    # which a plain id-based dedup can't catch. Key by the gift's true identity
    # (collection + item number) instead, and keep the first occurrence — which
    # respects our existing priority order: Telegram-native, then GetGems, then
    # Fragment, then MarketApp.
    if len(results) > 1:
        seen_phys, deduped = set(), []
        for r in results:
            n = str(r.get("num") or "").strip()
            base = str(r.get("slug") or r.get("name") or "").strip().lower()
            key = f"{base}#{n}" if (base and n) else f"id:{r.get('id')}"
            if key in seen_phys:
                continue
            seen_phys.add(key)
            deduped.append(r)
        results = deduped

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
    # General is the default: keep the natural order the marketplaces return, and
    # only rank by price when the user explicitly taps Lowest or Highest.
    if sort in ("price_asc", "price_desc"):
        # Convert every listing to a USD value for fair cross-currency ranking.
        # Stars: official Telegram sell rate (from payments.getStarsRevenueStats).
        # GRAM/TON: live Binance spot × price in GRAM.
        # USDT: 1:1 (it's already USD-pegged).
        # Display always stays in the listing's real currency — this only affects order.
        ton_usd   = await _ton_usd_rate()
        stars_usd = await _stars_usd_rate()
        def _usd_value(r):
            p = r.get("price")
            if p is None:
                return None
            cur = str(r.get("currency") or "").lower()
            if cur == "usdt":
                return float(p)
            if cur.startswith("star"):
                return float(p) * (stars_usd or STARS_USD_RATE)
            # GRAM / TON — use gram_value if present (already in GRAM), else price
            gv = r.get("gram_value")
            return float(gv if gv is not None else p) * (ton_usd or USDT_PER_TON)
        rev = (sort == "price_desc")
        results.sort(key=lambda r: (_usd_value(r) is None,
                                    -(_usd_value(r) or 0) if rev else (_usd_value(r) or 0)))

    # Stamp the collection's gift_id onto every result so the client can persist it
    # (e.g. when saving a gift) and later ask /api/check_listings whether it's sold.
    if gift_id:
        for r in results:
            r["gift_id"] = gift_id

    payload = {"results": results, "next_offset": next_offset, "count": len(results)}
    if flood:
        payload["flood"] = True
    # Never pin a flood-degraded page into the 25s cache — the next request may
    # get the full Telegram slice back and should be allowed to.
    if _cache_key and not flood:
        cache_set(_cache_key, payload, 25)
    return payload


@app.get("/api/gift")
async def gift(request: Request, slug: str = Query(...),
               x_init_data: str = Header(default="", alias="X-Init-Data")):
    _vuid, _blocked = _data_guard(request, x_init_data)
    if _blocked is not None:
        return _blocked
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


@app.get("/api/channel_member")
async def channel_member(uid: str = Query(""), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Check whether the calling user is a member of the official GiftTrove channel.
    Used by the frontend to decide whether to show the 'Join our channel' sheet.
    Returns {is_member: bool, invite_url: str}."""
    verified = verify_init_data(x_init_data)
    eff_uid = verified or _digits(uid)
    if not eff_uid:
        return {"is_member": False, "invite_url": CHANNEL_INVITE_URL}
    try:
        result = await _bot_api("getChatMember", {"chat_id": CHANNEL_ID, "user_id": int(eff_uid)})
        status = (result or {}).get("result", {}).get("status", "left")
        is_member = status in ("creator", "administrator", "member", "restricted")
        return {"is_member": is_member, "invite_url": CHANNEL_INVITE_URL}
    except Exception as e:
        log.info("channel_member check failed: %s", e)
        # Fail open — don't pester the user with the sheet on every API hiccup
        return {"is_member": True, "invite_url": CHANNEL_INVITE_URL}


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


# In-memory avatar cache: uid -> (jpeg_bytes_or_None, fetched_at). Profile photos
# rarely change and downloading via MTProto can hit flood waits, so we cache both
# hits and misses for a while to keep the endpoint cheap.
_AVATAR_CACHE = {}
_AVATAR_TTL = 6 * 3600  # 6h

@app.get("/api/avatar")
async def avatar(uid: str = Query(""), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Return the caller's Telegram profile photo as a JPEG.

    The frontend tries the `photo_url` Telegram puts in initData first; it's only
    present in some launch contexts, so this is the fallback. Identity comes from
    the signed initData when available (can't be spoofed); we fall back to the uid
    param only for resolving which user to fetch. Any failure returns 404 and the
    client shows the mascot instead.
    """
    from fastapi.responses import Response
    eff = verify_init_data(x_init_data) or _digits(uid)
    if not eff:
        return JSONResponse(status_code=404, content={"ok": False})

    now = time.time()
    cached = _AVATAR_CACHE.get(eff)
    if cached and now - cached[1] < _AVATAR_TTL:
        if cached[0]:
            return Response(content=cached[0], media_type="image/jpeg",
                            headers={"Cache-Control": "public, max-age=21600"})
        return JSONResponse(status_code=404, content={"ok": False})

    data = None
    try:
        # The full user client resolves arbitrary users by id and can fetch their
        # public profile photo. Guard with a tight timeout so a flood-wait or
        # cross-DC stall can never hang the request.
        if client is not None:
            import io
            buf = io.BytesIO()
            await asyncio.wait_for(
                client.download_profile_photo(int(eff), file=buf), timeout=8
            )
            b = buf.getvalue()
            data = b if b else None
    except Exception as e:
        log.info("avatar fetch failed for %s: %s", eff, type(e).__name__)
        data = None

    _AVATAR_CACHE[eff] = (data, now)
    if data:
        return Response(content=data, media_type="image/jpeg",
                        headers={"Cache-Control": "public, max-age=21600"})
    return JSONResponse(status_code=404, content={"ok": False})


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

@app.post("/api/subscription/cancel")
async def subscription_cancel(x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Cancel the caller's recurring Stars subscription. We ask Telegram to stop
    the auto-renewal (so they aren't charged again) and DM a confirmation that
    also reminds them they keep their benefits until the period ends. The sub
    row is left intact so existing benefits persist until expires_at."""
    uid = verify_init_data(x_init_data)
    if not uid:
        return {"ok": False, "error": "unauthorized"}
    tier, exp = sub_info(uid)
    if tier not in ("plus", "pro"):
        return {"ok": False, "error": "no_active_subscription"}
    row = _sub_row(uid)
    charge_id = row[1] if row else ""
    # Mark cancelled so a later lapse isn't mistaken for a failed renewal (the
    # insufficient-balance sweep skips rows with cancelled=1).
    try:
        with db() as conn:
            conn.execute("UPDATE subs SET cancelled=1 WHERE uid=?", (str(uid),))
            conn.commit()
    except Exception as e:
        log.info("mark sub cancelled failed: %s", e)
    cancelled = False
    if charge_id and bot is not None:
        try:
            from telethon.tl import functions as _fn
            peer = await bot.get_input_entity(int(uid))
            await bot(_fn.payments.BotCancelStarsSubscriptionRequest(
                user_id=peer, charge_id=charge_id))
            cancelled = True
            log.info("subscription cancel requested: uid=%s tier=%s", uid, tier)
        except Exception as e:
            log.warning("subscription cancel via Telegram failed (uid=%s): %s", uid, e)
    try:
        await _send_sub_cancel_confirmation(uid, tier)
    except Exception as e:
        log.info("cancel confirmation DM failed: %s", e)
    # Whether or not the API cancel succeeded, point the user to Telegram's own
    # subscription management as the authoritative place to stop renewal.
    return {"ok": True, "cancelled": cancelled, "tier": tier, "expires_at": exp}


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


@app.post("/api/lang")
async def set_lang(payload: dict = Body(...), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Sync the in-app language switcher to the backend so the bot DMs the member
    in the language they chose. Accepts 'en' | 'ru' | 'zh' (case-insensitive)."""
    uid = verify_init_data(x_init_data)
    if not uid:
        return {"ok": False}
    lang = _norm_lang(str(payload.get("lang") or ""))
    _set_user_lang(uid, lang)
    return {"ok": True, "lang": lang}


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
    if mkt not in ("Telegram", "MarketApp", "Thermos", "Fragment"):
        return {"ok": False, "error": "marketplace"}
    # All marketplaces: user provides collection + num; bot auto-fetches the listing.
    # No links needed from the frontend.
    fields = {
        "collection": name,
        "gift_id": gift_id,
        "slug": slug,
        "num": _digits(payload.get("num")),
        "model": "", "symbol": "", "backdrop": "",
        "marketplace": mkt,
        "amount": "", "currency": "",
        "link": "",
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

_promo_verify_ts: dict = {}   # pid -> last_verified unix ts

async def _bg_verify_promo(p: dict):
    """Background: re-check a specific-num promo is still listed; mark sold if not.
    A lookup error (network hiccup, marketplace API hiccup, etc.) is NOT treated
    as "sold" — only a confirmed, successful lookup that finds nothing does that.
    Otherwise a brief outage could wrongly mark a perfectly live gift as sold.
    Shares the Telegram concurrency cap and the listing-check cache with
    /api/check_listings so the two never compound into a connection traffic jam."""
    pid = p.get("id") or ""
    if not pid:
        return
    mkt = p.get("marketplace") or ""
    slug = p.get("slug") or ""
    num = p.get("num") or ""
    cache_key = f"{mkt}:{slug}:{num}" if (mkt and slug and num) else ""
    now = time.time()
    cached = _listing_check_cache.get(cache_key) if cache_key else None
    if cached and (now - cached[0]) < _LISTING_CHECK_TTL:
        fetch = {} if cached[1] else {"_cached_live": True}
    else:
        try:
            if mkt == "Telegram":
                async with _telegram_check_sema:
                    fetch = await _promo_auto_fetch(
                        mkt, slug, p.get("gift_id") or "", num, p.get("collection") or "")
            else:
                fetch = await _promo_auto_fetch(
                    mkt, slug, p.get("gift_id") or "", num, p.get("collection") or "")
        except Exception as e:
            log.info("promo %s verify skipped (inconclusive): %s", pid, e)
            return
        if cache_key:
            _listing_check_cache[cache_key] = (now, not fetch)
    if not fetch:
        _promo_set_status(pid, "sold")
        log.info("promo %s marked sold: listing no longer found on %s", pid, p.get("marketplace"))
        # Notify the promoter that their gift was sold / de-listed.
        uid = p.get("uid") or ""
        slug = p.get("slug") or ""
        num = p.get("num") or ""
        mkt = p.get("marketplace") or "Telegram"
        coll = p.get("collection") or "your gift"
        if uid:
            try:
                label, gift_url = _promo_label_link(coll, num, slug, p.get("link"), mkt)
                await _send_promo_bought(uid, label, gift_url)
            except Exception as e:
                log.info("sold notify DM failed: %s", e)


async def _bg_expire_promos():
    """Periodic background task: notify promoters when their promo has just expired."""
    while True:
        try:
            await asyncio.sleep(3600)   # check every hour
            now = int(time.time())
            window = now - 3660         # up to ~61 min ago (catches last hour's expiries)
            try:
                with db() as conn:
                    rows = conn.execute(
                        "SELECT id, uid, collection, num, slug, marketplace, link FROM promos "
                        "WHERE status='active' AND expires_at<=? AND expires_at>=?",
                        (now, window)).fetchall()
            except Exception:
                rows = []
            for r in rows:
                pid = r["id"] or ""
                if not pid:
                    continue
                _promo_set_status(pid, "expired")
                uid = r["uid"] or ""
                coll = r["collection"] or "your gift"
                num = r["num"] or ""
                slug = r["slug"] or ""
                mkt = r["marketplace"] or "Telegram"
                if uid:
                    try:
                        label, gift_url = _promo_label_link(coll, num, slug, r["link"], mkt)
                        await _send_promo_ended(uid, label, gift_url)
                    except Exception as e:
                        log.info("expiry notify DM failed: %s", e)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.info("_bg_expire_promos error: %s", e)


async def _bg_expire_subs():
    """Periodic: (1) DM members whose subscription period ended WITHOUT a renewal
    (most commonly because their Star balance was too low to auto-charge) — only
    rows that weren't cancelled in-app (cancelled=0) and weren't already notified
    (end_notified=0) are messaged; (2) clear the community-chat member tag for
    EVERY expiry, cancelled or not — tags don't auto-expire on Telegram's side
    the way tier access does, so this is what actually removes the badge the
    moment a subscription truly ends, regardless of how it ended.

    Caveat: Telegram does not tell a bot *why* a renewal didn't happen, so a member
    who cancels directly inside Telegram (rather than via the GiftTrove app) would
    also receive this top-up nudge. The message is harmless in that case."""
    while True:
        try:
            await asyncio.sleep(3600)   # hourly
            now = int(time.time())
            window = now - 2 * 86400    # caught if it lapsed within the last ~2 days
            try:
                with db() as conn:
                    rows = conn.execute(
                        "SELECT uid, tier FROM subs WHERE expires_at<=? AND expires_at>=? "
                        "AND COALESCE(cancelled,0)=0 AND COALESCE(end_notified,0)=0 "
                        "AND tier IN ('plus','pro')",
                        (now, window)).fetchall()
            except Exception:
                rows = []
            for r in rows:
                uid = r["uid"] or ""
                tier = r["tier"] or "plus"
                if not uid:
                    continue
                try:
                    with db() as conn:
                        conn.execute("UPDATE subs SET end_notified=1 WHERE uid=?", (str(uid),))
                        conn.commit()
                except Exception:
                    pass
                try:
                    await _send_sub_insufficient(uid, tier)
                except Exception as e:
                    log.info("insufficient-balance DM failed: %s", e)

            # Tag-clear pass: every expired plus/pro row, cancelled or not, that
            # hasn't had its tag cleared yet.
            try:
                with db() as conn:
                    tag_rows = conn.execute(
                        "SELECT uid FROM subs WHERE expires_at<=? AND expires_at>=? "
                        "AND COALESCE(tag_cleared,0)=0 AND tier IN ('plus','pro')",
                        (now, window)).fetchall()
            except Exception:
                tag_rows = []
            for r in tag_rows:
                uid = r["uid"] or ""
                if not uid:
                    continue
                try:
                    with db() as conn:
                        conn.execute("UPDATE subs SET tag_cleared=1 WHERE uid=?", (str(uid),))
                        conn.commit()
                except Exception:
                    pass
                try:
                    await _set_member_tag(uid, "free")
                except Exception as e:
                    log.info("member tag clear failed: %s", e)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.info("_bg_expire_subs error: %s", e)


@app.get("/api/promos")
async def promos(gift_id: str = Query(""), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Active promotions for a gift collection, to blend at the top of a scout.
    Scout Pro members get none (their perk is an ad-free scout).
    Promos with specific gift numbers are background-verified every 5 min;
    sold / de-listed gifts are automatically removed from display."""
    uid = verify_init_data(x_init_data)
    if uid and get_tier(uid) == "pro":
        return {"promos": []}
    active = _promo_active_for(_digits(gift_id))
    # Schedule background listing-check for number-specific promos (non-blocking)
    now = time.time()
    for p in active:
        pid = p.get("id") or ""
        if pid and p.get("num") and (now - _promo_verify_ts.get(pid, 0)) > 300:
            _promo_verify_ts[pid] = now
            asyncio.get_running_loop().create_task(_bg_verify_promo(p))
    return {"promos": active, "redirect": MINIAPP_URL}

@app.post("/api/check_listings")
async def check_listings(payload: dict = Body(...)):
    """Batch-check whether saved gifts are still listed for sale on their
    marketplace. Used by the Saved tab to show SOLD instead of Buy.
    Body: {items: [{id, gift_id, slug, num, marketplace, name}, ...]}
    Returns: {sold: [id, id, ...]}  (only the ones no longer listed)
    Results are cached for 20 minutes per gift, and Telegram lookups (the only
    marketplace sharing the app's single MTProto connection) are concurrency-
    capped, so this can never tie up the connection the way an unbounded burst
    of direct lookups would."""
    items = payload.get("items")
    if not isinstance(items, list):
        return {"sold": []}
    items = items[:20]
    sold_ids = []
    listed_ids = []
    now = time.time()

    async def _check(it):
        if not isinstance(it, dict):
            return
        iid = _clamp(it.get("id"), 128)
        num = _digits(it.get("num"))
        mkt = it.get("marketplace") or it.get("market") or ""
        if not (iid and num and mkt in ("Telegram", "Fragment", "MarketApp")):
            return   # can't verify without a specific gift number; assume still valid
        raw_slug = str(it.get("slug") or "")
        # item.slug from search results is "{collectionSlug}-{num}" (see cdn_full());
        # strip that suffix to recover the bare collection slug Fragment/MarketApp need.
        suffix = f"-{num}"
        bare_slug = raw_slug[: -len(suffix)] if raw_slug.lower().endswith(suffix.lower()) else raw_slug
        slug = _safe_slug(bare_slug)
        gift_id = _digits(it.get("gift_id"))
        name = _clamp(it.get("name") or it.get("collection") or "", 64)
        # Guard against false positives: only verify when we actually have what
        # that marketplace's lookup needs. Older saved items (saved before this
        # check existed) may be missing gift_id/slug — skip rather than risk
        # wrongly flagging a still-live gift as sold.
        if mkt == "Telegram" and not slug:
            return
        if mkt == "Fragment" and not slug:
            return
        if mkt == "MarketApp" and not (slug or name):
            return

        cache_key = f"{mkt}:{slug}:{num}"
        cached = _listing_check_cache.get(cache_key)
        if cached and (now - cached[0]) < _LISTING_CHECK_TTL:
            if cached[1]:
                sold_ids.append(iid)
            else:
                listed_ids.append(iid)
            return

        try:
            if mkt == "Telegram":
                async with _telegram_check_sema:
                    fetch = await asyncio.wait_for(
                        _promo_auto_fetch(mkt, slug, gift_id, num, name), timeout=10)
            else:
                fetch = await asyncio.wait_for(
                    _promo_auto_fetch(mkt, slug, gift_id, num, name), timeout=10)
        except Exception:
            return   # network hiccup — don't falsely mark sold, don't cache either

        is_sold = not fetch
        _listing_check_cache[cache_key] = (now, is_sold)
        if is_sold:
            sold_ids.append(iid)
        else:
            listed_ids.append(iid)

    await asyncio.gather(*[_check(it) for it in items], return_exceptions=True)
    return {"sold": sold_ids, "listed": listed_ids}


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
            gram = await _gram_from_stars(stars)
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
    gram_val = await _gram_from_stars(s["available"])
    stars_usd_now = await _stars_usd_rate()
    return {
        "ok": True, "is_pro": tier == "pro", "pct": AFFILIATE_PCT,
        "min_withdraw": AFFILIATE_MIN_WITHDRAW,
        "earned": s["earned"], "paid": s["paid"], "pending": s["pending"],
        "available": s["available"], "referees": s["referees"], "payers": s["payers"],
        "ton_value": gram_val, "gram_value": gram_val,
        "payouts": _affiliate_payouts(uid), "series": _affiliate_series(uid),
        "star_to_gram": stars_usd_now,   # informational only; the real conversion is live, not a flat ratio
    }

@app.post("/api/affiliate/withdraw")
async def affiliate_withdraw(payload: dict = Body(...), x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Request a payout. Pro-only; needs >= AFFILIATE_MIN_WITHDRAW available Stars.
    The amount is immediately held as 'pending' (deducted from available), the
    member gets an 'under review' DM, and the owner receives an interactive request
    (user id, Stars + GRAM, wallet address) with Approve / Decline buttons."""
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
    # Optional custom amount (the member can withdraw less than everything they
    # have available). Falls back to the full available balance if omitted, so
    # older clients keep working unchanged.
    raw_amount = payload.get("amount")
    if raw_amount is not None:
        try:
            stars = int(raw_amount)
        except Exception:
            return {"ok": False, "error": "amount"}
        if stars < AFFILIATE_MIN_WITHDRAW:
            return {"ok": False, "error": "min", "available": s["available"], "min": AFFILIATE_MIN_WITHDRAW}
        if stars > s["available"]:
            return {"ok": False, "error": "amount", "available": s["available"]}
    else:
        stars = s["available"]
    pid = _affiliate_request_payout(uid, stars, addr)
    if not pid:
        return {"ok": False, "error": "failed"}
    payout = {"id": pid, "uid": uid, "stars": stars, "ton_address": addr}
    # 1) Tell the member it's under review (their balance is now held as pending).
    try:
        await _send_payout_under_review(uid, stars)
    except Exception as e:
        log.info("under-review DM failed: %s", e)
    # 2) Send the owner the interactive Approve / Decline request.
    try:
        await _notify_admin_payout(payout, stats=s)
    except Exception as e:
        log.info("admin payout notify failed: %s", e)
    return {"ok": True, "requested": stars, "gram": await _gram_from_stars(stars)}

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


@app.get("/api/admin/user-scan")
async def admin_user_scan(target: str = Query(""), code: str = Query(""),
                          x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Admin: authenticity scan for a user id (advisory signals + stats)."""
    verified = verify_init_data(x_init_data)
    code_ok = bool(ADMIN_CODE) and _clamp(code, 60) == ADMIN_CODE
    id_ok = (verified is None) or (verified == ANALYTICS_ADMIN_ID)
    if not (code_ok and id_ok):
        return {"ok": False, "error": "forbidden"}
    tgt = _digits(target)
    if not tgt:
        return {"ok": False, "error": "uid"}
    try:
        report = _user_authenticity(tgt)
        report["ok"] = True
        report["available_gram"] = await _gram_from_stars(report["stats"].get("available", 0))
        return report
    except Exception as e:
        log.error("user-scan failed: %s", e)
        return {"ok": False, "error": "failed"}


@app.post("/api/admin/credit-stars")
async def admin_credit_stars(payload: dict = Body(...), code: str = Query(""),
                             x_init_data: str = Header(default="", alias="X-Init-Data")):
    """Admin TEST tool: credit (or debit, with a negative amount) affiliate Stars to
    a user id so the payout flow can be exercised end-to-end. Recorded as a normal
    affiliate_earnings row tagged '__admin_credit__' so it's distinguishable from
    real commission. Does NOT touch the user's real Telegram Star balance."""
    verified = verify_init_data(x_init_data)
    code_ok = bool(ADMIN_CODE) and _clamp(code, 60) == ADMIN_CODE
    id_ok = (verified is None) or (verified == ANALYTICS_ADMIN_ID)
    if not (code_ok and id_ok):
        return {"ok": False, "error": "forbidden"}
    tgt = _digits(payload.get("uid"))
    try:
        stars = int(payload.get("stars") or 0)
    except Exception:
        stars = 0
    if not tgt or stars == 0:
        return {"ok": False, "error": "input"}
    stars = max(-1000000, min(1000000, stars))   # clamp magnitude for safety
    try:
        with db() as conn:
            conn.execute(
                "INSERT INTO affiliate_earnings(id, referrer, referee, tier, stars, charge_id, ts) "
                "VALUES(?,?,?,?,?,?,?)",
                (_secrets.token_hex(8), tgt, "__admin_credit__", "test", stars, "admin", int(time.time())))
            conn.commit()
        s = _affiliate_stats(tgt)
        return {"ok": True, "credited": stars, "uid": tgt,
                "available": s["available"], "available_gram": await _gram_from_stars(s["available"]),
                "earned": s["earned"]}
    except Exception as e:
        log.error("credit-stars failed: %s", e)
        return {"ok": False, "error": "failed"}


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


async def _dm_photo(uid, photo_url, caption, link_ranges=None):
    """Send a photo (by URL — Telegram fetches it server-side, no upload
    needed) with a text caption, optionally with tappable text-link entities
    (same (start, length, url) shape as _dm's link_ranges). Falls back to a
    plain text DM if the photo send fails for any reason (e.g. the image host
    is briefly unreachable), so the actual notification is never silently
    lost over a decorative image."""
    if not uid:
        return False
    try:
        payload = {"chat_id": int(uid), "photo": photo_url, "caption": str(caption or "")}
        if link_ranges:
            txt = str(caption or "")
            def _u16(s):
                return len(s.encode("utf-16-le")) // 2
            entities = []
            for start, length, url in link_ranges:
                if start < 0 or length <= 0 or start + length > len(txt) or not url:
                    continue
                entities.append({"type": "text_link", "offset": _u16(txt[:start]), "length": _u16(txt[start:start + length]), "url": url})
            if entities:
                payload["caption_entities"] = entities
        res = await _bot_api("sendPhoto", payload)
        if res and res.get("ok"):
            return True
    except Exception as e:
        log.info("_dm_photo failed, falling back to plain text: %s", e)
    return await _dm(uid, caption, link_ranges=link_ranges)


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


# Community chats where Scout+/Scout Pro members get a visible group member tag.
# This is Telegram's regular-member "Member Tags" feature (Bot API 9.5) — distinct
# from admin custom titles, so the member does NOT need to be promoted to admin;
# the BOT just needs the can_manage_tags admin right in each of these chats.
# Tags only apply to people who are actually members of the chat, and a failure
# in one chat never blocks the other.
COMMUNITY_GROUP_IDS = [-1004432206418, -1003951015996]
_TAG_BY_TIER = {"plus": "Scout+", "pro": "Scout Pro"}

async def _set_member_tag(uid, tier):
    """Set (or clear, if tier is falsy/free) this member's group tag in EVERY
    community chat. Fire-and-forget from callers — failures are common and
    harmless (the member simply isn't in that chat) and are never allowed to
    slow down a payment or expiry sweep, so this only logs at info level and
    never raises."""
    tag = _TAG_BY_TIER.get(tier, "")
    for chat_id in COMMUNITY_GROUP_IDS:
        try:
            resp = await _bot_api("setChatMemberTag", {
                "chat_id": chat_id, "user_id": int(uid), "tag": tag,
            })
            if not (resp and resp.get("ok")):
                log.info("setChatMemberTag skipped (chat=%s uid=%s tier=%s): %s", chat_id, uid, tier, resp)
        except Exception as e:
            log.info("setChatMemberTag failed (chat=%s uid=%s tier=%s): %s", chat_id, uid, tier, e)


async def _backfill_member_tags():
    """One-time pass (each cold start; cheap and idempotent) over every
    currently-active plus/pro subscriber, so people who upgraded BEFORE the
    member-tag feature shipped — or whose tag-set silently no-op'd because they
    weren't a community member yet at the time — get tagged without waiting for
    their next renewal. Runs once, off the request path, no recurring cost."""
    try:
        now = int(time.time())
        with db() as conn:
            rows = conn.execute(
                "SELECT uid, tier FROM subs WHERE expires_at>? AND tier IN ('plus','pro')",
                (now,)).fetchall()
        for r in rows:
            uid = r["uid"] or ""
            tier = r["tier"] or ""
            if uid and tier:
                await _set_member_tag(uid, tier)
                await asyncio.sleep(0.05)   # gentle pacing, never a burst against the Bot API
        if rows:
            log.info("member-tag backfill: processed %d active subscriber(s)", len(rows))
    except Exception as e:
        log.info("member-tag backfill skipped: %s", e)


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
    slug = _safe_slug(_clamp(payload.get("slug", ""), 96))
    market_url = _clamp(payload.get("marketUrl", ""), 256)
    if not market_url.startswith("https://"):
        market_url = ""
    # The exact t.me gift link — used to hyperlink the gift name and to drive the
    # message's link preview (so Telegram renders the gift's own card).
    gift_url = _clamp(payload.get("giftUrl", ""), 256)
    if not gift_url.startswith("https://t.me/"):
        gift_url = ""
    # Build the GiftTrove deep-link HERE, server-side, stamping the sharer's
    # CURRENT referral code from our own records. We deliberately ignore any
    # link the client sent: the client value can carry a stale code (e.g. the
    # auto-generated base36 one) if it raced ahead of the vanity-code fetch, or
    # simply an out-of-date cached code. The DB is the single source of truth,
    # so a shared card always carries the user's latest (custom) code.
    my_code = get_ref_code(uid) or ""
    if slug:
        link = f"{MINIAPP_URL}?startapp=g_{slug}_{my_code}" if my_code else f"{MINIAPP_URL}?startapp=g_{slug}"
    else:
        link = f"{MINIAPP_URL}?startapp={my_code}" if my_code else MINIAPP_URL
    _mid = " \u00b7 "

    lang = _user_lang(uid)
    hey = _t("share_hey", lang)
    scout = _t("share_scout", lang)
    open_lbl = _t("share_open", lang)
    title = f"{name}{(' #' + num) if num else ''}"

    # Localized share card (follows the sharer's language):
    #   Hey! Check out {gift} {ID}
    #   {marketplace} · {value} {currency}
    #
    #   Scout unique Telegram gifts on GiftTrove
    #   Open in GiftTrove ✦ t.me
    # "Open in GiftTrove" deep-links to this exact gift in the app; "t.me" is the
    # normal gift link. A fixed banner image rides along with every share.
    segs = [("text", hey), ("text", title)]
    segs.append(("text", f"\n{market}{(_mid + price) if price else ''}"))
    segs.append(("text", f"\n\n{scout}\n"))
    foot = []
    if link:
        foot.append(("link", open_lbl, link))
    if gift_url:
        foot.append(("link", "t.me", gift_url))
    for i, seg in enumerate(foot):
        if i:
            segs.append(("text", " \u2726 "))
        segs.append(seg)

    text, off, entities = "", 0, []
    for seg in segs:
        s = seg[1]
        ln = _u16len(s)
        if seg[0] == "link":
            entities.append({"type": "text_link", "offset": off, "length": ln, "url": seg[2]})
        text += s
        off += ln

    import uuid as _uuid
    _rid = _uuid.uuid4().hex[:32]
    # Always use article (text-only): the gift's own t.me link is wired in as a
    # hyperlink so Telegram renders the gift's OWN native card when the recipient
    # opens it. A separate banner image (cached_photo) attached to the share card
    # showed a generic GiftTrove graphic, not the actual gift — confusing and wrong.
    mid_line = f"{market}{(_mid + price) if price else ''}"
    result = {
        "type": "article",
        "id": _rid,
        "title": title,
        "description": mid_line,
        "input_message_content": {
            "message_text": text,
            "entities": entities,
            "link_preview_options": {"is_disabled": False, "url": gift_url} if gift_url else {"is_disabled": True},
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
            # Count the share off the critical path so the DB write doesn't delay
            # the share prompt from popping up.
            try:
                asyncio.create_task(asyncio.to_thread(track_share, name))
            except Exception:
                pass
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
