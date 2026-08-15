#!/usr/bin/env python3
"""
CALLRELAY MANAGER — fleet edition
=================================
Runs MANY userbots in one process + ONE control bot to manage them all live.

  - Each userbot: listens to its source channels, extracts CAs (SOL/EVM),
    dedups (per-userbot), and reposts to a FILTERED set of groups it joined.
  - Control bot (BotFather): admin-gated command console. Pause/resume,
    add/remove sources, edit allowlist, toggle dry-run, view stats — live,
    no restart, changes persisted to fleet.json.

All userbots share ONE api_id/api_hash (api_id is per-app, not per-account);
each userbot just has its own session (its own phone login).

SAFETY: userbots SEND -> use ALT/burner accounts, allowlist your OWN groups.
Control bot only obeys admin_user_ids in fleet.json.

Run:
  python3 manager.py                 # run the fleet + control bot
  python3 manager.py --list-groups <session>   # dump joined groups for one userbot
"""

import asyncio
import collections
import json
import logging
import os
import re
import socket
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import base58
from dotenv import load_dotenv
from telethon import TelegramClient, events, utils

try:
    from telethon import Button          # inline keyboards on the MTProto path
except ImportError:                       # pragma: no cover
    Button = None
from telethon.errors import (
    ChatWriteForbiddenError,
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)

BASE = Path(__file__).parent
load_dotenv(BASE / ".env")

API_ID_RAW = (os.getenv("API_ID") or "").strip()
API_HASH = (os.getenv("API_HASH") or "").strip()
CONTROL_BOT_TOKEN = (os.getenv("CONTROL_BOT_TOKEN") or "").strip()

FLEET_PATH = BASE / "fleet.json"
DB_PATH = BASE / "callrelay.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("manager")

START_TIME = time.time()
BOT_USERNAME = ""      # filled in once the control bot logs in

# Many VPSes advertise IPv6 but have no working route. Python then burns the
# whole socket timeout on the AAAA address before falling back to IPv4, which
# showed up as every reply taking ~15s. Resolve A records only unless asked.
FORCE_IPV4 = (os.getenv("ALLOW_IPV6") or "").strip().lower() not in ("1", "true", "yes")
if FORCE_IPV4:
    _real_getaddrinfo = socket.getaddrinfo

    def _getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
        return _real_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

    socket.getaddrinfo = _getaddrinfo_ipv4
UPDATE_STATS = collections.Counter()   # what Telegram actually delivers — backs /diag
LOG_RING = collections.deque(maxlen=200)   # backs /log so the console can show recent lines


class _RingHandler(logging.Handler):
    def emit(self, record):
        try:
            LOG_RING.append(self.format(record))
        except Exception:
            pass


_ring = _RingHandler()
_ring.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
logging.getLogger().addHandler(_ring)

GET_CREDS = ("Ambil api_id + api_hash di https://my.telegram.org "
             "(login → API development tools), terus isi ke .env. Lihat HANDOFF.md")

# Userbots talk MTProto and need api_id/api_hash. The control bot doesn't:
# without valid creds we still bring it up over the plain Bot API (token only),
# so the console answers and can explain what is missing.
CREDS_OK = bool(API_ID_RAW.isdigit() and re.fullmatch(r"[0-9a-fA-F]{32}", API_HASH))
API_ID = int(API_ID_RAW) if API_ID_RAW.isdigit() else 0

if not CREDS_OK and not CONTROL_BOT_TOKEN:
    raise SystemExit(
        f"API_ID/API_HASH di .env belum valid (API_ID={API_ID_RAW!r}, API_HASH={API_HASH!r}) "
        f"dan CONTROL_BOT_TOKEN juga kosong — nggak ada yang bisa dijalanin. {GET_CREDS}"
    )
if not FLEET_PATH.exists():
    raise SystemExit("fleet.json not found — see HANDOFF.md")

with open(FLEET_PATH) as f:
    FLEET_CONFIG = json.load(f)

ADMIN_IDS = set(FLEET_CONFIG.get("admin_user_ids", []))

# Relay that runs on the bot token alone — see BotUserbot. Only covers chats the
# bot itself was added to, but needs no api_id.
BOT_RELAY = FLEET_CONFIG.setdefault("bot_relay", {})
BOT_RELAY.setdefault("name", "bot")
BOT_RELAY.setdefault("session", "-")
BOT_RELAY.setdefault("source_channels", [])
BOT_RELAY.setdefault("chains", ["sol", "evm"])
BOT_RELAY.setdefault("dedup_hours", 0)
BOT_RELAY.setdefault("delay_between_groups_sec", 3)
BOT_RELAY.setdefault("attribution", True)
BOT_RELAY.setdefault("template", None)
BOT_RELAY.setdefault("batch_window_sec", 0)
BOT_RELAY.setdefault("max_per_day_per_group", 0)
BOT_RELAY.setdefault("quiet_hours", None)
BOT_RELAY.setdefault("chats", {})          # id -> {title, type} learned as messages arrive
BOT_RELAY.setdefault("send_filter", {
    "mode": "allowlist", "allowlist": [], "blocklist": [], "title_contains": [],
    "include_groups": True, "include_channels": True, "dry_run": True,
})
USERBOT_CFGS = FLEET_CONFIG.get("userbots", [])
if not USERBOT_CFGS and not CONTROL_BOT_TOKEN:
    raise SystemExit("fleet.json needs at least one userbot in 'userbots' — or set "
                     "CONTROL_BOT_TOKEN and add one live with /addnumber")


def save_fleet():
    with open(FLEET_PATH, "w") as f:
        json.dump(FLEET_CONFIG, f, indent=2)
    log.info("fleet.json saved")


# ---------------------------------------------------------------- dedup db (per userbot)

db = sqlite3.connect(DB_PATH)
db.execute(
    """CREATE TABLE IF NOT EXISTS posted (
        bot TEXT, ca TEXT, chain TEXT, source TEXT, first_seen INTEGER,
        PRIMARY KEY (bot, ca)
    )"""
)
db.commit()


def already_posted(bot: str, ca: str, dedup_hours: int) -> bool:
    row = db.execute("SELECT first_seen FROM posted WHERE bot=? AND ca=?", (bot, ca)).fetchone()
    if row is None:
        return False
    if dedup_hours <= 0:
        return True
    return (time.time() - row[0]) < dedup_hours * 3600


def mark_posted(bot, ca, chain, source):
    db.execute(
        "INSERT OR REPLACE INTO posted (bot, ca, chain, source, first_seen) VALUES (?,?,?,?,?)",
        (bot, ca, chain, source, int(time.time())),
    )
    db.commit()


# ---------------------------------------------------------------- per-group daily cap

db.execute(
    """CREATE TABLE IF NOT EXISTS sends (
        bot TEXT, gid INTEGER, day TEXT, count INTEGER,
        PRIMARY KEY (bot, gid, day)
    )"""
)
db.commit()


def today_str():
    """Local date — set the VPS clock with: timedatectl set-timezone Asia/Jakarta"""
    return time.strftime("%Y-%m-%d")


def sends_today(bot: str, gid: int) -> int:
    row = db.execute("SELECT count FROM sends WHERE bot=? AND gid=? AND day=?",
                     (bot, gid, today_str())).fetchone()
    return row[0] if row else 0


def bump_send(bot: str, gid: int):
    db.execute(
        "INSERT INTO sends (bot, gid, day, count) VALUES (?,?,?,1) "
        "ON CONFLICT(bot, gid, day) DO UPDATE SET count = count + 1",
        (bot, gid, today_str()),
    )
    db.commit()


def ago(ts):
    d = max(0, int(time.time() - ts))
    if d < 60:
        return f"{d}d"
    if d < 3600:
        return f"{d // 60}m"
    if d < 86400:
        return f"{d // 3600}j"
    return f"{d // 86400}h"


def is_muted(cfg, cid) -> bool:
    return cid in {int(x) for x in cfg.get("muted_sources", []) if str(x).lstrip("-").isdigit()}


def cap_reached(cfg, gid: int) -> bool:
    cap = cfg.get("max_per_day_per_group", 0) or 0
    return cap > 0 and sends_today(cfg["name"], gid) >= cap


# ---------------------------------------------------------------- quiet hours

QUIET_RE = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)-([01]?\d|2[0-3]):([0-5]\d)$")


def parse_quiet(s):
    """'23:00-07:00' -> (1380, 420) in minutes-since-midnight, or None."""
    m = QUIET_RE.match((s or "").strip())
    if not m:
        return None
    h1, m1, h2, m2 = (int(x) for x in m.groups())
    return h1 * 60 + m1, h2 * 60 + m2


def in_quiet_hours(cfg, now_minutes=None) -> bool:
    rng = parse_quiet(cfg.get("quiet_hours"))
    if not rng:
        return False
    start, end = rng
    if now_minutes is None:
        lt = time.localtime()
        now_minutes = lt.tm_hour * 60 + lt.tm_min
    if start == end:
        return False
    if start < end:                       # 09:00-17:00
        return start <= now_minutes < end
    return now_minutes >= start or now_minutes < end   # 23:00-07:00, lewat tengah malam


# ---------------------------------------------------------------- CA extraction / format

EVM_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
SOL_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")


def is_valid_sol(ca):
    try:
        return len(base58.b58decode(ca)) == 32
    except Exception:
        return False


def get_all_text(msg):
    chunks = [msg.raw_text or ""]
    try:
        for e in (msg.entities or []):
            if getattr(e, "url", None):
                chunks.append(e.url)
    except Exception:
        pass
    try:
        for row in (msg.buttons or []):
            for b in row:
                if getattr(b, "url", None):
                    chunks.append(b.url)
    except Exception:
        pass
    return "\n".join(chunks)


def extract_cas(text, chains):
    found = []
    if not text:
        return found
    if "evm" in chains:
        for m in EVM_RE.findall(text):
            found.append((m, "evm"))
    if "sol" in chains:
        for m in SOL_RE.findall(text):
            if is_valid_sol(m):
                found.append((m, "sol"))
    seen, out = set(), []
    for ca, c in found:
        if ca not in seen:
            seen.add(ca)
            out.append((ca, c))
    return out


DEFAULT_TEMPLATE = (
    "🚨 **NEW CALL** — {chain}\n\n`{ca}`\n\n{links}\n\n{source}\n\n🔍 DYOR | NFA"
)


def ca_links(ca, chain):
    if chain == "sol":
        return (
            f"[GMGN](https://gmgn.ai/sol/token/{ca}) | "
            f"[DexScreener](https://dexscreener.com/solana/{ca}) | "
            f"[Photon](https://photon-sol.tinyastro.io/en/lp/{ca})"
        ), "SOL"
    return f"[DexScreener](https://dexscreener.com/search?q={ca})", "EVM"


def build_message(cfg, ca, chain, source_name):
    links, label = ca_links(ca, chain)
    src = f"📡 Source: {source_name}" if cfg.get("attribution", True) else ""
    tpl = cfg.get("template") or DEFAULT_TEMPLATE
    return tpl.format(ca=ca, chain=label, links=links, source=src).strip()


def build_digest(cfg, items):
    """One message for a whole batch — the anti-spam format."""
    if len(items) == 1:
        ca, chain, source = items[0]
        return build_message(cfg, ca, chain, source)
    attrib = cfg.get("attribution", True)
    window_min = max(1, round(cfg.get("batch_window_sec", 0) / 60))
    lines = [f"📊 **CALL RECAP** — {len(items)} CA / {window_min}m", ""]
    for i, (ca, chain, source) in enumerate(items, start=1):
        links, label = ca_links(ca, chain)
        tail = f" · {source}" if attrib else ""
        lines.append(f"**{i}.** `{ca}`")
        lines.append(f"{label}{tail} — {links}")
        lines.append("")
    lines.append("🔍 DYOR | NFA")
    return "\n".join(lines)


# ---------------------------------------------------------------- filter

def dialog_name(d):
    ent = d.entity
    title = getattr(ent, "title", None) or "?"
    uname = getattr(ent, "username", None)
    return f"{title} (@{uname})" if uname else f"{title} [{d.id}]"


def dialog_ref(d):
    """Config-friendly reference to a dialog: @username when it has one, else id."""
    u = getattr(d.entity, "username", None)
    return f"@{u}" if u else d.id


def dialog_matches(d, entry):
    ent = d.entity
    if isinstance(entry, int):
        return d.id == entry
    s = str(entry).strip()
    if s.startswith("@"):
        u = getattr(ent, "username", None)
        return u is not None and u.lower() == s[1:].lower()
    if s.lstrip("-").isdigit():
        return d.id == int(s)
    title = getattr(ent, "title", "") or ""
    return s.lower() in title.lower()


def target_kind_ok(d, fil):
    """send_filter decides which dialog kinds can be targets at all."""
    is_broadcast = d.is_channel and not d.is_group
    if is_broadcast:
        return fil.get("include_channels", False)
    if d.is_group:
        return fil.get("include_groups", True)
    return False


async def resolve_targets(client, cfg, source_ids=()):
    fil = cfg.get("send_filter", {})
    mode = fil.get("mode", "allowlist")
    allow = fil.get("allowlist", [])
    block = fil.get("blocklist", [])
    title_contains = fil.get("title_contains", [])

    out = []
    for d in await client.get_dialogs():
        if d.is_user:
            continue
        if not target_kind_ok(d, fil):
            continue
        if d.id in source_ids:
            # never post back into a channel we are listening to — that echoes
            continue
        if title_contains:
            t = getattr(d.entity, "title", "") or ""
            if not any(k.lower() in t.lower() for k in title_contains):
                continue
        if mode == "allowlist":
            if not any(dialog_matches(d, e) for e in allow):
                continue
        elif mode == "blocklist":
            if any(dialog_matches(d, e) for e in block):
                continue
        elif mode == "all":
            pass
        else:
            log.error(f"[{cfg['name']}] unknown mode {mode}")
            continue
        out.append(d)
    return out


# ---------------------------------------------------------------- userbot state

class Userbot:
    def __init__(self, cfg):
        self.cfg = cfg                    # reference into FLEET_CONFIG -> persists on save
        self.name = cfg["name"]
        self.client = None
        self.queue = asyncio.Queue()
        self.targets = []
        self.source_ids = set()           # marked ids
        self.source_names = {}            # id -> title
        self.inflight = set()             # CAs queued but not yet committed to the dedup db
        self.paused = False
        self.counters = {"relayed": 0, "dup_skips": 0, "sends_ok": 0, "sends_fail": 0}
        self.listing = {"channel": [], "group": []}   # last /listchannels + /listgroups result
        self.sender_task = None
        self.runner = None

    async def refresh_sources(self):
        self.source_ids.clear()
        self.source_names.clear()
        for s in self.cfg.get("source_channels", []):
            try:
                ent = await self.client.get_entity(s)
                pid = utils.get_peer_id(ent)
                self.source_ids.add(pid)
                self.source_names[pid] = getattr(ent, "title", str(s))
            except Exception as e:
                log.error(f"[{self.name}] cannot resolve source {s}: {e}")

    async def refresh_targets(self):
        self.targets = await resolve_targets(self.client, self.cfg, self.source_ids)


FLEET = []  # list[Userbot]


def find_bots(selector):
    """selector: bot name or 'all' -> list[Userbot]"""
    if selector == "all":
        return list(FLEET)
    return [b for b in FLEET if b.name == selector]


# ---------------------------------------------------------------- dialog pickers

MAX_LIST = 40    # keep a listing under Telegram's 4096-char message cap
MAX_BATCH = 15   # hard ceiling on CAs per digest, so one message stays readable


async def collect_dialogs(bot, kind, keyword=None):
    """Joined dialogs to pick from, title/@name filtered.

    kind='channel' -> every broadcast channel (source candidates)
    kind='group'   -> whatever send_filter allows as a target right now
                      (groups, channels, or both — see /target), sources excluded
    """
    fil = bot.cfg.get("send_filter", {})
    out = []
    for d in await bot.client.get_dialogs():
        if d.is_user:
            continue
        is_broadcast = d.is_channel and not d.is_group
        if kind == "channel" and not is_broadcast:
            continue
        if kind == "group":
            if not target_kind_ok(d, fil) or d.id in bot.source_ids:
                continue
        if keyword:
            k = keyword.lower()
            title = (getattr(d.entity, "title", "") or "").lower()
            uname = (getattr(d.entity, "username", "") or "").lower()
            if k not in title and k not in uname:
                continue
        out.append(d)
    return out


def render_listing(dialogs, chosen_ids):
    lines = []
    for i, d in enumerate(dialogs[:MAX_LIST], start=1):
        mark = "✅" if d.id in chosen_ids else "▫️"
        u = getattr(d.entity, "username", None)
        tail = f" (@{u})" if u else f" [`{d.id}`]"
        lines.append(f"`{i:>2}` {mark} {getattr(d.entity, 'title', '?') or '?'}{tail}")
    if len(dialogs) > MAX_LIST:
        lines.append(f"_…{len(dialogs) - MAX_LIST} lagi — persempit pakai keyword_")
    return lines


def is_index_arg(tokens):
    """True when the user is picking by number: '#1,3' / '#1 #3'."""
    return bool(tokens) and tokens[0].startswith("#")


def parse_indices(tokens, dialogs):
    """'#1,3 #5' -> ([dialog, ...], [bad token, ...]) against the cached listing."""
    picked, bad = [], []
    for tok in tokens:
        for part in tok.replace("#", " ").replace(",", " ").split():
            if not part.isdigit():
                bad.append(part)
                continue
            i = int(part)
            if 1 <= i <= len(dialogs[:MAX_LIST]):
                picked.append(dialogs[i - 1])
            else:
                bad.append(part)
    return picked, bad


# ---------------------------------------------------------------- live userbot attach / detach

def new_bot_cfg(name, session):
    """Safe defaults for a freshly added account: dry-run on, nothing targeted yet."""
    return {
        "name": name,
        "session": session,
        "source_channels": [],
        "chains": ["sol", "evm"],
        "dedup_hours": 0,
        "delay_between_groups_sec": 5,
        "attribution": True,
        "template": None,
        "batch_window_sec": 0,
        "max_per_day_per_group": 0,
        "quiet_hours": None,
        "send_filter": {
            "mode": "allowlist",
            "allowlist": [],
            "blocklist": [],
            "title_contains": [],
            "include_groups": True,
            "include_channels": False,
            "dry_run": True,
        },
    }


def make_handler(bot):
    async def handler(event):
        try:
            pid = utils.get_peer_id(await event.get_chat())
            if pid not in bot.source_ids or is_muted(bot.cfg, pid):
                return
            src = bot.source_names.get(pid, "unknown")
            for ca, chain in extract_cas(get_all_text(event.message), bot.cfg.get("chains", ["sol", "evm"])):
                # inflight = queued by an earlier message, not committed yet
                if ca in bot.inflight or already_posted(bot.name, ca, bot.cfg.get("dedup_hours", 0)):
                    bot.counters["dup_skips"] += 1
                    continue
                bot.inflight.add(ca)
                log.info(f"[{bot.name}] NEW {chain.upper()} {ca} from {src} -> queue")
                await bot.queue.put((ca, chain, src))
        except Exception as e:
            log.error(f"[{bot.name}] handler error: {e}")
    return handler


async def attach_userbot(cfg, client):
    """Wire a logged-in client into the fleet: resolve, listen, start sending."""
    b = Userbot(cfg)
    b.client = client
    await b.refresh_sources()
    await b.refresh_targets()
    client.add_event_handler(make_handler(b), events.NewMessage())
    b.sender_task = asyncio.create_task(sender_loop(b))
    b.runner = asyncio.create_task(client.run_until_disconnected())
    FLEET.append(b)
    return b


async def detach_userbot(bot):
    for t in (bot.sender_task, bot.runner):
        if t:
            t.cancel()
    try:
        await bot.client.disconnect()
    except Exception:
        pass
    if bot in FLEET:
        FLEET.remove(bot)
    FLEET_CONFIG["userbots"] = [c for c in FLEET_CONFIG.get("userbots", []) if c.get("name") != bot.name]


PENDING_LOGINS = {}   # bot name -> {client, phone, hash, session}


# ---------------------------------------------------------------- bot-token relay (no api_id)

def pseudo_dialog(cid, title, ctype="channel"):
    """A dialog-shaped object built from a Bot API chat, so the filter/sender
    code written against Telethon dialogs works untouched."""
    ent = types_ns(id=cid, title=title, username=None)
    return types_ns(id=cid, entity=ent, is_user=False,
                    is_group=ctype in ("group", "supergroup"),
                    is_channel=True)


class _BotClient:
    """send_message() on top of the Bot API, so sender_loop needs no changes."""

    def __init__(self, ctrl):
        self.ctrl = ctrl

    async def send_message(self, entity, text, **_kw):
        ok = await self.ctrl.send_checked(entity.id, text)
        if not ok:
            raise ChatWriteForbiddenError()


class BotUserbot(Userbot):
    """Relay driven by the bot token instead of a user account.

    Sees only chats the bot has been added to — it cannot read someone else's
    channel the way a userbot can. Everything downstream (dedup, batching, cap,
    quiet hours, filters, counters) is the shared code path.
    """

    def __init__(self, cfg, ctrl):
        super().__init__(cfg)
        self.ctrl = ctrl
        self.client = _BotClient(ctrl)
        self.botmode = True

    def known(self):
        return {int(k): v for k, v in self.cfg.get("chats", {}).items()}

    async def refresh_sources(self):
        self.source_ids.clear()
        self.source_names.clear()
        known = self.known()
        for s in self.cfg.get("source_channels", []):
            try:
                cid = int(s)
            except (TypeError, ValueError):
                log.warning(f"[{self.name}] source {s!r} bukan chat id — bot mode butuh id numerik")
                continue
            self.source_ids.add(cid)
            self.source_names[cid] = (known.get(cid) or {}).get("title", str(cid))

    async def refresh_targets(self):
        fil = self.cfg.get("send_filter", {})
        out = []
        for cid, meta in self.known().items():
            d = pseudo_dialog(cid, meta.get("title", str(cid)), meta.get("type", "channel"))
            if not target_kind_ok(d, fil) or cid in self.source_ids:
                continue
            titles = fil.get("title_contains", [])
            if titles and not any(k.lower() in (meta.get("title") or "").lower() for k in titles):
                continue
            mode = fil.get("mode", "allowlist")
            if mode == "allowlist" and not any(dialog_matches(d, e) for e in fil.get("allowlist", [])):
                continue
            if mode == "blocklist" and any(dialog_matches(d, e) for e in fil.get("blocklist", [])):
                continue
            out.append(d)
        self.targets = out

    def learned_dialogs(self, kind, keyword=None):
        """Stand-in for collect_dialogs(): the chats the bot has been added to."""
        fil = self.cfg.get("send_filter", {})
        out = []
        for cid, meta in sorted(self.known().items(), key=lambda kv: kv[1].get("title", "")):
            ctype = meta.get("type", "channel")
            d = pseudo_dialog(cid, meta.get("title", str(cid)), ctype)
            if kind == "channel" and ctype != "channel":
                continue
            if kind == "group":
                if not target_kind_ok(d, fil) or cid in self.source_ids:
                    continue
            if keyword and keyword.lower() not in (meta.get("title") or "").lower():
                continue
            out.append(d)
        return out


def remember_chat(chat):
    """Record a chat the bot can see, so it can be picked as source/target."""
    ctype = chat.get("type")
    if ctype not in ("group", "supergroup", "channel"):
        return False
    cid = str(chat.get("id"))
    prev = BOT_RELAY["chats"].get(cid)
    entry = {"title": chat.get("title") or cid, "type": ctype}
    if prev != entry:
        BOT_RELAY["chats"][cid] = entry
        save_fleet()
        log.info(f"[bot] kenal chat baru: {entry['title']} ({cid})")
        return True
    return False


def botapi_text(msg):
    """Message text + hidden hyperlink URLs + inline button URLs, Bot API shape."""
    chunks = [msg.get("text") or msg.get("caption") or ""]
    for key in ("entities", "caption_entities"):
        for e in msg.get(key) or []:
            if e.get("url"):
                chunks.append(e["url"])
    for row in (msg.get("reply_markup") or {}).get("inline_keyboard") or []:
        for b in row:
            if b.get("url"):
                chunks.append(b["url"])
    return "\n".join(chunks)


async def on_bot_message(chat, msg):
    """A message landed in a chat the bot is in — relay it if that chat is a source."""
    remember_chat(chat)
    bots = [b for b in FLEET if getattr(b, "botmode", False)]
    if not bots:
        return
    bot = bots[0]
    cid = chat.get("id")
    if cid not in bot.source_ids or is_muted(bot.cfg, cid):
        return
    src = bot.source_names.get(cid, chat.get("title") or str(cid))
    for ca, chain in extract_cas(botapi_text(msg), bot.cfg.get("chains", ["sol", "evm"])):
        if ca in bot.inflight or already_posted(bot.name, ca, bot.cfg.get("dedup_hours", 0)):
            bot.counters["dup_skips"] += 1
            continue
        bot.inflight.add(ca)
        log.info(f"[{bot.name}] NEW {chain.upper()} {ca} from {src} -> queue")
        await bot.queue.put((ca, chain, src))


async def attach_bot_relay(ctrl):
    b = BotUserbot(BOT_RELAY, ctrl)
    await b.refresh_sources()
    await b.refresh_targets()
    b.sender_task = asyncio.create_task(sender_loop(b))
    FLEET.append(b)
    log.info(f"[{b.name}] bot-mode relay siap — {len(b.source_ids)} source -> "
             f"{len(b.targets)} target, {len(b.known())} chat dikenal")
    return b


# ---------------------------------------------------------------- sender loop

async def sender_loop(bot: Userbot):
    """Fan one CA out to every target group.

    The CA is only committed to the dedup db once it actually reached at least
    one group — a paused/failed fanout leaves it un-marked so the next sighting
    is relayed instead of silently swallowed. Dry runs never touch the db.
    """
    while True:
        items = [await bot.queue.get()]
        try:
            cfg = bot.cfg
            window = cfg.get("batch_window_sec", 0) or 0
            if window > 0:
                # hold the line open: collect whatever else lands inside the window,
                # so a burst of calls becomes ONE digest instead of N messages
                loop = asyncio.get_running_loop()
                deadline = loop.time() + window
                while len(items) < MAX_BATCH:
                    left = deadline - loop.time()
                    if left <= 0:
                        break
                    try:
                        items.append(await asyncio.wait_for(bot.queue.get(), timeout=left))
                    except asyncio.TimeoutError:
                        break
                log.info(f"[{bot.name}] batch window closed — {len(items)} CA")

            dry = cfg.get("send_filter", {}).get("dry_run", False)
            delay = cfg.get("delay_between_groups_sec", 5)
            text = build_digest(cfg, items)
            label = f"{len(items)} CA" if len(items) > 1 else f"{items[0][0][:10]}…"
            delivered = 0
            quiet = in_quiet_hours(cfg)

            if quiet:
                log.info(f"[{bot.name}] quiet hours {cfg.get('quiet_hours')} — holding {label}")
            else:
                for d in bot.targets:
                    if bot.paused:
                        log.info(f"[{bot.name}] paused — dropping remainder of {label}")
                        break
                    if cap_reached(cfg, d.id):
                        log.info(f"[{bot.name}] daily cap reached for {dialog_name(d)} — skip")
                        continue
                    if dry:
                        log.info(f"[{bot.name}][DRY] would send {label} -> {dialog_name(d)}")
                        continue
                    try:
                        await bot.client.send_message(d.entity, text, parse_mode="md", link_preview=False)
                        bot.counters["sends_ok"] += 1
                        delivered += 1
                        bump_send(bot.name, d.id)
                        log.info(f"[{bot.name}] sent {label} -> {dialog_name(d)}")
                    except FloodWaitError as e:
                        log.warning(f"[{bot.name}] floodwait {e.seconds}s")
                        await asyncio.sleep(e.seconds + 2)
                        try:
                            await bot.client.send_message(d.entity, text, parse_mode="md", link_preview=False)
                            bot.counters["sends_ok"] += 1
                            delivered += 1
                            bump_send(bot.name, d.id)
                        except Exception as e2:
                            bot.counters["sends_fail"] += 1
                            log.error(f"[{bot.name}] retry failed {dialog_name(d)}: {e2}")
                    except ChatWriteForbiddenError:
                        bot.counters["sends_fail"] += 1
                        log.error(f"[{bot.name}] no write perm in {dialog_name(d)} — skip")
                    except Exception as e:
                        bot.counters["sends_fail"] += 1
                        log.error(f"[{bot.name}] send failed {dialog_name(d)}: {e}")
                    await asyncio.sleep(delay)

            if dry and not quiet:
                log.info(f"[{bot.name}][DRY] {label} previewed — dedup db untouched")
            elif delivered:
                for ca, chain, source in items:
                    mark_posted(bot.name, ca, chain, source)
                bot.counters["relayed"] += len(items)
            else:
                log.warning(f"[{bot.name}] {label} reached 0 groups — not recorded, "
                            f"will relay again next time it shows up")
        except Exception as e:
            log.error(f"[{bot.name}] sender error: {e}")
        finally:
            for ca, _, _ in items:
                bot.inflight.discard(ca)
                bot.queue.task_done()


# ---------------------------------------------------------------- control bot

# ---------------------------------------------------------------- inline keyboards
# Buttons carry the command they stand for, so a tap and a typed command take
# the exact same path through on_cmd(). Works on both transports.

def menu_main():
    return [
        [("📊 Status", "/status"), ("📈 Stats", "/stats")],
        [("🤖 Userbot", "/bots"), ("📋 Chat bot", "/chats")],
        [("🛡 Anti-spam", "/antispam"), ("📜 Riwayat", "/last")],
        [("❓ Command", "/help"), ("🩺 Info", "/version")],
    ]


def menu_bot(b):
    dry = b.cfg.get("send_filter", {}).get("dry_run", False)
    return [
        [("▶️ Resume" if b.paused else "⏸ Pause", f"/{'resume' if b.paused else 'pause'} {b.name}"),
         ("🚀 Go live" if dry else "🧪 Dry-run", f"/dryrun {b.name} {'off' if dry else 'on'}")],
        [("📡 Source", f"/listchannels {b.name}"), ("🎯 Target", f"/listgroups {b.name}")],
        [("👁 Preview", f"/preview {b.name}"), ("🧪 Test CA", f"/testca {b.name}")],
        [("🔧 Test kirim", f"/test {b.name}"), ("🔄 Reload", f"/reload {b.name}")],
        [("📜 Riwayat", "/last"), ("⬅️ Balik", "/bots")],
    ]


def to_telethon_buttons(rows):
    if not rows or Button is None:
        return None
    return [[Button.inline(label, data.encode()) for label, data in row] for row in rows]


def to_botapi_markup(rows):
    return json.dumps({"inline_keyboard": [
        [{"text": label, "callback_data": data[:64]} for label, data in row] for row in rows
    ]})


HELP = (
    "**CALLRELAY control**\n"
    "_tap `/menu` buat versi tombol_\n\n"
    "**Akun**\n"
    "`/addnumber <name> <+62...>` — tambah userbot baru (login OTP)\n"
    "`/code <name> <kode>` · `/pass <name> <2fa>` · `/cancel <name>`\n"
    "`/bots` — daftar userbot · `/bot <name>` — panel satu userbot\n"
    "`/delbot <name>` — copot userbot dari fleet\n\n"
    "**Pilih source channel**\n"
    "`/listchannels <bot> [keyword]` — channel yang di-join, bernomor\n"
    "`/addsource <bot> <#1,3 | @ch>` · `/delsource <bot> <#1,3 | @ch>`\n"
    "`/sources <bot>` — daftar source + tombol mute\n"
    "`/mute <bot> <id>` · `/unmute <bot> <id>` — matiin source sementara\n\n"
    "**Pilih target group/channel**\n"
    "`/target <bot> <group|channel|both>` — jenis chat yang boleh jadi target\n"
    "`/listgroups <bot> [keyword]` — target yang di-join, bernomor\n"
    "`/allow <bot> <#1,3 | @grp|id|substr>` · `/unallow <bot> <#1,3 | entry>`\n"
    "`/mode <bot> <allowlist|blocklist|all>`\n"
    "`/block <bot> <#1,3>` · `/unblock <bot> <#1,3>`\n"
    "`/titlefilter <bot> <kata|clear>` — saring by judul\n"
    "`/groups <bot>` — target yang kepilih sekarang\n\n"
    "**Anti-spam**\n"
    "`/batch <bot> <menit|off>` — kumpulin CA → 1 pesan recap\n"
    "`/cap <bot> <n|off>` — max pesan per group per hari\n"
    "`/quiet <bot> <23:00-07:00|off>` — jam tenang\n"
    "`/delay <bot> <sec>` — jeda antar group\n\n"
    "**Format pesan**\n"
    "`/preview <bot>` — contoh pesan yang bakal dikirim\n"
    "`/template <bot> <teks|reset>` — format sendiri: `{ca}` `{chain}` `{links}` `{source}`\n"
    "`/attribution <bot> <on|off>` — tampilin nama source\n"
    "`/chains <bot> <sol|evm|both>` · `/dedup <bot> <jam>`\n\n"
    "**Operasi**\n"
    "`/menu` · `/status` · `/stats <bot|all>`\n"
    "`/pause <bot|all>` · `/resume <bot|all>`\n"
    "`/dryrun <bot|all> <on|off>`\n"
    "`/reload <bot|all>` — re-resolve habis join/leave\n"
    "`/test <bot>` — kirim pesan tes ke semua target (cek izin)\n"
    "`/testca <bot> [ca]` — suntik CA palsu, uji pipeline relay\n"
    "`/broadcast <bot> <teks>` — kirim pesan manual ke semua target\n"
    "`/dedupreset <bot>` — lupain CA yang udah pernah dipost\n\n"
    "**Riwayat**\n"
    "`/last [n]` — CA terakhir yang direlay\n"
    "`/top` — source paling produktif\n"
    "`/find <ca>` — CA ini udah pernah dipost belum\n"
    "`/resetstats <bot|all>` — nolin counter\n\n"
    "**Sistem**\n"
    "`/ping` — ukur delay bot\n"
    "`/version` — mode, uptime, jumlah bot\n"
    "`/log [n]` — log terakhir\n"
    "`/admins` · `/addadmin <id>` · `/deladmin <id>`\n"
    "`/restart` — restart proses (pm2 nyalain lagi)\n"
)


def register_control(control):
    @control.on(events.NewMessage(pattern=r"^/"))
    async def on_cmd(event):
        if event.sender_id not in ADMIN_IDS:
            return  # silently ignore non-admins
        parts = (event.raw_text or "").split()
        if not parts:
            return
        cmd = parts[0].lower().lstrip("/").split("@")[0]   # /status@mybot -> status
        args = parts[1:]
        if not cmd:                                        # a bare "/" — show the menu
            cmd = "menu"

        async def reply(msg, buttons=None):
            kw = {}
            if buttons:
                # BotApiEvent takes the neutral form; the MTProto path needs Button objects
                kw["buttons"] = (buttons if isinstance(event, BotApiEvent)
                                 else to_telethon_buttons(buttons))
            await event.reply(msg, parse_mode="md", link_preview=False, **kw)

        async def finish_login(name):
            """Sign-in done — build the config, attach to the fleet, persist."""
            p = PENDING_LOGINS.pop(name)
            client = p["client"]
            me = await client.get_me()
            cfg = new_bot_cfg(name, p["session"])
            FLEET_CONFIG.setdefault("userbots", []).append(cfg)
            b = await attach_userbot(cfg, client)
            save_fleet()
            log.info(f"[{name}] added live as @{me.username} ({me.first_name})")
            await reply(
                f"✅ `{name}` login sebagai **{me.first_name}** (@{me.username})\n"
                f"state: 🧪 dry-run · 0 source · 0 target group\n\n"
                f"lanjut:\n"
                f"1. `/listchannels {name}` → `/addsource {name} #1,2`\n"
                f"2. `/listgroups {name}` → `/allow {name} #1,2`\n"
                f"3. cek `/groups {name}` → kalau bener: `/dryrun {name} off`\n\n"
                f"⚠️ hapus pesan kode OTP lo dari chat ini."
            )

        try:
            if cmd in ("help", "start", "menu"):
                if cmd == "help":
                    await reply(HELP, menu_main())
                else:
                    head = "**CALLRELAY**\n"
                    if not CREDS_OK:
                        head += "⚠️ mode terbatas — belum ada `api_id`, relay belum jalan\n"
                    head += (f"\n{len(FLEET)} userbot · jam server {time.strftime('%H:%M')}\n"
                             f"_tap tombol, atau ketik `/help` buat semua command_")
                    await reply(head, menu_main())

            # ---------------------------------------------------- userbot panel
            elif cmd == "bots":
                if not FLEET:
                    return await reply(
                        "belum ada userbot.\n\n"
                        + ("tambah: `/addnumber ub1 +628...`" if CREDS_OK
                           else "⚠️ butuh `api_id` dulu sebelum bisa nambah akun"),
                        menu_main())
                rows = [[(f"{'⏸' if b.paused else '🧪' if b.cfg.get('send_filter',{}).get('dry_run') else '▶️'} {b.name}",
                          f"/bot {b.name}")] for b in FLEET]
                rows.append([("⬅️ Menu", "/menu")])
                await reply(f"**{len(FLEET)} userbot** — pilih buat ngatur:", rows)

            elif cmd == "bot":
                bots = find_bots(args[0]) if args else []
                if not bots:
                    return await reply("usage: `/bot <name>`")
                b = bots[0]
                fil = b.cfg.get("send_filter", {})
                dry = fil.get("dry_run", False)
                state = "⏸ paused" if b.paused else ("🧪 dry-run" if dry else "▶️ live")
                c = b.counters
                txt = (
                    f"**{b.name}** — {state}\n\n"
                    f"📡 source: {len(b.source_ids)}\n"
                    f"🎯 target: {len(b.targets)} ({'group+channel' if fil.get('include_channels') and fil.get('include_groups', True) else 'channel' if fil.get('include_channels') else 'group'})\n"
                    f"⛓ chain: {', '.join(b.cfg.get('chains', []))}\n"
                    f"🔁 dedup: {b.cfg.get('dedup_hours', 0) or 'selamanya'}\n"
                    f"⏱ delay: {b.cfg.get('delay_between_groups_sec', 5)}s"
                    + (f" · batch {b.cfg['batch_window_sec'] // 60}m" if b.cfg.get("batch_window_sec") else "")
                    + (f" · cap {b.cfg['max_per_day_per_group']}/hari" if b.cfg.get("max_per_day_per_group") else "")
                    + (f" · quiet {b.cfg['quiet_hours']}" if b.cfg.get("quiet_hours") else "")
                    + f"\n\n📊 relayed {c['relayed']} · dup {c['dup_skips']} · ok {c['sends_ok']} · fail {c['sends_fail']}"
                )
                await reply(txt, menu_bot(b))

            elif cmd == "here":
                chat = getattr(event, "chat", None) or {}
                if chat.get("type") not in ("group", "supergroup", "channel"):
                    return await reply("Ketik `/here` **di dalam** group/channel yang mau dipakai, "
                                       "bukan di chat pribadi ini.")
                remember_chat(chat)
                bots = [b for b in FLEET if getattr(b, "botmode", False)]
                if bots:
                    await bots[0].refresh_targets()
                await reply(f"✅ `{chat.get('title')}` kecatat (id `{chat.get('id')}`).\n"
                            f"Sekarang bisa dipilih lewat `/listchannels bot` atau `/listgroups bot`.")

            elif cmd == "chats":
                bots = [b for b in FLEET if getattr(b, "botmode", False)]
                if not bots:
                    return await reply("bot-mode relay nggak aktif")
                b = bots[0]
                known = b.known()
                if not known:
                    return await reply(
                        "**Belum ada chat yang dikenal**\n\n"
                        "Cara nambahin:\n"
                        "1. Add @" + (BOT_USERNAME or "botlo") + " ke channel/group\n"
                        "2. Khusus channel: jadiin **admin** + hak *Post Messages*\n"
                        "3. Kirim 1 pesan di sana, atau ketik `/here` di sana\n\n"
                        "_Bot cuma bisa baca chat yang dia di-add. Buat nyedot channel "
                        "orang, butuh api_id + userbot._")
                lines = [f"**{len(known)} chat dikenal**", "📡 = source · 🎯 = target", ""]
                tids = {d.id for d in b.targets}
                for cid, meta in known.items():
                    mark = "📡" if cid in b.source_ids else ("🎯" if cid in tids else "▫️")
                    lines.append(f"{mark} {meta.get('title')} — `{cid}` ({meta.get('type')})")
                lines += ["", f"jadiin source: `/listchannels bot` → `/addsource bot #1`",
                          f"jadiin target: `/listgroups bot` → `/allow bot #1`"]
                await reply("\n".join(lines), [[("📡 Source", "/listchannels bot"),
                                                ("🎯 Target", "/listgroups bot")]])

            elif cmd == "antispam":
                lines = ["**Anti-spam**", ""]
                if not FLEET:
                    lines.append("_belum ada userbot_")
                for b in FLEET:
                    lines.append(
                        f"• `{b.name}` — batch "
                        f"{str(b.cfg.get('batch_window_sec', 0) // 60) + 'm' if b.cfg.get('batch_window_sec') else 'off'}"
                        f" · cap {b.cfg.get('max_per_day_per_group') or 'off'}"
                        f" · quiet {b.cfg.get('quiet_hours') or 'off'}"
                        f" · delay {b.cfg.get('delay_between_groups_sec', 5)}s")
                lines += [
                    "",
                    "`/batch <bot> <menit>` — kumpulin CA jadi 1 pesan recap",
                    "`/cap <bot> <n>` — max pesan per group per hari",
                    "`/quiet <bot> <23:00-07:00>` — jam tenang",
                    "`/delay <bot> <sec>` — jeda antar group",
                ]
                await reply("\n".join(lines), [[("⬅️ Menu", "/menu")]])

            # ---------------------------------------------------- format pesan
            elif cmd == "preview":
                bots = find_bots(args[0]) if args else []
                if not bots:
                    return await reply("usage: `/preview <bot>`")
                b = bots[0]
                demo = [("So11111111111111111111111111111111111111112", "sol", "Alpha Calls"),
                        ("0x1f9840a85d5af5bf1d1762f925bdaddc4201f984", "evm", "Beta Signals")]
                one = build_message(b.cfg, *demo[0])
                out = f"**Preview `{b.name}` — pesan satuan**\n\n{one}"
                if b.cfg.get("batch_window_sec"):
                    out += f"\n\n──────\n**Kalau batch kena ≥2 CA**\n\n{build_digest(b.cfg, demo)}"
                await reply(out, [[("⬅️ Balik", f"/bot {b.name}")]])

            elif cmd == "template":
                if len(args) < 2:
                    return await reply("usage: `/template <bot> <teks|reset>`\n"
                                       "placeholder: `{ca}` `{chain}` `{links}` `{source}`")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                b = bots[0]
                raw = " ".join(args[1:])
                if raw.lower() == "reset":
                    b.cfg["template"] = None
                else:
                    tpl = raw.replace("\\n", "\n")
                    try:
                        tpl.format(ca="x", chain="SOL", links="l", source="s")
                    except (KeyError, IndexError, ValueError) as e:
                        return await reply(f"template ditolak: `{e}`\n"
                                           f"cuma boleh `{{ca}}` `{{chain}}` `{{links}}` `{{source}}`")
                    b.cfg["template"] = tpl
                save_fleet()
                await reply(f"`{b.name}` template " + ("di-reset ke bawaan" if not b.cfg["template"] else "diganti"),
                            [[("👁 Preview", f"/preview {b.name}")]])

            elif cmd == "attribution":
                if len(args) < 2 or args[1].lower() not in ("on", "off"):
                    return await reply("usage: `/attribution <bot> <on|off>`")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                bots[0].cfg["attribution"] = args[1].lower() == "on"
                save_fleet()
                await reply(f"`{bots[0].name}` attribution = {args[1].lower()}",
                            [[("👁 Preview", f"/preview {bots[0].name}")]])

            elif cmd == "chains":
                if len(args) < 2 or args[1].lower() not in ("sol", "evm", "both"):
                    return await reply("usage: `/chains <bot> <sol|evm|both>`")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                val = args[1].lower()
                bots[0].cfg["chains"] = ["sol", "evm"] if val == "both" else [val]
                save_fleet()
                await reply(f"`{bots[0].name}` chains = {', '.join(bots[0].cfg['chains'])}")

            elif cmd == "dedup":
                if len(args) < 2:
                    return await reply("usage: `/dedup <bot> <jam>` — `0` = CA yang udah dipost "
                                       "nggak pernah diulang")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                try:
                    h = max(0, int(args[1]))
                except ValueError:
                    return await reply("jamnya angka ya")
                bots[0].cfg["dedup_hours"] = h
                save_fleet()
                await reply(f"`{bots[0].name}` dedup = " + ("selamanya (0)" if not h else f"{h} jam"))

            elif cmd == "dedupreset":
                bots = find_bots(args[0]) if args else []
                if not bots:
                    return await reply("usage: `/dedupreset <bot>`")
                n = db.execute("DELETE FROM posted WHERE bot=?", (bots[0].name,)).rowcount
                db.commit()
                await reply(f"`{bots[0].name}` lupa {n} CA — bakal di-relay lagi kalau muncul")

            # ---------------------------------------------------- test kirim
            elif cmd == "test":
                bots = find_bots(args[0]) if args else []
                if not bots:
                    return await reply("usage: `/test <bot>` — kirim pesan tes ke semua target")
                b = bots[0]
                if not b.targets:
                    return await reply(f"`{b.name}` belum punya target. `/listgroups {b.name}` dulu")
                await reply(f"ngirim tes ke {len(b.targets)} chat…")
                probe = ("🔧 **CALLRELAY test**\n\nKalau lo lihat pesan ini, izin kirim ke chat "
                         "ini aman. Aman dihapus.")
                ok, fail = [], []
                for d in b.targets:
                    try:
                        await b.client.send_message(d.entity, probe, parse_mode="md", link_preview=False)
                        ok.append(dialog_name(d))
                    except Exception as e:
                        fail.append(f"{dialog_name(d)} — {type(e).__name__}")
                    await asyncio.sleep(b.cfg.get("delay_between_groups_sec", 5))
                out = [f"**Hasil tes `{b.name}`**", ""]
                out += [f"✅ {x}" for x in ok]
                out += [f"❌ {x}" for x in fail]
                if fail:
                    out += ["", "_yang ❌ biasanya akun belum punya izin kirim di situ_"]
                await reply("\n".join(out), [[("⬅️ Balik", f"/bot {b.name}")]])

            # ---------------------------------------------------- sistem
            # ---------------------------------------------------- uji & kirim manual
            elif cmd == "testca":
                bots = find_bots(args[0]) if args else []
                if not bots:
                    return await reply("usage: `/testca <bot> [ca]`\n"
                                       "nyuntik CA palsu ke pipeline — lewat filter, dedup, "
                                       "batch, dry-run, semuanya. Buat mastiin relay jalan "
                                       "tanpa nunggu call beneran.")
                b = bots[0]
                ca = args[1] if len(args) > 1 else "So11111111111111111111111111111111111111112"
                chain = "evm" if ca.lower().startswith("0x") else "sol"
                if not b.targets:
                    return await reply(f"`{b.name}` belum punya target — `/listgroups {b.name}` dulu")
                if ca in b.inflight or already_posted(b.name, ca, b.cfg.get("dedup_hours", 0)):
                    return await reply(f"CA itu udah pernah dipost, bakal ke-skip dedup.\n"
                                       f"Pakai CA lain, atau `/dedupreset {b.name}` dulu.")
                b.inflight.add(ca)
                await b.queue.put((ca, chain, "TEST"))
                dry = b.cfg.get("send_filter", {}).get("dry_run", False)
                await reply(f"🧪 CA tes dimasukin ke antrean `{b.name}`\n"
                            f"target: {len(b.targets)} chat · "
                            + ("**dry-run** — cuma muncul di log, nggak beneran dikirim"
                               if dry else "**live** — bakal beneran kekirim")
                            + f"\n\ncek: `/log 10`",
                            [[("📜 Log", "/log 10"), ("📊 Status", "/status")]])

            elif cmd == "broadcast":
                if len(args) < 2:
                    return await reply("usage: `/broadcast <bot> <teks>` — kirim pesan manual "
                                       "ke semua target")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                b = bots[0]
                if not b.targets:
                    return await reply(f"`{b.name}` belum punya target")
                text = " ".join(args[1:]).replace("\\n", "\n")
                await reply(f"ngirim ke {len(b.targets)} chat…")
                ok, fail = 0, []
                for d in b.targets:
                    try:
                        await b.client.send_message(d.entity, text, parse_mode="md", link_preview=False)
                        ok += 1
                    except Exception as e:
                        fail.append(f"{dialog_name(d)} — {type(e).__name__}")
                    await asyncio.sleep(b.cfg.get("delay_between_groups_sec", 5))
                out = [f"📢 broadcast `{b.name}`: {ok} sukses"]
                if fail:
                    out += [f"❌ {x}" for x in fail]
                await reply("\n".join(out))

            # ---------------------------------------------------- riwayat & statistik
            elif cmd == "last":
                n = 10
                if args and args[0].isdigit():
                    n = max(1, min(30, int(args[0])))
                rows = db.execute(
                    "SELECT ca, chain, source, first_seen, bot FROM posted "
                    "ORDER BY first_seen DESC LIMIT ?", (n,)).fetchall()
                if not rows:
                    return await reply("belum ada CA yang direlay")
                lines = [f"**{len(rows)} CA terakhir**", ""]
                for ca, chain, source, ts, bot_name in rows:
                    lines.append(f"`{ca[:12]}…` {chain.upper()} · {source} · {ago(ts)} lalu")
                await reply("\n".join(lines), [[("🔄 Refresh", f"/last {n}"), ("⬅️ Menu", "/menu")]])

            elif cmd == "top":
                rows = db.execute(
                    "SELECT source, COUNT(*) c FROM posted GROUP BY source ORDER BY c DESC LIMIT 15"
                ).fetchall()
                if not rows:
                    return await reply("belum ada data — belum ada CA yang direlay")
                total = sum(c for _, c in rows)
                lines = [f"**Source paling produktif** ({total} CA total)", ""]
                for i, (source, c) in enumerate(rows, start=1):
                    bar = "█" * max(1, round(c / rows[0][1] * 10))
                    lines.append(f"`{i:>2}` {bar} **{c}** — {source}")
                lines.append("\n_source yang sepi tinggal `/mute` atau `/delsource`_")
                await reply("\n".join(lines))

            elif cmd == "find":
                if not args:
                    return await reply("usage: `/find <ca>` — cek CA ini udah pernah dipost apa belum "
                                       "(boleh sebagian aja)")
                q = args[0]
                rows = db.execute(
                    "SELECT bot, ca, chain, source, first_seen FROM posted WHERE ca LIKE ? LIMIT 10",
                    (q + "%",)).fetchall()
                if not rows:
                    rows = db.execute(
                        "SELECT bot, ca, chain, source, first_seen FROM posted WHERE ca LIKE ? LIMIT 10",
                        ("%" + q + "%",)).fetchall()
                if not rows:
                    return await reply(f"`{q}` belum pernah dipost — kalau muncul di source, bakal direlay")
                lines = ["**Ketemu**", ""]
                for bot_name, ca, chain, source, ts in rows:
                    lines.append(f"`{ca}`\n{chain.upper()} · dari {source} · {ago(ts)} lalu · bot `{bot_name}`")
                await reply("\n".join(lines))

            elif cmd == "resetstats":
                bots = find_bots(args[0]) if args else FLEET
                if not bots:
                    return await reply("usage: `/resetstats <bot|all>`")
                for b in bots:
                    for k in b.counters:
                        b.counters[k] = 0
                await reply("counter direset: " + ", ".join(f"`{b.name}`" for b in bots))

            # ---------------------------------------------------- mute source
            elif cmd in ("mute", "unmute"):
                if len(args) < 2:
                    return await reply(f"usage: `/{cmd} <bot> <id source>`\n"
                                       f"`/sources <bot>` buat liat daftarnya + tombol")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                b = bots[0]
                raw = args[1]
                if not raw.lstrip("-").isdigit():
                    return await reply("pakai id numerik — liat `/sources`")
                cid = int(raw)
                muted = b.cfg.setdefault("muted_sources", [])
                if cmd == "mute":
                    if cid not in muted:
                        muted.append(cid)
                else:
                    b.cfg["muted_sources"] = [x for x in muted if int(x) != cid]
                save_fleet()
                name = b.source_names.get(cid, str(cid))
                await reply(f"{'🔇 di-mute' if cmd == 'mute' else '🔊 di-unmute'}: {name}\n"
                            f"_source-nya tetep kedaftar, cuma pesannya diabaikan_",
                            [[("📡 Sources", f"/sources {b.name}")]])

            # ---------------------------------------------------- blocklist
            elif cmd in ("block", "unblock"):
                if len(args) < 2:
                    return await reply(f"usage: `/{cmd} <bot> <#1,3 | @grp|id>` — kepake pas "
                                       f"`/mode <bot> blocklist`")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                b = bots[0]
                rest = args[1:]
                if is_index_arg(rest):
                    if not b.listing["group"]:
                        return await reply(f"jalanin `/listgroups {b.name}` dulu")
                    picked, bad = parse_indices(rest, b.listing["group"])
                    if bad:
                        return await reply(f"nomor nggak valid: {', '.join(bad)}")
                    vals = [d.id for d in picked]
                else:
                    e = " ".join(rest)
                    vals = [int(e) if e.lstrip("-").isdigit() else e]
                fil = b.cfg.setdefault("send_filter", {})
                bl = fil.setdefault("blocklist", [])
                for v in vals:
                    if cmd == "block":
                        if v not in bl:
                            bl.append(v)
                    else:
                        bl[:] = [x for x in bl if str(x) != str(v)]
                await b.refresh_targets()
                save_fleet()
                note = "" if fil.get("mode") == "blocklist" else (
                    f"\n⚠️ mode `{b.name}` sekarang `{fil.get('mode', 'allowlist')}` — "
                    f"blocklist kesimpen tapi belum ngefek")
                await reply(f"`{b.name}` blocklist: {len(bl)} entri → {len(b.targets)} target" + note)

            elif cmd == "restart":
                await reply("♻️ restart… bakal balik sendiri dalam beberapa detik "
                            "(pm2 yang ngidupin lagi).")
                log.warning("restart diminta lewat /restart")
                asyncio.get_running_loop().call_later(1.0, os._exit, 0)

            elif cmd == "diag":
                s = UPDATE_STATS
                taps = s.get("callback_query", 0)
                lines = [
                    "**Diagnosa update**",
                    f"poll: {s.get('polls', 0)} · uptime {int(time.time() - START_TIME)}s",
                    "",
                    f"💬 message: {s.get('message', 0)}",
                    f"📢 channel_post: {s.get('channel_post', 0)}",
                    f"👆 callback_query (tap tombol): **{taps}**",
                    f"offset: `{getattr(control, 'offset', '-')}`",
                    "",
                ]
                if taps == 0:
                    lines += [
                        "⚠️ **Belum ada tap tombol yang nyampe.**",
                        "Coba tap tombol mana aja sekarang, terus `/diag` lagi.",
                        "Kalau angkanya tetep 0, Telegram nggak ngirim tap-nya — "
                        "biasanya karena pesan tombolnya dari sebelum bot restart, "
                        "atau app-nya perlu di-refresh.",
                    ]
                else:
                    lines.append("✅ tap tombol nyampe dan diproses")
                await reply("\n".join(lines), [[("👆 Tes tap", "/diag"), ("⬅️ Menu", "/menu")]])

            elif cmd == "ping":
                t0 = time.time()
                sent = getattr(event, "date", None)
                lag = f"{t0 - sent:.1f}s" if sent else "?"
                await reply(f"🏓 pong\n"
                            f"pesan lo → gue: **{lag}**\n"
                            f"_kalau angkanya > 3s terus-terusan, kemungkinan ada 2 proses "
                            f"polling bot yang sama — cek log `/log 10`_")
                log.info(f"/ping lag={lag} reply={time.time() - t0:.2f}s")

            elif cmd in ("version", "info"):
                up = int(time.time() - START_TIME)
                hh, mm = up // 3600, (up % 3600) // 60
                try:
                    import telethon as _tl
                    tlver = getattr(_tl, "__version__", "?")
                except Exception:
                    tlver = "?"
                await reply(
                    f"**CALLRELAY**\n"
                    f"mode: {'✅ penuh (MTProto)' if CREDS_OK else '⚠️ terbatas (Bot API, belum ada api_id)'}\n"
                    f"uptime: {hh}j {mm}m · jam server {time.strftime('%H:%M %Z')}\n"
                    f"userbot: {len(FLEET)} · admin: {len(ADMIN_IDS)}\n"
                    f"python {sys.version.split()[0]} · telethon {tlver}\n"
                    f"db: `{DB_PATH.name}`",
                    [[("⬅️ Menu", "/menu")]])

            elif cmd == "log":
                n = 15
                if args:
                    try:
                        n = max(1, min(50, int(args[0])))
                    except ValueError:
                        pass
                tail = list(LOG_RING)[-n:]
                if not tail:
                    return await reply("log masih kosong")
                await reply("**Log terakhir**\n```\n" + "\n".join(tail)[-3500:] + "\n```")

            elif cmd == "admins":
                await reply("**Admin**\n" + "\n".join(f"• `{i}`" for i in sorted(ADMIN_IDS))
                            + "\n\n`/addadmin <id>` · `/deladmin <id>`")

            elif cmd in ("addadmin", "deladmin"):
                if not args or not args[0].lstrip("-").isdigit():
                    return await reply(f"usage: `/{cmd} <user_id>` — ID numerik, dari @userinfobot")
                uid = int(args[0])
                if cmd == "addadmin":
                    ADMIN_IDS.add(uid)
                else:
                    if uid == event.sender_id:
                        return await reply("nggak bisa ngehapus diri sendiri")
                    if len(ADMIN_IDS) <= 1:
                        return await reply("ini admin terakhir — nggak bisa dihapus")
                    ADMIN_IDS.discard(uid)
                FLEET_CONFIG["admin_user_ids"] = sorted(ADMIN_IDS)
                save_fleet()
                await reply(f"admin sekarang: {', '.join(f'`{i}`' for i in sorted(ADMIN_IDS))}")

            # ---------------------------------------------------- add account by phone
            elif cmd in ("addnumber", "addbot"):
                if not CREDS_OK:
                    return await reply(
                        "⚠️ `/addnumber` butuh `api_id` — nambah userbot itu login akun "
                        "Telegram beneran, nggak bisa pakai token bot.\n\n"
                        "**Tapi relay tetep bisa jalan sekarang** pakai bot ini langsung:\n"
                        "1. Add @" + (BOT_USERNAME or "bot lo") + " ke channel sumber "
                        "(jadiin admin kalau itu channel)\n"
                        "2. Add juga ke group/channel tujuan, kasih hak kirim\n"
                        "3. Balik ke sini → `/chats` buat liat yang kebaca\n\n"
                        "_Bedanya: bot cuma bisa baca chat yang dia di-add. Userbot bisa "
                        "baca channel mana pun yang akunnya join._\n\n"
                        "Kalau tetep mau userbot: ambil api_id di https://my.telegram.org "
                        "(*API development tools*), isi ke `.env`, terus `pm2 restart callrelay`.",
                        [[("📋 Lihat chat", "/chats")]]
                    )
                if len(args) < 2:
                    return await reply("usage: `/addnumber <name> <+62812xxxx>`")
                name, phone = args[0], "".join(args[1:])
                phone = re.sub(r"[^\d+]", "", phone)
                if name == "all":
                    return await reply("`all` itu keyword — pilih nama lain")
                if find_bots(name) or name in PENDING_LOGINS:
                    return await reply(f"nama `{name}` udah kepake")
                if not phone.startswith("+") or len(phone) < 8:
                    return await reply("nomor harus format internasional, contoh `+628123456789`")
                session = f"session_{name}"
                client = TelegramClient(str(BASE / session), API_ID, API_HASH)
                await client.connect()
                if await client.is_user_authorized():
                    PENDING_LOGINS[name] = {"client": client, "phone": phone, "hash": None, "session": session}
                    return await finish_login(name)
                try:
                    sent = await client.send_code_request(phone)
                except PhoneNumberInvalidError:
                    await client.disconnect()
                    return await reply("nomor nggak valid")
                except FloodWaitError as e:
                    await client.disconnect()
                    return await reply(f"kena floodwait {e.seconds}s — coba lagi nanti")
                except Exception as e:
                    await client.disconnect()
                    return await reply(f"gagal minta kode: `{e}`")
                PENDING_LOGINS[name] = {
                    "client": client, "phone": phone,
                    "hash": sent.phone_code_hash, "session": session,
                }
                await reply(
                    f"📲 kode dikirim ke `{phone}`.\n"
                    f"balas: `/code {name} <kode>`\n\n"
                    f"⚠️ Telegram nge-invalidate kode yang ditulis mentahan di chat. "
                    f"Tulis pisah spasi — `/code {name} 1 2 3 4 5` — terus hapus pesannya.\n"
                    f"Batal: `/cancel {name}`"
                )

            elif cmd == "code":
                if len(args) < 2:
                    return await reply("usage: `/code <name> <kode>`")
                name = args[0]
                p = PENDING_LOGINS.get(name)
                if not p:
                    return await reply(f"nggak ada login yang nunggu buat `{name}` — mulai `/addnumber`")
                code = re.sub(r"\D", "", "".join(args[1:]))
                if not code:
                    return await reply("kodenya mana?")
                try:
                    await p["client"].sign_in(phone=p["phone"], code=code, phone_code_hash=p["hash"])
                except SessionPasswordNeededError:
                    return await reply(f"akun ini pakai 2FA → `/pass {name} <password>`")
                except PhoneCodeInvalidError:
                    return await reply("kode salah — coba lagi")
                except PhoneCodeExpiredError:
                    await p["client"].disconnect()
                    PENDING_LOGINS.pop(name, None)
                    return await reply("kode expired — ulang `/addnumber`")
                except Exception as e:
                    return await reply(f"sign-in gagal: `{e}`")
                await finish_login(name)

            elif cmd == "pass":
                if len(args) < 2:
                    return await reply("usage: `/pass <name> <password>`")
                name = args[0]
                p = PENDING_LOGINS.get(name)
                if not p:
                    return await reply(f"nggak ada login yang nunggu buat `{name}`")
                try:
                    await p["client"].sign_in(password=" ".join(args[1:]))
                except Exception as e:
                    return await reply(f"2FA gagal: `{e}`")
                await finish_login(name)

            elif cmd == "cancel":
                name = args[0] if args else ""
                p = PENDING_LOGINS.pop(name, None)
                if not p:
                    return await reply("nggak ada login yang nunggu")
                await p["client"].disconnect()
                await reply(f"login `{name}` dibatalin")

            elif cmd == "delbot":
                bots = [b for b in FLEET if b.name == (args[0] if args else "")]
                if not bots:
                    return await reply("usage: `/delbot <name>` (nama persis, `all` nggak dipake di sini)")
                b = bots[0]
                await detach_userbot(b)
                save_fleet()
                await reply(f"🗑 `{b.name}` dicopot dari fleet.\n"
                            f"file session `{b.cfg.get('session')}.session` masih ada di disk — "
                            f"hapus manual kalau mau logout beneran.")

            # ---------------------------------------------------- pickers
            elif cmd in ("listchannels", "listgroups"):
                bots = find_bots(args[0]) if args else []
                if not bots:
                    return await reply(f"usage: `/{cmd} <bot> [keyword]`")
                b = bots[0]
                kind = "channel" if cmd == "listchannels" else "group"
                keyword = " ".join(args[1:]) or None
                dialogs = (b.learned_dialogs(kind, keyword) if getattr(b, "botmode", False)
                           else await collect_dialogs(b, kind, keyword))
                if getattr(b, "botmode", False) and not dialogs and not b.known():
                    return await reply(
                        "Bot ini belum di-add ke chat mana pun.\n\n"
                        "1. Add @" + (BOT_USERNAME or "botlo") + " ke channel/group\n"
                        "2. Buat channel: jadiin **admin** dengan hak *Post Messages*\n"
                        "3. Kirim 1 pesan di situ (atau ketik `/here` di sana)\n"
                        "4. Balik ke sini, ulang command-nya")
                b.listing[kind] = dialogs
                if not dialogs:
                    return await reply(f"`{b.name}` nggak punya {kind} yang cocok"
                                       + (f" sama `{keyword}`" if keyword else ""))
                chosen = b.source_ids if kind == "channel" else {d.id for d in b.targets}
                head = (f"**{b.name} — {kind} ({len(dialogs)})**"
                        + (f" filter `{keyword}`" if keyword else "")
                        + "\n✅ = udah kepilih\n")
                pick = (f"`/addsource {b.name} #1,3`" if kind == "channel"
                        else f"`/allow {b.name} #1,3`")
                await reply(head + "\n".join(render_listing(dialogs, chosen)) + f"\n\npilih: {pick}")

            # ---------------------------------------------------- anti-spam knobs
            elif cmd == "batch":
                if len(args) < 2:
                    return await reply("usage: `/batch <bot> <menit|off>`\n"
                                       "kumpulin CA selama N menit → kirim 1 pesan recap")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                val = args[1].lower()
                if val in ("off", "0"):
                    secs = 0
                else:
                    try:
                        secs = int(round(float(val) * 60))
                    except ValueError:
                        return await reply("menitnya angka ya, contoh `/batch ub1 10`")
                    if secs < 60:
                        return await reply("minimal 1 menit")
                for b in bots:
                    b.cfg["batch_window_sec"] = secs
                save_fleet()
                await reply(f"`{bots[0].name}` batch = "
                            + ("off (kirim satuan)" if not secs else f"{secs // 60} menit / max {MAX_BATCH} CA per pesan"))

            elif cmd == "cap":
                if len(args) < 2:
                    return await reply("usage: `/cap <bot> <n|off>` — max pesan per group per hari")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                val = args[1].lower()
                try:
                    n = 0 if val in ("off", "0") else int(val)
                except ValueError:
                    return await reply("angkanya ya, contoh `/cap ub1 20`")
                for b in bots:
                    b.cfg["max_per_day_per_group"] = max(0, n)
                save_fleet()
                await reply(f"`{bots[0].name}` cap = " + ("off" if n <= 0 else f"{n} pesan/group/hari"))

            elif cmd == "quiet":
                if len(args) < 2:
                    lt = time.strftime("%H:%M")
                    return await reply(f"usage: `/quiet <bot> <HH:MM-HH:MM|off>`\n"
                                       f"contoh `/quiet ub1 23:00-07:00`\n"
                                       f"jam server sekarang: **{lt}**")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                val = args[1].lower()
                if val == "off":
                    rng = None
                elif parse_quiet(args[1]):
                    rng = args[1]
                else:
                    return await reply("format jamnya `HH:MM-HH:MM`, contoh `23:00-07:00`")
                for b in bots:
                    b.cfg["quiet_hours"] = rng
                save_fleet()
                now = "🌙 lagi jam tenang" if in_quiet_hours(bots[0].cfg) else "☀️ lagi jam aktif"
                await reply(f"`{bots[0].name}` quiet hours = {rng or 'off'}\n"
                            f"jam server **{time.strftime('%H:%M')}** — {now}")

            elif cmd == "target":
                if len(args) < 2 or args[1] not in ("group", "channel", "both"):
                    return await reply("usage: `/target <bot> <group|channel|both>`\n"
                                       "nentuin jenis chat yang boleh jadi target kiriman")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                b = bots[0]
                fil = b.cfg.setdefault("send_filter", {})
                fil["include_groups"] = args[1] in ("group", "both")
                fil["include_channels"] = args[1] in ("channel", "both")
                await b.refresh_targets()
                save_fleet()
                await reply(f"`{b.name}` target = `{args[1]}` → {len(b.targets)} chat\n"
                            f"cek daftarnya: `/listgroups {b.name}`\n"
                            f"_note: buat channel, akun userbot harus admin dengan hak post._")

            elif cmd == "titlefilter":
                if len(args) < 2:
                    return await reply("usage: `/titlefilter <bot> <kata|clear>`")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                b = bots[0]
                fil = b.cfg.setdefault("send_filter", {})
                word = " ".join(args[1:])
                if word.lower() == "clear":
                    fil["title_contains"] = []
                else:
                    fil["title_contains"] = [word]
                await b.refresh_targets()
                save_fleet()
                await reply(f"`{b.name}` title filter = "
                            f"{'(off)' if not fil['title_contains'] else '`' + word + '`'} "
                            f"→ {len(b.targets)} group")

            elif cmd == "status":
                lines = [f"**Fleet status** — jam server {time.strftime('%H:%M')}"]
                if not CREDS_OK:
                    lines += [
                        "",
                        "⚠️ **MODE BOT** — belum ada `api_id`, jadi userbot mati.",
                        "Tapi relay tetep bisa jalan pakai token bot: bot cuma baca chat",
                        "yang dia di-add. Mulai dari `/chats`.",
                        "_Buat nyedot channel orang (tanpa bisa nge-add bot), butuh api_id._",
                        "",
                    ]
                if not FLEET:
                    lines.append("_belum ada userbot_")
                for b in FLEET:
                    dry = b.cfg.get("send_filter", {}).get("dry_run", False)
                    state = "⏸ paused" if b.paused else ("🧪 dry" if dry else "▶️ live")
                    if in_quiet_hours(b.cfg):
                        state += " 🌙"
                    knobs = []
                    if b.cfg.get("batch_window_sec"):
                        knobs.append(f"batch {b.cfg['batch_window_sec'] // 60}m")
                    if b.cfg.get("max_per_day_per_group"):
                        knobs.append(f"cap {b.cfg['max_per_day_per_group']}/hari")
                    if b.cfg.get("quiet_hours"):
                        knobs.append(f"quiet {b.cfg['quiet_hours']}")
                    lines.append(
                        f"• `{b.name}` {state} — {len(b.source_ids)} src → "
                        f"{len(b.targets)} grp | relayed {b.counters['relayed']} "
                        f"ok {b.counters['sends_ok']} fail {b.counters['sends_fail']}"
                        + (f"\n   ⚙️ {' · '.join(knobs)}" if knobs else "")
                    )
                rows = [[(b.name, f"/bot {b.name}")] for b in FLEET[:6]]
                rows.append([("🔄 Refresh", "/status"), ("⬅️ Menu", "/menu")])
                await reply("\n".join(lines), rows)

            elif cmd in ("pause", "resume"):
                bots = find_bots(args[0]) if args else []
                if not bots:
                    return await reply("usage: `/pause <bot|all>`")
                for b in bots:
                    b.paused = (cmd == "pause")
                await reply(f"{'⏸ paused' if cmd=='pause' else '▶️ resumed'}: {', '.join(b.name for b in bots)}")

            elif cmd == "dryrun":
                if len(args) < 2 or args[1].lower() not in ("on", "off"):
                    return await reply("usage: `/dryrun <bot|all> <on|off>`")
                bots = find_bots(args[0])
                val = args[1].lower() == "on"
                for b in bots:
                    b.cfg.setdefault("send_filter", {})["dry_run"] = val
                save_fleet()
                await reply(f"dry_run={'on' if val else 'off'}: {', '.join(b.name for b in bots)}")

            elif cmd == "delay":
                if len(args) < 2:
                    return await reply("usage: `/delay <bot> <sec>`")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                sec = max(1, int(args[1]))
                for b in bots:
                    b.cfg["delay_between_groups_sec"] = sec
                save_fleet()
                await reply(f"delay={sec}s: {', '.join(b.name for b in bots)}")

            elif cmd == "sources":
                bots = find_bots(args[0]) if args else []
                if not bots:
                    return await reply("usage: `/sources <bot>`")
                b = bots[0]
                if not b.source_names:
                    return await reply(f"`{b.name}` belum punya source — "
                                       f"`/listchannels {b.name}` buat milih")
                lines = [f"**{b.name} — {len(b.source_names)} source**", ""]
                rows = []
                for cid, title in list(b.source_names.items())[:8]:
                    m = is_muted(b.cfg, cid)
                    lines.append(f"{'🔇' if m else '📡'} {title} — `{cid}`")
                    rows.append([(f"{'🔊 Unmute' if m else '🔇 Mute'} {title[:14]}",
                                  f"/{'unmute' if m else 'mute'} {b.name} {cid}")])
                rows.append([("⬅️ Balik", f"/bot {b.name}")])
                await reply("\n".join(lines), rows)

            elif cmd in ("addsource", "delsource"):
                if len(args) < 2:
                    return await reply(f"usage: `/{cmd} <bot> <#1,3 | @channel>`\n"
                                       f"nomor `#n` ngikutin `/listchannels <bot>` terakhir")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                b = bots[0]
                rest = args[1:]

                if is_index_arg(rest):
                    if not b.listing["channel"]:
                        return await reply(f"jalanin `/listchannels {b.name}` dulu biar ada nomornya")
                    picked, bad = parse_indices(rest, b.listing["channel"])
                    if bad:
                        return await reply(f"nomor nggak valid: {', '.join(bad)}")
                    # bot mode addresses chats by numeric id only
                    entries = [d.id if getattr(b, "botmode", False) else dialog_ref(d)
                               for d in picked]
                else:
                    entries = [rest[0]]

                srcs = b.cfg.setdefault("source_channels", [])
                touched = []
                for e in entries:
                    if cmd == "addsource":
                        if e not in srcs:
                            srcs.append(e)
                            touched.append(str(e))
                    else:
                        before = len(srcs)
                        srcs[:] = [x for x in srcs if str(x) != str(e)]
                        if len(srcs) != before:
                            touched.append(str(e))
                await b.refresh_sources()
                save_fleet()
                verb = "added" if cmd == "addsource" else "removed"
                await reply(f"{verb}: {', '.join(f'`{t}`' for t in touched) or '(nothing)'}\n"
                            f"`{b.name}` sekarang {len(b.source_ids)} source aktif")

            elif cmd == "mode":
                if len(args) < 2 or args[1] not in ("allowlist", "blocklist", "all"):
                    return await reply("usage: `/mode <bot> <allowlist|blocklist|all>`")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                b = bots[0]
                b.cfg.setdefault("send_filter", {})["mode"] = args[1]
                await b.refresh_targets()
                save_fleet()
                await reply(f"`{b.name}` mode={args[1]} → {len(b.targets)} groups")

            elif cmd in ("allow", "unallow"):
                if len(args) < 2:
                    return await reply(f"usage: `/{cmd} <bot> <#1,3 | @grp|id|substr>`\n"
                                       f"nomor `#n` ngikutin `/listgroups <bot>` terakhir")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                b = bots[0]
                rest = args[1:]

                if is_index_arg(rest):
                    if not b.listing["group"]:
                        return await reply(f"jalanin `/listgroups {b.name}` dulu biar ada nomornya")
                    picked, bad = parse_indices(rest, b.listing["group"])
                    if bad:
                        return await reply(f"nomor nggak valid: {', '.join(bad)}")
                    entry_vals = [d.id for d in picked]     # id = paling stabil
                else:
                    entry = " ".join(rest)
                    entry_vals = [int(entry) if entry.lstrip("-").isdigit() else entry]

                fil = b.cfg.setdefault("send_filter", {})
                al = fil.setdefault("allowlist", [])
                for entry_val in entry_vals:
                    if cmd == "allow":
                        if entry_val not in al:
                            al.append(entry_val)
                    else:
                        al[:] = [x for x in al if str(x) != str(entry_val)]
                await b.refresh_targets()
                save_fleet()
                mode = fil.get("mode", "allowlist")
                note = "" if mode == "allowlist" else (
                    f"\n⚠️ `{b.name}` is in `{mode}` mode — the allowlist is saved but not "
                    f"filtering anything. Switch with `/mode {b.name} allowlist`."
                )
                await reply(f"`{b.name}` allowlist updated "
                            f"({len(fil.get('allowlist', []))} entries) → "
                            f"{len(b.targets)} groups match" + note)

            elif cmd == "groups":
                bots = find_bots(args[0]) if args else []
                if not bots:
                    return await reply("usage: `/groups <bot>`")
                b = bots[0]
                if not b.targets:
                    return await reply(f"`{b.name}` — no target groups match filter")
                await reply(f"**{b.name} targets ({len(b.targets)})**\n" +
                            "\n".join(f"• {dialog_name(d)}" for d in b.targets[:50]))

            elif cmd == "reload":
                bots = find_bots(args[0]) if args else []
                if not bots:
                    return await reply("usage: `/reload <bot|all>`")
                for b in bots:
                    await b.refresh_sources()
                    await b.refresh_targets()
                await reply("reloaded: " + ", ".join(f"{b.name}({len(b.targets)}grp)" for b in bots))

            elif cmd == "stats":
                bots = find_bots(args[0]) if args else FLEET
                lines = ["**Stats**"]
                for b in bots:
                    c = b.counters
                    lines.append(f"• `{b.name}` relayed {c['relayed']} · dup {c['dup_skips']} · "
                                 f"ok {c['sends_ok']} · fail {c['sends_fail']}")
                await reply("\n".join(lines))

            else:
                await reply("unknown command — `/help`", menu_main())
        except Exception as e:
            log.error(f"control error: {e}")
            await reply(f"error: `{e}`")

    # MTProto path: taps arrive as CallbackQuery, not messages. The Bot API path
    # folds them into its own poller, so this is Telethon-only.
    if not isinstance(control, BotApiControl) and hasattr(events, "CallbackQuery"):
        @control.on(events.CallbackQuery())
        async def on_tap(cb):
            try:
                await cb.answer()
            except Exception:
                pass
            data = cb.data.decode() if isinstance(cb.data, bytes) else str(cb.data or "")
            if data.startswith("/"):
                await on_cmd(_CBEvent(cb, data))


# ---------------------------------------------------------------- Bot API fallback console

def botapi_post(token, method, params, timeout=30):
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = urllib.parse.urlencode(params).encode()
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(url, data=data, timeout=timeout) as r:
            out = json.loads(r.read().decode())
        dt = time.monotonic() - t0
        if dt > 3 and method != "getUpdates":
            log.warning(f"{method} lambat: {dt:.1f}s (jaringan VPS ke Telegram)")
        return out
    except urllib.error.HTTPError as e:
        # Telegram explains itself in the body ("Conflict: terminated by other
        # getUpdates request", "Forbidden: bot was blocked", …) — surface that
        # instead of a bare "HTTP Error 409".
        try:
            return json.loads(e.read().decode())
        except Exception:
            return {"ok": False, "description": f"HTTP {e.code}"}


# Telegram only shows the "/" autocomplete for commands registered with
# setMyCommands. Without this the console works but looks empty in the client.
BOT_COMMANDS = [
    ("menu", "Menu utama (tombol)"),
    ("status", "Status semua bot"),
    ("ping", "Ukur delay bot"),
    ("diag", "Cek update dari Telegram nyampe apa nggak"),
    ("chats", "Chat yang kebaca bot"),
    ("here", "Daftarin chat ini (ketik di dalam group)"),
    ("bots", "Daftar userbot"),
    ("listchannels", "Pilih source channel — /listchannels <bot>"),
    ("addsource", "Tambah source — /addsource <bot> #1,3"),
    ("delsource", "Hapus source — /delsource <bot> #1"),
    ("sources", "Daftar source + tombol mute"),
    ("mute", "Matiin source sementara — /mute <bot> <id>"),
    ("unmute", "Nyalain lagi — /unmute <bot> <id>"),
    ("listgroups", "Pilih target — /listgroups <bot>"),
    ("allow", "Tambah target — /allow <bot> #1,3"),
    ("unallow", "Hapus target — /unallow <bot> #1"),
    ("groups", "Target yang kepilih"),
    ("target", "Jenis target — /target <bot> group|channel|both"),
    ("mode", "Mode filter — /mode <bot> allowlist|blocklist|all"),
    ("titlefilter", "Saring target by judul"),
    ("testca", "Suntik CA palsu buat ngetes — /testca <bot>"),
    ("test", "Kirim pesan tes ke semua target"),
    ("broadcast", "Kirim pesan manual — /broadcast <bot> <teks>"),
    ("preview", "Contoh pesan yang bakal dikirim"),
    ("template", "Format pesan sendiri"),
    ("dryrun", "Preview-only on/off — /dryrun <bot> off"),
    ("batch", "Kumpulin CA jadi 1 recap — /batch <bot> <menit>"),
    ("cap", "Max pesan per group per hari"),
    ("quiet", "Jam tenang — /quiet <bot> 23:00-07:00"),
    ("delay", "Jeda antar group"),
    ("pause", "Stop kirim sementara"),
    ("resume", "Lanjut kirim"),
    ("reload", "Re-resolve source & target"),
    ("last", "CA terakhir yang direlay"),
    ("top", "Source paling produktif"),
    ("find", "Cari CA — /find <ca>"),
    ("stats", "Counter relay"),
    ("log", "Log terakhir — /log 10"),
    ("version", "Mode, uptime, jumlah bot"),
    ("addnumber", "Tambah userbot (butuh api_id)"),
    ("help", "Semua command"),
]


async def setup_bot_commands(control):
    """Register the command list so Telegram's "/" menu is populated."""
    payload = json.dumps([{"command": c, "description": d} for c, d in BOT_COMMANDS])
    try:
        if isinstance(control, BotApiControl):
            r = await control.call("setMyCommands", commands=payload)
        else:
            r = await asyncio.to_thread(botapi_post, CONTROL_BOT_TOKEN,
                                        "setMyCommands", {"commands": payload}, 15)
        if r.get("ok"):
            log.info(f"{len(BOT_COMMANDS)} command kedaftar di menu Telegram")
        else:
            log.warning(f"setMyCommands ditolak: {r.get('description')}")
    except Exception as e:
        log.warning(f"setMyCommands gagal: {e}")


class BotApiControl:
    """Control bot over the plain HTTP Bot API — token only, no api_id.

    Used when API_ID/API_HASH are missing so the console still answers and can
    say what is missing. Userbots are unaffected: they need MTProto either way.
    Exposes just enough of the Telethon surface (.on / .get_me / .run_until_disconnected)
    for register_control() to work unchanged.
    """

    def __init__(self, token):
        self.token = token
        self.handler = None
        self.offset = 0
        self.me = {}

    # --- telethon-shaped bits ------------------------------------------------
    def on(self, _event):
        def deco(fn):
            self.handler = fn
            return fn
        return deco

    async def get_me(self):
        return types_ns(username=self.me.get("username"), first_name=self.me.get("first_name"))

    # --- http ---------------------------------------------------------------
    def _post(self, method, params, timeout):
        return botapi_post(self.token, method, params, timeout)

    async def call(self, method, http_timeout=15, **params):
        return await asyncio.to_thread(self._post, method, params, http_timeout)

    async def send_checked(self, chat_id, text):
        """Like send() but reports failure, so the relay can count it."""
        try:
            r = await self.call("sendMessage", chat_id=chat_id, text=text,
                                parse_mode="Markdown", disable_web_page_preview="true")
            if r.get("ok"):
                return True
            log.warning(f"relay send rejected ({chat_id}): {r.get('description')}")
        except Exception as e:
            log.warning(f"relay send failed ({chat_id}): {e}")
        try:
            r = await self.call("sendMessage", chat_id=chat_id, text=text,
                                disable_web_page_preview="true")
            return bool(r.get("ok"))
        except Exception:
            return False

    async def send(self, chat_id, text, buttons=None):
        extra = {"reply_markup": to_botapi_markup(buttons)} if buttons else {}
        try:
            r = await self.call("sendMessage", chat_id=chat_id, text=text,
                                parse_mode="Markdown", disable_web_page_preview="true", **extra)
            if r.get("ok"):
                return
            log.warning(f"control send rejected: {r.get('description')}")
        except Exception as e:
            log.warning(f"control send failed: {e}")
        try:   # markdown in the payload can trip the parser — resend as plain text
            await self.call("sendMessage", chat_id=chat_id, text=text,
                            disable_web_page_preview="true", **extra)
        except Exception as e:
            log.error(f"control send failed (plain): {e}")

    async def start(self):
        r = await self.call("getMe")
        if not r.get("ok"):
            raise SystemExit(f"CONTROL_BOT_TOKEN ditolak Telegram: {r.get('description')}")
        self.me = r["result"]
        return self

    async def run_until_disconnected(self):
        log.info("control bot polling via Bot API (long poll 50s)")
        while True:
            try:
                r = await self.call("getUpdates", http_timeout=70, offset=self.offset,
                                    timeout=50,
                                    allowed_updates='["message","channel_post","callback_query"]')
            except Exception as e:
                log.warning(f"getUpdates failed: {e}")
                await asyncio.sleep(3)
                continue
            if not r.get("ok"):
                desc = r.get("description", "?")
                if "Conflict" in desc:
                    # two pollers on one token: Telegram hands each update to whoever
                    # asks first, so replies look randomly slow or missing
                    log.error("KONFLIK: ada proses lain yang polling bot yang sama. "
                              "Cek `pm2 list` dan `ps aux | grep manager.py`, sisain satu.")
                else:
                    log.warning(f"getUpdates: {desc}")
                await asyncio.sleep(3)
                continue
            UPDATE_STATS["polls"] += 1
            for upd in r.get("result", []):
                self.offset = upd["update_id"] + 1
                for k in upd:
                    if k != "update_id":
                        UPDATE_STATS[k] += 1
                if not self.handler:
                    continue
                if "callback_query" in upd:          # a tapped button
                    cb = upd["callback_query"]
                    text = cb.get("data") or ""
                    msg = {"chat": (cb.get("message") or {}).get("chat", {}),
                           "from": cb.get("from", {})}
                    log.info(f"tap {text!r} from {(cb.get('from') or {}).get('id')}")
                    try:
                        await self.call("answerCallbackQuery", callback_query_id=cb["id"])
                    except Exception as e:
                        log.warning(f"answerCallbackQuery gagal: {e}")
                    if not msg.get("chat"):
                        log.warning("callback tanpa chat — nggak bisa dibales")
                        continue
                else:
                    msg = upd.get("message") or upd.get("channel_post") or {}
                    text = msg.get("text") or ""
                    chat = msg.get("chat") or {}
                    if chat.get("type") in ("group", "supergroup", "channel"):
                        try:      # learn the chat, and relay it if it is a source
                            await on_bot_message(chat, msg)
                        except Exception as e:
                            log.error(f"relay error: {e}")
                if not text.startswith("/"):
                    continue
                lag = time.time() - msg["date"] if msg.get("date") else None
                log.info(f"cmd {text.split()[0]} from {(msg.get('from') or {}).get('id')}"
                         + (f" (lag {lag:.1f}s)" if lag is not None else ""))
                # dispatch off the poll loop: a slow command (/test walks every
                # target with a delay) must not hold up the next getUpdates
                asyncio.create_task(self._dispatch(BotApiEvent(self, msg, text)))

    async def _dispatch(self, event):
        t0 = time.time()
        try:
            await self.handler(event)
        except Exception as e:
            log.error(f"control handler error: {e}")
        else:
            log.info(f"cmd {event.raw_text.split()[0]} done in {time.time() - t0:.1f}s")


class BotApiEvent:
    """Telethon-event lookalike for the Bot API path."""

    def __init__(self, ctrl, msg, text):
        self._ctrl = ctrl
        self.chat = msg.get("chat") or {}
        self._chat = self.chat.get("id")
        self.raw_text = text
        self.sender_id = (msg.get("from") or {}).get("id")
        self.date = msg.get("date")          # unix ts, lets /ping measure real lag

    async def reply(self, text, buttons=None, **_kw):
        await self._ctrl.send(self._chat, text, buttons)


class _CBEvent:
    """Telethon CallbackQuery wrapped to look like a message event, so a tapped
    button runs the exact same handler as the typed command."""

    def __init__(self, cb, text):
        self._cb = cb
        self.raw_text = text
        self.sender_id = cb.sender_id

    async def reply(self, text, **kw):
        await self._cb.respond(text, **kw)


def types_ns(**kw):
    class _NS:
        pass
    ns = _NS()
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


# ---------------------------------------------------------------- --list-groups

async def list_groups(session):
    client = TelegramClient(str(BASE / session), API_ID, API_HASH)
    await client.start()
    print(f"\n=== JOINED GROUPS for session '{session}' ===")
    print(f"{'id':>16}  {'type':<9} {'username':<22} title")
    print("-" * 80)
    for d in await client.get_dialogs():
        if d.is_user:
            continue
        kind = "group" if d.is_group else ("channel" if d.is_channel else "other")
        u = getattr(d.entity, "username", None)
        u = f"@{u}" if u else "-"
        print(f"{d.id:>16}  {kind:<9} {u:<22} {getattr(d.entity,'title','') or ''}")
    print()
    await client.disconnect()


# ---------------------------------------------------------------- boot

async def main():
    if not CREDS_OK:
        log.warning("=" * 70)
        log.warning("API_ID/API_HASH di .env belum valid — MODE TERBATAS.")
        log.warning("Control bot tetep nyala (Bot API) dan bakal bales chat lo,")
        log.warning("tapi userbot belum bisa jalan: relay & /addnumber butuh api_id.")
        log.warning(GET_CREDS)
        log.warning("=" * 70)

    # start each userbot listed in fleet.json (more can be added live via /addnumber)
    if CREDS_OK:
        for cfg in USERBOT_CFGS:
            client = TelegramClient(str(BASE / cfg["session"]), API_ID, API_HASH)
            await client.start()  # interactive first run per session
            me = await client.get_me()
            log.info(f"[{cfg['name']}] logged in as {me.first_name} (@{me.username})")
            b = await attach_userbot(cfg, client)
            log.info(f"[{b.name}] {len(b.source_ids)} sources -> {len(b.targets)} groups "
                     f"(mode={cfg.get('send_filter',{}).get('mode')} "
                     f"dry={cfg.get('send_filter',{}).get('dry_run')})")
    elif USERBOT_CFGS:
        log.warning(f"{len(USERBOT_CFGS)} userbot di fleet.json dilewatin (butuh api_id)")

    # start control bot — Telethon when we have creds, Bot API when we don't
    control = None
    if CONTROL_BOT_TOKEN:
        if CREDS_OK:
            control = await TelegramClient(str(BASE / "control_bot"), API_ID,
                                           API_HASH).start(bot_token=CONTROL_BOT_TOKEN)
        else:
            control = await BotApiControl(CONTROL_BOT_TOKEN).start()
        register_control(control)
        me = await control.get_me()
        globals()["BOT_USERNAME"] = me.username or ""
        log.info(f"control bot live: @{me.username} (admins: {sorted(ADMIN_IDS)})")
        asyncio.create_task(setup_bot_commands(control))   # background: never blocks boot
        if isinstance(control, BotApiControl):
            # no api_id: the bot token itself can still relay, for chats the bot is in
            await attach_bot_relay(control)
    else:
        log.warning("No CONTROL_BOT_TOKEN — running without control bot; /addnumber unavailable")

    log.info(f"CALLRELAY MANAGER up — {len(FLEET)} userbots"
             + ("" if CREDS_OK else "  [MODE TERBATAS — nunggu api_id]"))
    # the bot-mode relay has no runner of its own — it rides the control poller
    runners = [b.runner for b in FLEET if b.runner is not None]
    if control:
        runners.append(asyncio.create_task(control.run_until_disconnected()))
    if not runners:
        raise SystemExit("nggak ada userbot dan nggak ada control bot — cek .env & fleet.json")
    await asyncio.gather(*runners)


if __name__ == "__main__":
    if "--list-groups" in sys.argv:
        idx = sys.argv.index("--list-groups")
        if len(sys.argv) > idx + 1:
            session = sys.argv[idx + 1]
        elif USERBOT_CFGS:
            session = USERBOT_CFGS[0]["session"]
        else:
            raise SystemExit("no userbot in fleet.json — usage: --list-groups <session>")
        asyncio.run(list_groups(session))
    else:
        asyncio.run(main())
