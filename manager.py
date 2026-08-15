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
import json
import logging
import os
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import base58
from dotenv import load_dotenv
from telethon import TelegramClient, events, utils
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
            if pid not in bot.source_ids:
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

HELP = (
    "**CALLRELAY control**\n\n"
    "**Akun**\n"
    "`/addnumber <name> <+62...>` — tambah userbot baru (login OTP)\n"
    "`/code <name> <kode>` · `/pass <name> <2fa>` · `/cancel <name>`\n"
    "`/delbot <name>` — copot userbot dari fleet\n\n"
    "**Pilih source channel**\n"
    "`/listchannels <bot> [keyword]` — channel yang di-join, bernomor\n"
    "`/addsource <bot> <#1,3 | @ch>` · `/delsource <bot> <#1,3 | @ch>`\n"
    "`/sources <bot>`\n\n"
    "**Pilih target group/channel**\n"
    "`/target <bot> <group|channel|both>` — jenis chat yang boleh jadi target\n"
    "`/listgroups <bot> [keyword]` — target yang di-join, bernomor\n"
    "`/allow <bot> <#1,3 | @grp|id|substr>` · `/unallow <bot> <#1,3 | entry>`\n"
    "`/mode <bot> <allowlist|blocklist|all>`\n"
    "`/titlefilter <bot> <kata|clear>` — saring by judul\n"
    "`/groups <bot>` — target yang kepilih sekarang\n\n"
    "**Anti-spam**\n"
    "`/batch <bot> <menit|off>` — kumpulin CA → 1 pesan recap\n"
    "`/cap <bot> <n|off>` — max pesan per group per hari\n"
    "`/quiet <bot> <23:00-07:00|off>` — jam tenang\n"
    "`/delay <bot> <sec>` — jeda antar group\n\n"
    "**Operasi**\n"
    "`/status` · `/stats <bot|all>`\n"
    "`/pause <bot|all>` · `/resume <bot|all>`\n"
    "`/dryrun <bot|all> <on|off>`\n"
    "`/reload <bot|all>` — re-resolve habis join/leave\n"
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

        async def reply(msg):
            await event.reply(msg, parse_mode="md", link_preview=False)

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
            if cmd in ("help", "start"):
                await reply(HELP)

            # ---------------------------------------------------- add account by phone
            elif cmd in ("addnumber", "addbot"):
                if not CREDS_OK:
                    return await reply(
                        "⚠️ **Mode terbatas** — `/addnumber` belum bisa dipakai.\n\n"
                        "Nambah userbot itu login akun Telegram beneran, dan itu butuh "
                        "`API_ID` + `API_HASH` di `.env` (bukan token bot).\n\n"
                        "Ambil di https://my.telegram.org → *API development tools*, "
                        "isi ke `.env`, terus `pm2 restart callrelay`."
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
                dialogs = await collect_dialogs(b, kind, keyword)
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
                        "⚠️ **MODE TERBATAS** — `API_ID`/`API_HASH` di `.env` belum diisi.",
                        "Control bot jalan (lo lagi ngobrol sama gue), tapi userbot belum bisa:",
                        "relay CA dan `/addnumber` butuh api_id dari my.telegram.org.",
                        "Yang udah bisa dicoba sekarang: `/help`, `/status`, `/stats`.",
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
                await reply("\n".join(lines))

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
                    return await reply(f"`{b.name}` has no sources")
                await reply(f"**{b.name} sources**\n" + "\n".join(f"• {v}" for v in b.source_names.values()))

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
                    entries = [dialog_ref(d) for d in picked]
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
                await reply("unknown command — `/help`")
        except Exception as e:
            log.error(f"control error: {e}")
            await reply(f"error: `{e}`")


# ---------------------------------------------------------------- Bot API fallback console

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
        url = f"https://api.telegram.org/bot{self.token}/{method}"
        data = urllib.parse.urlencode(params).encode()
        with urllib.request.urlopen(url, data=data, timeout=timeout) as r:
            return json.loads(r.read().decode())

    async def call(self, method, http_timeout=15, **params):
        return await asyncio.to_thread(self._post, method, params, http_timeout)

    async def send(self, chat_id, text):
        try:
            r = await self.call("sendMessage", chat_id=chat_id, text=text,
                                parse_mode="Markdown", disable_web_page_preview="true")
            if r.get("ok"):
                return
            log.warning(f"control send rejected: {r.get('description')}")
        except Exception as e:
            log.warning(f"control send failed: {e}")
        try:   # markdown in the payload can trip the parser — resend as plain text
            await self.call("sendMessage", chat_id=chat_id, text=text,
                            disable_web_page_preview="true")
        except Exception as e:
            log.error(f"control send failed (plain): {e}")

    async def start(self):
        r = await self.call("getMe")
        if not r.get("ok"):
            raise SystemExit(f"CONTROL_BOT_TOKEN ditolak Telegram: {r.get('description')}")
        self.me = r["result"]
        return self

    async def run_until_disconnected(self):
        log.info("control bot polling via Bot API (mode terbatas — tanpa api_id)")
        while True:
            try:
                r = await self.call("getUpdates", http_timeout=70, offset=self.offset,
                                    timeout=50, allowed_updates='["message"]')
            except Exception as e:
                log.warning(f"getUpdates failed: {e}")
                await asyncio.sleep(5)
                continue
            for upd in r.get("result", []):
                self.offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                text = msg.get("text") or ""
                if not text.startswith("/") or not self.handler:
                    continue
                try:
                    await self.handler(BotApiEvent(self, msg, text))
                except Exception as e:
                    log.error(f"control handler error: {e}")


class BotApiEvent:
    """Telethon-event lookalike for the Bot API path."""

    def __init__(self, ctrl, msg, text):
        self._ctrl = ctrl
        self._chat = msg["chat"]["id"]
        self.raw_text = text
        self.sender_id = (msg.get("from") or {}).get("id")

    async def reply(self, text, **_kw):
        await self._ctrl.send(self._chat, text)


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
        log.info(f"control bot live: @{me.username} (admins: {sorted(ADMIN_IDS)})")
    else:
        log.warning("No CONTROL_BOT_TOKEN — running without control bot; /addnumber unavailable")

    log.info(f"CALLRELAY MANAGER up — {len(FLEET)} userbots"
             + ("" if CREDS_OK else "  [MODE TERBATAS — nunggu api_id]"))
    runners = [b.runner for b in FLEET]
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
