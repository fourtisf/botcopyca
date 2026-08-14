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
from pathlib import Path

import base58
from dotenv import load_dotenv
from telethon import TelegramClient, events, utils
from telethon.errors import FloodWaitError, ChatWriteForbiddenError

BASE = Path(__file__).parent
load_dotenv(BASE / ".env")

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
CONTROL_BOT_TOKEN = os.getenv("CONTROL_BOT_TOKEN", "")

FLEET_PATH = BASE / "fleet.json"
DB_PATH = BASE / "callrelay.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("manager")

if not API_ID or not API_HASH:
    raise SystemExit("Missing API_ID / API_HASH in .env — see HANDOFF.md")
if not FLEET_PATH.exists():
    raise SystemExit("fleet.json not found — see HANDOFF.md")

with open(FLEET_PATH) as f:
    FLEET_CONFIG = json.load(f)

ADMIN_IDS = set(FLEET_CONFIG.get("admin_user_ids", []))
USERBOT_CFGS = FLEET_CONFIG.get("userbots", [])
if not USERBOT_CFGS:
    raise SystemExit("fleet.json needs at least one userbot in 'userbots'")


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


def build_message(cfg, ca, chain, source_name):
    if chain == "sol":
        links = (
            f"[GMGN](https://gmgn.ai/sol/token/{ca}) | "
            f"[DexScreener](https://dexscreener.com/solana/{ca}) | "
            f"[Photon](https://photon-sol.tinyastro.io/en/lp/{ca})"
        )
        label = "SOL"
    else:
        links = f"[DexScreener](https://dexscreener.com/search?q={ca})"
        label = "EVM"
    src = f"📡 Source: {source_name}" if cfg.get("attribution", True) else ""
    tpl = cfg.get("template") or DEFAULT_TEMPLATE
    return tpl.format(ca=ca, chain=label, links=links, source=src).strip()


# ---------------------------------------------------------------- filter

def dialog_name(d):
    ent = d.entity
    title = getattr(ent, "title", None) or "?"
    uname = getattr(ent, "username", None)
    return f"{title} (@{uname})" if uname else f"{title} [{d.id}]"


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


async def resolve_targets(client, cfg):
    fil = cfg.get("send_filter", {})
    mode = fil.get("mode", "allowlist")
    allow = fil.get("allowlist", [])
    block = fil.get("blocklist", [])
    title_contains = fil.get("title_contains", [])
    include_channels = fil.get("include_channels", False)

    out = []
    for d in await client.get_dialogs():
        if d.is_user:
            continue
        is_broadcast = d.is_channel and not d.is_group
        if is_broadcast and not include_channels:
            continue
        if not (d.is_group or (is_broadcast and include_channels)):
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
        self.paused = False
        self.counters = {"relayed": 0, "dup_skips": 0, "sends_ok": 0, "sends_fail": 0}

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
        self.targets = await resolve_targets(self.client, self.cfg)


FLEET = []  # list[Userbot]


def find_bots(selector):
    """selector: bot name or 'all' -> list[Userbot]"""
    if selector == "all":
        return list(FLEET)
    return [b for b in FLEET if b.name == selector]


# ---------------------------------------------------------------- sender loop

async def sender_loop(bot: Userbot):
    while True:
        ca, chain, source = await bot.queue.get()
        cfg = bot.cfg
        dry = cfg.get("send_filter", {}).get("dry_run", False)
        delay = cfg.get("delay_between_groups_sec", 5)
        text = build_message(cfg, ca, chain, source)
        for d in bot.targets:
            if bot.paused:
                log.info(f"[{bot.name}] paused — dropping remainder of {ca[:10]}…")
                break
            if dry:
                log.info(f"[{bot.name}][DRY] would send {ca[:10]}… -> {dialog_name(d)}")
                continue
            try:
                await bot.client.send_message(d.entity, text, parse_mode="md", link_preview=False)
                bot.counters["sends_ok"] += 1
                log.info(f"[{bot.name}] sent {ca[:10]}… -> {dialog_name(d)}")
            except FloodWaitError as e:
                log.warning(f"[{bot.name}] floodwait {e.seconds}s")
                await asyncio.sleep(e.seconds + 2)
                try:
                    await bot.client.send_message(d.entity, text, parse_mode="md", link_preview=False)
                    bot.counters["sends_ok"] += 1
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


# ---------------------------------------------------------------- control bot

HELP = (
    "**CALLRELAY control**\n\n"
    "`/status` — all userbots overview\n"
    "`/pause <bot|all>` · `/resume <bot|all>`\n"
    "`/dryrun <bot|all> <on|off>`\n"
    "`/delay <bot> <sec>`\n"
    "`/sources <bot>` · `/addsource <bot> <@ch>` · `/delsource <bot> <@ch>`\n"
    "`/mode <bot> <allowlist|blocklist|all>`\n"
    "`/allow <bot> <@grp|id|substr>` · `/unallow <bot> <entry>`\n"
    "`/groups <bot>` — resolved target groups\n"
    "`/reload <bot|all>` — re-resolve sources+groups (after join/leave)\n"
    "`/stats <bot|all>`\n"
)


def register_control(control):
    @control.on(events.NewMessage(pattern=r"^/"))
    async def on_cmd(event):
        if event.sender_id not in ADMIN_IDS:
            return  # silently ignore non-admins
        parts = (event.raw_text or "").split()
        cmd = parts[0].lower().lstrip("/")
        args = parts[1:]

        async def reply(msg):
            await event.reply(msg, parse_mode="md", link_preview=False)

        try:
            if cmd in ("help", "start"):
                await reply(HELP)

            elif cmd == "status":
                lines = ["**Fleet status**"]
                for b in FLEET:
                    dry = b.cfg.get("send_filter", {}).get("dry_run", False)
                    state = "⏸ paused" if b.paused else ("🧪 dry" if dry else "▶️ live")
                    lines.append(
                        f"• `{b.name}` {state} — {len(b.source_ids)} src → "
                        f"{len(b.targets)} grp | relayed {b.counters['relayed']} "
                        f"ok {b.counters['sends_ok']} fail {b.counters['sends_fail']}"
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

            elif cmd == "addsource":
                if len(args) < 2:
                    return await reply("usage: `/addsource <bot> <@channel>`")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                b = bots[0]
                ch = args[1]
                b.cfg.setdefault("source_channels", [])
                if ch not in b.cfg["source_channels"]:
                    b.cfg["source_channels"].append(ch)
                await b.refresh_sources()
                save_fleet()
                await reply(f"added source `{ch}` to `{b.name}` ({len(b.source_ids)} active)")

            elif cmd == "delsource":
                if len(args) < 2:
                    return await reply("usage: `/delsource <bot> <@channel>`")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                b = bots[0]
                ch = args[1]
                b.cfg["source_channels"] = [x for x in b.cfg.get("source_channels", []) if str(x) != ch]
                await b.refresh_sources()
                save_fleet()
                await reply(f"removed `{ch}` from `{b.name}` ({len(b.source_ids)} active)")

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
                    return await reply(f"usage: `/{cmd} <bot> <@grp|id|substr>`")
                bots = find_bots(args[0])
                if not bots:
                    return await reply("no such bot")
                b = bots[0]
                entry = " ".join(args[1:])
                try:
                    entry_val = int(entry) if entry.lstrip("-").isdigit() else entry
                except Exception:
                    entry_val = entry
                fil = b.cfg.setdefault("send_filter", {})
                al = fil.setdefault("allowlist", [])
                if cmd == "allow":
                    if entry_val not in al:
                        al.append(entry_val)
                else:
                    fil["allowlist"] = [x for x in al if str(x) != str(entry_val)]
                await b.refresh_targets()
                save_fleet()
                await reply(f"`{b.name}` allowlist updated → {len(b.targets)} groups match")

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
    # start each userbot
    for cfg in USERBOT_CFGS:
        b = Userbot(cfg)
        b.client = TelegramClient(str(BASE / cfg["session"]), API_ID, API_HASH)
        await b.client.start()  # interactive first run per session
        me = await b.client.get_me()
        log.info(f"[{b.name}] logged in as {me.first_name} (@{me.username})")
        await b.refresh_sources()
        await b.refresh_targets()
        log.info(f"[{b.name}] {len(b.source_ids)} sources -> {len(b.targets)} groups "
                 f"(mode={cfg.get('send_filter',{}).get('mode')} "
                 f"dry={cfg.get('send_filter',{}).get('dry_run')})")

        def make_handler(bot):
            async def handler(event):
                if utils.get_peer_id(await event.get_chat()) not in bot.source_ids:
                    return
                try:
                    src = bot.source_names.get(utils.get_peer_id(await event.get_chat()), "unknown")
                    for ca, chain in extract_cas(get_all_text(event.message), bot.cfg.get("chains", ["sol", "evm"])):
                        if already_posted(bot.name, ca, bot.cfg.get("dedup_hours", 0)):
                            bot.counters["dup_skips"] += 1
                            continue
                        mark_posted(bot.name, ca, chain, src)
                        bot.counters["relayed"] += 1
                        log.info(f"[{bot.name}] NEW {chain.upper()} {ca} from {src} -> queue")
                        await bot.queue.put((ca, chain, src))
                except Exception as e:
                    log.error(f"[{bot.name}] handler error: {e}")
            return handler

        b.client.add_event_handler(make_handler(b), events.NewMessage())
        asyncio.get_event_loop().create_task(sender_loop(b))
        FLEET.append(b)

    # start control bot
    control = None
    if CONTROL_BOT_TOKEN:
        control = await TelegramClient(str(BASE / "control_bot"), API_ID, API_HASH).start(bot_token=CONTROL_BOT_TOKEN)
        register_control(control)
        me = await control.get_me()
        log.info(f"control bot live: @{me.username} (admins: {sorted(ADMIN_IDS)})")
    else:
        log.warning("No CONTROL_BOT_TOKEN — running without control bot")

    log.info(f"CALLRELAY MANAGER up — {len(FLEET)} userbots")
    clients = [b.client for b in FLEET] + ([control] if control else [])
    await asyncio.gather(*[c.run_until_disconnected() for c in clients])


if __name__ == "__main__":
    if "--list-groups" in sys.argv:
        idx = sys.argv.index("--list-groups")
        session = sys.argv[idx + 1] if len(sys.argv) > idx + 1 else USERBOT_CFGS[0]["session"]
        asyncio.run(list_groups(session))
    else:
        asyncio.run(main())
