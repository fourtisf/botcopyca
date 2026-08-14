#!/usr/bin/env python3
"""
CALLRELAY (userbot edition) — realtime CA relay for Telegram
============================================================
One userbot does BOTH jobs:
  1. listens to source call channels (read-only)
  2. reposts extracted CAs to a FILTERED set of groups it has joined

Because the userbot now sends, ban-risk applies -> use an ALT / burner
account, never the main brand account.

Modes:
  python3 callrelay.py                 # run the relay
  python3 callrelay.py --list-groups   # print every joined group + id (build allowlist)

Group selection is controlled by config.json -> "send_filter".
Default mode is "allowlist" (opt-in). "dry_run": true previews without sending.
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
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, ChatWriteForbiddenError

# ---------------------------------------------------------------- setup

BASE = Path(__file__).parent
load_dotenv(BASE / ".env")

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

CONFIG_PATH = BASE / "config.json"
DB_PATH = BASE / "callrelay.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("callrelay")

if not API_ID or not API_HASH:
    raise SystemExit("Missing API_ID / API_HASH in .env — see HANDOFF.md")
if not CONFIG_PATH.exists():
    raise SystemExit("config.json not found — see HANDOFF.md")

with open(CONFIG_PATH) as f:
    CFG = json.load(f)

SOURCES = CFG.get("source_channels", [])
CHAINS = CFG.get("chains", ["sol", "evm"])
DEDUP_HOURS = CFG.get("dedup_hours", 0)              # 0 = never repost same CA
SEND_DELAY = CFG.get("delay_between_groups_sec", 5)  # userbot -> keep this slow
ATTRIBUTION = CFG.get("attribution", True)

FILTER = CFG.get("send_filter", {})
FILTER_MODE = FILTER.get("mode", "allowlist")            # allowlist | blocklist | all
ALLOWLIST = FILTER.get("allowlist", [])
BLOCKLIST = FILTER.get("blocklist", [])
TITLE_CONTAINS = FILTER.get("title_contains", [])        # only groups whose title has one of these
INCLUDE_CHANNELS = FILTER.get("include_channels", False) # include broadcast channels you admin
DRY_RUN = FILTER.get("dry_run", False)

# ---------------------------------------------------------------- dedup db

db = sqlite3.connect(DB_PATH)
db.execute(
    """CREATE TABLE IF NOT EXISTS posted (
        ca TEXT PRIMARY KEY, chain TEXT, source TEXT, first_seen INTEGER
    )"""
)
db.commit()


def already_posted(ca: str) -> bool:
    row = db.execute("SELECT first_seen FROM posted WHERE ca = ?", (ca,)).fetchone()
    if row is None:
        return False
    if DEDUP_HOURS <= 0:
        return True
    return (time.time() - row[0]) < DEDUP_HOURS * 3600


def mark_posted(ca: str, chain: str, source: str) -> None:
    db.execute(
        "INSERT OR REPLACE INTO posted (ca, chain, source, first_seen) VALUES (?, ?, ?, ?)",
        (ca, chain, source, int(time.time())),
    )
    db.commit()


# ---------------------------------------------------------------- CA extraction

EVM_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
SOL_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")


def is_valid_sol(ca: str) -> bool:
    try:
        return len(base58.b58decode(ca)) == 32
    except Exception:
        return False


def get_all_text(msg) -> str:
    """Message text + hidden hyperlink URLs + inline button URLs."""
    chunks = [msg.raw_text or ""]
    try:
        for e in (msg.entities or []):
            url = getattr(e, "url", None)
            if url:
                chunks.append(url)
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


def extract_cas(text: str):
    found = []
    if not text:
        return found
    if "evm" in CHAINS:
        for m in EVM_RE.findall(text):
            found.append((m, "evm"))
    if "sol" in CHAINS:
        for m in SOL_RE.findall(text):
            if is_valid_sol(m):
                found.append((m, "sol"))
    seen, out = set(), []
    for ca, chain in found:
        if ca not in seen:
            seen.add(ca)
            out.append((ca, chain))
    return out


# ---------------------------------------------------------------- message format
# NOTE: Telethon markdown uses **bold** (double asterisk) and `code`.

DEFAULT_TEMPLATE = (
    "🚨 **NEW CALL** — {chain}\n"
    "\n"
    "`{ca}`\n"
    "\n"
    "{links}\n"
    "\n"
    "{source}\n"
    "\n"
    "🔍 DYOR | NFA"
)


def build_message(ca: str, chain: str, source_name: str) -> str:
    if chain == "sol":
        links = (
            f"[GMGN](https://gmgn.ai/sol/token/{ca}) | "
            f"[DexScreener](https://dexscreener.com/solana/{ca}) | "
            f"[Photon](https://photon-sol.tinyastro.io/en/lp/{ca})"
        )
        chain_label = "SOL"
    else:
        links = f"[DexScreener](https://dexscreener.com/search?q={ca})"
        chain_label = "EVM"
    src = f"📡 Source: {source_name}" if ATTRIBUTION else ""
    tpl = CFG.get("template") or DEFAULT_TEMPLATE
    return tpl.format(ca=ca, chain=chain_label, links=links, source=src).strip()


# ---------------------------------------------------------------- group filter

def dialog_name(d) -> str:
    ent = d.entity
    title = getattr(ent, "title", None) or "?"
    uname = getattr(ent, "username", None)
    return f"{title} (@{uname})" if uname else f"{title} [{d.id}]"


def dialog_matches(d, entry) -> bool:
    """entry can be a numeric id, '@username', or a title substring."""
    ent = d.entity
    if isinstance(entry, int):
        return d.id == entry or getattr(ent, "id", None) == entry
    s = str(entry).strip()
    if s.startswith("@"):
        uname = getattr(ent, "username", None)
        return uname is not None and uname.lower() == s[1:].lower()
    if s.lstrip("-").isdigit():
        return d.id == int(s)
    title = getattr(ent, "title", "") or ""
    return s.lower() in title.lower()


async def resolve_targets(client):
    """Apply send_filter to the userbot's joined dialogs."""
    dialogs = await client.get_dialogs()
    targets = []
    for d in dialogs:
        if d.is_user:
            continue
        is_broadcast = d.is_channel and not d.is_group
        if is_broadcast and not INCLUDE_CHANNELS:
            continue
        if not (d.is_group or (is_broadcast and INCLUDE_CHANNELS)):
            continue

        if TITLE_CONTAINS:
            title = getattr(d.entity, "title", "") or ""
            if not any(k.lower() in title.lower() for k in TITLE_CONTAINS):
                continue

        if FILTER_MODE == "allowlist":
            if not any(dialog_matches(d, e) for e in ALLOWLIST):
                continue
        elif FILTER_MODE == "blocklist":
            if any(dialog_matches(d, e) for e in BLOCKLIST):
                continue
        elif FILTER_MODE == "all":
            pass
        else:
            raise SystemExit(f"unknown send_filter.mode: {FILTER_MODE}")

        targets.append(d)
    return targets


# ---------------------------------------------------------------- sender

queue: asyncio.Queue = asyncio.Queue()
inflight: set = set()   # CAs queued but not yet committed to the dedup db


async def sender_loop(client, targets):
    """Fan one CA out to every target group.

    The CA is only committed to the dedup db once it actually reached at least
    one group — a failed fanout leaves it un-marked so the next sighting is
    relayed instead of silently swallowed. Dry runs never touch the db.
    """
    while True:
        ca, chain, source = await queue.get()
        try:
            text = build_message(ca, chain, source)
            delivered = 0
            for d in targets:
                if DRY_RUN:
                    log.info(f"[DRY] would send {ca[:10]}… -> {dialog_name(d)}")
                    continue
                try:
                    await client.send_message(d.entity, text, parse_mode="md", link_preview=False)
                    delivered += 1
                    log.info(f"sent {ca[:10]}… -> {dialog_name(d)}")
                except FloodWaitError as e:
                    log.warning(f"floodwait {e.seconds}s (Telegram throttling the account)")
                    await asyncio.sleep(e.seconds + 2)
                    try:
                        await client.send_message(d.entity, text, parse_mode="md", link_preview=False)
                        delivered += 1
                        log.info(f"sent (retry) {ca[:10]}… -> {dialog_name(d)}")
                    except Exception as e2:
                        log.error(f"retry failed {dialog_name(d)}: {e2}")
                except ChatWriteForbiddenError:
                    log.error(f"no write permission in {dialog_name(d)} — skipping")
                except Exception as e:
                    log.error(f"send failed {dialog_name(d)}: {e}")
                await asyncio.sleep(SEND_DELAY)

            if DRY_RUN:
                log.info(f"[DRY] {ca[:10]}… previewed — dedup db untouched")
            elif delivered:
                mark_posted(ca, chain, source)
            else:
                log.warning(f"{ca[:10]}… reached 0 groups — not recorded, "
                            f"will relay again next time it shows up")
        except Exception as e:
            log.error(f"sender error on {ca[:10]}…: {e}")
        finally:
            inflight.discard(ca)
            queue.task_done()


# ---------------------------------------------------------------- --list-groups

async def list_groups():
    client = TelegramClient(str(BASE / "listener"), API_ID, API_HASH)
    await client.start()
    dialogs = await client.get_dialogs()
    print("\n=== JOINED GROUPS / CHANNELS ===")
    print(f"{'id':>16}  {'type':<11} {'username':<22} title")
    print("-" * 80)
    for d in dialogs:
        if d.is_user:
            continue
        if d.is_group:
            kind = "group"
        elif d.is_channel:
            kind = "channel"
        else:
            kind = "other"
        uname = getattr(d.entity, "username", None)
        uname = f"@{uname}" if uname else "-"
        title = getattr(d.entity, "title", "") or ""
        print(f"{d.id:>16}  {kind:<11} {uname:<22} {title}")
    print("\nCopy the id (or @username) of the groups you want into config.json -> send_filter.allowlist\n")
    await client.disconnect()


# ---------------------------------------------------------------- main relay

async def main():
    client = TelegramClient(str(BASE / "listener"), API_ID, API_HASH)
    await client.start()  # interactive first run: phone + login code
    me = await client.get_me()
    log.info(f"userbot logged in as: {me.first_name} (@{me.username})")

    resolved_sources = []
    for s in SOURCES:
        try:
            ent = await client.get_entity(s)
            resolved_sources.append(ent)
            log.info(f"listening source: {getattr(ent, 'title', s)}")
        except Exception as e:
            log.error(f"cannot resolve source {s}: {e} — join it with this account first")
    if not resolved_sources:
        raise SystemExit("No source channels resolved. Join them first, then restart.")

    targets = await resolve_targets(client)
    log.info(f"send_filter mode={FILTER_MODE} dry_run={DRY_RUN} -> {len(targets)} target groups:")
    for d in targets:
        log.info(f"   -> {dialog_name(d)}")
    if not targets:
        log.warning("No target groups matched the filter. Nothing will be sent. "
                    "Run: python3 callrelay.py --list-groups")

    @client.on(events.NewMessage(chats=resolved_sources))
    async def handler(event):
        try:
            chat = await event.get_chat()
            source_name = getattr(chat, "title", None) or getattr(chat, "username", "unknown")
            for ca, chain in extract_cas(get_all_text(event.message)):
                # inflight = queued by an earlier message, not committed yet
                if ca in inflight or already_posted(ca):
                    log.info(f"dup skip {ca[:10]}… (from {source_name})")
                    continue
                inflight.add(ca)
                log.info(f"NEW {chain.upper()} CA {ca} from {source_name} -> queue")
                await queue.put((ca, chain, source_name))
        except Exception as e:
            log.error(f"handler error: {e}")

    asyncio.create_task(sender_loop(client, targets))
    log.info(f"CALLRELAY running — {len(resolved_sources)} sources -> {len(targets)} groups"
             + ("  [DRY RUN]" if DRY_RUN else ""))
    await client.run_until_disconnected()


if __name__ == "__main__":
    if "--list-groups" in sys.argv:
        asyncio.run(list_groups())
    else:
        asyncio.run(main())
