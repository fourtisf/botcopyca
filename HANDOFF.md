# CALLRELAY MANAGER (fleet edition) — HANDOFF (untuk Michael)

Satu proses jalanin **banyak userbot** sekaligus + **satu control bot** buat ngatur semuanya live via chat (pause, tambah source, edit allowlist, stats) tanpa restart. Perubahan disimpen ke `fleet.json`.

Deliverable utama: **`manager.py`**. (File `callrelay.py` versi single-instance masih ada di zip buat referensi, tapi deploy pakai `manager.py` — fleet berisi 1 userbot = sama persis kayak single mode.)

> ⚠️ Userbot SEND → **tiap userbot pakai akun ALT/burner, jangan akun utama ALFA.** Allowlist isi **group sendiri** aja. Multi-userbot BUKAN buat blast lebih banyak group ngelewatin limit — itu bikin cluster akun ke-ban barengan.

## Arsitektur

```
                         fleet.json  <---- di-save otomatis tiap ada perubahan
                             |
  ┌──────────── manager.py (1 proses) ────────────┐
  │  userbot ub1 ─ listen src → extract → dedup → send → [groups ub1]   │
  │  userbot ub2 ─ listen src → extract → dedup → send → [groups ub2]   │
  │  control bot ─ chat commands (admin-gated) → mutate live state       │
  └────────────────────────────────────────────────┘
```

Semua userbot **share satu `api_id`/`api_hash`** (api_id itu per-app, bukan per-account). Tiap userbot cuma beda **session** (login HP sendiri). Dedup per-userbot (bot beda audiens = boleh dapet CA sama).

## Setup

1. **API creds:** https://my.telegram.org → `api_id` + `api_hash` (sekali, dipakai semua).
2. **Control bot:** @BotFather `/newbot` → token.
3. **Admin user ID lo:** chat @userinfobot → catat ID numerik → masuk `fleet.json -> admin_user_ids`.
4. Tiap akun userbot: **join semua source channel + join semua group tujuan**-nya.
5. Di VPS:
   ```bash
   cd /opt && unzip callrelay.zip && cd callrelay
   pip3 install -r requirements.txt
   cp .env.example .env && nano .env       # API_ID, API_HASH, CONTROL_BOT_TOKEN
   nano fleet.json                          # admin id + tiap userbot: session, sources, allowlist
   ```
6. **Lihat group tiap userbot** buat isi allowlist:
   ```bash
   python3 manager.py --list-groups ub1
   python3 manager.py --list-groups ub2
   ```
7. **First run interaktif** (login tiap session sekali — bakal minta HP+OTP per akun berurutan):
   ```bash
   python3 manager.py
   # login ub1, ub2, ... lalu control bot connect
   # cek log: tiap userbot nunjukin jumlah target group; biarin dry_run true dulu
   ```
   Chat control bot lo → kirim `/status`. Kalau target udah bener, matiin dry-run per bot (`/dryrun ub1 off`) atau Ctrl+C & edit fleet.json.
8. **Deploy:**
   ```bash
   pm2 start manager.py --name callrelay --interpreter python3
   pm2 save && pm2 logs callrelay
   ```

## Control bot commands (chat langsung ke bot)

### Tambah akun (nomor HP) — tanpa restart

| Command | Fungsi |
|---|---|
| `/addnumber <name> <+62...>` | Bikin userbot baru + kirim OTP ke nomor itu |
| `/code <name> <kode>` | Masukin OTP. Angka doang yang dibaca, jadi boleh dipisah spasi |
| `/pass <name> <password>` | Kalau akunnya pakai 2FA |
| `/cancel <name>` | Batalin login yang lagi nunggu |
| `/delbot <name>` | Copot userbot dari fleet (file session tetep di disk) |

Contoh:
```
/addnumber ub3 +628123456789
   → "kode dikirim, balas /code ub3 <kode>"
/code ub3 1 2 3 4 5
   → ✅ login, langsung masuk fleet dalam keadaan dry-run + 0 target
```

> ⚠️ **Telegram nge-invalidate kode login yang ditulis mentahan di chat Telegram.** Makanya `/code` nerima angka yang dipisah (`1 2 3 4 5`, `12-345`) — non-digit dibuang otomatis. Habis login, **hapus pesan kodenya**. Kalau kodenya kadung mati, ulang `/addnumber`.

Userbot baru selalu lahir **aman**: `dry_run: true`, 0 source, allowlist kosong — jadi nggak bakal ngirim apa-apa sebelum lo isi sendiri.

### Pilih source channel & target group (bernomor)

| Command | Fungsi |
|---|---|
| `/listchannels <bot> [keyword]` | Channel yang di-join akun itu, bernomor. `keyword` nyaring by judul/@username. ✅ = udah jadi source |
| `/addsource <bot> <#1,3>` `/delsource <bot> <#1,3>` | Pilih/buang source pakai nomor dari listing barusan (`@channel` tetep bisa) |
| `/listgroups <bot> [keyword]` | Group yang di-join, bernomor. ✅ = udah jadi target |
| `/allow <bot> <#1,3>` `/unallow <bot> <#1,3>` | Pilih/buang target group pakai nomor (`@grp`/id/substring tetep bisa) |
| `/titlefilter <bot> <kata\|clear>` | Saring target: cuma group yang judulnya ngandung kata itu |
| `/groups <bot>` | Target yang kepilih sekarang (hasil akhir semua filter) |

Alur normalnya:
```
/listchannels ub3 call     →  daftar channel yang judulnya ada "call"
/addsource ub3 #1,2,5      →  jadiin source
/listgroups ub3            →  daftar group
/allow ub3 #1,3            →  jadiin target
/groups ub3                →  cek hasil akhir
/dryrun ub3 off            →  baru live
```
Nomor `#n` ngikutin listing **terakhir** buat bot itu. Habis join/leave group baru, jalanin listing-nya lagi biar nomornya fresh.

### Operasi harian

| Command | Fungsi |
|---|---|
| `/status` | Overview semua userbot: state (live/dry/paused), jumlah source→group, counter |
| `/pause <bot\|all>` `/resume <bot\|all>` | Stop/lanjut kirim (tetep listen, cuma nahan send) |
| `/dryrun <bot\|all> <on\|off>` | Toggle preview-only |
| `/delay <bot> <sec>` | Ubah jeda antar group |
| `/sources <bot>` | List source channel yang aktif |
| `/mode <bot> <allowlist\|blocklist\|all>` | Ganti mode filter group |
| `/reload <bot\|all>` | Re-resolve source+group (habis join/leave group baru) |
| `/stats <bot\|all>` | Counter: relayed / dup skip / send ok / fail |

Semua perubahan lewat command langsung ke-save ke `fleet.json`. Non-admin yang chat bot → di-ignore diam-diam.

Karena akun bisa ditambah lewat chat, `fleet.json` boleh mulai dengan `"userbots": []` — asal `CONTROL_BOT_TOKEN` keisi, prosesnya tetep jalan dan lo tinggal `/addnumber`.

## fleet.json

```
admin_user_ids : [id numerik lo]   <- cuma ID ini yang bisa nyetir control bot
userbots[]     : tiap userbot:
  name                        : label unik (dipake di command)
  session                     : nama file session (login akun ini)
  source_channels             : channel di-monitor
  chains / dedup_hours / delay_between_groups_sec / attribution / template
  send_filter                 : mode / allowlist / blocklist / title_contains / include_channels / dry_run
```

send_filter sama persis kayak versi sebelumnya (allowlist default, entry bisa id / @username / substring judul, dry_run buat preview).

## Rate & safety

- Delay default 5s/group per userbot. Jangan diturunin — userbot gampang kena flood.
- Kode handle `FloodWaitError` (auto-sleep+retry) & `ChatWriteForbiddenError` (skip group tanpa izin).
- Kalau total group per userbot banyak (>15), naikin delay atau pecah ke userbot lain.
- `/pause all` = kill switch cepet kalau ada yang aneh.

## Known edges (v1)

- Target group di-resolve pas start / `/reload` / edit allowlist. Join group baru pas jalan → `/reload <bot>`.
- Semua userbot + control bot 1 proses. Proses mati = semua mati (PM2 auto-restart). Mau isolasi per-userbot → Redis pub/sub (v2).
- Counter lifetime (reset pas restart), belum per-hari.
- Dedup ditulis **setelah** CA kekirim minimal ke 1 group. Kalau semua send gagal / lagi `/pause`, CA-nya nggak dicatet → bakal di-relay lagi pas muncul berikutnya. Dry-run cuma preview, nggak nyentuh dedup db (jadi habis `/dryrun off`, CA yang tadi ke-preview masih bisa kekirim beneran).
- `/allow` pas bot lagi mode `blocklist`/`all` → allowlist tetep ke-save tapi belum ngefek; control bot bakal ngewanti-wanti + kasih perintah `/mode <bot> allowlist`.
- Edited message di-skip. Wallet-vs-CA belum dibedain (butuh DexScreener check, v2).

## v2

1. **MC/liq filter** via DexScreener sebelum post (+ solve wallet-vs-CA).
2. **WR tracker per source** — auto-drop channel jelek.
3. Counter harian + `/top` (source dengan WR/volume tertinggi).
4. `/broadcast <bot> <text>` manual push, `/mute <source>` per-channel.
