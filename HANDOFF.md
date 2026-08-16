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
3. **Admin:** nggak usah diisi manual — orang **pertama** yang chat control bot otomatis jadi admin (kesimpen ke `fleet.json`). Mau manual? chat @userinfobot → ID numerik → `fleet.json -> admin_user_ids`.
4. Tiap akun userbot: **join semua source channel + join semua group tujuan**-nya.
5. Di VPS:
   ```bash
   cd /opt && unzip callrelay.zip && cd callrelay
   pip3 install -r requirements.txt
   cp .env.example .env && nano .env       # API_ID, API_HASH, CONTROL_BOT_TOKEN
   # fleet.json dibikin otomatis pas pertama jalan — nggak usah disiapin.
   # Mau isi manual: cp fleet.example.json fleet.json && nano fleet.json
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

## Dua cara jalan: bot mode vs userbot

| | **Bot mode** (token doang) | **Userbot** (butuh api_id) |
|---|---|---|
| Butuh apa | `CONTROL_BOT_TOKEN` | `API_ID` + `API_HASH` + login nomor HP |
| Bisa baca | cuma chat yang **bot-nya di-add** | channel mana pun yang **akunnya join** |
| Nyedot channel orang | ❌ nggak bisa | ✅ bisa |
| Risiko ban akun | nggak ada | ada (makanya pakai akun alt) |
| Setup | add bot ke chat, kelar | my.telegram.org + OTP |

Kalau `.env` belum ada api_id valid, manager otomatis jalan **bot mode**: control bot nyala dan satu relay bernama `bot` kedaftar di fleet. Semua fitur (dedup, batch, cap, quiet, filter, template, counter) jalan sama persis — bedanya cuma sumber datanya.

### Setup bot mode (3 menit, tanpa api_id)

1. Add bot lo ke **channel sumber**. Kalau itu channel (bukan group), jadiin **admin** — bot nggak dapet pesan channel kalau bukan admin.
2. Add juga ke **group/channel tujuan**, pastiin punya hak kirim.
3. Chat bot lo:
   ```
   /chats                      → daftar chat yang kebaca
   /listchannels bot           → pilih sumber
   /addsource bot #1
   /listgroups bot             → pilih tujuan
   /allow bot #1,2
   /groups bot                 → cek
   /dryrun bot off             → live
   ```

Bot belajar chat sendiri begitu ada pesan masuk di situ. Kalau males nunggu, ketik `/here` **di dalam** chat itu — langsung kecatat.

Source channel otomatis dikecualiin dari daftar target, jadi nggak mungkin echo.

## Control bot commands (chat langsung ke bot)

### Tambah akun (nomor HP) — tanpa restart

| Command | Fungsi |
|---|---|
| `/addnumber <name> <+62...>` | Bikin userbot baru + kirim OTP ke nomor itu |
| `/code <name> <kode>` | Masukin OTP. Angka doang yang dibaca, jadi boleh dipisah spasi |
| `/pass <name> <password>` | Kalau akunnya pakai 2FA. Nggak usah diketik manual — begitu bot minta 2FA, **pesan berikutnya apa pun yang lo kirim dianggap passwordnya** |
| `/reset` | Hapus SEMUA: userbot, source, target, login yang lagi nunggu. Nanya konfirmasi dulu. Admin & file session nggak ikut kehapus |
| `/cancel <name>` | Batalin login yang lagi nunggu |
| `/delbot` | Daftar akun buat dihapus (tombol) |
| `/delbot <name>` | Copot userbot dari fleet — nanya konfirmasi dulu; file session tetep di disk |

Contoh:
```
/addnumber ub3 +628123456789
   → "kode dikirim, balas /code ub3 <kode>"
/code ub3 1 2 3 4 5
   → ✅ login, langsung masuk fleet dalam keadaan dry-run + 0 target
```

> ⚠️ **Telegram nge-invalidate kode login yang ditulis mentahan di chat Telegram.** Makanya `/code` nerima angka yang dipisah (`1 2 3 4 5`, `12-345`) — non-digit dibuang otomatis. Habis login, **hapus pesan kodenya**. Kalau kodenya kadung mati, ulang `/addnumber`.

**File session dinamain dari nomornya** (`session_acc_628…`), bukan dari label. Ini penting: label kayak `ub1` bisa dipakai ulang setelah `/delbot`, dan kalau session ikut label, akun lama bakal kepakai lagi walau lo masukin nomor yang beda. Efek sampingnya enak — nambah nomor yang **udah pernah** login di server ini langsung konek tanpa OTP (dan balasannya bilang begitu).

Pengaman lain di jalur login:
- nomor yang lagi diproses dikunci, jadi ngirim nomor dua kali nggak buka dua client ke file session yang sama (dulu bikin `database is locked`)
- nomor yang udah kepasang ditolak, biar nggak ada dua userbot dengan akun sama
- kalau file session isinya akun lain, login dibatalin dan bot nyebutin akun siapa yang ada di situ

Userbot baru selalu lahir **aman**: `dry_run: true`, 0 source, allowlist kosong — jadi nggak bakal ngirim apa-apa sebelum lo isi sendiri.

**Label userbot ngikut akunnya**, bukan urutan: login pakai akun @Jeffryyoung jadi `Jeffryyoung`, akun tanpa username jadi `akun1118` (4 digit terakhir nomor). Userbot lama yang masih bernama `ub1`/`ub2` ikut diganti otomatis pas proses start — nggak perlu login ulang, dan label yang lo tentuin sendiri nggak diutak-atik. Jadi nggak ada lagi tebak-tebakan `ub1` itu akun siapa. Nama manual lewat `/addnumber <nama> <nomor>` tetep dihormati sampai login kelar.

`/bots` nampilin tiap akun lengkap: label (`ub1`), **@username**, **nomor HP**, status, jumlah source & target. Identitas itu dibaca dari `get_me()` tiap kali proses start dan disimpen di `fleet.json -> userbots[].account`, jadi akun lama pun kebaca tanpa login ulang.

### Pilih source channel & target group (bernomor)

| Command | Fungsi |
|---|---|
| `/listchannels <bot> [keyword]` | Channel yang di-join akun itu, bernomor. `keyword` nyaring by judul/@username. ✅ = udah jadi source |
| `/addsource <bot> <#1,3>` `/delsource <bot> <#1,3>` | Pilih/buang source pakai nomor dari listing barusan |
| `/addsource <bot> @ch1 @ch2` | Tambah source langsung pakai @username atau link `t.me/...` — boleh beberapa sekaligus. Kalau cuma ada 1 userbot, cukup kirim `@namachannel` doang tanpa command. Username di-resolve dulu; kalau akunnya belum join, ditolak dengan alasan yang jelas |
| `/clearsource <bot>` | Hapus **semua** source sekaligus, termasuk entry yang udah nggak bisa di-resolve. Tombolnya ada di picker source & `/sources` |
| `/target <bot> <group\|channel\|both>` | Jenis chat yang boleh jadi target. Default `group`. Buat channel, akun userbot harus **admin dengan hak post** |
| `/listgroups <bot> [keyword]` | Target yang di-join, bernomor — isinya ngikutin `/target`. ✅ = udah jadi target |
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

**Source channel nggak akan pernah jadi target.** Dia otomatis kefilter dari `/listgroups` dan dari daftar target — biar CA yang lo relay nggak masuk balik ke channel yang lagi lo pantau (echo).

### Join channel/group

Userbot **cuma bisa baca dan kirim di chat yang dia udah join** — itu batasan Telegram, bukan batasan bot ini. Buat channel tujuan, akunnya malah harus **admin dengan hak Post Messages**.

| Command | Fungsi |
|---|---|
**Join-nya otomatis.** `/addsource <bot> @nama` dan `/allow <bot> @nama` ngecek dulu akunnya udah member apa belum; kalau belum, dia join sendiri baru dipasang. Ini penting buat channel publik: `get_entity` tetep sukses walau belum join, tapi pesannya nggak akan pernah masuk — dulu ini gagal diem-diem. Ada jeda 1 detik antar-join biar nggak kena limit, dan yang udah join nggak dijoinin ulang.

Kalau join-nya ditolak (channel privat tanpa invite, kena limit), alasannya disebutin di balasan.

`/join <bot> @ch1 @ch2` masih ada buat join manual duluan, tapi nggak wajib — nggak ditaro di menu command biar nggak numpuk.

### Auto kirim CA

| Command | Fungsi |
|---|---|
| `/auto <bot>` | Panel auto-kirim: status hidup/mati, jumlah source & target, jeda, plus tombol pilih channel sumber & group tujuan. Kalau belum jalan, dia nyebutin apa yang kurang |
| `/auto <bot> on` `off` | Nyalain / matiin auto-kirim (sama dengan `/dryrun <bot> off|on`, tapi `on` juga ngelepas pause) |

Selama auto-kirim mati, CA tetep dibaca dan dicatat — cuma nggak dikirim. Jadi aman buat ngetes dulu.

**Tombolnya nulis ulang pesan yang sama.** Tiap tap ngedit pesan itu juga (`editMessageText`), bukan bikin pesan baru — jadi nggak ada pesan lama nyangkut dengan centang yang udah nggak berlaku. Kalau Telegram nolak ngedit (pesannya kelamaan), otomatis jatuh ke kirim pesan baru.

### Format pesan

Bawaannya **CA doang** — satu pesan = satu string CA, tanpa judul, link, nama source, atau DYOR. Gampang di-copy, gampang di-paste ke bot beli. Pas batch nyala, recap-nya juga CA doang, satu per baris.

| Command | Fungsi |
|---|---|
| `/template <bot> full` | Balik ke format rame: `🚨 NEW CALL — SOL` + link GMGN/DexScreener/Photon + nama source + DYOR |
| `/template <bot> reset` | Balik ke CA doang |
| `/template <bot> <teks>` | Bikin sendiri — placeholder `{ca}` `{chain}` `{links}` `{source}` |

### Saringan wallet (verifikasi token)

Alamat wallet dan alamat token **bentuknya identik** — EVM dua-duanya `0x` + 40 hex, Solana dua-duanya base58 32 byte. Jadi channel yang ngepost hasil wallet-tracker (`WR: 76% · PNL: +$39 · TXs: 156`) keliatan persis kayak call, dan alamat wallet-nya ikut kerelay.

Nggak ada cara mastiin dari bentuk alamatnya. Yang bisa: tanya apakah alamat itu punya pair yang diperdagangkan. Token punya, wallet nggak.

Tiap CA dicek ke `api.dexscreener.com/latest/dex/tokens/<ca>` sebelum masuk antrean:

- **ada pair** → lanjut dikirim
- **nggak ada pair** → dibuang, admin dikasih tau alamatnya + channel asalnya
- **API nggak kejangkau** (dicoba 2x) → **tetep dikirim** — mending kelewat sekali daripada kehilangan call beneran

Hasilnya di-cache 6 jam per alamat, jadi repost nggak nanya ulang. Alamat yang ditolak **nggak** dicatet di dedup db — kalau ternyata token baru yang belum kelisting, dia masih punya kesempatan lain kali.

| Command | Fungsi |
|---|---|
| `/verify all on` `off` `strict` | Nyalain/matiin saringan buat **semua** userbot sekaligus (tanpa nama bot = `all`, karena saringan wallet nggak masuk akal kalau cuma nyala di satu akun). Default **on**. Kesimpen di `fleet.json -> verify_token` tiap bot |

**`strict`** nutup satu-satunya lubang yang tersisa: kalau DexScreener nggak kejangkau, mode normal ngeloloskan alamatnya (fail-open), mode strict nahan. Paling aman, tapi pas API orang lagi down lo bisa kehilangan call beneran. Yang ditahan tetep dilaporin, jadi ketauan.

**Gerbangnya ada sebelum antrean, bukan sebelum kirim.** Artinya alamat yang ditolak nggak pernah nyampe sender sama sekali — otomatis berlaku ke **semua** group tujuan, semua batch, semua bot. Nggak ada jalur per-group yang bisa kelewat. Satu-satunya yang sengaja nggak lewat gerbang ini: `/testca` (emang buat ngetes) dan `/broadcast` (teks manual lo sendiri).

Kelemahannya jujur aja: token yang **baru banget** launching dan belum ada pair-nya di DexScreener bakal ikut kesaring. Kalau lo ngejar detik-detik pertama launch, matiin (`/verify <bot> off`) — konsekuensinya alamat wallet bisa lolos lagi.

### Laporan kirim

Tiap kali CA selesai difanout, admin dapet satu laporan — bukan satu per group, satu per pengiriman:

```
✅ Jeffryyoung — 2/3 group

So11111111111111111111111111111111111111112

✅ My VIP Group
✅ Trading Squad
❌ Second Group — nggak punya izin kirim
```

| Command | Fungsi |
|---|---|
| `/lapor on` `off` | Nyalain/matiin laporan. Kesimpen di `fleet.json -> report_sends`. Tombolnya ada di `/auto` |

Laporan juga muncul buat kondisi yang bikin CA nggak jadi dikirim: jam tenang, pause, atau belum ada group tujuan — jadi ketauan kenapa sepi. Dry-run nggak dilaporin (biar nggak berisik pas lagi nyetel), dan kalau ngirim laporannya gagal, jalur kirim CA tetep jalan.

### Anti-spam

| Command | Fungsi |
|---|---|
| `/batch <bot> <menit\|off>` | Kumpulin CA selama N menit → kirim **1 pesan recap**, bukan N pesan |
| `/cap <bot> <n\|off>` | Max pesan per group per hari. Lewat itu, group-nya dilewatin |
| `/quiet <bot> <23:00-07:00\|off>` | Jam tenang — nggak ngirim sama sekali di rentang itu |
| `/delay <bot> <sec>` | Jeda antar group (default 5s, jangan diturunin) |

**Kenapa `/batch` yang paling ngefek.** `delay` cuma ngatur kecepatan kirim per akun (buat ngehindarin floodwait). Yang bikin group lo kerasa spam itu **berapa sering group itu dikirimin**. Kalau source lagi rame, 10 call dalam 2 menit = 10 notif. Dengan `/batch ub1 10`, sepuluh-duanya jadi satu pesan:

```
📊 CALL RECAP — 4 CA / 10m

1. `7xKX...`
SOL · Alpha Calls — GMGN | DexScreener | Photon

2. `0x91a...`
EVM · Beta Signals — DexScreener
...
🔍 DYOR | NFA
```

Detailnya:
- Window mulai ngitung dari **CA pertama** yang masuk, bukan dari jam bulat.
- Max 15 CA per pesan. Kalau kepenuhan sebelum window abis, langsung dikirim.
- 1 CA doang → format `NEW CALL` biasa, bukan recap.
- `batch_window_sec: 0` (default) = perilaku lama, kirim satuan.

**Quiet hours** pakai jam server. Set dulu timezone VPS-nya:
```bash
timedatectl set-timezone Asia/Jakarta
```
CA yang masuk pas jam tenang **nggak dicatet di dedup db**, jadi kalau muncul lagi besoknya tetep kekirim.

**Cap harian** kehitung per group per hari, kesimpen di `callrelay.db` — jadi restart nggak nge-reset.

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

⚠️ **`fleet.json` itu state hidup punya server, bukan bagian repo** — dia ada di `.gitignore` bareng `.env`, `*.session`, dan `*.db`. Contohnya ada di `fleet.example.json`. Artinya `git pull` **nggak akan** nimpa config lo, dan jangan pernah `git stash` di folder deploy: itu bakal ngebalikin file yang ke-track ke versi repo. Kalau `git pull` nolak gara-gara ada perubahan lokal, cek dulu `git status` — bukan langsung di-stash.

Backup config: `cp fleet.json ~/fleet.backup.json` (isinya login mapping, source, target — bukan rahasia login, tapi sayang kalau ilang).

## fleet.json

```
admin_user_ids : [id numerik lo]   <- cuma ID ini yang bisa nyetir control bot
userbots[]     : tiap userbot:
  name                        : label unik (dipake di command)
  session                     : nama file session (login akun ini)
  source_channels             : channel di-monitor
  chains / dedup_hours / delay_between_groups_sec / attribution / template
  batch_window_sec            : 0 = off, >0 = kumpulin CA sekian detik jadi 1 recap
  max_per_day_per_group       : 0 = unlimited
  quiet_hours                 : null atau "23:00-07:00" (jam server)
  send_filter                 : mode / allowlist / blocklist / title_contains /
                                include_groups / include_channels / dry_run
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
