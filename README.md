# YouTube Live → IPTV Gateway

On-demand HTTP gateway that turns YouTube Live (or any HLS live) into an IPTV
stream for internal use. Nothing runs until someone actually watches: a client
request resolves the YouTube URL via `yt-dlp` and byte-for-byte proxies the HLS
— **no transcode, no remux, no FFmpeg, ~0 CPU** while idle or streaming.

Multiple clients share a single upstream connection (segments are fetched once
and cached per channel).

## Features

- ⚡ On-demand: zero processes when nobody is watching; auto-stop after 60s idle
- 🔗 Byte-for-byte HLS proxy — H.264/AAC passthrough, no decoding/re-encoding
- 👥 Multi-client: N viewers = 1 upstream fetch, shared segment cache (64MB)
- 🔄 Handles YouTube live URL rotation (~30s) with proactive warm re-resolve +
  403 auto-invalidation — no mid-stream freezes
- 🌐 Built-in web player (`/player`) — watch from any browser
- 📋 IPTV playlist endpoint (`/iptv.m3u`) for VLC / Kodi / IPTV apps
- 📦 Stdlib-only Python — zero dependencies (runs on system `python3`)

## Architecture

```
IPTV player / browser
        │  GET /live/<channel>.m3u8
        ▼
┌─────────────────────────────────────────────┐
│ gateway.py (stdlib asyncio-free http server) │
│  有人看 → yt-dlp 解析 URL → HLS byte proxy    │
│  多人看 → 共用同一條 upstream（segment 快取） │
│  60s 沒人 → 停 upstream，回待機              │
└─────────────────────────────────────────────┘
```

Endpoints:

| Path | Description |
|---|---|
| `/` | Channel index |
| `/iptv.m3u` | IPTV playlist (feed to VLC/Kodi/IPTV apps) |
| `/player` | Web player (hls.js) |
| `/live/<ch>.m3u8` | Proxied live HLS manifest |
| `/live/<ch>/seg?u=...` | Proxied segment (rewritten from upstream) |
| `/healthz` | Liveness |

## Install

Requires macOS/Linux with `python3` (≥3.8) and [yt-dlp](https://github.com/yt-dlp/yt-dlp).

```bash
brew install yt-dlp          # macOS; or pipx/pip install yt-dlp elsewhere
git clone <this-repo> ~/iptv-gateway
cd ~/iptv-gateway
python3 gateway.py           # listens on 0.0.0.0:8080
```

### Channels config (`channels.json`)

```json
{
  "news": {
    "title": "My News 24h",
    "youtube_url": "https://www.youtube.com/watch?v=XXXXXXXXXXX",
    "format": "96"
  }
}
```

- `format` = yt-dlp format id. Pick **H.264 (avc1)** formats so byte-proxy works
  without transcoding: `96` = 1080p, `95` = 720p, `91–94` lower
  (check with `yt-dlp -F <url>`). Never use vp9/av1 formats.
- Editing `channels.json` takes effect after a restart; `/iptv.m3u` and
  `/player` are generated from it on every request.

### macOS launchd (auto-start)

See `examples/com.neo.iptv-gateway.plist` — copy to
`~/Library/LaunchAgents/`, adjust paths, then:

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.neo.iptv-gateway.plist
```

Logs: `/tmp/iptv-gateway.log` (stdout), `/tmp/iptv-gateway.err.log` (stderr).

## How YouTube live URL rotation works (the important part)

YouTube live segment URLs **rotate every ~30 seconds**. A naive proxy that holds
the `yt-dlp -g` URL will hit 403 storms and freeze every ~30s. This gateway:

1. **Warm re-resolve**: while a channel is active, a background thread
   re-resolves the URL every 15s (`IPTV_WARM_INTERVAL`), so the manifest is
   always fetched from a URL ≤15s old. Verified: 4-min playback = 0×403, 0×502,
   zero dropped frames.
2. **Reactive backstop**: on any segment/manifest HTTP 403, the upstream is
   invalidated and re-resolved on the next request.

## Environment variables

| Var | Default | Meaning |
|---|---|---|
| `IPTV_PORT` | `8080` | Listen port |
| `IPTV_IDLE_TIMEOUT` | `60` | Seconds without clients before dropping upstream |
| `IPTV_URL_TTL` | `30` | Max age of a resolved YouTube URL |
| `IPTV_WARM_INTERVAL` | `15` | Proactive re-resolve interval while active |
| `IPTV_YTDLP` | `/opt/homebrew/bin/yt-dlp` | Path to yt-dlp binary |

## Verify

```bash
# manifest is proxied (segment URIs rewritten to local)
curl -s http://127.0.0.1:8080/live/news.m3u8 | head -8

# real playback through the gateway (expect exit 0)
ffmpeg -v error -i http://127.0.0.1:8080/live/news.m3u8 -t 20 -f null -
```

## Notes

- Internal/personal use only — don't expose publicly (YouTube ToS).
- No ad-blocking: this is a faithful HLS proxy. If the upstream injects SSAI
  ads into the manifest (check for `EXT-X-CUE-OUT`/`EXT-X-DATERANGE`), they
  pass through. Most 24/7 news channels don't inject ads.
- Latency ≈ YouTube native + manifest poll (~5–15s).
- Web player pins `hls.js@1.5` — newer hls.js (1.6+) has an interstitials
  controller that stalls on live DVR playlists.

## License

MIT
