# YouTube Live → IPTV HLS Gateway (streamlink)

把 YouTube 直播（或任何 HLS 直播）轉成內部 IPTV 串流的 on-demand gateway，供牆上播放器 /
電視盒 / VLC / Kodi 使用。

跟「逐段 proxy」的舊做法不同，這版用 **streamlink 連續緩衝拉流**（`--hls-live-edge 6`
+ 32MB ring buffer），把 YouTube 每 ~30 秒的 URL 輪替在內部吸收掉，再 `ffmpeg -c copy`
（零重編碼）切成 HLS。AVPlayer 直接可播，不會因為 URL 輪替卡頓。

## 為什麼用 streamlink，不用 yt-dlp 逐段 proxy

YouTube 直播的 segment URL **每 ~30 秒輪替一次**。舊做法是「牆上要一段 → gateway 才去
YouTube 抓一段」：輪替瞬間舊 URL 變 403，得 invalidate + 重解析 → 出現空窗 → 卡頓。

streamlink 的做法是**持續讀取 + 大緩衝**：自己維持連線、持續抓 segment 放進 ring buffer，
URL 輪替由它內部處理，外面看起來就是一條不斷流的連續 TS。這才是直播穩定的關鍵。

## 架構

```
IPTV player / wall player
        │  GET /live/<channel>.m3u8
        ▼
┌──────────────────────────────────────────────┐
│ streamlink_gateway.py (stdlib http server)    │
│  有人看 → spawn streamlink(720p 連續緩衝)      │
│           │ ffmpeg -c copy → HLS segment      │
│  120s 沒人 → 停掉 upstream，回待機             │
└──────────────────────────────────────────────┘
```

## 功能

- ⚡ On-demand：沒人看就 idle（零 process）；120s 沒人自動停
- 🔁 streamlink 連續緩衝 → 對抗 YouTube URL 輪替，不斷流
- 📦 ffmpeg `-c copy` 零重編碼，CPU ≈ 0%
- 📋 `/iptv.m3u` 播放清單，餵給 VLC / Kodi / IPTV app / AVPlayer
- 🎚️ `SL_STREAM` 環境變數切換 720p / 1080p
- 🐍 stdlib-only Python（跑在系統 python3）

## Endpoints

| Path | 說明 |
|---|---|
| `/` | 頻道清單 |
| `/iptv.m3u` | IPTV 播放清單（餵給播放器）|
| `/live/<ch>.m3u8` | HLS manifest（on-demand spawn）|
| `/live/<ch>/<seg>` | HLS segment (.ts) |
| `/healthz` | liveness |

## 安裝

macOS / Linux，需要 `python3`（≥3.8）、[streamlink](https://github.com/streamlink/streamlink)、ffmpeg：

```bash
brew install streamlink ffmpeg      # macOS
git clone <this-repo> ~/iptv-gateway
cd ~/iptv-gateway
python3 streamlink_gateway.py       # 監聽 0.0.0.0:8081
```

### 頻道設定（channels.json）

```json
{
  "tvbs": {
    "title": "TVBS NEWS 24hr",
    "youtube_url": "https://www.youtube.com/watch?v=XXXXXXXXXXX"
  }
}
```

每台只要給 YouTube 直播網址即可；舊 proxy 的 `format` 欄位已不需要。改完重啟生效。

### 畫質切換

`SL_STREAM` 環境變數，預設 `720p,best`（碼率低、最穩）。要 1080p 就設 `1080p,best`。
注意 1080p 碼率會隨畫面內容浮動（0.9–5.6 Mbps），高動態畫面可能超出低階播放器的解碼能力。

### macOS launchd（開機自啟）

見 `examples/com.neo.iptv-streamlink.plist` — 複製到 `~/Library/LaunchAgents/`、把路徑改成
你自己的後：

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.neo.iptv-streamlink.plist
```

Log：`/tmp/streamlink-gateway.log`（stdout）、`/tmp/streamlink-gateway.err.log`（stderr）。

## 環境變數

| Var | 預設 | 說明 |
|---|---|---|
| `SL_PORT` | `8081` | 監聽 port |
| `SL_STREAM` | `720p,best` | 畫質（`720p,best` / `1080p,best`）|
| `SL_IDLE_TIMEOUT` | `120` | 幾秒沒人看就停 upstream |
| `SL_STREAMLINK` | `/opt/homebrew/bin/streamlink` | streamlink 路徑 |
| `SL_FFMPEG` | `/opt/homebrew/bin/ffmpeg` | ffmpeg 路徑 |
| `SL_HLS_ROOT` | `/tmp/streamlink-hls` | HLS segment 暫存目錄 |
| `SL_CHANNELS` | `./channels.json` | 頻道設定檔 |

## 驗證

```bash
# manifest 有 segment
curl -s http://127.0.0.1:8081/live/tvbs.m3u8 | head -8

# 端到端實際播放（exit 0 = 可播）
ffmpeg -v error -i http://127.0.0.1:8081/live/tvbs.m3u8 -t 20 -f null -
```

## 注意

- 內部/個人使用，勿公開（YouTube ToS）。
- 不做去廣告；絕大多數 24/7 新聞台不插廣告。
- 延遲 ≈ YouTube 原生 + manifest 輪詢（~5–15s）。

## License

MIT
