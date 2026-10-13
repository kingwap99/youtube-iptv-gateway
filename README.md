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
- 🎞️ **雙解析度**：既有端點維持 720p；每台頻道另開 `<ch>-hd`（1080p/2K）第二條管線，
  獨立 spawn／回收，互不干擾。高解析走 `/iptv-hd.m3u`
- 🛠️ 內建 `/admin` WebUI：新增/刪除頻道，即時生效免重啟
- 🐍 stdlib-only Python（跑在系統 python3）

## Endpoints

| Path | 說明 |
|---|---|
| `/` | 頻道清單 |
| `/iptv.m3u` | IPTV 播放清單（餵給播放器）|
| `/iptv-hd.m3u` | 高解析（HD）播放清單，指向 `<ch>-hd` |
| `/live/<ch>.m3u8` | HLS manifest（on-demand spawn）|
| `/live/<ch>-hd.m3u8` | 高解析（1080p/2K）manifest，on-demand spawn |
| `/live/<ch>/<seg>` | HLS segment (.ts) |
| `/healthz` | liveness |
| `/admin` | WebUI：頻道管理（新增/刪除）|
| `POST /admin/add` | 新增頻道 (form: `name`, `title`, `youtube_url`) |
| `POST /admin/del` | 刪除頻道 (form: `name`) |
| `POST /admin/restart` | 重啟單一頻道串流 (form: `name`) |
| `POST /admin/restart-gateway` | 重啟整個 gateway 服務 |

## WebUI：頻道管理（/admin）

內建管理頁面（首頁有「管理頻道」連結），新增或刪除頻道**即時生效**，不用重啟 gateway。

- **新增**：填 ID、顯示名稱、YouTube 直播網址。ID 限 `a-z 0-9 - _`、1–32 字元；
  URL 限 http(s)；ID 重複會擋。原本的額外欄位（例如舊版的 `format`）會保留，不會被弄丟。
- **刪除**：會 kill 該頻道的 streamlink/ffmpeg 管線、清除該頻道的 HLS 暫存目錄，再更新設定檔。
- **重啟（單一頻道）**：kill 該頻道的 streamlink/ffmpeg 管線、清暫存後立即重新拉流
  （畫面卡住／無訊號時用），不動設定檔。
- **重啟 Gateway**：重啟整個服務（內部用 `launchctl kickstart -k gui/<uid>/` +
  環境變數 `SL_LAUNCHD_LABEL`，預設 `com.neo.iptv-streamlink`），正在播放的頻道
  會中斷數秒後由 launchd 自動拉回。
- 每次變更都以原子寫入（tmp + rename）同步 `channels.json`，runtime 與設定檔一致，立即生效；
  但**正在播放中的其他頻道不受影響**。

> ⚠️ `/admin` 沒有身分驗證，任何連得到 8081 的人都能改頻道。這隻 gateway 僅供內部/個人
> 網路使用，請勿暴露到公網。

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
| `SL_STREAM_HD` | `1440p,1080p,best` | 高解析管線畫質（偏好 2K，退回 1080p）|
| `SL_HD` | `1` | 設 `0` 關閉高解析管線（只留 720p）|
| `SL_IDLE_TIMEOUT` | `120` | 幾秒沒人看就停 upstream |
| `SL_STREAMLINK` | `/opt/homebrew/bin/streamlink` | streamlink 路徑 |
| `SL_FFMPEG` | `/opt/homebrew/bin/ffmpeg` | ffmpeg 路徑 |
| `SL_HLS_ROOT` | `/tmp/streamlink-hls` | HLS segment 暫存目錄 |
| `SL_CHANNELS` | `./channels.json` | 頻道設定檔 |
| `SL_LAUNCHD_LABEL` | `com.neo.iptv-streamlink` | gateway 重啟用的 launchd label |

## 驗證

```bash
# manifest 有 segment
curl -s http://127.0.0.1:8081/live/tvbs.m3u8 | head -8

# 端到端實際播放（exit 0 = 可播）
ffmpeg -v error -i http://127.0.0.1:8081/live/tvbs.m3u8 -t 20 -f null -
```

## 疑難排解

### 播放器顯示「停止／無串流」，manifest 尾端是 `#EXT-X-ENDLIST`

ffmpeg 被關掉時（閒置 `SL_IDLE_TIMEOUT` 秒回收、gateway 重啟…）會把 `index.m3u8`
收尾成帶 `#EXT-X-ENDLIST` 的「已結束」清單。若這份檔案留在頻道目錄裡，下次開啟
同一台時，gateway 雖然重新 spawn 了管線，卻會先讀到這份舊清單就回給播放器，
播放器隨即判定串流結束（停在停止／無串流，而且不會自己恢復）。

現在的處理：

- `_spawn_locked()` 重開管線前先清空該頻道目錄，舊的死清單沒有機會被讀到。
- `manifest()` 只接受這條管線自己寫出、且不含 `#EXT-X-ENDLIST` 的清單，最多等 12 秒。

## 注意

- 內部/個人使用，勿公開（YouTube ToS）。
- 不做去廣告；絕大多數 24/7 新聞台不插廣告。
- 延遲 ≈ YouTube 原生 + manifest 輪詢（~5–15s）。

## License

MIT
