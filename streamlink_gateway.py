#!/usr/bin/env python3
"""YouTube Live -> HLS gateway (streamlink 版, 獨立於 TVHeadend).

跟主 gateway.py 的差異：不用 yt-dlp 逐段 proxy（會被 YouTube ~30s URL 輪替
打斷造成卡頓），改用 streamlink 維持「連續緩衝」連線（--hls-live-edge 6
+ 32MB ring buffer），把 URL 輪替在內部吸收掉，再 ffmpeg -c copy 切成 HLS。
上游直接是 YouTube，不經過 :9981 TVHeadend。

Endpoints:
  GET /                    channel list
  GET /iptv.m3u            IPTV playlist (feed to wall player)
  GET /live/<ch>.m3u8      HLS manifest (spawn streamlink|ffmpeg on demand)
  GET /live/<ch>/<seg>     HLS segment (.ts)
  GET /healthz             liveness
  GET /admin               WebUI: 新增/刪除頻道
  POST /admin/add          add channel (form: name, title, youtube_url)
  POST /admin/del          delete channel (form: name)
  POST /admin/restart      restart one channel pipeline (form: name)
  POST /admin/restart-gateway   restart whole gateway service
"""
import html
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STREAMLINK = os.environ.get("SL_STREAMLINK", "/opt/homebrew/bin/streamlink")
FFMPEG = os.environ.get("SL_FFMPEG", "/opt/homebrew/bin/ffmpeg")
PORT = int(os.environ.get("SL_PORT", "8081"))
HLS_ROOT = os.environ.get("SL_HLS_ROOT", "/tmp/streamlink-hls")
CHANNELS_FILE = os.environ.get("SL_CHANNELS", os.path.join(BASE_DIR, "channels.json"))
IDLE_TIMEOUT = int(os.environ.get("SL_IDLE_TIMEOUT", "120"))
HLS_TIME = 4            # seconds per segment
HLS_LIST_SIZE = 6       # keep ~24s of live buffer
# streamlink 連續緩衝參數（與 TVHeadend 裡實證穩定的設定一致）
LIVE_EDGE = 6
RINGBUFFER = "32M"
STREAM = os.environ.get("SL_STREAM", "720p,best")   # 畫質切換: 720p,best / 1080p,best
# 高解析度（HD）第二條管線：既有端點維持低解析（720p），
# 高解析走 <name>-hd 後綴與 /iptv-hd.m3u，各自獨立 spawn，互不干擾。
HD_SUFFIX = "-hd"
STREAM_HD = os.environ.get("SL_STREAM_HD", "1440p,1080p,best")  # 偏好 2K，退回 1080p
HD_ENABLED = os.environ.get("SL_HD", "1").lower() not in ("0", "false", "no")


ADMIN_HTML = """<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Streamlink Gateway 管理</title>
<style>
 body{font-family:system-ui,-apple-system,sans-serif;background:#101114;color:#eee;margin:0;padding:24px;max-width:960px}
 h1{font-size:20px;margin:0 0 16px}
 h2{font-size:16px;margin:24px 0 8px}
 table{border-collapse:collapse;width:100%%;font-size:14px}
 th,td{border:1px solid #333;padding:6px 10px;text-align:left}
 th{background:#1c1e22}
 td.url{font-family:ui-monospace,monospace;font-size:12px;word-break:break-all;max-width:420px}
 input{background:#1c1e22;color:#eee;border:1px solid #444;border-radius:5px;padding:6px 8px;margin:0 8px 8px 0}
 input[name=name]{min-width:180px}
 input[name=title]{min-width:200px}
 input[name=youtube_url]{min-width:340px}
 button{background:#2d5c9e;border:none;color:#fff;padding:7px 14px;border-radius:5px;cursor:pointer}
 button.warn{background:#8b6f2f}
 button.del{background:#8b2f2f}
 a.play{color:#7fb4f5}
 .ok{color:#7bd88f;padding:6px 0}
 .err{color:#f08a8a;padding:6px 0}
 form.inline{display:inline}
</style>
</head>
<body>
<h1>Streamlink Gateway · 頻道管理</h1>
%(msg)s
<form method="post" action="/admin/restart-gateway"
      onsubmit="return confirm('重新啟動整個 Gateway？正在播放的頻道會中斷幾秒。')">
  <button type="submit" class="warn">重啟 Gateway</button>
</form>
<h2>新增頻道</h2>
<form method="post" action="/admin/add">
  <input name="name" placeholder="ID (a-z 0-9 - _)" required pattern="[A-Za-z0-9_-]{1,32}" maxlength="32">
  <input name="title" placeholder="顯示名稱" required maxlength="64">
  <input name="youtube_url" type="url" placeholder="https://www.youtube.com/watch?v=..." required size="50">
  <button type="submit">新增頻道</button>
</form>
<h2>頻道清單（%(count)s 台）</h2>
<table>
<thead><tr><th>ID</th><th>名稱</th><th>YouTube URL</th><th></th><th></th><th></th></tr></thead>
<tbody>
%(rows)s
</tbody>
</table>
<p><a href="/">← 頻道首頁</a> · <a href="/iptv.m3u">/iptv.m3u</a> · <a href="/healthz">healthz</a></p>
</body>
</html>
"""


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


class Pipeline:
    # 一條 streamlink|ffmpeg 管線，綁定一個 HLS 目錄與一種解析度。
    # name 同時是 HLS 目錄名與路由 token（低解析 "ctv"、高解析 "ctv-hd"）。
    def __init__(self, name, youtube_url, stream, label):
        self.name = name
        self.youtube_url = youtube_url
        self.stream = stream
        self.label = label            # log 用顯示名
        self.dir = os.path.join(HLS_ROOT, name)
        self.proc = None
        self.last_req = 0.0
        self.spawned_at = 0.0
        self.lock = Lock()

    def _spawn_locked(self):
        os.makedirs(self.dir, exist_ok=True)
        # 上一條管線被關掉（閒置回收 / gateway 重啟）時，ffmpeg 收尾會把 index.m3u8
        # 寫成帶 #EXT-X-ENDLIST 的「已結束」清單。若不清掉，下次有人打開頻道時
        # 會先端出這份死清單，播放器立刻判定串流停止（就是「停止／無串流」）。
        for stale in os.listdir(self.dir):
            try:
                os.remove(os.path.join(self.dir, stale))
            except OSError:
                pass
        # streamlink 連續拉流(%s) | ffmpeg -c copy 切 HLS
        cmd = (
            shlex.quote(STREAMLINK) + " --stdout"
            " --hls-live-edge %d" % LIVE_EDGE +
            " --ringbuffer-size %s" % RINGBUFFER +
            " -4 --default-stream " + self.stream + " --url " + shlex.quote(self.youtube_url) +
            " | " + shlex.quote(FFMPEG) +
            " -hide_banner -loglevel error"
            " -fflags +genpts -f mpegts -i pipe:0"
            " -c copy -f hls"
            " -hls_time %d" % HLS_TIME +
            " -hls_list_size %d" % HLS_LIST_SIZE +
            " -hls_flags delete_segments"
            " -hls_segment_filename seg_%05d.ts"
            " index.m3u8"
        )
        try:
            self.proc = subprocess.Popen(
                cmd, shell=True, cwd=self.dir,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True)   # 新 process group，方便整條管線一起殺
            self.spawned_at = time.time()
            log("spawn streamlink|ffmpeg %s [%s]" % (self.name, self.label))
        except Exception as e:
            log("spawn FAILED %s: %s" % (self.name, e))
            self.proc = None

    def ensure_running(self):
        with self.lock:
            if self.proc is not None and self.proc.poll() is None:
                return True
            self._spawn_locked()
            return self.proc is not None

    def manifest(self):
        self.last_req = time.time()
        if not self.ensure_running():
            return None
        path = os.path.join(self.dir, "index.m3u8")
        deadline = time.time() + 12.0         # streamlink resolve + 首段較慢
        man = ""
        while time.time() < deadline:
            # 只接受這條管線自己寫出來、而且還在「進行中」的清單；
            # 帶 ENDLIST 的代表串流已結束，等下一輪再說。
            try:
                if os.path.getmtime(path) >= self.spawned_at:
                    with open(path, "r", encoding="utf-8", errors="replace") as f:
                        man = f.read()
            except OSError:
                man = ""
            if man.strip() and "#EXT-X-ENDLIST" not in man:
                break
            man = ""
            time.sleep(0.4)
        if not man.strip():
            return None
        out = []
        for line in man.splitlines():
            s = line.strip()
            if s and not s.startswith("#") and not s.startswith("http"):
                out.append("/live/%s/%s" % (self.name, s))
            else:
                out.append(line)
        return "\n".join(out) + "\n"

    def segment(self, segfile):
        self.last_req = time.time()
        if "/" in segfile or "\\" in segfile or ".." in segfile:
            return None
        try:
            with open(os.path.join(self.dir, segfile), "rb") as f:
                return f.read()
        except OSError:
            return None

    def reap(self):
        now = time.time()
        with self.lock:
            if self.proc is None:
                return
            idle = (now - self.last_req) > IDLE_TIMEOUT
            if idle:
                self._kill_locked()
                log("idle -> kill streamlink|ffmpeg %s" % self.name)
            elif self.proc.poll() is not None:
                self.proc = None
                log("pipeline died %s (respawn on next request)" % self.name)

    def _kill_locked(self):
        if self.proc is None:
            return
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
        except Exception:
            pass
        try:
            self.proc.wait(timeout=4)
        except Exception:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            except Exception:
                pass
        self.proc = None

    def restart(self):
        with self.lock:
            self._kill_locked()
            if os.path.isdir(self.dir):
                shutil.rmtree(self.dir, ignore_errors=True)
            self._spawn_locked()
        log("admin: restarted channel %s" % self.name)
        return self.proc is not None


class Channel:
    # 一台頻道 = 低解析管線（既有 <name>）＋可選高解析管線（<name>-hd）。
    # 兩條管線獨立 spawn / 回收，互不干擾；低解析維持現行行為不變。
    def __init__(self, name, title, youtube_url, extra=None):
        self.name = name
        self.title = title
        self.youtube_url = youtube_url
        self.extra = extra or {}
        self.sd = Pipeline(name, youtube_url, STREAM, "%s · %s" % (title, STREAM))
        self.hd = None
        if HD_ENABLED:
            self.hd = Pipeline(name + HD_SUFFIX, youtube_url, STREAM_HD,
                               "%s · %s" % (title, STREAM_HD))

    @property
    def dir(self):                 # 相容舊呼叫（刪除時清目錄）
        return self.sd.dir

    def pipelines(self):
        return [p for p in (self.sd, self.hd) if p is not None]

    def reap(self):
        for p in self.pipelines():
            p.reap()

    def restart(self):
        for p in self.pipelines():
            p.restart()
        log("admin: restarted channel %s (sd%s)" % (self.name, "+hd" if self.hd else ""))
        return True


def load_channels():
    with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    chans = []
    for name, c in cfg.items():
        extra = {k: v for k, v in c.items() if k not in ("title", "youtube_url")}
        ch = Channel(name, c.get("title", name), c["youtube_url"], extra)
        chans.append(ch)
    return chans


class Bridge:
    def __init__(self):
        self.lock = Lock()
        self._sweep_stale_pipelines()
        self.channels = {c.name: c for c in load_channels()}
        log("loaded %d YouTube channels" % len(self.channels))
        Thread(target=self._reaper, daemon=True).start()

    def _sweep_stale_pipelines(self):
        # gateway 結束時（crash / launchd 重啟 / 手動 kickstart）來不及收掉自己 spawn 的
        # streamlink|ffmpeg，它們會變孤兒繼續寫同一個 HLS 目錄：和新管線搶寫，
        # 段落編號交錯（例如 ctv 目錄同時有 seg_00070 與 seg_02290），
        # 播放器拿到的清單指向被另一方刪掉的段落就卡住停止。
        # 啟動時先清掉殘留管線、再清空 HLS 暫存，確保只有一條寫手。
        marker = "seg_%05d.ts index.m3u8"
        try:
            out = subprocess.run(
                ["ps", "-axo", "pid=,pgid=,command="],
                capture_output=True, text=True, timeout=10).stdout
        except Exception as e:
            log("sweep: ps failed (%s) -> skip cleanup" % e)
            return
        pgids = set()
        for line in out.splitlines():
            parts = line.split(maxsplit=2)
            if len(parts) < 3:
                continue
            pid, pgid, cmd = parts
            try:
                if int(pid) == os.getpid():
                    continue
                pgid_i = int(pgid)
            except ValueError:
                continue
            if marker in cmd and ("/bin/sh -c" in cmd
                                  or STREAMLINK in cmd or FFMPEG in cmd):
                pgids.add(pgid_i)
        if not pgids:
            log("sweep: no stale pipelines found")
            return
        for gid in pgids:
            try:
                os.killpg(gid, signal.SIGTERM)
            except Exception:
                pass
        time.sleep(1.0)
        for gid in pgids:   # SIGKILL 兜底
            try:
                os.killpg(gid, signal.SIGKILL)
            except Exception:
                pass
        log("sweep: killed %d stale pipeline group(s)" % len(pgids))
        if os.path.isdir(HLS_ROOT):
            shutil.rmtree(HLS_ROOT, ignore_errors=True)
        log("sweep: cleared %s" % HLS_ROOT)

    def _reaper(self):
        while True:
            time.sleep(15)
            for ch in self.channels.values():
                ch.reap()

    def _save(self):
        cfg = {}
        for name, c in self.channels.items():
            entry = {"title": c.title, "youtube_url": c.youtube_url}
            entry.update(c.extra)
            cfg[name] = entry
        tmp = CHANNELS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp, CHANNELS_FILE)

    def add_channel(self, name, title, url):
        with self.lock:
            if name in self.channels:
                return False, "exists"
            self.channels[name] = Channel(name, title, url)
            self._save()
        log("admin: added channel %s (%s)" % (name, title))
        return True, ""

    def remove_channel(self, name):
        with self.lock:
            ch = self.channels.pop(name, None)
            if ch is None:
                return False
            self._save()
        for p in ch.pipelines():
            with p.lock:
                p._kill_locked()
            if os.path.isdir(p.dir):
                shutil.rmtree(p.dir, ignore_errors=True)
        log("admin: removed channel %s" % name)
        return True

    def restart_channel(self, name):
        with self.lock:
            ch = self.channels.get(name)
        if ch is None:
            return False
        return ch.restart()

    def restart_gateway(self):
        label = os.environ.get("SL_LAUNCHD_LABEL", "com.neo.iptv-streamlink")
        sh = "sleep 1; exec launchctl kickstart -k gui/%d/%s" % (os.getuid(), label)
        try:
            subprocess.Popen(["sh", "-c", sh], start_new_session=True,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            log("admin: restart-gateway spawn FAILED: %s" % e)
            return False
        log("admin: restart gateway scheduled via launchctl %s" % label)
        return True


class Handler(BaseHTTPRequestHandler):
    server_version = "Streamlink-Gateway/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        log("%s %s" % (self.address_string(), fmt % args))

    def _send(self, code, body, ctype, extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        if extra:
            for k, v in extra.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _send_redirect(self, location):
        self.send_response(302)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    @staticmethod
    def _resolve_pipeline(br, token):
        # "ctv" -> 低解析；"ctv-hd" -> 高解析（HD 關閉時回 None）。
        if token.endswith(HD_SUFFIX):
            ch = br.channels.get(token[:-len(HD_SUFFIX)])
            return (ch.hd if ch and ch.hd else None)
        ch = br.channels.get(token)
        return ch.sd if ch else None

    def _admin_page(self):
        br = self.server.bridge
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        msg = ""
        if q.get("ok"):
            okmap = {"added": "已新增頻道", "deleted": "已刪除頻道",
                     "restarted": "已重啟頻道串流",
                     "restarting": "Gateway 重啟中，約幾秒後恢復"}
            label = okmap.get(q["ok"][0], q["ok"][0])
            msg = '<div class="ok">完成: %s</div>' % html.escape(label)
        elif q.get("err"):
            errmap = {"exists": "頻道 ID 已存在", "invalid name": "ID 格式不正確",
                      "invalid url": "URL 格式不正確", "notfound": "找不到頻道",
                      "restart fail": "重啟指令送出失敗"}
            label = errmap.get(q["err"][0], q["err"][0])
            msg = '<div class="err">失敗: %s</div>' % html.escape(label)
        rows = []
        for c in sorted(br.channels.values(), key=lambda c: c.name):
            rows.append(
                "<tr><td>%s</td><td>%s</td><td class='url'>%s</td>"
                "<td><a class='play' href='/live/%s.m3u8' target='_blank'>播放</a>"
                "%s</td>"
                "<td><form class='inline' method='post' action='/admin/restart' "
                "onsubmit=\"return confirm('重啟 %s 的串流？')\">"
                "<input type='hidden' name='name' value='%s'>"
                "<button type='submit' class='warn'>重啟</button></form></td>"
                "<td><form class='inline' method='post' action='/admin/del' "
                "onsubmit=\"return confirm('確定刪除 %s ?')\">"
                "<input type='hidden' name='name' value='%s'>"
                "<button type='submit' class='del'>刪除</button></form></td></tr>"
                % (html.escape(c.name), html.escape(c.title),
                   html.escape(c.youtube_url), html.escape(c.name),
                   (' <a class="play" href="/live/%s%s.m3u8" target="_blank">HD</a>'
                    % (html.escape(c.name), HD_SUFFIX)) if c.hd else "",
                   html.escape(c.title), html.escape(c.name),
                   html.escape(c.title), html.escape(c.name)))
        return ADMIN_HTML % {"msg": msg, "count": len(br.channels),
                             "rows": "\n".join(rows)}

    def do_GET(self):
        br = self.server.bridge
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/healthz":
            self._send(200, b"ok\n", "text/plain")
            return
        if path in ("/", "/index"):
            rows = "".join(
                '<li><a href="/live/%s.m3u8">%s</a> <code>/live/%s.m3u8</code>%s</li>'
                % (c.name, c.title, c.name,
                   (' · <a href="/live/%s%s.m3u8">%s HD</a> <code>/live/%s%s.m3u8</code>'
                    % (c.name, HD_SUFFIX, c.title, c.name, HD_SUFFIX)) if c.hd else "")
                for c in br.channels.values())
            body = ("<h1>Streamlink Gateway</h1><ul>%s</ul>"
                    '<p>Playlist: <a href="/iptv.m3u">/iptv.m3u</a>（720p）· '
                    '<a href="/iptv-hd.m3u">/iptv-hd.m3u</a>（HD）· '
                    '<a href="/admin">管理頻道</a></p>' % rows).encode()
            self._send(200, body, "text/html; charset=utf-8")
            return
        if path == "/iptv.m3u":
            host = self.headers.get("Host", "127.0.0.1:%d" % PORT)
            lines = ["#EXTM3U"]
            for c in br.channels.values():
                lines.append('#EXTINF:-1 tvg-id="%s" tvg-name="%s",%s' % (c.name, c.title, c.title))
                lines.append("http://%s/live/%s.m3u8" % (host, c.name))
            self._send(200, ("\n".join(lines) + "\n").encode(), "application/x-mpegurl")
            return
        if path == "/iptv-hd.m3u":
            host = self.headers.get("Host", "127.0.0.1:%d" % PORT)
            lines = ["#EXTM3U"]
            for c in br.channels.values():
                if not c.hd:
                    continue
                lines.append('#EXTINF:-1 tvg-id="%s" tvg-name="%s HD",%s HD' % (c.name, c.title, c.title))
                lines.append("http://%s/live/%s%s.m3u8" % (host, c.name, HD_SUFFIX))
            self._send(200, ("\n".join(lines) + "\n").encode(), "application/x-mpegurl")
            return
        if path == "/admin":
            body = self._admin_page().encode()
            self._send(200, body, "text/html; charset=utf-8")
            return
        parts = path.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "live":
            token = parts[1]
            if token.endswith(".m3u8"):
                token = token[:-5]
            pipe = self._resolve_pipeline(br, token)
            if not pipe:
                self._send(404, b"no such channel\n", "text/plain")
                return
            if len(parts) == 2:
                man = pipe.manifest()
                if man is None:
                    self._send(503, b"hls not ready\n", "text/plain")
                    return
                self._send(200, man.encode(), "application/vnd.apple.mpegurl",
                           {"Cache-Control": "no-cache"})
                return
            if len(parts) == 3:
                data = pipe.segment(parts[2])
                if data is None:
                    self._send(404, b"segment not found\n", "text/plain")
                    return
                self._send(200, data, "video/mp2t", {"Accept-Ranges": "bytes"})
                return
        self._send(404, b"not found\n", "text/plain")

    def do_POST(self):
        br = self.server.bridge
        parsed = urllib.parse.urlparse(self.path)
        try:
            length = int(self.headers.get("Content-Length", 0) or 0)
        except ValueError:
            length = 0
        if 0 < length <= 1 << 20:
            raw = self.rfile.read(length).decode("utf-8", "replace")
        else:
            raw = ""
        form = urllib.parse.parse_qs(raw, keep_blank_values=True)
        if parsed.path not in ("/admin/add", "/admin/del",
                               "/admin/restart", "/admin/restart-gateway"):
            self._send(404, b"not found\n", "text/plain")
            return

        def val(k):
            v = form.get(k)
            return v[0].strip() if v else ""

        if parsed.path == "/admin/add":
            name = val("name")
            title = val("title") or name
            url = val("youtube_url")
            if not re.match(r"^[A-Za-z0-9_-]{1,32}$", name):
                self._send_redirect("/admin?err=invalid+name")
                return
            if not url.startswith(("http://", "https://")):
                self._send_redirect("/admin?err=invalid+url")
                return
            ok, _ = br.add_channel(name, title, url)
            self._send_redirect("/admin?ok=added" if ok else "/admin?err=exists")
            return
        if parsed.path == "/admin/del":
            name = val("name")
            ok = br.remove_channel(name)
            self._send_redirect("/admin?ok=deleted" if ok else "/admin?err=notfound")
            return
        if parsed.path == "/admin/restart":
            name = val("name")
            ok = br.restart_channel(name)
            self._send_redirect("/admin?ok=restarted" if ok else "/admin?err=notfound")
            return
        if parsed.path == "/admin/restart-gateway":
            ok = br.restart_gateway()
            self._send_redirect("/admin?ok=restarting" if ok else "/admin?err=restart+fail")
            return


class QuietServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        log("conn error from %s" % (client_address,))


def main():
    br = Bridge()
    srv = QuietServer(("0.0.0.0", PORT), Handler)
    srv.bridge = br
    log("Streamlink gateway listening on 0.0.0.0:%d" % PORT)

    def shutdown(signum, frame):
        log("received signal %d -> killing channel pipelines" % signum)
        for ch in br.channels.values():
            for p in ch.pipelines():
                with p.lock:
                    p._kill_locked()
        log("shutdown complete")
        raise SystemExit(0)

    # launchd 重啟/停止會送 SIGTERM：先收掉自家管線，免得留下孤兒和新管線搶寫。
    signal.signal(signal.SIGTERM, shutdown)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    main()
