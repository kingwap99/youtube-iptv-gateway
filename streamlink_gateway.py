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
"""
import json
import os
import shlex
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


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


class Channel:
    def __init__(self, name, title, youtube_url):
        self.name = name
        self.title = title
        self.youtube_url = youtube_url
        self.dir = os.path.join(HLS_ROOT, name)
        self.proc = None
        self.last_req = 0.0
        self.lock = Lock()

    def _spawn_locked(self):
        os.makedirs(self.dir, exist_ok=True)
        # streamlink 連續拉流(720p) | ffmpeg -c copy 切 HLS
        cmd = (
            shlex.quote(STREAMLINK) + " --stdout"
            " --hls-live-edge %d" % LIVE_EDGE +
            " --ringbuffer-size %s" % RINGBUFFER +
            " -4 --default-stream " + STREAM + " --url " + shlex.quote(self.youtube_url) +
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
            log("spawn streamlink|ffmpeg %s (%s)" % (self.name, self.title))
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
        while time.time() < deadline:
            if os.path.exists(path) and os.path.getsize(path) > 0:
                break
            time.sleep(0.4)
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                man = f.read()
        except OSError:
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


def load_channels():
    with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    chans = [Channel(name, c.get("title", name), c["youtube_url"])
             for name, c in cfg.items()]
    return chans


class Bridge:
    def __init__(self):
        self.channels = {c.name: c for c in load_channels()}
        log("loaded %d YouTube channels" % len(self.channels))
        Thread(target=self._reaper, daemon=True).start()

    def _reaper(self):
        while True:
            time.sleep(15)
            for ch in self.channels.values():
                ch.reap()


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

    def do_GET(self):
        br = self.server.bridge
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/healthz":
            self._send(200, b"ok\n", "text/plain")
            return
        if path in ("/", "/index"):
            rows = "".join(
                '<li><a href="/live/%s.m3u8">%s</a> <code>/live/%s.m3u8</code></li>'
                % (c.name, c.title, c.name) for c in br.channels.values())
            body = ("<h1>Streamlink Gateway</h1><ul>%s</ul>"
                    '<p>Playlist: <a href="/iptv.m3u">/iptv.m3u</a></p>' % rows).encode()
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
        parts = path.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "live":
            cname = parts[1]
            if cname.endswith(".m3u8"):
                cname = cname[:-5]
            ch = br.channels.get(cname)
            if not ch:
                self._send(404, b"no such channel\n", "text/plain")
                return
            if len(parts) == 2:
                man = ch.manifest()
                if man is None:
                    self._send(503, b"hls not ready\n", "text/plain")
                    return
                self._send(200, man.encode(), "application/vnd.apple.mpegurl",
                           {"Cache-Control": "no-cache"})
                return
            if len(parts) == 3:
                data = ch.segment(parts[2])
                if data is None:
                    self._send(404, b"segment not found\n", "text/plain")
                    return
                self._send(200, data, "video/mp2t", {"Accept-Ranges": "bytes"})
                return
        self._send(404, b"not found\n", "text/plain")


class QuietServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        log("conn error from %s" % (client_address,))


def main():
    br = Bridge()
    srv = QuietServer(("0.0.0.0", PORT), Handler)
    srv.bridge = br
    log("Streamlink gateway listening on 0.0.0.0:%d" % PORT)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
