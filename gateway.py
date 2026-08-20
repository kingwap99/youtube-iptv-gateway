#!/usr/bin/env python3
"""On-demand YouTube Live -> IPTV HLS gateway (stdlib only, no deps).

Behavior:
  - Idle: nothing runs (gateway itself is ~0 CPU).
  - First client GETs /live/<ch>.m3u8 -> resolve YouTube URL via yt-dlp
    -> byte-for-byte proxy the upstream HLS (no transcode, no remux).
  - Multiple clients share the same upstream; segments are fetched once
    and cached per channel.
  - No clients for IPTV_IDLE_TIMEOUT seconds -> drop upstream state, idle.

Endpoints:
  GET /                  channel list
  GET /iptv.m3u          IPTV playlist (feed this to your IPTV player)
  GET /live/<ch>.m3u8    proxied live HLS manifest
  GET /live/<ch>/seg?u=  proxied segment (URL-encoded upstream URL)
  GET /healthz           liveness
"""
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.environ.get("IPTV_CONFIG", os.path.join(BASE_DIR, "channels.json"))
YTDLP = os.environ.get("IPTV_YTDLP", "/opt/homebrew/bin/yt-dlp")
PORT = int(os.environ.get("IPTV_PORT", "8080"))
IDLE_TIMEOUT = int(os.environ.get("IPTV_IDLE_TIMEOUT", "60"))
MANIFEST_TTL = 3.0       # cache upstream manifest seconds
SEGMENT_TTL = 120.0      # keep segments in cache seconds
URL_TTL = int(os.environ.get("IPTV_URL_TTL", "30"))   # re-resolve youtube url seconds (live URLs rotate ~30s)
WARM_INTERVAL = int(os.environ.get("IPTV_WARM_INTERVAL", "15"))  # proactive re-resolve while active
MAX_CACHE_BYTES = 64 * 1024 * 1024
RESOLVE_TIMEOUT = 35
FETCH_TIMEOUT = 25
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"

PLAYER_HTML = """<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>IPTV Player</title>
<style>
 body{background:#111;color:#eee;font-family:system-ui,-apple-system,sans-serif;margin:0;display:flex;flex-direction:column;min-height:100vh}
 header{padding:12px 16px;display:flex;gap:12px;align-items:center;flex-wrap:wrap}
 video{width:100%;max-height:calc(100vh - 130px);background:#000;outline:none}
 select{background:#222;color:#eee;border:1px solid #444;border-radius:6px;padding:8px 10px;font-size:15px}
 .status{font-size:13px;color:#9a9a9a;margin-left:auto}
</style>
</head>
<body>
<header>
 <strong>IPTV</strong>
 <select id="ch"></select>
 <span class="status" id="status">載入中…</span>
</header>
<video id="v" controls autoplay muted playsinline></video>
<script src="https://cdn.jsdelivr.net/npm/hls.js@1.5"></script>
<script>
var v=document.getElementById('v'), st=document.getElementById('status');
var channels=__CHANNELS__;
var hlsPlayer=null;
function play(url){
  if(hlsPlayer){ try{hlsPlayer.destroy();}catch(e){} hlsPlayer=null; }
  if(Hls.isSupported()){
    hlsPlayer=new Hls({liveDurationInfinity:true});
    hlsPlayer.loadSource(url); hlsPlayer.attachMedia(v);
    hlsPlayer.on(Hls.Events.ERROR,function(e,d){
      st.textContent='HLS 錯誤: '+d.type+'/'+d.details;
      if(d.fatal){ try{hlsPlayer.destroy();}catch(e){} hlsPlayer=null; }
    });
    hlsPlayer.on(Hls.Events.MANIFEST_PARSED,function(){ st.textContent='直播中'; v.play().catch(function(){}); });
  } else if(v.canPlayType('application/vnd.apple.mpegurl')){
    v.src=url; v.play().catch(function(){});
  } else { st.textContent='此瀏覽器不支援 HLS'; }
}
var sel=document.getElementById('ch');
Object.keys(channels).forEach(function(k){
  var o=document.createElement('option'); o.value='/live/'+k+'.m3u8'; o.textContent=channels[k]; sel.appendChild(o);
});
sel.onchange=function(){ play(sel.value); };
play(sel.value);
</script>
</body>
</html>
"""


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


class Channel:
    def __init__(self, name, cfg):
        self.name = name
        self.youtube_url = cfg["youtube_url"]
        self.fmt = str(cfg.get("format", "b"))
        self.title = cfg.get("title", name)
        self.lock = Lock()
        self.resolve_lock = Lock()
        self.resolved_url = None
        self.resolved_at = 0.0
        self.manifest = None
        self.manifest_at = 0.0
        self.segments = {}          # url -> (bytes, fetched_at)
        self._seg_locks = {}        # url -> Lock (per-segment fetch dedupe)
        self.cache_bytes = 0
        self.last_activity = time.time()

    def touch(self):
        self.last_activity = time.time()

    # ---------- upstream ----------
    def resolve(self, force=False):
        with self.lock:
            if not force and self.resolved_url and (time.time() - self.resolved_at) < URL_TTL:
                return self.resolved_url
        if not self.resolve_lock.acquire(blocking=False):
            return self.resolved_url or None      # another thread is resolving
        try:
            cmd = [YTDLP, "-g", "-f", self.fmt, "--no-playlist", "--no-warnings", self.youtube_url]
            try:
                out = subprocess.run(cmd, capture_output=True, text=True,
                                     timeout=RESOLVE_TIMEOUT).stdout.strip().splitlines()
            except Exception as e:
                log("resolve error %s: %s" % (self.name, e))
                out = []
            if out and out[-1].startswith("http"):
                with self.lock:
                    self.resolved_url = out[-1].strip()
                    self.resolved_at = time.time()
                log("resolved %s -> ...%s" % (self.name, self.resolved_url[-40:]))
                return self.resolved_url
            if self.resolved_url:                 # stale fallback
                return self.resolved_url
            log("resolve FAILED for %s (yt-dlp output empty)" % self.name)
            return None
        finally:
            self.resolve_lock.release()

    def invalidate(self):
        with self.lock:
            self.resolved_url = None
            self.resolved_at = 0.0
            self.manifest = None
            self.segments = {}
            self._seg_locks = {}
            self.cache_bytes = 0
        log("channel %s upstream invalidated (URL rotated / fetch failed)" % self.name)

    def fetch(self, url, timeout=FETCH_TIMEOUT):
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()

    def get_manifest(self):
        self.touch()
        with self.lock:
            now = time.time()
            if self.manifest and (now - self.manifest_at) < MANIFEST_TTL:
                return self.manifest
        url = self.resolve()
        if not url:
            return None
        try:
            body = self.fetch(url).decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            log("manifest HTTP %s %s (URL rotated?)" % (e.code, self.name))
            self.invalidate()
            body = None
        except Exception as e:
            log("manifest fetch error %s: %s" % (self.name, e))
            self.invalidate()
            body = None
        if body:
            with self.lock:
                self.manifest = body
                self.manifest_at = time.time()
            return body
        return None

    def get_segment(self, seg_url):
        self.touch()
        with self.lock:
            hit = self.segments.get(seg_url)
            if isinstance(hit, tuple):
                return hit[0]
            lock = self._seg_locks.get(seg_url) or Lock()
            self._seg_locks[seg_url] = lock
        with lock:
            with self.lock:
                hit = self.segments.get(seg_url)
            if isinstance(hit, tuple):
                return hit[0]
            try:
                data = self.fetch(seg_url)
            except urllib.error.HTTPError as e:
                log("segment HTTP %s %s (URL rotated, invalidating)" % (e.code, self.name))
                self.invalidate()
                return None
            except Exception as e:
                log("segment error %s: %s" % (self.name, e))
                return None
            with self.lock:
                self.segments[seg_url] = (data, time.time())
                self.cache_bytes += len(data)
                self._evict()
            return data

    def _evict(self):
        while self.cache_bytes > MAX_CACHE_BYTES:
            tuples = {k: v for k, v in self.segments.items() if isinstance(v, tuple)}
            if not tuples:
                break
            oldest = min(tuples, key=lambda k: tuples[k][1])
            self.cache_bytes -= len(tuples[oldest][0])
            del self.segments[oldest]

    def expire(self):
        with self.lock:
            self.resolved_url = None
            self.manifest = None
            self.segments = {}
            self._seg_locks = {}
            self.cache_bytes = 0
            self.last_activity = time.time()
        log("channel %s idle -> upstream dropped" % self.name)


class Gateway:
    def __init__(self):
        with open(CONFIG) as f:
            cfg = json.load(f)
        self.channels = {}
        for name, c in cfg.items():
            self.channels[name] = Channel(name, c)
        log("loaded channels: %s" % ", ".join(self.channels))
        Thread(target=self._reaper, daemon=True).start()

    def _reaper(self):
        while True:
            time.sleep(10)
            now = time.time()
            for ch in self.channels.values():
                if (now - ch.last_activity) > IDLE_TIMEOUT and ch.manifest:
                    ch.expire()
                elif ch.manifest and (now - ch.resolved_at) > WARM_INTERVAL:
                    # active channel: keep a fresh upstream URL ready (live URLs rotate ~30s)
                    Thread(target=ch.resolve, args=(True,), daemon=True).start()


class Handler(BaseHTTPRequestHandler):
    server_version = "IPTV-Gateway/1.0"
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

    def _channel(self):
        return self.server.gateway.channels.get(self.path.strip("/").split("/")[0])

    def do_GET(self):
        gw = self.server.gateway
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if path == "/healthz":
            self._send(200, b"ok\n", "text/plain")
            return
        if path == "/" or path == "/index":
            rows = "".join(
                '<li><a href="/live/%s.m3u8">%s</a> <code>/live/%s.m3u8</code></li>'
                % (n, c.title, n) for n, c in gw.channels.items())
            body = ("<h1>IPTV Gateway</h1><ul>%s</ul>"
                    '<p>Playlist: <a href="/iptv.m3u">/iptv.m3u</a></p>' % rows).encode()
            self._send(200, body, "text/html; charset=utf-8")
            return
        if path == "/iptv.m3u":
            lines = ["#EXTM3U"]
            host = self.headers.get("Host", "127.0.0.1:%d" % PORT)
            for n, c in gw.channels.items():
                lines.append('#EXTINF:-1 tvg-id="%s" tvg-name="%s",%s' % (n, c.title, c.title))
                lines.append("http://%s/live/%s.m3u8" % (host, n))
            self._send(200, ("\n".join(lines) + "\n").encode(), "application/x-mpegurl")
            return
        if path == "/player":
            chans = json.dumps({n: c.title for n, c in gw.channels.items()})
            body = PLAYER_HTML.replace("__CHANNELS__", chans)
            self._send(200, body.encode(), "text/html; charset=utf-8")
            return
        # /live/<ch>.m3u8  or  /live/<ch>/seg?u=...
        parts = path.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "live":
            ch_name = parts[1]
            if ch_name.endswith(".m3u8"):
                ch_name = ch_name[:-5]
            ch = gw.channels.get(ch_name)
            if not ch:
                self._send(404, b"no such channel\n", "text/plain")
                return
            if len(parts) == 2:
                man = ch.get_manifest()
                if man is None:
                    self._send(503, b"upstream unavailable\n", "text/plain")
                    return
                # rewrite segment URIs to local proxy
                out = []
                for line in man.splitlines():
                    s = line.strip()
                    if s.startswith("http") and not s.startswith("#"):
                        enc = urllib.parse.quote(s, safe="")
                        out.append("/live/%s/seg?u=%s" % (ch.name, enc))
                    else:
                        out.append(line)
                self._send(200, ("\n".join(out) + "\n").encode(),
                           "application/vnd.apple.mpegurl")
                return
            if len(parts) == 3 and parts[2] == "seg":
                q = urllib.parse.parse_qs(parsed.query)
                seg_url = q.get("u", [""])[0]
                if not seg_url:
                    self._send(400, b"missing u\n", "text/plain")
                    return
                data = ch.get_segment(seg_url)
                if data is None:
                    self._send(502, b"segment fetch failed\n", "text/plain")
                    return
                # Range support (players like Kodi may request partial)
                rng = self.headers.get("Range")
                if rng and rng.startswith("bytes="):
                    try:
                        start, _, end = rng[6:].partition("-")
                        start = int(start) if start else 0
                        end = int(end) if end else len(data) - 1
                        end = min(end, len(data) - 1)
                        body = data[start:end + 1]
                        self._send(206, body, "video/mp2t", {
                            "Content-Range": "bytes %d-%d/%d" % (start, end, len(data)),
                            "Accept-Ranges": "bytes",
                        })
                        return
                    except ValueError:
                        pass
                self._send(200, data, "video/mp2t", {"Accept-Ranges": "bytes"})
                return
        self._send(404, b"not found\n", "text/plain")


class QuietServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        log("conn error from %s" % (client_address,))


def main():
    gw = Gateway()
    srv = QuietServer(("0.0.0.0", PORT), Handler)
    srv.gateway = gw
    log("listening on 0.0.0.0:%d" % PORT)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
