#!/usr/bin/env python3
"""Parisa signaling + static server. Standard library only.

Signaling uses plain HTTPS long-polling (no WebSocket), so it passes through
proxies/firewalls that only allow ordinary web traffic. Media is WebRTC,
peer-to-peer or relayed through a TURN server (see README).

Env: PORT, HOST, STUN_URLS, TURN_URLS, TURN_USER, TURN_PASS, TURN_SECRET
"""
import argparse, base64, hashlib, hmac, json, mimetypes, os, ssl, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
SLOT_TTL = 45      # seconds without polling before a slot is free again
POLL_WAIT = 25     # long-poll duration

cond = threading.Condition()
rooms = {}  # room -> {"epoch": int, "slots": {"a"/"b": {"cid", "seen"}}, "q": {"a": [], "b": []}, "seq": int}


def now():
    return time.time()


def get_room(rid):
    return rooms.setdefault(rid, {"epoch": 0, "slots": {}, "q": {"a": [], "b": []}, "seq": 0})


def alive(room, role):
    s = room["slots"].get(role)
    return bool(s) and now() - s["seen"] < SLOT_TTL


def push(room, role, msg):
    room["seq"] += 1
    room["q"][role].append(dict(msg, seq=room["seq"]))
    del room["q"][role][:-200]
    cond.notify_all()


def join(rid, cid):
    """Claim a slot. Returns (role, epoch) or None if room is full."""
    room = get_room(rid)
    for role in "ab":
        s = room["slots"].get(role)
        if s and s["cid"] == cid and alive(room, role):
            s["seen"] = now()
            return role, room["epoch"]
    for role in "ab":
        if not alive(room, role):
            room["slots"][role] = {"cid": cid, "seen": now()}
            room["q"][role] = []
            room["epoch"] += 1
            other = "b" if role == "a" else "a"
            if alive(room, other):
                push(room, other, {"type": "peer-joined", "epoch": room["epoch"]})
            return role, room["epoch"]
    return None


def role_of(room, cid):
    for role in "ab":
        s = room["slots"].get(role)
        if s and s["cid"] == cid:
            return role


def ice_config():
    servers = []
    stun = os.environ.get("STUN_URLS", "stun:stun.l.google.com:19302")
    if stun:
        servers.append({"urls": [u.strip() for u in stun.split(",") if u.strip()]})
    turn = os.environ.get("TURN_URLS", "")
    if turn:
        urls = [u.strip() for u in turn.split(",") if u.strip()]
        secret = os.environ.get("TURN_SECRET")
        if secret:  # coturn use-auth-secret (ephemeral credentials)
            user = str(int(now()) + 6 * 3600)
            cred = base64.b64encode(hmac.new(secret.encode(), user.encode(), hashlib.sha1).digest()).decode()
        else:
            user, cred = os.environ.get("TURN_USER", ""), os.environ.get("TURN_PASS", "")
        servers.append({"urls": urls, "username": user, "credential": cred})
    return {"iceServers": servers}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "Parisa"

    def log_message(self, *a):
        pass

    def send_json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def body_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n > 65536:
            return {}
        try:
            return json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            return {}

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if u.path == "/api/ice":
            return self.send_json(ice_config())
        if u.path == "/api/poll":
            return self.poll(q)
        if u.path == "/api/time":
            return self.send_json({"t": now()})
        self.static(u.path)

    def do_POST(self):
        u = urlparse(self.path)
        d = self.body_json()
        rid, cid = str(d.get("room", ""))[:64], str(d.get("cid", ""))[:64]
        if not rid or not cid:
            return self.send_json({"error": "bad request"}, 400)
        with cond:
            if u.path == "/api/join":
                r = join(rid, cid)
                if not r:
                    return self.send_json({"error": "full"}, 409)
                room = get_room(rid)
                other = "b" if r[0] == "a" else "a"
                return self.send_json({"role": r[0], "epoch": r[1], "peer": alive(room, other)})
            if u.path == "/api/send":
                room = get_room(rid)
                role = role_of(room, cid)
                if not role or d.get("epoch") != room["epoch"]:
                    return self.send_json({"ok": False})
                other = "b" if role == "a" else "a"
                if alive(room, other):
                    push(room, other, {"type": "signal", "epoch": room["epoch"], "data": d.get("data")})
                return self.send_json({"ok": True})
            if u.path == "/api/reset":  # force both sides to rebuild their connection
                room = get_room(rid)
                role = role_of(room, cid)
                if not role:
                    return self.send_json({"ok": False})
                room["epoch"] += 1
                other = "b" if role == "a" else "a"
                if alive(room, other):
                    push(room, other, {"type": "peer-joined", "epoch": room["epoch"]})
                return self.send_json({"ok": True, "epoch": room["epoch"]})
            if u.path == "/api/leave":
                room = get_room(rid)
                role = role_of(room, cid)
                if role:
                    room["slots"].pop(role, None)
                    other = "b" if role == "a" else "a"
                    if alive(room, other):
                        push(room, other, {"type": "peer-left", "epoch": room["epoch"]})
                return self.send_json({"ok": True})
        self.send_json({"error": "not found"}, 404)

    def poll(self, q):
        rid, cid, after = q.get("room", ""), q.get("cid", ""), int(q.get("after", 0) or 0)
        deadline = now() + POLL_WAIT
        with cond:
            while True:
                room = rooms.get(rid)
                role = role_of(room, cid) if room else None
                if not role:
                    return self.send_json({"error": "not joined"}, 410)
                room["slots"][role]["seen"] = now()
                msgs = [m for m in room["q"][role] if m["seq"] > after]
                other = "b" if role == "a" else "a"
                if msgs or now() >= deadline:
                    return self.send_json({"msgs": msgs, "peer": alive(room, other), "epoch": room["epoch"]})
                cond.wait(timeout=max(0.1, min(5, deadline - now())))

    def static(self, path):
        if path == "/":
            path = "/index.html"
        fp = os.path.normpath(os.path.join(STATIC, path.lstrip("/")))
        if not fp.startswith(STATIC + os.sep) or not os.path.isfile(fp):
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        with open(fp, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", mimetypes.guess_type(fp)[0] or "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)


def janitor():
    while True:
        time.sleep(60)
        with cond:
            for rid in [r for r, v in rooms.items() if not any(alive(v, x) for x in "ab")]:
                del rooms[rid]


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8080)))
    ap.add_argument("--cert")
    ap.add_argument("--key")
    a = ap.parse_args()
    ThreadingHTTPServer.daemon_threads = True
    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    if a.cert and a.key:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(a.cert, a.key)
        srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    threading.Thread(target=janitor, daemon=True).start()
    print(f"Parisa on {'https' if a.cert else 'http'}://{a.host}:{a.port}")
    srv.serve_forever()
