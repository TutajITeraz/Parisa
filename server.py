#!/usr/bin/env python3
"""Parisa signaling + static server, as a WSGI app. Standard library only.

* Runs under any WSGI host (cPanel "Setup Python App"/Passenger, gunicorn, uwsgi, mod_wsgi)
  via `application`, or standalone:  python3 server.py --port 8080
* State lives in SQLite, so several worker processes can share it.
* Written for old Pythons (3.5+).
* Signaling is plain HTTPS long-polling (no WebSocket).

Env: PARISA_DB (sqlite path), PARISA_POLL_WAIT (seconds a poll may block, 0 = short polling),
     STUN_URLS, TURN_URLS, TURN_USER, TURN_PASS, TURN_SECRET
"""
import base64, hashlib, hmac, json, mimetypes, os, sqlite3, time

BASE = os.path.dirname(os.path.abspath(__file__))
STATIC = os.path.join(BASE, "static")
DB_PATH = os.environ.get("PARISA_DB", os.path.join(BASE, "parisa.sqlite"))
SLOT_TTL = 45
POLL_WAIT = float(os.environ.get("PARISA_POLL_WAIT", "12"))

_ready = False


def db():
    global _ready
    c = sqlite3.connect(DB_PATH, timeout=15, isolation_level=None)
    c.row_factory = sqlite3.Row
    if not _ready:
        try:
            c.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error:
            pass
        c.executescript("""
            CREATE TABLE IF NOT EXISTS rooms(room TEXT PRIMARY KEY, epoch INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS slots(room TEXT, role TEXT, cid TEXT, seen REAL, PRIMARY KEY(room, role));
            CREATE TABLE IF NOT EXISTS msgs(id INTEGER PRIMARY KEY AUTOINCREMENT, room TEXT, role TEXT, body TEXT, ts REAL);
            CREATE INDEX IF NOT EXISTS msgs_ix ON msgs(room, role, id);
        """)
        _ready = True
    return c


def other(role):
    return "b" if role == "a" else "a"


def slot(c, rid, role):
    return c.execute("SELECT cid, seen FROM slots WHERE room=? AND role=?", (rid, role)).fetchone()


def alive(s):
    return s is not None and time.time() - s["seen"] < SLOT_TTL


def epoch_of(c, rid):
    r = c.execute("SELECT epoch FROM rooms WHERE room=?", (rid,)).fetchone()
    return r["epoch"] if r else 0


def push(c, rid, role, msg):
    c.execute("INSERT INTO msgs(room, role, body, ts) VALUES(?,?,?,?)", (rid, role, json.dumps(msg), time.time()))


def role_of(c, rid, cid):
    for role in "ab":
        s = slot(c, rid, role)
        if s and s["cid"] == cid:
            return role
    return None


def bump(c, rid):
    c.execute("INSERT OR IGNORE INTO rooms(room, epoch) VALUES(?, 0)", (rid,))
    c.execute("UPDATE rooms SET epoch = epoch + 1 WHERE room=?", (rid,))
    return epoch_of(c, rid)


def api_join(rid, cid):
    c = db()
    try:
        c.execute("BEGIN IMMEDIATE")
        now = time.time()
        c.execute("DELETE FROM msgs WHERE ts < ?", (now - 600,))
        c.execute("DELETE FROM slots WHERE seen < ?", (now - 3600,))
        c.execute("INSERT OR IGNORE INTO rooms(room, epoch) VALUES(?, 0)", (rid,))
        for role in "ab":
            s = slot(c, rid, role)
            if s and s["cid"] == cid and alive(s):
                c.execute("UPDATE slots SET seen=? WHERE room=? AND role=?", (now, rid, role))
                c.execute("COMMIT")
                return 200, {"role": role, "epoch": epoch_of(c, rid), "peer": alive(slot(c, rid, other(role)))}
        for role in "ab":
            if not alive(slot(c, rid, role)):
                c.execute("INSERT OR REPLACE INTO slots(room, role, cid, seen) VALUES(?,?,?,?)", (rid, role, cid, now))
                c.execute("DELETE FROM msgs WHERE room=? AND role=?", (rid, role))
                ep = bump(c, rid)
                peer = alive(slot(c, rid, other(role)))
                if peer:
                    push(c, rid, other(role), {"type": "peer-joined", "epoch": ep})
                c.execute("COMMIT")
                return 200, {"role": role, "epoch": ep, "peer": peer}
        c.execute("ROLLBACK")
        return 409, {"error": "full"}
    finally:
        c.close()


def api_send(rid, cid, d):
    c = db()
    try:
        c.execute("BEGIN IMMEDIATE")
        role = role_of(c, rid, cid)
        ep = epoch_of(c, rid)
        ok = False
        if role and d.get("epoch") == ep:
            ok = True
            if alive(slot(c, rid, other(role))):
                push(c, rid, other(role), {"type": "signal", "epoch": ep, "data": d.get("data")})
        c.execute("COMMIT")
        return 200, {"ok": ok}
    finally:
        c.close()


def api_reset(rid, cid):
    c = db()
    try:
        c.execute("BEGIN IMMEDIATE")
        role = role_of(c, rid, cid)
        if not role:
            c.execute("COMMIT")
            return 200, {"ok": False}
        ep = bump(c, rid)
        if alive(slot(c, rid, other(role))):
            push(c, rid, other(role), {"type": "peer-joined", "epoch": ep})
        c.execute("COMMIT")
        return 200, {"ok": True, "epoch": ep}
    finally:
        c.close()


def api_leave(rid, cid):
    c = db()
    try:
        c.execute("BEGIN IMMEDIATE")
        role = role_of(c, rid, cid)
        if role:
            c.execute("DELETE FROM slots WHERE room=? AND role=?", (rid, role))
            if alive(slot(c, rid, other(role))):
                push(c, rid, other(role), {"type": "peer-left", "epoch": epoch_of(c, rid)})
        c.execute("COMMIT")
        return 200, {"ok": True}
    finally:
        c.close()


def api_poll(rid, cid, after):
    deadline = time.time() + POLL_WAIT
    c = db()
    last_touch = 0
    try:
        while True:
            role = role_of(c, rid, cid)
            if not role:
                return 410, {"error": "not joined"}
            now = time.time()
            if now - last_touch > 5:
                c.execute("UPDATE slots SET seen=? WHERE room=? AND role=?", (now, rid, role))
                last_touch = now
            rows = c.execute("SELECT id, body FROM msgs WHERE room=? AND role=? AND id>? ORDER BY id LIMIT 100",
                             (rid, role, after)).fetchall()
            if rows or now >= deadline:
                msgs = []
                for r in rows:
                    m = json.loads(r["body"])
                    m["seq"] = r["id"]
                    msgs.append(m)
                return 200, {"msgs": msgs, "peer": alive(slot(c, rid, other(role))), "epoch": epoch_of(c, rid)}
            time.sleep(0.3)
    finally:
        c.close()


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
            user = str(int(time.time()) + 6 * 3600)
            cred = base64.b64encode(hmac.new(secret.encode(), user.encode(), hashlib.sha1).digest()).decode()
        else:
            user, cred = os.environ.get("TURN_USER", ""), os.environ.get("TURN_PASS", "")
        servers.append({"urls": urls, "username": user, "credential": cred})
    return {"iceServers": servers}


def respond(start_response, code, obj):
    body = json.dumps(obj).encode()
    start_response("%d OK" % code if code == 200 else "%d Error" % code,
                   [("Content-Type", "application/json"), ("Content-Length", str(len(body))), ("Cache-Control", "no-store")])
    return [body]


def static(start_response, path):
    if path in ("", "/"):
        path = "/index.html"
    fp = os.path.normpath(os.path.join(STATIC, path.lstrip("/")))
    if not fp.startswith(STATIC + os.sep) or not os.path.isfile(fp):
        start_response("404 Not Found", [("Content-Length", "0")])
        return [b""]
    with open(fp, "rb") as f:
        data = f.read()
    start_response("200 OK", [("Content-Type", mimetypes.guess_type(fp)[0] or "application/octet-stream"),
                              ("Content-Length", str(len(data))), ("Cache-Control", "no-cache")])
    return [data]


def application(environ, start_response):
    path = environ.get("PATH_INFO", "") or ""
    method = environ.get("REQUEST_METHOD", "GET")
    if path == "" and method == "GET":   # mounted at /parisa without trailing slash -> relative URLs would break
        loc = (environ.get("SCRIPT_NAME", "") or "") + "/"
        start_response("301 Moved Permanently", [("Location", loc), ("Content-Length", "0")])
        return [b""]
    try:
        if path.startswith("/api/"):
            if path == "/api/ice":
                return respond(start_response, 200, ice_config())
            if path == "/api/poll" and method == "GET":
                from urllib.parse import parse_qs
                q = {k: v[0] for k, v in parse_qs(environ.get("QUERY_STRING", "")).items()}
                try:
                    after = int(q.get("after", 0) or 0)
                except ValueError:
                    after = 0
                return respond(start_response, *api_poll(q.get("room", "")[:64], q.get("cid", "")[:64], after))
            if method == "POST":
                try:
                    n = int(environ.get("CONTENT_LENGTH") or 0)
                except ValueError:
                    n = 0
                try:
                    d = json.loads(environ["wsgi.input"].read(min(n, 65536)).decode("utf-8") or "{}") if n else {}
                except ValueError:
                    d = {}
                rid, cid = str(d.get("room", ""))[:64], str(d.get("cid", ""))[:64]
                if not rid or not cid:
                    return respond(start_response, 400, {"error": "bad request"})
                fn = {"/api/join": lambda: api_join(rid, cid), "/api/send": lambda: api_send(rid, cid, d),
                      "/api/reset": lambda: api_reset(rid, cid), "/api/leave": lambda: api_leave(rid, cid)}.get(path)
                if fn:
                    return respond(start_response, *fn())
            return respond(start_response, 404, {"error": "not found"})
        if method not in ("GET", "HEAD"):
            return respond(start_response, 405, {"error": "method not allowed"})
        return static(start_response, path)
    except sqlite3.Error as e:
        return respond(start_response, 503, {"error": "db: %s" % e})


if __name__ == "__main__":
    import argparse, ssl
    from socketserver import ThreadingMixIn
    from wsgiref.simple_server import make_server, WSGIServer, WSGIRequestHandler

    class Threaded(ThreadingMixIn, WSGIServer):
        daemon_threads = True

    class Quiet(WSGIRequestHandler):
        def log_message(self, *a):
            pass

    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8080)))
    ap.add_argument("--cert")
    ap.add_argument("--key")
    a = ap.parse_args()
    srv = make_server(a.host, a.port, application, server_class=Threaded, handler_class=Quiet)
    if a.cert and a.key:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER) if hasattr(ssl, "PROTOCOL_TLS_SERVER") else ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ctx.load_cert_chain(a.cert, a.key)
        srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    print("Parisa on %s://%s:%d" % ("https" if a.cert else "http", a.host, a.port))
    srv.serve_forever()
