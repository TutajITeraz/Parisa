# Parisa

Two-person video call for weak, filtered internet. Mobile-first, no accounts, no VPN.
Plain HTML/JS client + a ~200-line standard-library Python server.

## Run

```bash
python3 server.py --port 8080                      # http (localhost only works for mic/camera)
python3 server.py --port 443 --cert fullchain.pem --key privkey.pem   # https
```
Browsers only allow mic/camera on **https** (or localhost), so put it behind TLS
(built-in `--cert/--key`, or Caddy/nginx). Open the page on both phones, use the
same room code (the link `https://host/#room` carries it). A third person gets "room full".

## What it does

| Need | How |
|---|---|
| Works where traffic is filtered | Signaling is plain HTTPS long-polling (no WebSocket). Media is WebRTC; add a TURN server on **TCP/TLS 443** (below) so media looks like HTTPS when UDP is blocked. |
| Reconnects | ICE restart on drop / network change / app resume, full session rebuild if not recovered in 20 s. Page reload rejoins. |
| Connection bars + lag | Two 4-bar meters (You→them, Them→you) from RTT, loss and jitter, plus one-way delay estimate and the route in use (`host/udp`, `relay/tcp`…). |
| "Do they hear me?" | The other side reports the level it *receives* over a data channel. The cue shows ✔ they hear you / ✖ not heard / ⏳ still reaching them – wait (your words are still in flight) / clear – your turn. |
| Voice enhancement | Mic → high-pass → compressor → makeup gain → limiter, plus browser echo cancel + noise suppression. Opus mono, in-band FEC, ≤32 kbps. 🔊 button levels the incoming voice too. |
| Adaptive video | Ladder: audio-only → 256p/8fps/60k → 360p-low/15fps/150k → 360p/24fps/400k → 360p/30fps/800k. Drops fast on loss >10 %, RTT >900 ms or bandwidth estimate shortfall; climbs slowly (and slower after each drop). Codec preference VP8 → H.264 → VP9 → AV1. 📷 button = audio-only. |

## Restrictive networks: TURN

Direct UDP is often blocked. Run coturn on a server with a real certificate, listening on 443:

```
# /etc/turnserver.conf
listening-port=3478
tls-listening-port=443
cert=/etc/letsencrypt/live/turn.example.com/fullchain.pem
pkey=/etc/letsencrypt/live/turn.example.com/privkey.pem
use-auth-secret
static-auth-secret=CHANGE_ME
realm=turn.example.com
no-multicast-peers
```
Then start Parisa with:
```bash
export TURN_URLS="turns:turn.example.com:443?transport=tcp,turn:turn.example.com:3478?transport=tcp"
export TURN_SECRET=CHANGE_ME          # or TURN_USER / TURN_PASS
export STUN_URLS=""                    # optional: drop public STUN if it is blocked
python3 server.py
```
(Put TURN on a different host/IP than the web server if both need 443.) Add `?relay=1`
to the page URL to force relay-only for testing. Without TURN, calls still work when
the two phones can reach each other directly or via STUN.

## Files
`server.py` – static files + signaling + ICE config · `static/` – the app.
