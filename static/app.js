'use strict';
/* Parisa client: 2-person WebRTC call tuned for poor / filtered networks.
 * - signaling: HTTPS long-polling (server.py), works where WebSockets don't
 * - voice: mic -> highpass -> compressor -> makeup gain -> limiter -> Opus (FEC, low bitrate)
 * - video: bitrate / fps / resolution ladder driven by live loss, RTT and bandwidth estimates
 * - reconnect: ICE restart, then full session rebuild
 * - HUD: per-direction quality bars, delay, "they hear you" meter, talk/wait cue
 */
const $ = id => document.getElementById(id);
const params = new URLSearchParams(location.search);
const FORCE_RELAY = params.get('relay') === '1';

// Video ladder: index 0 = audio only. Capture is 640x360; scale divides it.
const TIERS = [
  { name: 'audio only', br: 0, fps: 0, scale: 1 },
  { name: '256p', br: 60e3, fps: 8, scale: 2.5 },
  { name: '360p-low', br: 150e3, fps: 15, scale: 1.75 },
  { name: '360p', br: 400e3, fps: 24, scale: 1 },
  { name: '360p-hq', br: 800e3, fps: 30, scale: 1 },
];
const START_TIER = 2;

const S = {
  cid: (crypto.randomUUID ? crypto.randomUUID() : String(Math.random()).slice(2) + Date.now()),
  room: '', role: '', epoch: 0, seq: 0, running: false,
  pending: [], pc: null, dc: null, polite: false, makingOffer: false, ignoreOffer: false,
  mic: null, cam: null, audioOut: null, ctx: null, facing: 'user',
  tier: START_TIER, goodTicks: 0, goodNeeded: 6, lastTierChange: 0, manualAudioOnly: false,
  micMuted: false, camOff: false,
  rxLevel: 0, txLevel: 0, peerRx: 0, peerRxAt: 0, peerTx: 0,
  speaking: false, speakStart: 0, lastSpeech: 0,
  rtt: null, pingRtt: null, prev: {}, st: {}, lastRx: 0, disconnectedAt: 0, wake: null,
};

/* ---------- signaling ---------- */
async function api(path, body, timeout = 10000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeout);
  try {
    const r = await fetch(path, {
      method: body ? 'POST' : 'GET', signal: ac.signal,
      headers: body ? { 'Content-Type': 'application/json' } : undefined,
      body: body ? JSON.stringify(Object.assign({ room: S.room, cid: S.cid }, body)) : undefined,
    });
    return { status: r.status, data: await r.json() };
  } finally { clearTimeout(t); }
}
const sendSignal = data => api('api/send', { epoch: S.epoch, data }).catch(() => {});

async function joinRoom() {
  const r = await api('api/join', {});
  if (r.status === 409) throw new Error('Room is full (2 people max).');
  if (r.status !== 200) throw new Error('Cannot join.');
  S.role = r.data.role; S.polite = S.role === 'b'; S.epoch = r.data.epoch; S.seq = 0;
  return r.data;
}

async function pollLoop() {
  let backoff = 500;
  while (S.running) {
    try {
      const t0 = Date.now();
      const r = await api(`api/poll?room=${encodeURIComponent(S.room)}&cid=${S.cid}&after=${S.seq}`, null, 35000);
      if (r.status === 410) { await joinRoom(); newSession(); continue; }
      backoff = 500;
      const d = r.data;
      for (const m of d.msgs) {
        S.seq = Math.max(S.seq, m.seq);
        if (m.type === 'peer-joined') { S.epoch = m.epoch; newSession(); }
        else if (m.type === 'peer-left') { setStatus('Other person left. Waiting…'); closePc(); }
        else if (m.type === 'signal' && m.epoch === S.epoch) {
          if (S.pc) onSignal(m.data).catch(console.warn); else S.pending.push(m.data);
        }
      }
      if (d.epoch !== S.epoch && d.peer) { S.epoch = d.epoch; newSession(); }
      else if (d.peer && !S.pc) newSession();
      // server may answer immediately (short-polling mode on shared hosting): don't hammer it
      if (!d.msgs.length && Date.now() - t0 < 400) await new Promise(r => setTimeout(r, 700));
    } catch (e) {
      await new Promise(r => setTimeout(r, backoff));
      backoff = Math.min(backoff * 2, 4000);
    }
  }
}

/* ---------- local media ---------- */
async function getMic() {
  return navigator.mediaDevices.getUserMedia({
    audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: false, channelCount: 1 },
  });
}
async function getCam() {
  return navigator.mediaDevices.getUserMedia({
    video: { facingMode: S.facing, width: { ideal: 640 }, height: { ideal: 360 }, frameRate: { ideal: 30 } },
  });
}

function buildAudioChain(micStream) {
  const ctx = S.ctx;
  const src = ctx.createMediaStreamSource(micStream);
  const hp = ctx.createBiquadFilter(); hp.type = 'highpass'; hp.frequency.value = 90;
  const comp = ctx.createDynamicsCompressor();
  comp.threshold.value = -45; comp.knee.value = 30; comp.ratio.value = 4; comp.attack.value = 0.005; comp.release.value = 0.25;
  const makeup = ctx.createGain(); makeup.gain.value = 2.2;
  const lim = ctx.createDynamicsCompressor();
  lim.threshold.value = -3; lim.knee.value = 0; lim.ratio.value = 20; lim.attack.value = 0.001; lim.release.value = 0.08;
  const dest = ctx.createMediaStreamDestination();
  src.connect(hp); hp.connect(comp); comp.connect(makeup); makeup.connect(lim); lim.connect(dest);
  const an = ctx.createAnalyser(); an.fftSize = 1024; hp.connect(an);   // raw-ish mic level for the meter
  S.micAnalyser = an;
  return dest.stream.getAudioTracks()[0];
}

function rms(an) {
  const b = new Float32Array(an.fftSize); an.getFloatTimeDomainData(b);
  let s = 0; for (let i = 0; i < b.length; i++) s += b[i] * b[i];
  return Math.sqrt(s / b.length);
}

/* ---------- peer connection ---------- */
async function iceServers() {
  try { return (await api('api/ice')).data.iceServers; } catch { return [{ urls: 'stun:stun.l.google.com:19302' }]; }
}

function closePc() {
  if (S.pc) { S.pc.onconnectionstatechange = S.pc.ontrack = S.pc.onnegotiationneeded = S.pc.onicecandidate = null; try { S.pc.close(); } catch {} }
  S.pc = null; S.dc = null; S.prev = {}; S.st = {}; S.peerRxAt = 0; S.rtt = null; S.pingRtt = null;
  $('remote').srcObject = null;
  if (S.remoteSrc) { try { S.remoteSrc.disconnect(); } catch {} S.remoteSrc = null; S.rxAnalyser = null; }
}

async function newSession() {
  closePc();
  S.pending = [];
  setStatus('Connecting…');
  const pc = new RTCPeerConnection({
    iceServers: await iceServers(),
    iceTransportPolicy: FORCE_RELAY ? 'relay' : 'all',
    bundlePolicy: 'max-bundle', rtcpMuxPolicy: 'require',
  });
  if (S.pc) { pc.close(); return; }   // raced with another newSession
  S.pc = pc; S.makingOffer = false; S.ignoreOffer = false;

  pc.onicecandidate = e => { if (e.candidate) sendSignal({ cand: e.candidate }); };
  pc.onnegotiationneeded = async () => {
    try {
      S.makingOffer = true;
      const o = await pc.createOffer(); o.sdp = tuneSdp(o.sdp);
      if (pc.signalingState !== 'stable') return;
      await pc.setLocalDescription(o);
      sendSignal({ desc: pc.localDescription });
    } catch (e) { console.warn(e); } finally { S.makingOffer = false; }
  };
  pc.ontrack = e => {
    const stream = e.streams[0] || new MediaStream([e.track]);
    $('remote').srcObject = stream;
    if (e.track.kind === 'audio') attachRemoteAudio(stream);
    $('remote').play().catch(() => {});
  };
  pc.onconnectionstatechange = onConnState;
  pc.oniceconnectionstatechange = onConnState;

  if (S.role === 'a') setupDc(pc.createDataChannel('ctl', { ordered: false, maxRetransmits: 0 }));
  else pc.ondatachannel = e => setupDc(e.channel);

  const at = S.audioOut, vt = S.cam && S.cam.getVideoTracks()[0];
  const stream = new MediaStream();
  const aSender = pc.addTrack(at, stream);
  const vSender = vt ? pc.addTrack(vt, stream) : null;
  S.aSender = aSender; S.vSender = vSender;
  try {
    const caps = RTCRtpReceiver.getCapabilities('video');
    const rank = c => { const m = c.mimeType.toLowerCase(); return m.includes('vp8') ? 0 : m.includes('h264') ? 1 : m.includes('vp9') ? 2 : m.includes('av1') ? 4 : 3; };
    const tr = pc.getTransceivers().find(t => t.sender === vSender);
    if (tr && caps) tr.setCodecPreferences([...caps.codecs].sort((x, y) => rank(x) - rank(y)));
  } catch {}
  try {
    const p = aSender.getParameters(); if (!p.encodings || !p.encodings.length) p.encodings = [{}];
    p.encodings[0].maxBitrate = 32000; p.encodings[0].priority = 'high'; p.encodings[0].networkPriority = 'high';
    aSender.setParameters(p).catch(() => {});
  } catch {}
  applyTier(true);
  const q = S.pending; S.pending = [];
  for (const d of q) await onSignal(d).catch(console.warn);
}

// Opus: in-band FEC, mono, ~32 kbps cap. Keeps voice intelligible under loss.
function tuneSdp(sdp) {
  const m = sdp.match(/a=rtpmap:(\d+) opus\/48000\/2/);
  if (!m) return sdp;
  const pt = m[1];
  const want = { minptime: 20, useinbandfec: 1, stereo: 0, 'sprop-stereo': 0, maxaveragebitrate: 32000, maxplaybackrate: 24000 };
  const re = new RegExp('a=fmtp:' + pt + ' ([^\\r\\n]*)');
  if (re.test(sdp)) {
    return sdp.replace(re, (_, old) => {
      const o = Object.fromEntries(old.split(';').filter(Boolean).map(kv => kv.split('=')));
      return 'a=fmtp:' + pt + ' ' + Object.entries(Object.assign(o, want)).map(([k, v]) => k + '=' + v).join(';');
    });
  }
  return sdp.replace(m[0], m[0] + '\r\na=fmtp:' + pt + ' ' + Object.entries(want).map(([k, v]) => k + '=' + v).join(';'));
}

async function onSignal(d) {
  const pc = S.pc; if (!pc) return;
  if (d.desc) {
    const offerCollision = d.desc.type === 'offer' && (S.makingOffer || pc.signalingState !== 'stable');
    S.ignoreOffer = !S.polite && offerCollision;
    if (S.ignoreOffer) return;
    await pc.setRemoteDescription(d.desc);
    if (d.desc.type === 'offer') {
      const a = await pc.createAnswer(); a.sdp = tuneSdp(a.sdp);
      await pc.setLocalDescription(a);
      sendSignal({ desc: pc.localDescription });
    }
  } else if (d.cand) {
    try { await pc.addIceCandidate(d.cand); } catch (e) { if (!S.ignoreOffer) console.warn(e); }
  }
}

/* ---------- reconnect ---------- */
function onConnState() {
  const pc = S.pc; if (!pc) return;
  const st = pc.connectionState, ice = pc.iceConnectionState;
  if (st === 'connected' || ice === 'connected' || ice === 'completed') {
    S.disconnectedAt = 0; setStatus('');
  } else if (st === 'disconnected' || ice === 'disconnected') {
    S.disconnectedAt = S.disconnectedAt || Date.now(); setStatus('Connection lost – reconnecting…');
    setTimeout(() => { if (S.pc === pc && pc.iceConnectionState === 'disconnected') pc.restartIce(); }, 2500);
  } else if (st === 'failed' || ice === 'failed') {
    S.disconnectedAt = S.disconnectedAt || Date.now(); setStatus('Connection failed – retrying…');
    pc.restartIce();
  }
}
setInterval(async () => {   // escalate: ICE restart didn't recover -> rebuild both sides
  if (!S.running || !S.pc || !S.disconnectedAt) return;
  if (Date.now() - S.disconnectedAt > 20000) {
    S.disconnectedAt = Date.now();
    try { const r = await api('api/reset', {}); if (r.data.ok) { S.epoch = r.data.epoch; newSession(); } } catch {}
  }
}, 3000);
window.addEventListener('online', () => { if (S.pc && S.pc.connectionState !== 'connected') S.pc.restartIce(); });
document.addEventListener('visibilitychange', async () => {
  if (document.visibilityState !== 'visible' || !S.running) return;
  if (S.ctx && S.ctx.state !== 'running') S.ctx.resume().catch(() => {});
  $('remote').play().catch(() => {});
  if (S.pc && S.pc.connectionState !== 'connected') S.pc.restartIce();
  requestWake();
});

/* ---------- data channel: ping + levels ---------- */
function setupDc(dc) {
  S.dc = dc;
  dc.onmessage = e => {
    let m; try { m = JSON.parse(e.data); } catch { return; }
    if (m.k === 'ping') dc.send(JSON.stringify({ k: 'pong', t: m.t }));
    else if (m.k === 'pong') { const r = performance.now() - m.t; S.pingRtt = S.pingRtt == null ? r : S.pingRtt * 0.7 + r * 0.3; }
    else if (m.k === 'lvl') { S.peerRx = m.rx; S.peerTx = m.tx; S.peerRxAt = performance.now(); }
  };
}
setInterval(() => {
  const dc = S.dc; if (!dc || dc.readyState !== 'open') return;
  try { dc.send(JSON.stringify({ k: 'lvl', rx: +S.rxLevel.toFixed(3), tx: +S.txLevel.toFixed(3) })); } catch {}
}, 150);
setInterval(() => {
  const dc = S.dc; if (dc && dc.readyState === 'open') try { dc.send(JSON.stringify({ k: 'ping', t: performance.now() })); } catch {}
}, 1000);

/* ---------- remote audio (level meter + optional voice leveling) ---------- */
function attachRemoteAudio(stream) {
  const ctx = S.ctx;
  try {
    if (S.remoteSrc) S.remoteSrc.disconnect();
    S.remoteSrc = ctx.createMediaStreamSource(stream);
    S.rxAnalyser = ctx.createAnalyser(); S.rxAnalyser.fftSize = 1024;
    S.remoteSrc.connect(S.rxAnalyser);
    applyBoost();
  } catch (e) { console.warn(e); }
}
function applyBoost() {
  const ctx = S.ctx; if (!S.remoteSrc) return;
  try { S.boostNodes && S.boostNodes.forEach(n => n.disconnect()); } catch {}
  S.boostNodes = [];
  if (S.boost) {
    const hp = ctx.createBiquadFilter(); hp.type = 'highpass'; hp.frequency.value = 90;
    const c = ctx.createDynamicsCompressor(); c.threshold.value = -40; c.knee.value = 25; c.ratio.value = 5; c.attack.value = 0.005; c.release.value = 0.25;
    const g = ctx.createGain(); g.gain.value = 2;
    S.remoteSrc.connect(hp); hp.connect(c); c.connect(g); g.connect(ctx.destination);
    S.boostNodes = [hp, c, g];
    $('remote').muted = true;
  } else {
    $('remote').muted = false;
  }
  $('btnBoost').classList.toggle('on', !!S.boost);
}

/* ---------- stats, quality bars, adaptive video ---------- */
function barLevel(rtt, lossPct, stalled) {
  if (stalled || rtt > 1200 || lossPct > 20) return 0;
  if (rtt > 600 || lossPct > 8) return 1;
  if (rtt > 300 || lossPct > 3) return 2;
  if (rtt > 150 || lossPct > 1) return 3;
  return 4;
}
function setBars(id, lvl, text) { $(id).dataset.l = lvl; $(id.replace('bars', 'val')).textContent = text; }

async function statsTick() {
  const pc = S.pc; if (!pc || pc.connectionState === 'new') return;
  let report; try { report = await pc.getStats(); } catch { return; }
  const cur = { inLost: 0, inRecv: 0, inBytes: 0, jitter: 0, jbDelay: 0, jbCount: 0, upLoss: null, avail: 0, rtt: null, route: '', outFps: 0, w: 0, limit: '', inFps: 0 };
  const byId = {}; report.forEach(r => byId[r.id] = r);
  report.forEach(r => {
    if (r.type === 'candidate-pair' && (r.selected || (r.nominated && r.state === 'succeeded'))) {
      if (r.currentRoundTripTime != null) cur.rtt = r.currentRoundTripTime * 1000;
      if (r.availableOutgoingBitrate) cur.avail = r.availableOutgoingBitrate;
      const l = byId[r.localCandidateId], rm = byId[r.remoteCandidateId];
      if (l) cur.route = (l.candidateType || '') + '/' + (l.relayProtocol || l.protocol || '') + (rm && rm.candidateType === 'relay' ? '+relay' : '');
    } else if (r.type === 'inbound-rtp' && !r.isRemote) {
      cur.inLost += r.packetsLost || 0; cur.inRecv += r.packetsReceived || 0; cur.inBytes += r.bytesReceived || 0;
      if (r.kind === 'audio') { cur.jitter = (r.jitter || 0) * 1000; cur.jbDelay = r.jitterBufferDelay || 0; cur.jbCount = r.jitterBufferEmittedCount || 0; }
      else cur.inFps = r.framesPerSecond || 0;
    } else if (r.type === 'remote-inbound-rtp') {
      if (r.fractionLost != null && (r.kind === 'audio' || cur.upLoss == null)) cur.upLoss = r.fractionLost * 100;
      if (cur.rtt == null && r.roundTripTime != null) cur.rtt = r.roundTripTime * 1000;
    } else if (r.type === 'outbound-rtp' && r.kind === 'video') {
      cur.outFps = r.framesPerSecond || 0; cur.w = r.frameWidth || 0; cur.limit = r.qualityLimitationReason || '';
    }
  });
  const p = S.prev, now = performance.now();
  const dLost = Math.max(0, cur.inLost - (p.inLost || 0)), dRecv = Math.max(0, cur.inRecv - (p.inRecv || 0));
  const downLoss = dLost + dRecv > 0 ? 100 * dLost / (dLost + dRecv) : 0;
  if (cur.inBytes > (p.inBytes || 0)) S.lastRx = now;
  const stalled = !!p.inBytes && now - S.lastRx > 3000;
  const dJb = cur.jbCount - (p.jbCount || 0);
  const jb = dJb > 0 ? 1000 * (cur.jbDelay - (p.jbDelay || 0)) / dJb : (p.jb || 0);
  S.prev = Object.assign({}, cur, { jb });

  let rtt = cur.rtt != null ? cur.rtt : S.pingRtt;
  if (S.pingRtt != null && rtt != null) rtt = Math.max(rtt, S.pingRtt * 0.8);   // trust the slower of the two
  S.rtt = rtt;
  const upLoss = cur.upLoss != null ? cur.upLoss : 0;
  const connected = pc.connectionState === 'connected' || pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed';
  if (!connected || rtt == null) {
    setBars('barsUp', 0, '–'); setBars('barsDown', 0, '–');
  } else {
    setBars('barsUp', barLevel(rtt, upLoss, false), `${Math.round(rtt)}ms · loss ${upLoss.toFixed(0)}%`);
    setBars('barsDown', barLevel(rtt + cur.jitter * 2, downLoss, stalled), stalled ? 'no data' : `jitter ${Math.round(cur.jitter)}ms · loss ${downLoss.toFixed(0)}%`);
  }
  S.st = { oneWay: rtt != null ? rtt / 2 + jb : null };
  const t = TIERS[S.tier];
  $('info').textContent = [
    S.st.oneWay != null ? `delay ≈ ${Math.round(S.st.oneWay)} ms` : '',
    S.manualAudioOnly ? 'audio only' : `send ${t.name}${cur.w ? ' ' + cur.w + 'px ' + Math.round(cur.outFps) + 'fps' : ''}`,
    cur.route,
  ].filter(Boolean).join(' · ');

  adapt({ upLoss, downLoss, rtt, avail: cur.avail, limit: cur.limit, connected });
}

function adapt(m) {
  if (!m.connected || !S.vSender || S.manualAudioOnly) return;
  const now = Date.now();
  const next = TIERS[Math.min(S.tier + 1, TIERS.length - 1)];
  const badLoss = Math.max(m.upLoss, 0) > 10, badRtt = m.rtt != null && m.rtt > 900;
  const overBw = m.avail > 0 && S.tier > 0 && m.avail < (TIERS[S.tier].br + 40e3) * 0.9;
  if ((badLoss || badRtt || overBw) && S.tier > 0) {
    if (now - S.lastTierChange > 3000) {
      S.tier--; S.goodTicks = 0; S.goodNeeded = Math.min(S.goodNeeded * 2, 40); S.lastTierChange = now; applyTier();
    }
    return;
  }
  const good = m.upLoss < 2 && (m.rtt == null || m.rtt < 350) && (m.avail === 0 || m.avail > (next.br + 40e3) * 1.3);
  if (good && S.tier < TIERS.length - 1) {
    if (++S.goodTicks >= S.goodNeeded) { S.tier++; S.goodTicks = 0; S.goodNeeded = Math.max(6, S.goodNeeded - 4); S.lastTierChange = now; applyTier(); }
  } else if (!good) { S.goodTicks = Math.max(0, S.goodTicks - 1); }
}

async function applyTier(initial) {
  const s = S.vSender; if (!s) return;
  const t = S.manualAudioOnly || S.camOff ? TIERS[0] : TIERS[S.tier];
  try {
    const p = s.getParameters(); if (!p.encodings || !p.encodings.length) p.encodings = [{}];
    const e = p.encodings[0];
    e.active = t.br > 0;
    if (t.br > 0) { e.maxBitrate = t.br; e.maxFramerate = t.fps; e.scaleResolutionDownBy = t.scale; }
    p.degradationPreference = 'maintain-framerate';
    await s.setParameters(p);
  } catch (e) { if (!initial) console.warn(e); }
}
setInterval(statsTick, 1000);

/* ---------- levels + talk cue ---------- */
function uiTick() {
  const now = performance.now();
  S.txLevel = S.micAnalyser && !S.micMuted ? rms(S.micAnalyser) : 0;
  S.rxLevel = S.rxAnalyser ? rms(S.rxAnalyser) : 0;
  const talk = S.txLevel > (S.speaking ? 0.012 : 0.02);
  if (talk) { if (!S.speaking) S.speakStart = now; S.lastSpeech = now; S.speaking = true; }
  else if (now - S.lastSpeech > 400) S.speaking = false;
  const peerFresh = now - S.peerRxAt < 1500;
  const hears = peerFresh && S.peerRx > 0.015;
  $('micFill').style.width = Math.min(100, S.txLevel * 400) + '%';
  $('hearFill').style.width = peerFresh ? Math.min(100, S.peerRx * 400) + '%' : '0%';
  const lag = S.st.oneWay != null ? ` (~${Math.round(S.st.oneWay)} ms)` : '';
  let txt, cls;
  if (!S.pc || !peerFresh) { txt = 'Waiting for link…'; cls = 'idle'; }
  else if (S.micMuted) { txt = 'You are muted'; cls = 'warn'; }
  else if (S.speaking && hears) { txt = '✔ They hear you'; cls = 'ok'; }
  else if (S.speaking) {
    const wait = now - S.speakStart;
    if (wait < 1500 + (S.st.oneWay || 0)) { txt = '… sending' + lag; cls = 'warn'; }
    else { txt = '✖ They are NOT hearing you'; cls = 'bad'; }
  }
  else if (hears) { txt = '⏳ Still reaching them – wait' + lag; cls = 'warn'; }
  else if (S.rxLevel > 0.015) { txt = '🗣 They are talking'; cls = 'info'; }
  else { txt = 'Clear – your turn'; cls = 'ok'; }
  const cue = $('cue'); cue.textContent = txt; cue.className = 'cue ' + cls;
}
setInterval(uiTick, 100);

/* ---------- UI ---------- */
function setStatus(t) { $('status').textContent = t; }

async function requestWake() {
  try { if ('wakeLock' in navigator && !S.wake) { S.wake = await navigator.wakeLock.request('screen'); S.wake.onrelease = () => S.wake = null; } } catch {}
}

async function start() {
  const room = $('room').value.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '');
  if (!room) { $('lobbyMsg').textContent = 'Enter a room code.'; return; }
  $('lobbyMsg').textContent = '';
  S.room = room; location.hash = room;
  try {
    S.ctx = new (window.AudioContext || window.webkitAudioContext)({ latencyHint: 'interactive' });
    await S.ctx.resume();
    S.mic = await getMic();
    S.audioOut = buildAudioChain(S.mic);
    try { S.cam = await getCam(); } catch (e) { console.warn('no camera', e); S.cam = null; S.camOff = true; $('btnCam').classList.add('off'); }
    const j = await joinRoom();
    $('call').hidden = false; $('lobby').hidden = true;
    if (S.cam) $('local').srcObject = S.cam;
    S.running = true;
    setStatus(j.peer ? 'Connecting…' : 'Waiting for the other person…\nShare this page link.');
    requestWake();
    pollLoop();
    if (j.peer) newSession();
  } catch (e) {
    const msg = e.name === 'NotAllowedError' ? 'Microphone permission denied.' : (e.message || 'Could not start.');
    $('lobbyMsg').textContent = msg;
    if (S.mic) S.mic.getTracks().forEach(t => t.stop());
  }
}

function hangup() {
  S.running = false;
  api('api/leave', {}).catch(() => {});
  closePc();
  [S.mic, S.cam].forEach(s => s && s.getTracks().forEach(t => t.stop()));
  if (S.ctx) S.ctx.close().catch(() => {});
  if (S.wake) S.wake.release().catch(() => {});
  location.reload();
}

$('join').onclick = start;
$('room').addEventListener('keydown', e => { if (e.key === 'Enter') start(); });
$('btnHang').onclick = hangup;
$('btnMic').onclick = () => {
  S.micMuted = !S.micMuted;
  if (S.audioOut) S.audioOut.enabled = !S.micMuted;
  $('btnMic').classList.toggle('off', S.micMuted); $('btnMic').textContent = S.micMuted ? '🔇' : '🎤';
};
$('btnCam').onclick = () => {
  if (!S.cam) return;
  S.camOff = !S.camOff;
  S.manualAudioOnly = S.camOff;
  S.cam.getVideoTracks().forEach(t => t.enabled = !S.camOff);
  $('btnCam').classList.toggle('off', S.camOff);
  applyTier();
};
$('btnFlip').onclick = async () => {
  if (!S.cam) return;
  S.facing = S.facing === 'user' ? 'environment' : 'user';
  try {
    S.cam.getTracks().forEach(t => t.stop());
    S.cam = await getCam();
    if (S.vSender) await S.vSender.replaceTrack(S.cam.getVideoTracks()[0]);
    $('local').srcObject = S.cam; $('local').classList.toggle('rear', S.facing === 'environment');
  } catch (e) { console.warn(e); }
};
$('btnBoost').onclick = () => { S.boost = !S.boost; applyBoost(); };
window.addEventListener('beforeunload', () => { if (S.running) navigator.sendBeacon && navigator.sendBeacon('api/leave', new Blob([JSON.stringify({ room: S.room, cid: S.cid })], { type: 'application/json' })); });

if (location.hash.length > 1) $('room').value = decodeURIComponent(location.hash.slice(1));
else $('room').value = Math.random().toString(36).slice(2, 8);
