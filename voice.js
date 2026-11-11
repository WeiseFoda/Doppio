/* Doppio · voz em tempo real (WebRTC em malha).
   Sinalização pelo WebSocket do servidor (mensagens voice-* e rtc); o áudio/vídeo vai direto entre as pessoas.
   Só funciona no modo servidor (abrindo o app por `node server/index.js`). Usa as variáveis globais do app
   (me, ss, REMOTE, RWS, localStream, camOn, screenOn, screenStream, muted, deafened) e é chamado por:
   joinVoice/leaveVoice, applyMuteState, câmera/tela (Voice.sync), updateCallUI (Voice.renderTiles) e rtMsg (Voice.onMsg). */
const Voice = (() => {
  const ICE_DEFAULT = [{ urls: 'stun:stun.l.google.com:19302' }];
  const out = { mic: new MediaStream(), cam: new MediaStream(), screen: new MediaStream() }; // só identificam os fluxos enviados
  const tiles = new Map(), an = new Map(); // uid -> bloco de vídeo ; uid|'me' -> analisador de voz
  let rosters = {}, watching = new Set(), st = fresh(), cfg = null, ac = null, tickT = null, statT = null, uiT = null, rendT = null;
  function fresh() { return { room: null, joined: false, peers: new Map(), meta: new Map(), speaking: new Set(), q: 3, reconnecting: false, lastState: '', micTry: 0, micFails: 0 }; }

  const avail = () => typeof REMOTE !== 'undefined' && REMOTE && !!window.RTCPeerConnection && !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);
  const send = o => { try { if (RWS && RWS.readyState === 1) { RWS.send(JSON.stringify(o)); return true; } } catch {} return false; };
  const sig = (to, d) => send({ t: 'rtc', to, d });
  const ctx = () => ac || (ac = new (window.AudioContext || window.webkitAudioContext)());
  const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
  const gx = (k, d) => (typeof X === 'function' ? X(k, d) : d);
  const vol = uid => clamp(gx('vol:' + uid, 100) / 100, 0, 1) * clamp(gx('va', 100) / 100, 0, 1);
  const schedUI = () => { if (uiT) return; uiT = setTimeout(() => { uiT = null; try { if (typeof inVoice !== 'undefined' && inVoice) updateCallUI(); } catch {} }, 60); };
  const schedRender = () => { if (rendT) return; rendT = setTimeout(() => { rendT = null; try { render(); } catch {} }, 120); };
  function beep(f) { try { if (typeof deafened !== 'undefined' && deafened) return; const c = ctx(), o = c.createOscillator(), g = c.createGain(); o.frequency.value = f; g.gain.value = .04; o.connect(g); g.connect(c.destination); o.start(); o.stop(c.currentTime + .09); } catch {} }

  /* ---- mídia local (vem do app) ---- */
  const micTrack = () => (typeof localStream !== 'undefined' && localStream) ? localStream.getAudioTracks().find(t => t.readyState === 'live') || null : null;
  const camTrack = () => (typeof camOn !== 'undefined' && camOn && localStream) ? localStream.getVideoTracks().find(t => t.readyState === 'live') || null : null;
  const scr = k => (typeof screenOn !== 'undefined' && screenOn && screenStream) ? screenStream.getTracks().find(t => t.kind === k && t.readyState === 'live') || null : null;

  /* ---- pares ---- */
  function reconcile(p) { // põe/tira câmera, tela e microfone nas conexões conforme o estado do app
    const m = micTrack(); if (p.tx.mic && p.tx.mic.track !== m) p.tx.mic.replaceTrack(m).catch(() => {});
    [['cam', camTrack(), out.cam], ['sv', scr('video'), out.screen], ['sa', scr('audio'), out.screen]].forEach(([k, t, os]) => {
      const s = p.tx[k];
      if (t && !s) { if (k === 'sv') t.contentHint = 'detail'; p.tx[k] = p.pc.addTrack(t, os); }
      else if (t && s && s.track !== t) s.replaceTrack(t).catch(() => {});
      else if (!t && s) { try { p.pc.removeTrack(s); } catch {} p.tx[k] = null; }
    });
  }
  function makePeer(uid, initiator) {
    const pc = new RTCPeerConnection({ iceServers: (cfg && cfg.iceServers) || ICE_DEFAULT, bundlePolicy: 'max-bundle' });
    // "negociação perfeita": um lado é educado (cede em colisão de ofertas) — decidido pelo id, igual nos dois lados
    const p = { uid, pc, polite: String(me.id) > String(uid), makingOffer: false, ignoreOffer: false, noNeg: !initiator, tx: {}, streams: new Map(), audio: new Map(), q: 3, last: null, wd: null, bytesIn: 0, rtt: null, since: (st.meta.get(uid) || {}).since };
    st.peers.set(uid, p);
    // Só quem oferece cria a linha de áudio (sempre ouve, mesmo sem microfone). Quem responde adota a linha da oferta (adopt):
    // o Chromium não reaproveita transceivers criados com addTransceiver para ofertas recebidas, o que geraria uma 2ª negociação.
    if (initiator) { p.tx.mic = pc.addTransceiver('audio', { direction: 'sendrecv', streams: [out.mic] }).sender; reconcile(p); }
    pc.onnegotiationneeded = async () => {
      if (p.noNeg) return;
      try { p.makingOffer = true; await pc.setLocalDescription(); sig(uid, { desc: pc.localDescription }); } catch {} finally { p.makingOffer = false; }
    };
    pc.onicecandidate = e => { if (e.candidate) sig(uid, { cand: e.candidate }); };
    pc.ontrack = e => attach(p, e);
    pc.onconnectionstatechange = () => onConn(p);
    return p;
  }
  function closePeer(uid, reset) {
    const p = st.peers.get(uid); if (!p) return;
    clearTimeout(p.wd); if (reset) sig(uid, { reset: 1 });
    p.audio.forEach(a => { try { a.pause(); a.srcObject = null; } catch {} });
    try { p.pc.onconnectionstatechange = p.pc.ontrack = p.pc.onnegotiationneeded = p.pc.onicecandidate = null; p.pc.close(); } catch {}
    an.delete(uid); st.peers.delete(uid); setSpeaking(uid, false);
  }
  function rebuild(uid) { // conexão não se recuperou: recria do zero e avisa o outro lado para fazer o mesmo
    const p = st.peers.get(uid); if (!p || !st.room) return;
    closePeer(uid, true); if (st.meta.has(uid)) makePeer(uid, true); schedUI();
  }
  function onConn(p) {
    const s = p.pc.connectionState; clearTimeout(p.wd); schedUI();
    if (s === 'failed') { try { p.pc.restartIce(); } catch {} p.wd = setTimeout(() => rebuild(p.uid), 10000); }
    else if (s === 'disconnected') p.wd = setTimeout(() => { try { p.pc.restartIce(); } catch {} p.wd = setTimeout(() => { if (p.pc.connectionState !== 'connected') rebuild(p.uid); }, 10000); }, 3000);
  }
  function adopt(p) { // depois de receber uma oferta: usa a linha de áudio que veio nela para também enviar o próprio microfone
    if (!p.tx.mic) {
      const t = p.pc.getTransceivers().find(x => x.mid !== null && x.receiver.track && x.receiver.track.kind === 'audio');
      if (t) { t.direction = 'sendrecv'; try { t.sender.setStreams(out.mic); } catch {} p.tx.mic = t.sender; }
    }
    reconcile(p);
  }
  async function onSignal(from, d) {
    if (!st.room || !st.meta.has(from) || !d || typeof d !== 'object') return;
    if (d.reset) return closePeer(from);
    const p = st.peers.get(from) || makePeer(from, false), pc = p.pc;
    try {
      if (d.desc) {
        const collision = d.desc.type === 'offer' && (p.makingOffer || pc.signalingState !== 'stable');
        p.ignoreOffer = !p.polite && collision; if (p.ignoreOffer) return;
        await pc.setRemoteDescription(d.desc);
        if (d.desc.type === 'offer') { adopt(p); await pc.setLocalDescription(); sig(from, { desc: pc.localDescription }); p.noNeg = false; }
      } else if (d.cand) {
        try { await pc.addIceCandidate(d.cand); } catch (e) { if (!p.ignoreOffer) throw e; }
      }
    } catch (e) { console.warn('rtc', e); }
  }

  /* ---- recepção: áudio, vídeo, quem está falando ---- */
  function applyAudio() {
    st.peers.forEach(p => p.audio.forEach(a => {
      a.muted = !!(typeof deafened !== 'undefined' && deafened); a.volume = vol(p.uid);
      const sink = gx('audiooutput', ''); if (sink && a.setSinkId) a.setSinkId(sink).catch(() => {});
    }));
  }
  function attach(p, e) {
    const stream = e.streams[0] || new MediaStream([e.track]); p.streams.set(stream.id, stream);
    stream.onremovetrack = schedUI; e.track.onended = schedUI; e.track.onmute = schedUI; e.track.onunmute = schedUI;
    if (e.track.kind === 'audio') {
      if (!p.audio.has(stream.id)) {
        const a = new Audio(); a.autoplay = true; a.playsInline = true; a.srcObject = stream; p.audio.set(stream.id, a); applyAudio();
        a.play().catch(() => {});
        if (!an.has(p.uid) && (st.meta.get(p.uid) || {}).screenStream !== stream.id) startAnalyser(p.uid, stream);
      }
    }
    schedUI();
  }
  function startAnalyser(key, stream) {
    try { const a = ctx().createAnalyser(); a.fftSize = 2048; ctx().createMediaStreamSource(stream).connect(a); an.set(key, { a, buf: new Uint8Array(a.fftSize), hold: 0, on: false, tid: null }); } catch {}
  }
  function setSpeaking(uid, on) {
    if (on) st.speaking.add(uid); else st.speaking.delete(uid);
    document.querySelectorAll('[data-uid]').forEach(e => { if (e.dataset.uid === uid) e.classList.toggle('speaking', on); });
  }
  function tick() {
    const mt = micTrack(), L = an.get('me');
    if (mt && (!L || L.tid !== mt.id)) { an.delete('me'); startAnalyser('me', new MediaStream([mt])); if (an.get('me')) an.get('me').tid = mt.id; }
    if (!mt && L) { an.delete('me'); setSpeaking(me.id, false); }
    const now = performance.now();
    an.forEach((o, key) => {
      o.a.getByteTimeDomainData(o.buf); let s = 0;
      for (let i = 0; i < o.buf.length; i++) { const v = (o.buf[i] - 128) / 128; s += v * v; }
      if (Math.sqrt(s / o.buf.length) > 0.02) o.hold = now + 400;
      const on = now < o.hold; if (on !== o.on) { o.on = on; setSpeaking(key === 'me' ? me.id : key, on); }
    });
    // microfone sumiu (dispositivo desconectado): tenta reabrir algumas vezes
    if (st.joined && typeof inVoice !== 'undefined' && inVoice && !mt && typeof localStream !== 'undefined' && localStream && st.micFails < 3 && now - st.micTry > 3000) {
      st.micTry = now; ensureMic().then(sync).catch(() => { st.micFails++; });
    }
  }
  async function stats() {
    st.reconnecting = !(RWS && RWS.readyState === 1) || [...st.peers.values()].some(p => p.pc.connectionState === 'disconnected' || p.pc.connectionState === 'failed');
    let worst = 3;
    for (const p of st.peers.values()) {
      try {
        const rep = await p.pc.getStats(); let rtt = null, lost = 0, recv = 0, bytes = 0;
        rep.forEach(r => {
          if (r.type === 'candidate-pair' && (r.nominated || r.selected) && r.state === 'succeeded' && r.currentRoundTripTime != null) rtt = r.currentRoundTripTime;
          if (r.type === 'inbound-rtp' && r.kind === 'audio') { lost += r.packetsLost || 0; recv += r.packetsReceived || 0; bytes += r.bytesReceived || 0; }
        });
        const dl = p.last ? lost - p.last.lost : 0, dr = p.last ? recv - p.last.recv : 0, loss = dl + dr > 0 ? dl / (dl + dr) : 0;
        p.last = { lost, recv }; p.bytesIn = bytes; p.rtt = rtt; p.loss = loss;
        p.q = p.pc.connectionState !== 'connected' ? 0 : ((rtt != null && rtt > .35) || loss > .1) ? 1 : ((rtt != null && rtt > .15) || loss > .03) ? 2 : 3;
      } catch {}
      worst = Math.min(worst, p.q);
    }
    st.q = st.peers.size ? worst : 3; schedUI();
  }
  function startLoops() { if (!tickT) tickT = setInterval(tick, 50); if (!statT) statT = setInterval(stats, 2500); }

  /* ---- sala ---- */
  function sendState() {
    const s = { t: 'voice-state', muted: !!muted, deaf: !!deafened, cam: !!camTrack(), screen: !!scr('video'), camStream: out.cam.id, screenStream: out.screen.id }, k = JSON.stringify(s);
    if (k !== st.lastState) { st.lastState = k; send(s); }
  }
  function sync() { if (!st.room || !st.joined) return; st.peers.forEach(reconcile); sendState(); applyAudio(); schedUI(); }
  function hardLeave(notify) {
    if (notify && st.room) send({ t: 'voice-leave' });
    [...st.peers.keys()].forEach(uid => closePeer(uid)); an.clear(); tiles.clear(); clearInterval(tickT); clearInterval(statT); tickT = statT = null;
    document.querySelectorAll('.speaking').forEach(e => e.classList.remove('speaking'));
    st = fresh(); schedRender();
  }
  function failLocal(msg) { toast(msg, true); st.room = null; hardLeave(false); if (typeof leaveVoice === 'function') leaveVoice(); }

  function onMsg(m) {
    switch (m.t) {
      case 'voice-rosters': Object.entries(m.rooms || {}).forEach(([cid, us]) => { rosters[m.sid + '/' + cid] = us; us.forEach(u => remoteUser(u.id)); }); Object.keys(rosters).forEach(k => { if (k.startsWith(m.sid + '/') && !(m.rooms || {})[k.split('/')[1]]) delete rosters[k]; }); schedRender(); break;
      case 'voice-roster': if (m.users && m.users.length) { rosters[m.sid + '/' + m.cid] = m.users; m.users.forEach(u => remoteUser(u.id)); } else delete rosters[m.sid + '/' + m.cid]; schedRender(); break;
      case 'voice-joined': {
        if (!st.room || st.room.sid !== m.sid || st.room.cid !== m.cid) return;
        const was = st.joined; st.joined = true; st.reconnecting = false;
        const ids = new Set(m.peers.map(p => p.id));
        [...st.peers.keys()].forEach(id => { const q = m.peers.find(p => p.id === id); if (!m.resumed || !q || q.since !== st.peers.get(id).since) closePeer(id, true); });
        [...st.meta.keys()].forEach(id => { if (!ids.has(id)) st.meta.delete(id); });
        m.peers.forEach(pu => { st.meta.set(pu.id, pu); remoteUser(pu.id); });
        m.peers.forEach(pu => { if (!st.peers.has(pu.id)) makePeer(pu.id, true); });
        st.lastState = ''; startLoops(); sync(); if (!was) beep(660); break;
      }
      case 'voice-peer': {
        if (!st.room || !st.joined) return; const id = m.peer.id;
        if (m.op === 'leave') { st.meta.delete(id); closePeer(id); beep(330); }
        else { if (m.op === 'join') { closePeer(id); beep(660); } st.meta.set(id, { ...(st.meta.get(id) || {}), ...m.peer }); remoteUser(id); }
        schedUI(); break;
      }
      case 'rtc': onSignal(m.from, m.d); break;
      case 'voice-kicked': if (st.room) failLocal(m.reason || 'Você saiu da sala de voz.'); break;
      case 'voice-error': if (st.room) failLocal(m.error || 'Não foi possível entrar na sala de voz.'); else toast(m.error || 'Erro de voz', true); break;
    }
  }

  /* ---- interface ---- */
  function volPop(uid, ev) {
    document.querySelectorAll('.ctxm').forEach(x => x.remove());
    const m = el('div', 'ctxm vpop'), r = el('input'), lab = el('span', '', ''); m.onclick = e => e.stopPropagation();
    r.type = 'range'; r.min = 0; r.max = 100; r.value = gx('vol:' + uid, 100); lab.textContent = r.value + '%';
    r.oninput = () => { lab.textContent = r.value + '%'; XS('vol:' + uid, +r.value); applyAudio(); };
    m.append(el('b', '', nm(uid)), el('div', 'mut', 'Volume para você'), r, lab); document.body.append(m);
    m.style.cssText = 'width:220px;padding:12px;left:' + clamp((ev ? ev.clientX : 100), 8, innerWidth - 236) + 'px;top:' + clamp((ev ? ev.clientY : 100), 8, innerHeight - 120) + 'px';
  }
  const bars = q => ['✕', '▂', '▂▄', '▂▄▆'][clamp(q, 0, 3)];
  function mkTile(uid) {
    const t = el('div', 'call-tile'), vb = el('button', 'tvol', '🔈'); t.dataset.uid = uid; vb.title = 'Volume desta pessoa';
    vb.onclick = e => { e.stopPropagation(); volPop(uid, e); };
    t.append(el('div', 'placeholder'), el('div', 'label'), vb); return t;
  }
  function updateTile(t, uid, m, p) {
    const ph = t.querySelector('.placeholder'), lab = t.querySelector('.label');
    const cs = p && p.streams.get(m.camStream), vt = cs && cs.getVideoTracks().find(x => x.readyState === 'live'); let v = t.querySelector('video');
    if (m.cam && vt) {
      if (!v) { v = document.createElement('video'); v.autoplay = true; v.muted = true; v.playsInline = true; t.insertBefore(v, ph); }
      if (v.srcObject !== cs) v.srcObject = cs; ph.hidden = true;
    } else { if (v) v.remove(); ph.hidden = false; ph.replaceChildren(av(uid, 72), el('span', '', nm(uid) + (m.muted ? ' (mudo)' : ''))); }
    const ic = (m.muted ? '🔇' : '🎤') + (m.deaf ? ' 🙉' : '') + (m.cam ? ' 📷' : '') + (m.screen ? ' 🖥️' : '');
    lab.replaceChildren(document.createTextNode(nm(uid) + (m.away ? ' · reconectando…' : '') + (p && p.rtt != null ? ' · ' + Math.round(p.rtt * 1000) + 'ms' : '') + (p && p.loss > 0.02 ? ' · ' + Math.round(p.loss * 100) + '%loss' : '')), el('span', 'icons', ic + ' '), el('span', 'q q' + (p ? p.q : 0), p ? bars(p.q) : '…'));
  }
  function renderTiles(grid) {
    if (!st.room) return;
    const keep = new Set();
    st.meta.forEach((m, uid) => {
      const p = st.peers.get(uid); let t = tiles.get(uid); if (!t) tiles.set(uid, t = mkTile(uid));
      keep.add(uid); updateTile(t, uid, m, p); grid.append(t);
      const ss0 = p && p.streams.get(m.screenStream), sv = ss0 && ss0.getVideoTracks().find(x => x.readyState === 'live'), key = uid + ':s';
      if (m.screen && sv) {
        let s = tiles.get(key);
        if (!s) {
          s = el('div', 'call-tile screen'); const v = document.createElement('video'); v.autoplay = true; v.muted = true; v.playsInline = true; v.ondblclick = () => v.requestFullscreen && v.requestFullscreen();
          s.append(v, el('div', 'label', '🖥️ Tela de ' + nm(uid) + ' (duplo clique: tela cheia)')); tiles.set(key, s);
        }
        const v = s.querySelector('video'); if (v.srcObject !== ss0) v.srcObject = ss0; keep.add(key); grid.append(s);
      }
    });
    [...tiles.keys()].forEach(k => { if (!keep.has(k)) tiles.delete(k); });
    grid.querySelectorAll('[data-uid]').forEach(e => e.classList.toggle('speaking', st.speaking.has(e.dataset.uid)));
  }
  function sidebar(c, imIn) { // quem está em cada canal de voz, para todos da comunidade
    let us = (rosters[ss + '/' + c.id] || []).slice();
    if (!(typeof REMOTE !== 'undefined' && REMOTE)) us = imIn ? [{ id: me.id, muted, deaf: deafened }] : [];
    if (!us.length) return null;
    const box = el('div', 'voice-users');
    us.forEach(u => {
      const d = el('div', 'vu' + (st.speaking.has(u.id) ? ' speaking' : '')); d.dataset.uid = u.id;
      d.append(av(u.id, 20), el('span', '', nm(u.id) + (u.id === me.id ? ' (você)' : '')),
        el('span', 'mute-icon', (u.muted ? '🔇' : '') + (u.deaf ? '🙉' : '') + (u.cam ? '📷' : '') + (u.screen ? '🖥️' : '')));
      if (u.id !== me.id && typeof REMOTE !== 'undefined' && REMOTE) { d.onclick = e => { e.stopPropagation(); volPop(u.id, e); }; d.title = 'Clique para ajustar o volume'; }
      box.append(d);
    });
    return box;
  }

  /* ---- estilos ---- */
  const css = document.createElement('style');
  css.textContent = `.call-tile{position:relative}.call-tile.speaking{outline:3px solid var(--green);outline-offset:-3px;transition:outline-color .1s}
.vu.speaking .av{box-shadow:0 0 0 2px var(--green)}.vu.speaking{color:var(--tx)}
.tvol{position:absolute;top:8px;right:8px;width:30px;height:30px;border-radius:50%;background:rgba(0,0,0,.5);opacity:0;transition:opacity .15s}.call-tile:hover .tvol,.tvol:focus{opacity:1}
.call-tile .q{margin-left:6px;font-size:11px;letter-spacing:-1px}.q3{color:var(--green)}.q2{color:var(--yellow)}.q1,.q0{color:var(--red)}
.vpop input{width:100%;margin:8px 0 2px;accent-color:var(--acc)}.call-tile.screen video{object-fit:contain;background:#000}`;
  document.head.append(css);
  window.addEventListener('pagehide', () => { if (st.room) send({ t: 'voice-leave' }); });

  return {
    avail,
    async join(ch) {
      if (!avail()) { toast('A voz entre pessoas só funciona abrindo o Doppio pelo servidor (node server/index.js).', true); return; }
      if (!window.isSecureContext) { toast('Microfone e câmera exigem HTTPS (ou localhost).', true); return; }
      if (st.room) hardLeave(true);
      try { cfg = await api('GET', '/api/rtc-config'); } catch { cfg = { iceServers: ICE_DEFAULT }; }
      try { await ctx().resume(); } catch {}
      st.room = { sid: ss, cid: ch.id };
      if (!send({ t: 'voice-join', sid: ss, cid: ch.id })) failLocal('Sem conexão com o servidor. Tente de novo em instantes.');
    },
    leave() { if (st.room) hardLeave(true); }, // avisa o servidor e fecha todas as conexões
    sync, onMsg, renderTiles, sidebar, volPop,
    watch(sid) { watching.forEach(s => { if (s !== sid) send({ t: 'voice-unwatch', sid: s }); }); watching = new Set(sid ? [sid] : []); if (sid) send({ t: 'voice-watch', sid }); },
    onReady() { // WebSocket (re)conectou: reassina os rostos e retoma a sala (o servidor guarda a vaga por alguns segundos)
      watching.forEach(s => send({ t: 'voice-watch', sid: s })); if (typeof ss !== 'undefined' && ss && !watching.has(ss)) this.watch(ss);
      if (st.room) { st.reconnecting = true; send({ t: 'voice-join', sid: st.room.sid, cid: st.room.cid }); }
    },
    statusText() {
      if (!st.room) return ''; if (!st.joined || st.reconnecting) return '⏳ Reconectando…'; if (!st.peers.size) return '';
      const label = ['', '📶 Conexão ruim', '📶 Conexão instável', '📶 Conexão boa'][clamp(st.q, 0, 3)];
      const peers = [...st.peers.values()];
      const rtts = peers.map(p => p.rtt).filter(x => x != null && isFinite(x));
      const losses = peers.map(p => p.loss).filter(x => x != null && isFinite(x));
      const parts = [label];
      if (rtts.length) parts.push(Math.round(rtts.reduce((a, b) => a + b, 0) / rtts.length * 1000) + ' ms');
      if (losses.length) {
        const avgL = losses.reduce((a, b) => a + b, 0) / losses.length;
        if (avgL > 0.005) parts.push((avgL * 100).toFixed(1) + '% loss');
      }
      return parts.filter(Boolean).join(' · ');
    },
    quality() {
      const peers = [...st.peers.values()];
      const rtts = peers.map(p => p.rtt).filter(x => x != null && isFinite(x));
      const losses = peers.map(p => p.loss).filter(x => x != null && isFinite(x));
      const avgMs = rtts.length ? Math.round(rtts.reduce((a, b) => a + b, 0) / rtts.length * 1000) : null;
      const avgLoss = losses.length ? losses.reduce((a, b) => a + b, 0) / losses.length : null;
      return { q: st.q, peers: st.peers.size, rttMs: avgMs, loss: avgLoss };
    },
    debug() {
      return { room: st.room, joined: st.joined, reconnecting: st.reconnecting, speaking: [...st.speaking], q: st.q, roster: rosters, meta: [...st.meta.keys()],
        peers: [...st.peers.values()].map(p => ({ uid: p.uid, state: p.pc.connectionState, ice: p.pc.iceConnectionState, bytesIn: p.bytesIn, q: p.q, rtt: p.rtt, polite: p.polite,
          audio: [...p.audio.values()].map(a => ({ muted: a.muted, volume: a.volume, paused: a.paused })),
          video: [...p.streams.values()].flatMap(s => s.getVideoTracks()).filter(t => t.readyState === 'live').length })) };
    },
    _test: { rebuild, peers: () => st.peers, dump: uid => { const p = st.peers.get(uid); return p && { sig: p.pc.signalingState, polite: p.polite, noNeg: p.noNeg, tr: p.pc.getTransceivers().map(t => ({ mid: t.mid, dir: t.direction, cur: t.currentDirection, send: !!t.sender.track, kind: (t.receiver.track || {}).kind })) }; } },
  };
})();
