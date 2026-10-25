/* Связь между игроками для «Дома у трассы».
   На claude.ai — комната Claude (capability room).
   На любом другом сайте — прямое соединение браузеров через PeerJS:
   создатель комнаты становится «узлом», остальные подключаются к нему,
   узел собирает presence всех игроков и рассылает обратно.
   Наружу у обоих вариантов одинаковый вид: room.peers(), room.presence(patch), room.onError(fn). */
(function () {
  'use strict';
  const PREFIX = 'domutrassy-';

  async function claudeRoom() {
    if (!window.claude || !window.claude.use) return null;
    try {
      const r = await Promise.race([window.claude.use('room'), new Promise(res => setTimeout(() => res(null), 4000))]);
      return r || null;
    } catch (e) { return null; }
  }

  // ---- комната Claude ----
  async function openClaude(lobby, code) {
    let rm;
    try { rm = await lobby.join('dom-' + code); } catch (e) { rm = lobby; }
    const errs = [];
    rm.onPeers(() => { }, e => errs.forEach(f => f(e && e.code)));
    return {
      kind: 'claude',
      peers: () => rm.peers().map(p => ({ peer: p.peer, sameTab: !!p.sameTab, presence: p.presence || {} })),
      presence: patch => rm.presence(patch),
      onError: fn => errs.push(fn),
      close: () => { try { rm.leave && rm.leave(); } catch (e) { } }
    };
  }

  // ---- PeerJS ----
  function newPeer(id) {
    return new Promise((res, rej) => {
      const p = id ? new window.Peer(id, { debug: 0 }) : new window.Peer({ debug: 0 });
      const t = setTimeout(() => { rej('timeout'); try { p.destroy(); } catch (e) { } }, 12000);
      p.on('open', () => { clearTimeout(t); res(p); });
      p.on('error', e => { clearTimeout(t); rej(e && e.type || 'error'); });
    });
  }
  function applyPatch(obj, patch) {
    const o = Object.assign({}, obj);
    for (const k in patch) { if (patch[k] === null) delete o[k]; else o[k] = patch[k]; }
    return o;
  }

  async function hostPeer(code) {
    const peer = await newPeer(PREFIX + code);
    const conns = new Map();        // id -> DataConnection
    const pres = new Map();         // id -> presence
    let mine = {};
    const errs = [];
    let snap = null;
    const broadcast = () => {
      const all = { [peer.id]: mine };
      pres.forEach((v, k) => { all[k] = v; });
      const msg = { t: 'all', all };
      conns.forEach(c => { if (c.open) { try { c.send(msg); } catch (e) { } } });
    };
    peer.on('connection', c => {
      c.on('open', () => { conns.set(c.peer, c); broadcast(); });
      c.on('data', m => { if (m && m.t === 'p' && m.p && typeof m.p === 'object') { pres.set(c.peer, m.p); snap = null; } });
      c.on('close', () => { conns.delete(c.peer); pres.delete(c.peer); snap = null; });
      c.on('error', () => { conns.delete(c.peer); pres.delete(c.peer); snap = null; });
    });
    peer.on('disconnected', () => { try { peer.reconnect(); } catch (e) { } });
    peer.on('error', e => { if (e && e.type === 'network') errs.forEach(f => f('network')); });
    return {
      kind: 'peer', isHub: true,
      peers: () => snap || (snap = [{ peer: peer.id, sameTab: true, presence: mine }].concat([...pres].map(([k, v]) => ({ peer: k, sameTab: false, presence: v })))),
      presence: async patch => { mine = applyPatch(mine, patch); snap = null; broadcast(); },
      onError: fn => errs.push(fn),
      close: () => { try { peer.destroy(); } catch (e) { } }
    };
  }

  async function joinPeer(code) {
    const peer = await newPeer(null);
    const conn = await new Promise((res, rej) => {
      const c = peer.connect(PREFIX + code, { reliable: true, serialization: 'json' });
      const t = setTimeout(() => rej('not_found'), 12000);
      c.on('open', () => { clearTimeout(t); res(c); });
      peer.on('error', e => { clearTimeout(t); rej(e && e.type === 'peer-unavailable' ? 'not_found' : (e && e.type) || 'error'); });
    });
    let all = {}, mine = {}, snap = null;
    const errs = [];
    conn.on('data', m => { if (m && m.t === 'all' && m.all && typeof m.all === 'object') { all = m.all; snap = null; } });
    const lost = () => errs.forEach(f => f('host_left'));
    conn.on('close', lost); conn.on('error', lost);
    return {
      kind: 'peer', isHub: false,
      peers: () => {
        if (snap) return snap;
        const list = [{ peer: peer.id, sameTab: true, presence: mine }];
        for (const k in all) if (k !== peer.id) list.push({ peer: k, sameTab: false, presence: all[k] || {} });
        return (snap = list);
      },
      presence: async patch => { mine = applyPatch(mine, patch); snap = null; if (conn.open) { try { conn.send({ t: 'p', p: mine }); } catch (e) { } } },
      onError: fn => errs.push(fn),
      close: () => { try { peer.destroy(); } catch (e) { } }
    };
  }

  let lobby = null, kind = null;
  window.DomNet = {
    // 'claude' | 'peer' | null
    async detect() {
      lobby = await claudeRoom();
      kind = lobby ? 'claude' : (window.Peer ? 'peer' : null);
      return kind;
    },
    // create: true — создаю комнату с этим кодом, false — вхожу в чужую
    async open(code, create) {
      if (kind === 'claude') return openClaude(lobby, code);
      if (kind === 'peer') return create ? hostPeer(code) : joinPeer(code);
      throw 'unavailable';
    },
    errorText(e) {
      switch (e) {
        case 'not_found': return 'Комната с таким кодом не найдена. Проверь код и что у друга открыто лобби.';
        case 'unavailable-id': return 'Такой код уже занят. Попробуй ещё раз.';
        case 'host_left': return 'Связь с хозяином комнаты пропала. Игра у него закрылась или пропал интернет.';
        case 'network': case 'server-error': case 'socket-error': case 'socket-closed': return 'Не получается связаться с сервером соединений. Проверь интернет и попробуй ещё раз.';
        case 'browser-incompatible': return 'Этот браузер не умеет прямые соединения. Попробуй Chrome, Safari или Firefox.';
        case 'timeout': return 'Соединение не установилось за 12 секунд. Попробуй ещё раз или другую сеть.';
        default: return 'Не получилось подключиться. Попробуй ещё раз.';
      }
    }
  };
})();
