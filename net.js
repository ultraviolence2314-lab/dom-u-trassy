/* Связь между игроками для «Дома у трассы».
   На claude.ai — комната Claude (capability room).
   На любом другом сайте — Firebase Realtime Database: каждый игрок пишет своё состояние
   в rooms/<код>/p/<id>, база сразу рассылает его остальным, а при закрытии вкладки сама удаляет запись.
   Наружу у обоих вариантов одинаковый вид: room.peers(), room.presence(patch), room.onError(fn). */
(function () {
  'use strict';
  // Проект Firebase «dom-u-trassy» (личный аккаунт). Для базы без входа по логину хватает адреса;
  // доступ ограничивают правила базы (см. ПРОГРЕСС.md).
  const FIREBASE_CONFIG = {
    projectId: 'dom-u-trassy',
    databaseURL: 'https://dom-u-trassy-default-rtdb.europe-west1.firebasedatabase.app'
  };
  const SEND_EVERY = 150;     // мс: не чаще ~7 записей в секунду
  const HEARTBEAT = 2000;     // мс: напоминаем о себе, даже если ничего не менялось
  const STALE = 15000;        // мс: игрок пропал, если молчит дольше
  const FIND_WAIT = 6000;     // мс: сколько ждать, пока появится создатель комнаты

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
      kind: 'claude', isHub: false,
      peers: () => rm.peers().map(p => ({ peer: p.peer, sameTab: !!p.sameTab, presence: p.presence || {} })),
      presence: patch => rm.presence(patch),
      onError: fn => errs.push(fn),
      close: () => { try { rm.leave && rm.leave(); } catch (e) { } }
    };
  }

  // ---- Firebase ----
  function applyPatch(obj, patch) {
    const o = Object.assign({}, obj);
    for (const k in patch) { if (patch[k] === null) delete o[k]; else o[k] = patch[k]; }
    return o;
  }
  let db = null;
  function database() {
    if (!db) { firebase.initializeApp(FIREBASE_CONFIG); db = firebase.database(); }
    return db;
  }
  function waitConnected(d, ms) {
    return new Promise((res, rej) => {
      const ref = d.ref('.info/connected');
      const t = setTimeout(() => { ref.off('value', on); rej('timeout'); }, ms);
      const on = s => { if (s.val() === true) { clearTimeout(t); ref.off('value', on); res(); } };
      ref.on('value', on);
    });
  }
  async function openFirebase(code, create) {
    const d = database();
    await waitConnected(d, 20000);
    const id = 'p' + Math.random().toString(36).slice(2, 10);
    const roomRef = d.ref('rooms/' + code + '/p'), myRef = roomRef.child(id);
    const others = new Map();   // id -> { presence, at }
    let mine = {}, snap = null, lastSent = 0, timer = null, offlineSince = 0, failed = false;
    const errs = [];
    // присутствие приходит строкой JSON: так Firebase не выбрасывает пустые поля и null
    const take = s => {
      if (s.key === id) return;
      const v = s.val(); let p = null;
      try { p = v && typeof v.j === 'string' ? JSON.parse(v.j) : null; } catch (e) { }
      if (p && typeof p === 'object') { others.set(s.key, { presence: p, at: Date.now() }); snap = null; }
    };
    roomRef.on('child_added', take);
    roomRef.on('child_changed', take);
    roomRef.on('child_removed', s => { others.delete(s.key); snap = null; });
    // проверка кода: при создании комната должна быть пустой, при входе — не пустой
    if (create) await new Promise(r => setTimeout(r, 800));
    else { const t0 = Date.now(); while (!others.size && Date.now() - t0 < FIND_WAIT) await new Promise(r => setTimeout(r, 200)); }
    if (create && others.size) { roomRef.off(); throw 'unavailable-id'; }
    if (!create && !others.size) { roomRef.off(); throw 'not_found'; }

    myRef.onDisconnect().remove();
    function flush() {
      timer = null; lastSent = Date.now();
      myRef.set({ j: JSON.stringify(mine), t: firebase.database.ServerValue.TIMESTAMP }).catch(() => { });
    }
    function schedule() {
      if (timer) return;
      timer = setTimeout(flush, Math.max(0, SEND_EVERY - (Date.now() - lastSent)));
    }
    d.ref('.info/connected').on('value', s => {
      if (s.val() === true) { offlineSince = 0; myRef.onDisconnect().remove(); flush(); }
      else if (!offlineSince) offlineSince = Date.now();
    });
    // напоминаем о себе; без связи дольше 15 с — сообщаем игре
    setInterval(() => {
      if (offlineSince && !failed && Date.now() - offlineSince > 15000) { failed = true; errs.forEach(f => f('network')); }
      if (Date.now() - lastSent > HEARTBEAT) flush();
    }, 1000);
    addEventListener('pagehide', () => { try { myRef.remove(); } catch (e) { } });
    const peersNow = () => {
      const now = Date.now(); let changed = false;
      others.forEach((v, k) => { if (now - v.at > STALE) { others.delete(k); changed = true; } });
      if (changed) snap = null;
      return snap || (snap = [{ peer: id, sameTab: true, presence: mine }].concat([...others].map(([k, v]) => ({ peer: k, sameTab: false, presence: v.presence }))));
    };
    return {
      kind: 'firebase', isHub: !!create,
      peers: peersNow,
      presence: async patch => { mine = applyPatch(mine, patch); snap = null; schedule(); },
      onError: fn => errs.push(fn),
      close: () => { try { roomRef.off(); myRef.remove(); } catch (e) { } }
    };
  }

  let lobby = null, kind = null;
  window.DomNet = {
    // 'claude' | 'firebase' | null
    async detect() {
      lobby = await claudeRoom();
      kind = lobby ? 'claude' : (window.firebase && FIREBASE_CONFIG ? 'firebase' : null);
      return kind;
    },
    // create: true — создаю комнату с этим кодом, false — вхожу в чужую
    async open(code, create) {
      if (kind === 'claude') return openClaude(lobby, code);
      if (kind === 'firebase') return openFirebase(code, create);
      throw 'unavailable';
    },
    errorText(e) {
      switch (e) {
        case 'not_found': return 'Комната с таким кодом не найдена. Проверь код и что у друга открыто лобби.';
        case 'unavailable-id': return 'Такой код уже занят. Попробуй ещё раз.';
        case 'host_left': return 'Связь с хозяином комнаты пропала. Игра у него закрылась или пропал интернет.';
        case 'network': case 'timeout': return 'Не получается связаться с сервером игры. Проверь интернет и попробуй ещё раз.';
        default: return 'Не получилось подключиться. Попробуй ещё раз.';
      }
    }
  };
})();
