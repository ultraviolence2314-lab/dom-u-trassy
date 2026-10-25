/* Связь между игроками для «Дома у трассы».
   На claude.ai — комната Claude (capability room).
   На любом другом сайте — публичный посредник MQTT по защищённому веб-соединению (wss):
   каждый игрок публикует своё состояние в тему комнаты и слушает остальных.
   Прямые соединения браузеров (WebRTC) не используем: их часто режут рабочие сети и VPN.
   Наружу у обоих вариантов одинаковый вид: room.peers(), room.presence(patch), room.onError(fn). */
(function () {
  'use strict';
  // Публичные серверы без регистрации. shiftr.io обычно быстрее, но иногда молчит —
  // тогда через 10 с переходим на HiveMQ (он медленнее, зато отвечает почти всегда).
  const BROKERS = [
    { url: 'wss://public.cloud.shiftr.io:443', opts: { username: 'public', password: 'public' }, wait: 10000 },
    { url: 'wss://broker.hivemq.com:8884/mqtt', opts: {}, wait: 25000 }
  ];
  const FIND_WAIT = 8000;     // мс: сколько ждать создателя комнаты на одном сервере
  const ROOT = 'domutrassy/v1/';
  const SEND_EVERY = 125;     // мс: не чаще 8 сообщений в секунду
  const STALE = 8000;         // мс: игрок пропал, если молчит дольше

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

  // ---- посредник MQTT ----
  function applyPatch(obj, patch) {
    const o = Object.assign({}, obj);
    for (const k in patch) { if (patch[k] === null) delete o[k]; else o[k] = patch[k]; }
    return o;
  }
  function connectBroker(b, id, will) {
    return new Promise((res, rej) => {
      const c = window.mqtt.connect(b.url, Object.assign({ clientId: id, keepalive: 20, connectTimeout: b.wait, reconnectPeriod: 2000, clean: true, will }, b.opts));
      const t = setTimeout(() => { try { c.end(true); } catch (e) { } rej('timeout'); }, b.wait + 500);
      c.once('connect', () => { clearTimeout(t); res(c); });
      c.once('error', () => { clearTimeout(t); try { c.end(true); } catch (e) { } rej('network'); });
    });
  }
  // Создатель комнаты берёт первый сервер, который ответил. Тот, кто входит, ищет комнату по очереди на всех.
  async function openRelay(code, create) {
    let lastErr = 'network';
    for (const b of BROKERS) {
      try { return await openOn(b, code, create); }
      catch (e) { lastErr = e; if (create && e === 'unavailable-id') throw e; }
    }
    throw lastErr;
  }
  async function openOn(broker, code, create) {
    const id = 'p' + Math.random().toString(36).slice(2, 10);
    const base = ROOT + code + '/p/', mineTopic = base + id;
    const will = { topic: mineTopic, payload: '', qos: 0, retain: false };
    const client = await connectBroker(broker, id, will);
    const others = new Map();   // id -> { presence, at }
    let mine = {}, snap = null, dirty = false, lastSent = 0, timer = null, offlineSince = 0, failed = false;
    const errs = [];
    client.on('message', (topic, buf) => {
      const pid = topic.slice(base.length); if (!pid || pid === id) return;
      const txt = buf && buf.toString();
      if (!txt) { others.delete(pid); snap = null; return; }
      try { const m = JSON.parse(txt); if (m && m.p && typeof m.p === 'object') { others.set(pid, { presence: m.p, at: Date.now() }); snap = null; } } catch (e) { }
    });
    client.on('offline', () => { if (!offlineSince) offlineSince = Date.now(); });
    client.on('connect', () => { offlineSince = 0; client.subscribe(base + '+'); });
    await new Promise((res, rej) => client.subscribe(base + '+', { qos: 0 }, e => e ? rej('network') : res()));
    function flush() {
      timer = null; if (!client.connected) return;
      lastSent = Date.now(); dirty = false;
      client.publish(mineTopic, JSON.stringify({ p: mine }), { qos: 0, retain: false });
    }
    function schedule() {
      dirty = true; if (timer) return;
      const wait = Math.max(0, SEND_EVERY - (Date.now() - lastSent));
      timer = setTimeout(flush, wait);
    }
    const peersNow = () => {
      const now = Date.now(); let changed = false;
      others.forEach((v, k) => { if (now - v.at > STALE) { others.delete(k); changed = true; } });
      if (changed) snap = null;
      return snap || (snap = [{ peer: id, sameTab: true, presence: mine }].concat([...others].map(([k, v]) => ({ peer: k, sameTab: false, presence: v.presence }))));
    };
    // проверка кода: при создании комната должна быть пустой, при входе — не пустой
    if (create) await new Promise(r => setTimeout(r, 1500));
    else { const t0 = Date.now(); while (!others.size && Date.now() - t0 < FIND_WAIT) await new Promise(r => setTimeout(r, 200)); }
    const count = others.size;
    if (create && count > 0) { try { client.end(true); } catch (e) { } throw 'unavailable-id'; }
    if (!create && count === 0) { try { client.end(true); } catch (e) { } throw 'not_found'; }
    // раз в секунду напоминаем о себе; без связи дольше 12 с — сообщаем игре
    setInterval(() => {
      if (offlineSince && !failed && Date.now() - offlineSince > 12000) { failed = true; errs.forEach(f => f('network')); }
      if (Date.now() - lastSent > 1000) flush();
    }, 1000);
    addEventListener('pagehide', () => { try { client.publish(mineTopic, '', { qos: 0, retain: false }); client.end(); } catch (e) { } });
    return {
      kind: 'relay', isHub: !!create,
      peers: peersNow,
      presence: async patch => { mine = applyPatch(mine, patch); snap = null; schedule(); },
      onError: fn => errs.push(fn),
      close: () => { try { client.publish(mineTopic, ''); client.end(); } catch (e) { } }
    };
  }

  let lobby = null, kind = null;
  window.DomNet = {
    // 'claude' | 'relay' | null
    async detect() {
      lobby = await claudeRoom();
      kind = lobby ? 'claude' : (window.mqtt ? 'relay' : null);
      return kind;
    },
    // create: true — создаю комнату с этим кодом, false — вхожу в чужую
    async open(code, create) {
      if (kind === 'claude') return openClaude(lobby, code);
      if (kind === 'relay') return openRelay(code, create);
      throw 'unavailable';
    },
    errorText(e) {
      switch (e) {
        case 'not_found': return 'Комната с таким кодом не найдена. Проверь код и что у друга открыто лобби.';
        case 'unavailable-id': return 'Такой код уже занят. Попробуй ещё раз.';
        case 'host_left': return 'Связь с хозяином комнаты пропала. Игра у него закрылась или пропал интернет.';
        case 'network': case 'timeout': return 'Не получается связаться с сервером игры. Проверь интернет и попробуй ещё раз. Если не выходит в рабочей сети или под VPN, попробуй дома или с телефона.';
        default: return 'Не получилось подключиться. Попробуй ещё раз.';
      }
    }
  };
})();
