(async () => {
  // =======================
  // UTILIDADES BÁSICAS
  // =======================
  const getCookie = (name) => {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop().split(';').shift();
    return undefined;
  };

  const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
  const encodeVars = (v) => encodeURIComponent(JSON.stringify(v));
  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
  const formatNumber = (n) => (n ?? 0).toLocaleString('es-ES');
  const formatDuration = (ms) => {
    if (!ms || ms < 0) return '—';
    const totalSeconds = Math.ceil(ms / 1000);
    if (totalSeconds < 60) return `${totalSeconds}s`;
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    if (minutes < 60) return `${minutes}m ${seconds}s`;
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    return `${hours}h ${mins}m`;
  };
  const formatDateTime = (ts) => {
    if (!ts) return '—';
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return '—';
    return d.toLocaleString('es-ES');
  };
  const cssEscape = (value) => {
    if (window.CSS && typeof window.CSS.escape === 'function') return window.CSS.escape(value);
    return String(value).replace(/[^a-zA-Z0-9_\-]/g, (char) => `\\${char}`);
  };

  const csrftoken = getCookie('csrftoken');
  const ds_user_id = getCookie('ds_user_id');

  if (!location.hostname.includes('instagram.com')) {
    throw new Error('Este script solo funciona dentro de instagram.com.');
  }
  if (!ds_user_id || !csrftoken) {
    throw new Error('No pude leer cookies clave (ds_user_id/csrftoken). Inicia sesión.');
  }

  // =======================
  // CONSTANTES GLOBALES
  // =======================
  const QUERY_HASH = '3dec7e2c57367ef3da3d987d89f9dbc8';
  const baseUrl = 'https://www.instagram.com/graphql/query/?';
  const STORAGE_KEYS = {
    session: 'igAudit.session.v2',
    favorites: 'igAudit.favorites',
    config: 'igAudit.config.v2'
  };

  const makeUrl = (after = null) => {
    const variables = {
      id: ds_user_id,
      include_reel: true,
      fetch_mutual: false,
      first: 24
    };
    if (after) variables.after = after;
    return `${baseUrl}query_hash=${QUERY_HASH}&variables=${encodeVars(variables)}`;
  };

  const unfollowUrl = (id) => `https://www.instagram.com/web/friendships/${id}/unfollow/`;

  // =======================
  // ESTADO
  // =======================
  const state = {
    totalFollowing: null,
    fetchedCount: 0,
    after: null,
    hasNext: true,
    running: false,
    aborted: false,
    throttleAvg: 1200,
    throttleJitter: 400,
    throttleMin: 600,
    throttleMax: 4000,
    coolDownEvery: 6,
    coolDownMs: 10000,
    coolDownCounter: 0,
    nonFollowers: [],
    errors: 0,
    page: 0,
    sessionRestored: false,
    autoSpeed: true,
    simulateUnfollow: false,
    favorites: new Set(),
    logs: [],
    lastSaved: null,

    massRunning: false,
    massAbort: false,
    unfollowed: 0,
    unfollowErrors: 0,
    simulatedUnfollowed: 0
  };

  // =======================
  // PERSISTENCIA
  // =======================
  function normalizeUser(raw) {
    if (!raw) return null;
    return {
      id: String(raw.id ?? ''),
      username: String(raw.username ?? ''),
      full_name: String(raw.full_name ?? ''),
      is_private: Boolean(raw.is_private),
      profile_pic_url: raw.profile_pic_url ?? '',
      is_verified: Boolean(raw.is_verified),
      is_business_account: Boolean(raw.is_business_account),
      category_name: raw.category_name || '',
      has_anonymous_profile_picture: Boolean(raw.has_anonymous_profile_picture),
      requested_by_viewer: Boolean(raw.requested_by_viewer),
      simulated: Boolean(raw.simulated)
    };
  }

  function loadConfig() {
    try {
      const raw = localStorage.getItem(STORAGE_KEYS.config);
      if (!raw) return;
      const cfg = JSON.parse(raw);
      if (typeof cfg.throttleAvg === 'number') state.throttleAvg = clamp(cfg.throttleAvg, state.throttleMin, state.throttleMax);
      if (typeof cfg.autoSpeed === 'boolean') state.autoSpeed = cfg.autoSpeed;
      if (typeof cfg.simulateUnfollow === 'boolean') state.simulateUnfollow = cfg.simulateUnfollow;
    } catch (err) {
      console.warn('No se pudo cargar la configuración previa.', err);
    }
  }

  function persistConfig() {
    try {
      const payload = {
        throttleAvg: state.throttleAvg,
        autoSpeed: state.autoSpeed,
        simulateUnfollow: state.simulateUnfollow
      };
      localStorage.setItem(STORAGE_KEYS.config, JSON.stringify(payload));
    } catch (err) {
      console.warn('No se pudo guardar la configuración.', err);
    }
  }

  function loadFavorites() {
    try {
      const raw = localStorage.getItem(STORAGE_KEYS.favorites);
      if (!raw) return;
      const arr = JSON.parse(raw);
      if (Array.isArray(arr)) {
        state.favorites = new Set(arr.map(String));
      }
    } catch (err) {
      console.warn('No se pudieron cargar los favoritos.', err);
    }
  }

  function persistFavorites() {
    try {
      localStorage.setItem(STORAGE_KEYS.favorites, JSON.stringify(Array.from(state.favorites)));
    } catch (err) {
      console.warn('No se pudieron guardar los favoritos.', err);
    }
  }

  function loadSessionFromStorage() {
    try {
      const raw = localStorage.getItem(STORAGE_KEYS.session);
      if (!raw) return false;
      const payload = JSON.parse(raw);
      if (!payload || typeof payload !== 'object') return false;
      const restoredUsers = Array.isArray(payload.nonFollowers)
        ? payload.nonFollowers.map(normalizeUser).filter(Boolean)
        : [];
      state.nonFollowers = restoredUsers;
      state.totalFollowing = payload.totalFollowing ?? state.totalFollowing;
      state.fetchedCount = payload.fetchedCount ?? state.fetchedCount;
      state.after = payload.after ?? state.after;
      state.hasNext = typeof payload.hasNext === 'boolean' ? payload.hasNext : state.hasNext;
      state.page = payload.page ?? state.page;
      state.lastSaved = payload.timestamp ?? null;
      state.sessionRestored = true;
      return true;
    } catch (err) {
      console.warn('No se pudo restaurar la sesión previa.', err);
      return false;
    }
  }

  function persistSession() {
    try {
      const payload = {
        nonFollowers: state.nonFollowers,
        totalFollowing: state.totalFollowing,
        fetchedCount: state.fetchedCount,
        after: state.after,
        hasNext: state.hasNext,
        page: state.page,
        timestamp: Date.now()
      };
      localStorage.setItem(STORAGE_KEYS.session, JSON.stringify(payload));
      state.lastSaved = payload.timestamp;
    } catch (err) {
      console.warn('No se pudo guardar la sesión.', err);
    }
  }

  function clearSessionStorage() {
    try {
      localStorage.removeItem(STORAGE_KEYS.session);
      state.lastSaved = null;
    } catch (err) {
      console.warn('No se pudo limpiar la sesión.', err);
    }
  }

  loadConfig();
  loadFavorites();

  // =======================
  // UI BONITA (Shadow DOM)
  // =======================
  const panel = document.createElement('div');
  panel.id = 'ig-audit-panel';
  panel.style.all = 'initial';
  panel.style.position = 'fixed';
  panel.style.top = '24px';
  panel.style.right = '24px';
  panel.style.zIndex = 2147483646;
  panel.style.width = 'min(520px, 96vw)';
  panel.style.height = 'min(84vh, 760px)';
  panel.style.boxShadow = '0 10px 30px rgba(0,0,0,.35)';
  panel.style.borderRadius = '16px';
  panel.style.overflow = 'hidden';
  panel.style.fontFamily = 'Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif';
  panel.style.background = 'linear-gradient(135deg, rgba(20,20,24,.92), rgba(18,18,22,.88))';
  panel.style.backdropFilter = 'blur(12px)';
  document.body.appendChild(panel);

  const root = panel.attachShadow({ mode: 'open' });
  root.innerHTML = `
    <style>
      :host { color: #f1f5f9; }
      * { box-sizing: border-box; }
      .wrap { display: grid; grid-template-rows: auto auto auto auto auto 1fr auto; height: 100%; }
      header {
        display: flex; align-items: center; gap: 10px;
        padding: 14px 16px; border-bottom: 1px solid rgba(255,255,255,.08);
        background: linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,0));
      }
      .dot {
        width: 10px; height: 10px; border-radius: 999px;
        background: #22c55e; box-shadow: 0 0 12px rgba(34,197,94,.7);
      }
      h1 { font-size: 14px; margin: 0; letter-spacing: .3px; }
      .muted { color: #94a3b8; font-size: 11px; }
      .actions { margin-left: auto; display: flex; gap: 8px; }
      button, .btn, select {
        appearance: none; border: 1px solid rgba(255,255,255,.08);
        background: rgba(255,255,255,.06); color: #e2e8f0; font-size: 12px;
        padding: 8px 10px; border-radius: 10px; cursor: pointer;
        transition: transform .06s ease, background .2s ease, border-color .2s ease, opacity .2s ease;
      }
      select { padding-right: 26px; }
      button:hover, select:hover { background: rgba(255,255,255,.12); border-color: rgba(255,255,255,.18); }
      button:active { transform: translateY(1px) scale(0.99); }
      button[disabled] { opacity: .6; cursor: not-allowed; }
      .danger { background: rgba(239,68,68,.12); border-color: rgba(239,68,68,.3); color: #fecaca; }
      .success { background: rgba(34,197,94,.12); border-color: rgba(34,197,94,.3); color: #bbf7d0; }
      .warn { background: rgba(245,158,11,.12); border-color: rgba(245,158,11,.3); color: #fde68a; }
      .row { display: flex; align-items: center; gap: 10px; }
      .progress {
        padding: 12px 16px; border-bottom: 1px solid rgba(255,255,255,.08);
        display: grid; gap: 6px;
      }
      .bar {
        height: 8px; background: rgba(255,255,255,.06); border-radius: 999px; overflow: hidden;
      }
      .bar > div {
        height: 100%;
        background: linear-gradient(90deg, #22c55e, #3b82f6, #f59e0b, #ef4444);
        width: 0%; transition: width .3s ease;
      }
      .stats { display: flex; gap: 10px; flex-wrap: wrap; font-size: 12px; color: #cbd5e1; }
      .chip {
        background: rgba(255,255,255,.06); border: 1px solid rgba(255,255,255,.08);
        padding: 6px 8px; border-radius: 999px;
      }
      .tools {
        display: flex; flex-wrap: wrap; gap: 10px; padding: 10px 16px;
        border-bottom: 1px solid rgba(255,255,255,.08);
      }
      .tools input[type="text"] {
        flex: 1 1 180px; padding: 8px 10px; border-radius: 10px;
        background: rgba(255,255,255,.06); border: 1px solid rgba(255,255,255,.12);
        color: #e2e8f0; outline: none; font-size: 12px;
      }
      .tools select { flex: 0 0 160px; }
      .tools .btn { flex: 0 0 auto; }
      .toggles {
        display: flex; flex-wrap: wrap; gap: 10px; padding: 8px 16px;
        border-bottom: 1px solid rgba(255,255,255,.08); font-size: 11px; color: #cbd5e1;
        align-items: center;
      }
      .toggle {
        display: inline-flex; align-items: center; gap: 6px;
        background: rgba(255,255,255,.04); border: 1px solid rgba(255,255,255,.08);
        padding: 6px 10px; border-radius: 999px;
      }
      .toggle input { accent-color: #3b82f6; }
      .analytics {
        display: grid; gap: 6px; padding: 10px 16px;
        grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
        border-bottom: 1px solid rgba(255,255,255,.08); font-size: 11px;
      }
      .metric {
        background: rgba(255,255,255,.04); border: 1px solid rgba(255,255,255,.08);
        padding: 8px 10px; border-radius: 12px; display: grid; gap: 4px;
      }
      .metric span { font-size: 14px; color: #e2e8f0; font-weight: 600; }
      .list { overflow: auto; padding: 12px; display: grid; gap: 8px; }
      .card {
        display: grid; grid-template-columns: 52px 1fr auto;
        gap: 10px; align-items: center;
        background: rgba(255,255,255,.04); border: 1px solid rgba(255,255,255,.08);
        border-radius: 12px; padding: 10px; position: relative;
        transition: border-color .2s ease, transform .2s ease, box-shadow .2s ease;
      }
      .card.favorite { border-color: rgba(34,197,94,.45); box-shadow: 0 0 0 1px rgba(34,197,94,.35); }
      .card.simulated { border-color: rgba(59,130,246,.45); box-shadow: 0 0 0 1px rgba(59,130,246,.25); }
      .avatar {
        width: 48px; height: 48px; border-radius: 999px; background: #111; object-fit: cover;
        border: 1px solid rgba(255,255,255,.08);
      }
      .meta { display: grid; gap: 4px; }
      .name { font-size: 13px; color: #e5e7eb; display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
      .user { font-size: 12px; color: #9ca3af; }
      .extra { font-size: 11px; color: #cbd5e1; }
      .category { font-size: 10px; color: #94a3b8; }
      .pill {
        font-size: 10px; padding: 2px 6px; border-radius: 999px;
        background: rgba(59,130,246,.15); border: 1px solid rgba(59,130,246,.35); color: #bfdbfe;
      }
      .pill-vip { background: rgba(249,115,22,.18); border-color: rgba(249,115,22,.35); color: #fed7aa; }
      .pill-business { background: rgba(14,165,233,.18); border-color: rgba(14,165,233,.4); color: #bae6fd; }
      .pill-private { background: rgba(234,179,8,.18); border-color: rgba(234,179,8,.4); color: #fef9c3; }
      .pill-low { background: rgba(248,113,113,.18); border-color: rgba(248,113,113,.4); color: #fee2e2; }
      .pill-sim { background: rgba(59,130,246,.18); border-color: rgba(59,130,246,.35); color: #dbeafe; }
      a { color: #93c5fd; text-decoration: none; }
      a:hover { text-decoration: underline; }
      footer {
        padding: 10px 12px; border-top: 1px solid rgba(255,255,255,.08); font-size: 11px; color: #94a3b8;
        display: grid; gap: 8px;
      }
      footer details {
        background: rgba(15,23,42,.55); border: 1px solid rgba(148,163,184,.18); border-radius: 12px;
        padding: 6px 10px;
      }
      footer summary { cursor: pointer; font-size: 12px; color: #e2e8f0; }
      .toast {
        position: absolute; bottom: 12px; right: 12px;
        background: rgba(17,24,39,.96); border: 1px solid rgba(255,255,255,.12);
        color: #e5e7eb; padding: 8px 10px; border-radius: 10px;
        box-shadow: 0 6px 16px rgba(0,0,0,.35); font-size: 12px; opacity: 0; transform: translateY(6px);
        transition: opacity .2s ease, transform .2s ease;
      }
      .toast.show { opacity: 1; transform: translateY(0); }
      .empty { text-align: center; color: #9ca3af; padding: 40px 10px; font-size: 13px; }
      .mini { padding: 6px 8px; font-size: 11px; }
      .pill span { font-weight: 500; }
      .log { max-height: 120px; overflow: auto; display: grid; gap: 4px; margin-top: 6px; font-size: 11px; }
      .log-item { padding: 6px 8px; border-radius: 8px; background: rgba(15,23,42,.55); border: 1px solid rgba(148,163,184,.18); }
      .log-item b { color: #f8fafc; }
      .log-fetch { border-color: rgba(96,165,250,.3); }
      .log-error { border-color: rgba(248,113,113,.45); }
      .log-action { border-color: rgba(74,222,128,.35); }
      .log-meta { border-color: rgba(196,181,253,.35); }
      .star-btn { font-size: 14px; padding: 6px 8px; width: 34px; text-align: center; }
      .speed-value { color: #a5b4fc; font-weight: 600; }
      .badge { font-size: 10px; padding: 2px 6px; border-radius: 999px; border: 1px solid rgba(59,130,246,.35); background: rgba(59,130,246,.15); color: #dbeafe; }
      @media (max-width: 480px) {
        header { flex-wrap: wrap; }
        .actions { width: 100%; justify-content: flex-end; }
      }
    </style>

    <div class="wrap">
      <header>
        <div class="dot" id="statusDot"></div>
        <div>
          <h1>IG • Radar de No Seguidores</h1>
          <div class="muted" id="subtitle">Preparando…</div>
        </div>
        <div class="actions">
          <button id="pauseBtn" class="btn">Pausar</button>
          <button id="resumeBtn" class="btn success" style="display:none;">Reanudar</button>
          <button id="closeBtn" class="btn danger">Cerrar</button>
        </div>
      </header>

      <div class="progress">
        <div class="row" style="justify-content: space-between; flex-wrap: wrap; gap: 12px;">
          <div class="stats">
            <span class="chip">Progreso: <b id="progText">0/0 · 0%</b></span>
            <span class="chip">No te siguen: <b id="countNF">0</b></span>
            <span class="chip">Errores: <b id="errNF">0</b></span>
            <span class="chip">Unfollows: <b id="ufCount">0</b></span>
            <span class="chip">Simulados: <b id="ufSim">0</b></span>
            <span class="chip">Fallos: <b id="ufErr">0</b></span>
            <span class="chip">ETA: <b id="etaText">—</b></span>
          </div>
          <div class="range-wrap" style="display:flex; align-items:center; gap:8px; color:#cbd5e1; font-size:11px;">
            Velocidad <span class="speed-value" id="speedValue">1200ms</span>
            <input id="speedRange" type="range" min="600" max="4000" step="100" value="1200" />
          </div>
        </div>
        <div class="bar"><div id="barFill"></div></div>
      </div>

      <div class="tools">
        <input id="searchInput" type="text" placeholder="Filtrar por usuario o nombre…" />
        <select id="segmentSelect">
          <option value="all">Segmento: Todos</option>
          <option value="private">Solo privados</option>
          <option value="verified">Solo verificados</option>
          <option value="business">Solo negocios</option>
          <option value="noavatar">Sin avatar</option>
          <option value="favorites">Favoritos</option>
          <option value="nonfavorites">Sin favoritos</option>
        </select>
        <button id="copyBtn" class="btn">Copiar</button>
        <button id="jsonBtn" class="btn">JSON</button>
        <button id="csvBtn" class="btn">CSV</button>
        <button id="highlightBtn" class="btn">Destacar</button>
        <button id="unfollowAllBtn" class="warn">Dejar de seguir a todos</button>
        <button id="stopMassBtn" class="btn danger" style="display:none;">Detener masivo</button>
      </div>

      <div class="toggles">
        <label class="toggle"><input id="autoSpeedToggle" type="checkbox" checked /> Velocidad adaptativa</label>
        <label class="toggle"><input id="simulateToggle" type="checkbox" /> Modo simulación (sin dejar de seguir)</label>
        <span class="toggle" title="La sesión se guarda para continuar luego.">Guardado: <span id="lastSaved">—</span></span>
        <button id="exportSessionBtn" class="btn mini">Exportar sesión</button>
        <button id="clearCacheBtn" class="btn mini danger">Limpiar caché</button>
      </div>

      <div class="analytics">
        <div class="metric">Privados <span id="statPrivate">0</span></div>
        <div class="metric">Verificados <span id="statVerified">0</span></div>
        <div class="metric">Negocios <span id="statBusiness">0</span></div>
        <div class="metric">Sin avatar <span id="statAnon">0</span></div>
        <div class="metric">Favoritos <span id="statFavorites">0</span></div>
        <div class="metric">Simulados <span id="statSimulated">0</span></div>
      </div>

      <div class="list" id="list"></div>

      <footer>
        Consejo: mezcla pausas largas con acciones pequeñas. Si ves errores 429, baja la velocidad o espera.
        <details id="logPanel">
          <summary>Actividad en vivo</summary>
          <div class="log" id="log"></div>
        </details>
      </footer>

      <div class="toast" id="toast"></div>
    </div>
  `;

  // =======================
  // REFERENCIAS UI
  // =======================
  const el = {
    subtitle: root.getElementById('subtitle'),
    statusDot: root.getElementById('statusDot'),
    pauseBtn: root.getElementById('pauseBtn'),
    resumeBtn: root.getElementById('resumeBtn'),
    closeBtn: root.getElementById('closeBtn'),
    progText: root.getElementById('progText'),
    countNF: root.getElementById('countNF'),
    errNF: root.getElementById('errNF'),
    ufCount: root.getElementById('ufCount'),
    ufErr: root.getElementById('ufErr'),
    ufSim: root.getElementById('ufSim'),
    barFill: root.getElementById('barFill'),
    list: root.getElementById('list'),
    searchInput: root.getElementById('searchInput'),
    segmentSelect: root.getElementById('segmentSelect'),
    copyBtn: root.getElementById('copyBtn'),
    jsonBtn: root.getElementById('jsonBtn'),
    csvBtn: root.getElementById('csvBtn'),
    highlightBtn: root.getElementById('highlightBtn'),
    speedRange: root.getElementById('speedRange'),
    speedValue: root.getElementById('speedValue'),
    toast: root.getElementById('toast'),
    unfollowAllBtn: root.getElementById('unfollowAllBtn'),
    stopMassBtn: root.getElementById('stopMassBtn'),
    autoSpeedToggle: root.getElementById('autoSpeedToggle'),
    simulateToggle: root.getElementById('simulateToggle'),
    lastSaved: root.getElementById('lastSaved'),
    statPrivate: root.getElementById('statPrivate'),
    statVerified: root.getElementById('statVerified'),
    statBusiness: root.getElementById('statBusiness'),
    statAnon: root.getElementById('statAnon'),
    statFavorites: root.getElementById('statFavorites'),
    statSimulated: root.getElementById('statSimulated'),
    etaText: root.getElementById('etaText'),
    log: root.getElementById('log'),
    exportSessionBtn: root.getElementById('exportSessionBtn'),
    clearCacheBtn: root.getElementById('clearCacheBtn')
  };

  function showToast(msg) {
    el.toast.textContent = msg;
    el.toast.classList.add('show');
    setTimeout(() => el.toast.classList.remove('show'), 1800);
  }

  function logEvent(type, message, meta = {}) {
    const entry = {
      id: crypto.randomUUID ? crypto.randomUUID() : String(Date.now() + Math.random()),
      type,
      message,
      meta,
      ts: Date.now()
    };
    state.logs.unshift(entry);
    if (state.logs.length > 60) state.logs.length = 60;
    renderLog();
  }

  function renderLog() {
    if (!el.log) return;
    el.log.innerHTML = '';
    const frag = document.createDocumentFragment();
    for (const item of state.logs) {
      const div = document.createElement('div');
      div.className = `log-item log-${item.type}`;
      const time = new Date(item.ts).toLocaleTimeString('es-ES', { hour12: false });
      div.innerHTML = `<b>${time}</b> · ${item.message}`;
      frag.appendChild(div);
    }
    el.log.appendChild(frag);
  }

  function setRunning(running) {
    state.running = running;
    el.statusDot.style.background = running ? '#22c55e' : '#f59e0b';
    el.statusDot.style.boxShadow = running
      ? '0 0 12px rgba(34,197,94,.7)'
      : '0 0 12px rgba(245,158,11,.7)';
    el.pauseBtn.style.display = running ? '' : 'none';
    el.resumeBtn.style.display = running ? 'none' : '';
    el.subtitle.textContent = running ? 'Recolectando datos…' : 'En pausa';
  }

  function percent() {
    if (!state.totalFollowing || state.totalFollowing === 0) return 0;
    return Math.min(100, Math.floor((state.fetchedCount / state.totalFollowing) * 100));
  }

  function estimateEta() {
    if (!state.totalFollowing || state.totalFollowing <= 0) return '—';
    const remaining = Math.max(0, state.totalFollowing - state.fetchedCount);
    if (remaining === 0) return '0s';
    const avg = clamp(state.throttleAvg, state.throttleMin, state.throttleMax);
    const estimated = remaining * avg;
    return formatDuration(estimated);
  }

  function updateSpeedInfo() {
    const value = Math.round(state.throttleAvg);
    el.speedRange.value = String(value);
    el.speedValue.textContent = `${value}ms`;
    el.speedRange.disabled = state.autoSpeed;
  }

  function updateProgress() {
    el.progText.textContent = `${formatNumber(state.fetchedCount)}/${formatNumber(state.totalFollowing ?? 0)} · ${percent()}%`;
    el.barFill.style.width = `${percent()}%`;
    el.countNF.textContent = formatNumber(state.nonFollowers.length);
    el.errNF.textContent = formatNumber(state.errors);
    el.ufCount.textContent = formatNumber(state.unfollowed);
    el.ufErr.textContent = formatNumber(state.unfollowErrors);
    el.ufSim.textContent = formatNumber(state.simulatedUnfollowed);
    el.etaText.textContent = estimateEta();
    el.lastSaved.textContent = formatDateTime(state.lastSaved);
    renderAnalytics();
  }

  function renderAnalytics() {
    const priv = state.nonFollowers.filter((u) => u.is_private).length;
    const ver = state.nonFollowers.filter((u) => u.is_verified).length;
    const biz = state.nonFollowers.filter((u) => u.is_business_account).length;
    const anon = state.nonFollowers.filter((u) => u.has_anonymous_profile_picture).length;
    const fav = state.nonFollowers.filter((u) => state.favorites.has(u.id)).length;
    const sim = state.nonFollowers.filter((u) => u.simulated).length;
    el.statPrivate.textContent = formatNumber(priv);
    el.statVerified.textContent = formatNumber(ver);
    el.statBusiness.textContent = formatNumber(biz);
    el.statAnon.textContent = formatNumber(anon);
    el.statFavorites.textContent = formatNumber(fav);
    el.statSimulated.textContent = formatNumber(sim);
  }

  function escapeHtml(s) {
    return (s ?? '').replace(/[&<>"']/g, (m) => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;'
    }[m]));
  }

  function personaFor(user) {
    if (user.is_verified) return { label: 'VIP', className: 'pill-vip', title: 'Cuenta verificada' };
    if (user.is_business_account) {
      return {
        label: 'Negocio',
        className: 'pill-business',
        title: user.category_name ? `Categoría: ${user.category_name}` : 'Cuenta comercial'
      };
    }
    if (user.is_private) return { label: 'Círculo cerrado', className: 'pill-private', title: 'Cuenta privada' };
    if (user.has_anonymous_profile_picture) return { label: 'Sin avatar', className: 'pill-low', title: 'Podría ser inactiva' };
    return { label: 'Explorar', className: 'pill', title: 'Cuenta pública estándar' };
  }

  function computeAffinityScore(user) {
    let score = 50;
    if (user.is_private) score += 12;
    if (user.is_verified) score += 25;
    if (user.is_business_account) score -= 8;
    if (user.has_anonymous_profile_picture) score -= 18;
    if (state.favorites.has(user.id)) score += 15;
    if (user.requested_by_viewer) score += 6;
    return clamp(Math.round(score), 0, 100);
  }

  function makeCard(user) {
    const link = `https://www.instagram.com/${encodeURIComponent(user.username)}/`;
    const persona = personaFor(user);
    const personaTag = `<span class="pill ${persona.className}" title="${escapeHtml(persona.title)}">${escapeHtml(persona.label)}</span>`;
    const simTag = user.simulated ? '<span class="pill pill-sim" title="Acción simulada">Simulado</span>' : '';
    const score = computeAffinityScore(user);
    const favorite = state.favorites.has(user.id);
    const favTitle = favorite ? 'Quitar de favoritos' : 'Marcar como favorito';
    const favIcon = favorite ? '★' : '☆';
    const category = user.category_name ? `<div class="category">${escapeHtml(user.category_name)}</div>` : '';
    const html = `
      <div class="card${favorite ? ' favorite' : ''}${user.simulated ? ' simulated' : ''}" data-id="${escapeHtml(user.id)}" data-username="${escapeHtml(user.username).toLowerCase()}" data-name="${escapeHtml(user.full_name).toLowerCase()}">
        <img class="avatar" loading="lazy" src="${escapeHtml(user.profile_pic_url)}" alt="${escapeHtml(user.username)}"/>
        <div class="meta">
          <div class="name">
            ${escapeHtml(user.full_name) || 'Sin nombre'} ${personaTag} ${simTag}
          </div>
          <div class="user">@<a class="profile-link" href="${link}" target="_blank" rel="noopener noreferrer">${escapeHtml(user.username)}</a></div>
          <div class="extra">Afinidad estimada: <b>${score}</b>/100</div>
          ${category}
        </div>
        <div class="row" style="flex-direction: column; gap: 6px;">
          <a class="btn mini" href="${link}" target="_blank" rel="noopener noreferrer">Ver perfil</a>
          <button class="btn mini danger" data-action="unfollow-one">Dejar de seguir</button>
          <button class="btn mini star-btn" data-action="toggle-favorite" title="${favTitle}">${favIcon}</button>
        </div>
      </div>
    `;
    const wrapper = document.createElement('div');
    wrapper.innerHTML = html.trim();
    return wrapper.firstElementChild;
  }

  function currentFiltered() {
    const term = (el.searchInput.value || '').trim().toLowerCase();
    const segment = el.segmentSelect.value;
    let items = state.nonFollowers;
    if (segment !== 'all') {
      items = items.filter((u) => {
        switch (segment) {
          case 'private': return u.is_private;
          case 'verified': return u.is_verified;
          case 'business': return u.is_business_account;
          case 'noavatar': return u.has_anonymous_profile_picture;
          case 'favorites': return state.favorites.has(u.id);
          case 'nonfavorites': return !state.favorites.has(u.id);
          default: return true;
        }
      });
    }
    if (term) {
      items = items.filter((u) =>
        u.username.toLowerCase().includes(term) ||
        (u.full_name || '').toLowerCase().includes(term)
      );
    }
    return items;
  }

  function renderList() {
    const items = currentFiltered();
    el.list.innerHTML = '';
    if (!items.length) {
      const empty = document.createElement('div');
      empty.className = 'empty';
      empty.textContent = state.nonFollowers.length
        ? 'No hay coincidencias con el filtro activo.'
        : 'Aún no se detectan usuarios.';
      el.list.appendChild(empty);
      return;
    }
    const frag = document.createDocumentFragment();
    for (const u of items) frag.appendChild(makeCard(u));
    el.list.appendChild(frag);
  }

  const download = (filename, content, type = 'application/octet-stream') => {
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([content], { type }));
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
  };

  const toCSV = (rows) => {
    const header = ['id', 'username', 'full_name', 'is_private', 'is_verified', 'is_business_account', 'category_name', 'has_anonymous_profile_picture', 'profile_pic_url'];
    const esc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
    return [header.join(','), ...rows.map((r) => header.map((k) => esc(r[k])).join(','))].join('\n');
  };

  async function copyUsernames() {
    const list = state.nonFollowers.map((u) => u.username).join('\n');
    try {
      await navigator.clipboard.writeText(list);
      showToast('Usernames copiados al portapapeles.');
    } catch {
      download('usernames.txt', list, 'text/plain;charset=utf-8');
      showToast('No se pudo copiar. Descargué un .txt.');
    }
  }

  function adjustAutoSpeed(reason) {
    if (!state.autoSpeed) return;
    if (reason === 'error') {
      state.throttleAvg = clamp(state.throttleAvg + 350, state.throttleMin, state.throttleMax);
    } else {
      state.throttleAvg = clamp(state.throttleAvg - 80, state.throttleMin, state.throttleMax);
    }
    updateSpeedInfo();
    persistConfig();
  }

  // =======================
  // UNFOLLOW API
  // =======================
  async function apiUnfollow(userId) {
    try {
      const res = await fetch(unfollowUrl(userId), {
        method: 'POST',
        credentials: 'include',
        headers: {
          'X-CSRFToken': csrftoken,
          'X-Requested-With': 'XMLHttpRequest',
          Referer: location.origin + '/',
          'Content-Type': 'application/x-www-form-urlencoded'
        },
        body: 'source=profile'
      });

      if (res.status === 429) {
        return { ok: false, rateLimited: true };
      }

      if (!res.ok) {
        return { ok: false, error: `HTTP ${res.status}` };
      }
      const j = await res.json().catch(() => ({}));
      if (j && j.status === 'ok') return { ok: true };
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e?.message || 'Error de red' };
    }
  }

  function removeFromState(userId) {
    const idx = state.nonFollowers.findIndex((u) => u.id === userId);
    if (idx >= 0) state.nonFollowers.splice(idx, 1);
  }

  function markSimulated(userId) {
    const user = state.nonFollowers.find((u) => u.id === userId);
    if (user) user.simulated = true;
  }

  function animateRemoveCard(card) {
    if (!card) return;
    card.style.transition = 'opacity .25s ease, transform .25s ease';
    card.style.opacity = '0';
    card.style.transform = 'scale(0.98)';
    setTimeout(() => card.remove(), 260);
  }

  async function handleUnfollowOne(userId, cardEl, opts = { silent: false }) {
    const originalBtn = cardEl?.querySelector('[data-action="unfollow-one"]');
    const favorite = state.favorites.has(userId);
    if (favorite && !opts.force) {
      if (!opts.silent) showToast('Protegido por favoritos. Usa el botón ☆ para quitarlo.');
      logEvent('meta', `Se evitó dejar de seguir a ${userId} por estar en favoritos.`);
      return false;
    }
    if (originalBtn) originalBtn.disabled = true;

    const jitter = (Math.random() * state.throttleJitter * 2) - state.throttleJitter;
    const wait = Math.max(400, state.throttleAvg + jitter);
    await sleep(200);

    if (state.simulateUnfollow) {
      state.simulatedUnfollowed += 1;
      markSimulated(userId);
      updateProgress();
      persistSession();
      if (!opts.silent) showToast('Simulación realizada. No se envió unfollow.');
      if (cardEl) cardEl.classList.add('simulated');
      logEvent('action', `Simulado unfollow de ${userId}.`);
      await sleep(wait);
      if (originalBtn) originalBtn.disabled = false;
      return true;
    }

    const res = await apiUnfollow(userId);

    if (res.rateLimited) {
      state.unfollowErrors += 1;
      updateProgress();
      adjustAutoSpeed('error');
      if (!opts.silent) showToast('Rate limit. Esperando 60s…');
      logEvent('error', `Rate limit al dejar de seguir a ${userId}. Espera extendida.`);
      await sleep(60000);
      if (originalBtn) originalBtn.disabled = false;
      return false;
    }

    if (!res.ok) {
      state.unfollowErrors += 1;
      updateProgress();
      adjustAutoSpeed('error');
      if (!opts.silent) showToast('Error al dejar de seguir.');
      logEvent('error', `Error al dejar de seguir a ${userId}: ${res.error ?? 'desconocido'}`);
      if (originalBtn) originalBtn.disabled = false;
      return false;
    }

    state.unfollowed += 1;
    removeFromState(userId);
    adjustAutoSpeed('success');
    updateProgress();
    persistSession();
    if (!opts.silent) showToast('Listo: dejaste de seguir.');
    logEvent('action', `Dejaste de seguir a ${userId}.`);
    animateRemoveCard(cardEl);
    await sleep(wait);
    return true;
  }

  function toggleFavorite(userId, card) {
    if (state.favorites.has(userId)) {
      state.favorites.delete(userId);
      card?.classList.remove('favorite');
      const btn = card?.querySelector('[data-action="toggle-favorite"]');
      if (btn) btn.textContent = '☆';
      showToast('Quitado de favoritos.');
      logEvent('meta', `Se quitó ${userId} de favoritos.`);
    } else {
      state.favorites.add(userId);
      card?.classList.add('favorite');
      const btn = card?.querySelector('[data-action="toggle-favorite"]');
      if (btn) btn.textContent = '★';
      showToast('Marcado como favorito.');
      logEvent('meta', `Se añadió ${userId} a favoritos.`);
    }
    persistFavorites();
    updateProgress();
  }

  function shouldSkipInMass(user) {
    return state.favorites.has(user.id);
  }

  // =======================
  // FETCH LOOP (RECOLECCIÓN)
  // =======================
  async function fetchPage() {
    const url = makeUrl(state.after);
    try {
      const res = await fetch(url, { credentials: 'include' });
      const data = await res.json();

      const edge = data?.data?.user?.edge_follow;
      if (!edge) throw new Error('Respuesta inesperada.');

      if (state.totalFollowing == null) state.totalFollowing = edge.count || 0;

      const edges = edge.edges || [];
      for (const item of edges) {
        const n = item.node || {};
        if (!n.follows_viewer) {
          const user = normalizeUser({
            id: n.id,
            username: n.username,
            full_name: n.full_name || '',
            is_private: !!n.is_private,
            profile_pic_url: n.profile_pic_url || '',
            is_verified: !!n.is_verified,
            is_business_account: !!n.is_business_account,
            category_name: n.category_name || '',
            has_anonymous_profile_picture: !!n.has_anonymous_profile_picture,
            requested_by_viewer: !!n.requested_by_viewer
          });
          if (user) state.nonFollowers.push(user);
        }
      }

      state.fetchedCount += edges.length;
      state.hasNext = !!edge.page_info?.has_next_page;
      state.after = edge.page_info?.end_cursor || null;
      state.page += 1;
      state.sessionRestored = false;

      updateProgress();
      renderList();
      persistSession();
      adjustAutoSpeed('success');
      logEvent('fetch', `Página ${state.page} procesada (${edges.length} cuentas).`);

    } catch (err) {
      state.errors += 1;
      updateProgress();
      adjustAutoSpeed('error');
      console.warn('Error en fetch:', err);
      logEvent('error', `Fallo al obtener página: ${err?.message ?? err}`);
    }
  }

  async function mainLoop() {
    if (!state.hasNext) {
      el.subtitle.textContent = 'Datos restaurados. Pulsa reanudar para refrescar.';
      setRunning(false);
      return;
    }
    setRunning(true);
    el.subtitle.textContent = 'Recolectando datos…';
    while (!state.aborted && state.hasNext) {
      if (!state.running) {
        await sleep(200);
        continue;
      }
      await fetchPage();

      const jitter = (Math.random() * state.throttleJitter * 2) - state.throttleJitter;
      const wait = Math.max(300, state.throttleAvg + jitter);
      await sleep(wait);

      state.coolDownCounter += 1;
      if (state.coolDownCounter >= state.coolDownEvery) {
        state.coolDownCounter = 0;
        el.subtitle.textContent = 'Pausa breve…';
        logEvent('meta', 'Aplicando auto-pausa preventiva.');
        await sleep(state.coolDownMs);
        el.subtitle.textContent = 'Recolectando datos…';
      }
    }
    setRunning(false);
    if (!state.aborted) {
      el.subtitle.textContent = 'Completado.';
      showToast('Listo. Exporta si quieres.');
      updateProgress();
      renderList();
      persistSession();
      download(
        'usersNotFollowingBack.json',
        JSON.stringify(state.nonFollowers, null, 2),
        'application/json;charset=utf-8'
      );
      logEvent('meta', 'Descarga automática generada.');
    }
  }

  async function massUnfollow() {
    if (state.massRunning) return;
    const targets = currentFiltered();
    if (!targets.length) {
      showToast('No hay usuarios que procesar.');
      return;
    }
    const actionable = targets.filter((u) => !shouldSkipInMass(u));
    if (!actionable.length) {
      showToast('Todos los usuarios filtrados están marcados como favoritos.');
      return;
    }
    const confirmMsg =
      `Vas a dejar de seguir a ${actionable.length} cuenta(s) (excluyendo favoritos). ` +
      'Esto puede activar límites. ¿Continuar?';
    if (!confirm(confirmMsg)) return;

    state.massRunning = true;
    state.massAbort = false;
    el.unfollowAllBtn.disabled = true;
    el.unfollowAllBtn.textContent = 'Procesando…';
    el.stopMassBtn.style.display = '';
    logEvent('meta', `Comienza la rutina masiva con ${actionable.length} cuentas.`);

    for (const u of actionable) {
      if (state.massAbort) break;
      const card = el.list.querySelector(`.card[data-id="${cssEscape(u.id)}"]`);
      await handleUnfollowOne(u.id, card, { silent: true });
    }

    state.massRunning = false;
    state.massAbort = false;
    el.unfollowAllBtn.disabled = false;
    el.unfollowAllBtn.textContent = 'Dejar de seguir a todos';
    el.stopMassBtn.style.display = 'none';
    renderList();
    updateProgress();
    showToast('Proceso masivo terminado.');
    logEvent('meta', 'Rutina masiva finalizada.');
  }

  function abortMass() {
    if (!state.massRunning) return;
    state.massAbort = true;
    el.stopMassBtn.style.display = 'none';
    el.unfollowAllBtn.disabled = false;
    el.unfollowAllBtn.textContent = 'Dejar de seguir a todos';
    showToast('Se detendrá al terminar la acción actual.');
    logEvent('meta', 'Se solicitó detener la rutina masiva.');
  }

  function highlightRandom() {
    const pool = currentFiltered().filter((u) => !state.favorites.has(u.id));
    if (!pool.length) {
      showToast('No hay candidatos para destacar (revisa filtros).');
      return;
    }
    const chosen = pool[Math.floor(Math.random() * pool.length)];
    const card = el.list.querySelector(`.card[data-id="${cssEscape(chosen.id)}"]`);
    if (!card) {
      showToast('No encontré la tarjeta a destacar.');
      return;
    }
    card.scrollIntoView({ behavior: 'smooth', block: 'center' });
    card.style.transition = 'box-shadow .4s ease';
    card.style.boxShadow = '0 0 0 2px rgba(59,130,246,.65)';
    setTimeout(() => { card.style.boxShadow = ''; }, 1400);
    logEvent('meta', `Destacado aleatorio: ${chosen.username}`);
  }

  function exportSession() {
    const payload = {
      generatedAt: new Date().toISOString(),
      totals: {
        totalFollowing: state.totalFollowing,
        fetchedCount: state.fetchedCount,
        nonFollowers: state.nonFollowers.length,
        unfollowed: state.unfollowed,
        simulated: state.simulatedUnfollowed
      },
      favorites: Array.from(state.favorites),
      nonFollowers: state.nonFollowers
    };
    download('ig-session-export.json', JSON.stringify(payload, null, 2), 'application/json;charset=utf-8');
    logEvent('meta', 'Sesión exportada manualmente.');
  }

  function hydrateFromStorage() {
    const restored = loadSessionFromStorage();
    updateProgress();
    renderList();
    if (restored) {
      showToast('Sesión restaurada.');
      logEvent('meta', `Sesión restaurada con ${state.nonFollowers.length} usuarios.`);
    }
  }

  // =======================
  // EVENTOS UI
  // =======================
  el.pauseBtn.addEventListener('click', () => {
    state.running = false;
    setRunning(false);
    showToast('Pausado.');
  });
  el.resumeBtn.addEventListener('click', () => {
    setRunning(true);
    showToast('Reanudando…');
    logEvent('meta', 'Se reanudó la recolección.');
  });
  el.closeBtn.addEventListener('click', () => {
    state.aborted = true;
    setRunning(false);
    panel.remove();
    showToast('Cerrado.');
    logEvent('meta', 'Panel cerrado por el usuario.');
  });

  el.searchInput.addEventListener('input', () => renderList());
  el.segmentSelect.addEventListener('change', () => renderList());
  el.copyBtn.addEventListener('click', copyUsernames);
  el.jsonBtn.addEventListener('click', () =>
    download('usersNotFollowingBack.json',
      JSON.stringify(state.nonFollowers, null, 2),
      'application/json;charset=utf-8')
  );
  el.csvBtn.addEventListener('click', () =>
    download('usersNotFollowingBack.csv',
      toCSV(state.nonFollowers),
      'text/csv;charset=utf-8')
  );
  el.highlightBtn.addEventListener('click', highlightRandom);
  el.speedRange.addEventListener('input', (e) => {
    if (state.autoSpeed) return;
    state.throttleAvg = Number(e.target.value);
    updateSpeedInfo();
    persistConfig();
  });
  el.unfollowAllBtn.addEventListener('click', massUnfollow);
  el.stopMassBtn.addEventListener('click', abortMass);
  el.autoSpeedToggle.addEventListener('change', (e) => {
    state.autoSpeed = e.target.checked;
    updateSpeedInfo();
    persistConfig();
    const msg = state.autoSpeed ? 'Velocidad adaptativa activada.' : 'Modo manual activado.';
    showToast(msg);
    logEvent('meta', msg);
  });
  el.simulateToggle.addEventListener('change', (e) => {
    state.simulateUnfollow = e.target.checked;
    persistConfig();
    showToast(state.simulateUnfollow ? 'Modo simulación activo.' : 'Modo real activo.');
    logEvent('meta', state.simulateUnfollow ? 'Simulación habilitada.' : 'Simulación deshabilitada.');
  });
  el.exportSessionBtn.addEventListener('click', exportSession);
  el.clearCacheBtn.addEventListener('click', () => {
    clearSessionStorage();
    showToast('Caché eliminada.');
    logEvent('meta', 'Se limpió la caché de sesión.');
    updateProgress();
  });

  el.list.addEventListener('click', async (e) => {
    const favBtn = e.target.closest('[data-action="toggle-favorite"]');
    if (favBtn) {
      const card = e.target.closest('.card');
      if (!card) return;
      const userId = card.getAttribute('data-id');
      if (!userId) return;
      toggleFavorite(userId, card);
      return;
    }
    const btn = e.target.closest('[data-action="unfollow-one"]');
    if (!btn) return;
    const card = e.target.closest('.card');
    if (!card) return;
    const userId = card.getAttribute('data-id');
    if (!userId) return;
    if (!confirm('¿Seguro que quieres dejar de seguir a esta cuenta?')) return;
    await handleUnfollowOne(userId, card, { silent: false });
    renderList();
  });

  // =======================
  // ARRANQUE
  // =======================
  el.autoSpeedToggle.checked = state.autoSpeed;
  el.simulateToggle.checked = state.simulateUnfollow;
  updateSpeedInfo();
  hydrateFromStorage();
  mainLoop();
})();
