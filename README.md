(async () => {
  // =======================
  // UTILIDADES BÁSICAS
  // =======================
  const getCookie = (name) => {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop().split(";").shift();
  };

  const sleep = (ms) => new Promise(res => setTimeout(res, ms));
  const encodeVars = (v) => encodeURIComponent(JSON.stringify(v));

  const csrftoken = getCookie("csrftoken");
  const ds_user_id = getCookie("ds_user_id");

  if (!location.hostname.includes("instagram.com")) {
    throw new Error("Este script solo funciona dentro de instagram.com.");
  }
  if (!ds_user_id || !csrftoken) {
    throw new Error("No pude leer cookies clave (ds_user_id/csrftoken). Inicia sesión.");
  }

  // Query de 'siguiendo' (edge_follow)
  const QUERY_HASH = "3dec7e2c57367ef3da3d987d89f9dbc8";
  const baseUrl = "https://www.instagram.com/graphql/query/?";
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

  // Endpoint para dejar de seguir
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
    throttleAvg: 1200, // ms promedio entre requests
    throttleJitter: 400, // +/- aleatorio
    coolDownEvery: 6, // cada N páginas, enfriar
    coolDownMs: 10000, // 10s
    coolDownCounter: 0,
    nonFollowers: [], // {id, username, full_name, is_private, profile_pic_url}
    errors: 0,

    // Unfollow
    massRunning: false,
    massAbort: false,
    unfollowed: 0,
    unfollowErrors: 0,
  };

  // =======================
  // UI BONITA (Shadow DOM)
  // =======================
  const panel = document.createElement("div");
  panel.id = "ig-audit-panel";
  panel.style.all = "initial";
  panel.style.position = "fixed";
  panel.style.top = "24px";
  panel.style.right = "24px";
  panel.style.zIndex = 2147483646;
  panel.style.width = "min(460px, 94vw)";
  panel.style.height = "min(82vh, 720px)";
  panel.style.boxShadow = "0 10px 30px rgba(0,0,0,.35)";
  panel.style.borderRadius = "16px";
  panel.style.overflow = "hidden";
  panel.style.fontFamily = "Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif";
  panel.style.background = "linear-gradient(135deg, rgba(20,20,24,.92), rgba(18,18,22,.88))";
  panel.style.backdropFilter = "blur(12px)";
  document.body.appendChild(panel);

  const root = panel.attachShadow({ mode: "open" });
  root.innerHTML = `
    <style>
      :host { color: #f1f5f9; }
      * { box-sizing: border-box; }
      .wrap { display: grid; grid-template-rows: auto auto auto 1fr auto; height: 100%; }
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
      button, .btn {
        appearance: none; border: 1px solid rgba(255,255,255,.08);
        background: rgba(255,255,255,.06); color: #e2e8f0; font-size: 12px;
        padding: 8px 10px; border-radius: 10px; cursor: pointer;
        transition: transform .06s ease, background .2s ease, border-color .2s ease, opacity .2s ease;
      }
      button:hover { background: rgba(255,255,255,.12); border-color: rgba(255,255,255,.18); }
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
        background: linear-gradient(90deg, #22c55e, #f59e0b, #ef4444);
        width: 0%; transition: width .3s ease;
      }
      .stats { display: flex; gap: 10px; flex-wrap: wrap; font-size: 12px; color: #cbd5e1; }
      .chip {
        background: rgba(255,255,255,.06); border: 1px solid rgba(255,255,255,.08);
        padding: 6px 8px; border-radius: 999px;
      }
      .tools {
        display: grid; gap: 10px; padding: 10px 16px;
        border-bottom: 1px solid rgba(255,255,255,.08);
        grid-template-columns: 1fr auto auto auto auto;
        align-items: center;
      }
      input[type="text"] {
        width: 100%; padding: 8px 10px; border-radius: 10px;
        background: rgba(255,255,255,.06); border: 1px solid rgba(255,255,255,.12);
        color: #e2e8f0; outline: none; font-size: 12px;
      }
      input[type="range"] { width: 120px; }
      .range-wrap { display: flex; align-items: center; gap: 8px; font-size: 11px; color: #9ca3af; }
      .list { overflow: auto; padding: 12px; display: grid; gap: 8px; }
      .card {
        display: grid; grid-template-columns: 44px 1fr auto;
        gap: 10px; align-items: center;
        background: rgba(255,255,255,.04); border: 1px solid rgba(255,255,255,.08);
        border-radius: 12px; padding: 8px;
      }
      .avatar {
        width: 44px; height: 44px; border-radius: 999px; background: #111; object-fit: cover;
        border: 1px solid rgba(255,255,255,.08);
      }
      .meta { display: grid; }
      .name { font-size: 13px; color: #e5e7eb; }
      .user { font-size: 12px; color: #9ca3af; }
      .tag { font-size: 10px; color: #fef3c7; background: rgba(245, 158, 11, .15); border: 1px solid rgba(245, 158, 11, .35); padding: 2px 6px; border-radius: 999px; }
      a { color: #93c5fd; text-decoration: none; }
      a:hover { text-decoration: underline; }
      footer { padding: 10px 12px; border-top: 1px solid rgba(255,255,255,.08); font-size: 11px; color: #94a3b8; display: flex; gap: 10px; align-items: center; }
      .toast {
        position: absolute; bottom: 12px; right: 12px;
        background: rgba(17,24,39,.96); border: 1px solid rgba(255,255,255,.12);
        color: #e5e7eb; padding: 8px 10px; border-radius: 10px;
        box-shadow: 0 6px 16px rgba(0,0,0,.35); font-size: 12px; opacity: 0; transform: translateY(6px);
        transition: opacity .2s ease, transform .2s ease;
      }
      .toast.show { opacity: 1; transform: translateY(0); }
      .empty { text-align: center; color: #9ca3af; padding: 40px 10px; font-size: 13px; }
      .count-pill { margin-left: 6px; background: rgba(59,130,246,.15); border: 1px solid rgba(59,130,246,.35); color: #bfdbfe; padding: 2px 6px; border-radius: 999px; font-size: 11px; }
      .mini { padding: 6px 8px; font-size: 11px; }
      .pill { font-size: 10px; padding: 2px 6px; border-radius: 999px; background: rgba(59,130,246,.15); border: 1px solid rgba(59,130,246,.35); color: #bfdbfe; }
    </style>

    <div class="wrap">
      <header>
        <div class="dot" id="statusDot"></div>
        <div>
          <h1>IG • No te siguen de vuelta</h1>
          <div class="muted" id="subtitle">Preparando…</div>
        </div>
        <div class="actions">
          <button id="pauseBtn" class="btn">Pausar</button>
          <button id="resumeBtn" class="btn success" style="display:none;">Reanudar</button>
          <button id="closeBtn" class="btn danger">Cerrar</button>
        </div>
      </header>

      <div class="progress">
        <div class="row" style="justify-content: space-between;">
          <div class="stats">
            <span class="chip">Progreso: <b id="progText">0/0 · 0%</b></span>
            <span class="chip">No te siguen: <b id="countNF">0</b></span>
            <span class="chip">Errores: <b id="errNF">0</b></span>
            <span class="chip">Unfollows: <b id="ufCount">0</b></span>
            <span class="chip">Fallos: <b id="ufErr">0</b></span>
          </div>
          <div class="range-wrap">
            Velocidad
            <input id="speedRange" type="range" min="700" max="3000" step="100" value="1200" />
          </div>
        </div>
        <div class="bar"><div id="barFill"></div></div>
      </div>

      <div class="tools">
        <input id="searchInput" type="text" placeholder="Filtrar por usuario o nombre…" />
        <button id="copyBtn">Copiar</button>
        <button id="jsonBtn">JSON</button>
        <button id="csvBtn">CSV</button>
        <button id="unfollowAllBtn" class="warn">Dejar de seguir a todos</button>
      </div>

      <div class="list" id="list"></div>

      <footer>
        Consejo: evita acciones masivas sin pausas. Si ves errores 429, baja la velocidad o espera.
        <span class="count-pill" id="cooldownInfo" title="Se aplica pausa corta cada cierto número de páginas">Auto-pausa activa</span>
      </footer>

      <div class="toast" id="toast"></div>
    </div>
  `;

  // =======================
  // REFERENCIAS UI
  // =======================
  const el = {
    subtitle: root.getElementById("subtitle"),
    statusDot: root.getElementById("statusDot"),
    pauseBtn: root.getElementById("pauseBtn"),
    resumeBtn: root.getElementById("resumeBtn"),
    closeBtn: root.getElementById("closeBtn"),
    progText: root.getElementById("progText"),
    countNF: root.getElementById("countNF"),
    errNF: root.getElementById("errNF"),
    ufCount: root.getElementById("ufCount"),
    ufErr: root.getElementById("ufErr"),
    barFill: root.getElementById("barFill"),
    list: root.getElementById("list"),
    searchInput: root.getElementById("searchInput"),
    copyBtn: root.getElementById("copyBtn"),
    jsonBtn: root.getElementById("jsonBtn"),
    csvBtn: root.getElementById("csvBtn"),
    speedRange: root.getElementById("speedRange"),
    toast: root.getElementById("toast"),
    cooldownInfo: root.getElementById("cooldownInfo"),
    unfollowAllBtn: root.getElementById("unfollowAllBtn"),
  };

  const showToast = (msg) => {
    el.toast.textContent = msg;
    el.toast.classList.add("show");
    setTimeout(() => el.toast.classList.remove("show"), 1800);
  };

  const setRunning = (running) => {
    state.running = running;
    el.statusDot.style.background = running ? "#22c55e" : "#f59e0b";
    el.statusDot.style.boxShadow = running
      ? "0 0 12px rgba(34,197,94,.7)"
      : "0 0 12px rgba(245,158,11,.7)";
    el.pauseBtn.style.display = running ? "" : "none";
    el.resumeBtn.style.display = running ? "none" : "";
    el.subtitle.textContent = running ? "Recolectando datos…" : "En pausa";
  };

  const percent = () => {
    if (!state.totalFollowing || state.totalFollowing === 0) return 0;
    return Math.min(100, Math.floor((state.fetchedCount / state.totalFollowing) * 100));
  };

  const updateProgress = () => {
    el.progText.textContent = `${state.fetchedCount}/${state.totalFollowing ?? 0} · ${percent()}%`;
    el.barFill.style.width = `${percent()}%`;
    el.countNF.textContent = String(state.nonFollowers.length);
    el.errNF.textContent = String(state.errors);
    el.ufCount.textContent = String(state.unfollowed);
    el.ufErr.textContent = String(state.unfollowErrors);
  };

  const escape = (s) => (s ?? "").replace(/[&<>"']/g, m => ({
    "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"
  }[m]));

  // =======================
  // UNFOLLOW API
  // =======================
  const apiUnfollow = async (userId) => {
    try {
      const res = await fetch(unfollowUrl(userId), {
        method: "POST",
        credentials: "include",
        headers: {
          "X-CSRFToken": csrftoken,
          "X-Requested-With": "XMLHttpRequest",
          "Referer": location.origin + "/",
          "Content-Type": "application/x-www-form-urlencoded"
          // "X-IG-App-ID": "936619743392459" // opcional; se usa en web. Déjalo comentado para menor fricción.
        },
        body: "source=profile"
      });

      if (res.status === 429) {
        // Rate limited: espera y devuelve señal
        return { ok: false, rateLimited: true };
      }

      if (!res.ok) {
        return { ok: false, error: `HTTP ${res.status}` };
      }
      const j = await res.json().catch(() => ({}));
      if (j && j.status === "ok") return { ok: true };
      return { ok: true }; // algunas respuestas no traen status, pero el 200 suele bastar
    } catch (e) {
      return { ok: false, error: e?.message || "Error de red" };
    }
  };

  // =======================
  // RENDER DE CARDS
  // =======================
  const makeCard = (user) => {
    const link = `https://www.instagram.com/${encodeURIComponent(user.username)}/`;
    const privateTag = user.is_private ? `<span class="tag" title="Cuenta privada">Privado</span>` : "";
    const imgSrc = user.profile_pic_url || "";
    const html = `
      <div class="card" data-id="${escape(user.id)}" data-username="${escape(user.username).toLowerCase()}" data-name="${escape(user.full_name).toLowerCase()}">
        <img class="avatar" loading="lazy" src="${imgSrc}" alt="${escape(user.username)}"/>
        <div class="meta">
          <div class="name">
            ${escape(user.full_name) || "Sin nombre"} ${privateTag}
          </div>
          <div class="user">@<a class="profile-link" href="${link}" target="_blank" rel="noopener noreferrer">${escape(user.username)}</a></div>
        </div>
        <div class="row">
          <a class="btn mini" href="${link}" target="_blank" rel="noopener noreferrer">Ver perfil</a>
          <button class="btn mini danger" data-action="unfollow-one">Dejar de seguir</button>
        </div>
      </div>
    `;
    const wrapper = document.createElement("div");
    wrapper.innerHTML = html.trim();
    return wrapper.firstElementChild;
  };

  const currentFiltered = () => {
    const term = (el.searchInput.value || "").trim().toLowerCase();
    let items = state.nonFollowers;
    if (term) {
      items = items.filter(u =>
        u.username.toLowerCase().includes(term) ||
        (u.full_name || "").toLowerCase().includes(term)
      );
    }
    return items;
  };

  const renderList = () => {
    const items = currentFiltered();
    el.list.innerHTML = "";
    if (!items.length) {
      const empty = document.createElement("div");
      empty.className = "empty";
      empty.textContent = state.nonFollowers.length
        ? "No hay coincidencias con el filtro."
        : "Aún no se detectan usuarios.";
      el.list.appendChild(empty);
      return;
    }
    const frag = document.createDocumentFragment();
    for (const u of items) frag.appendChild(makeCard(u));
    el.list.appendChild(frag);
  };

  // =======================
  // EXPORTS
  // =======================
  const download = (filename, content, type = "application/octet-stream") => {
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob([content], { type }));
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
  };

  const toCSV = (rows) => {
    const header = ["id","username","full_name","is_private","profile_pic_url"];
    const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
    return [header.join(","), ...rows.map(r => header.map(k => esc(r[k])).join(","))].join("\n");
  };

  const copyUsernames = async () => {
    const list = state.nonFollowers.map(u => u.username).join("\n");
    try {
      await navigator.clipboard.writeText(list);
      showToast("Usernames copiados al portapapeles.");
    } catch {
      download("usernames.txt", list, "text/plain;charset=utf-8");
      showToast("No se pudo copiar. Descargué un .txt.");
    }
  };

  // =======================
  // FETCH LOOP (RECOLECCIÓN)
  // =======================
  const fetchPage = async () => {
    const url = makeUrl(state.after);
    try {
      const res = await fetch(url, { credentials: "include" });
      const data = await res.json();

      const edge = data?.data?.user?.edge_follow;
      if (!edge) throw new Error("Respuesta inesperada.");

      if (state.totalFollowing == null) state.totalFollowing = edge.count || 0;

      const edges = edge.edges || [];
      for (const item of edges) {
        const n = item.node || {};
        // Si NO te sigue de vuelta
        if (!n.follows_viewer) {
          state.nonFollowers.push({
            id: n.id,
            username: n.username,
            full_name: n.full_name || "",
            is_private: !!n.is_private,
            profile_pic_url: n.profile_pic_url || ""
          });
        }
      }

      state.fetchedCount += edges.length;
      state.hasNext = !!edge.page_info?.has_next_page;
      state.after = edge.page_info?.end_cursor || null;

      updateProgress();
      renderList();

    } catch (err) {
      state.errors += 1;
      updateProgress();
      console.warn("Error en fetch:", err);
    }
  };

  const mainLoop = async () => {
    setRunning(true);
    el.subtitle.textContent = "Recolectando datos…";
    while (!state.aborted && state.hasNext) {
      if (!state.running) {
        await sleep(200);
        continue;
      }
      await fetchPage();

      // Throttle con jitter
      const jitter = (Math.random() * state.throttleJitter * 2) - state.throttleJitter;
      const wait = Math.max(300, state.throttleAvg + jitter);
      await sleep(wait);

      // Enfriamiento cada cierto número de páginas
      state.coolDownCounter++;
      if (state.coolDownCounter >= state.coolDownEvery) {
        state.coolDownCounter = 0;
        el.subtitle.textContent = "Pausa breve…";
        await sleep(state.coolDownMs);
        el.subtitle.textContent = "Recolectando datos…";
      }
    }
    setRunning(false);
    if (!state.aborted) {
      el.subtitle.textContent = "Completado.";
      showToast("Listo. Exporta si quieres.");
      updateProgress();
      renderList();
      download("usersNotFollowingBack.json",
        JSON.stringify(state.nonFollowers, null, 2),
        "application/json;charset=utf-8"
      );
    }
  };

  // =======================
  // UNFOLLOW LÓGICA
  // =======================
  const removeFromState = (userId) => {
    const idx = state.nonFollowers.findIndex(u => u.id === userId);
    if (idx >= 0) state.nonFollowers.splice(idx, 1);
  };

  const animateRemoveCard = (card) => {
    if (!card) return;
    card.style.transition = "opacity .25s ease, transform .25s ease";
    card.style.opacity = "0";
    card.style.transform = "scale(0.98)";
    setTimeout(() => card.remove(), 260);
  };

  const handleUnfollowOne = async (userId, cardEl, opts = { silent: false }) => {
    const originalBtn = cardEl?.querySelector('[data-action="unfollow-one"]');
    if (originalBtn) originalBtn.disabled = true;

    // Respeta el slider de velocidad
    const jitter = (Math.random() * state.throttleJitter * 2) - state.throttleJitter;
    const wait = Math.max(400, state.throttleAvg + jitter);
    await sleep(200); // pequeña espera inicial para no disparar todo al mismo tiempo

    const res = await apiUnfollow(userId);

    if (res.rateLimited) {
      // Rate limit: pausa más larga
      state.unfollowErrors += 1;
      updateProgress();
      if (!opts.silent) showToast("Rate limit. Esperando 60s…");
      await sleep(60000);
      if (originalBtn) originalBtn.disabled = false;
      return false;
    }

    if (!res.ok) {
      state.unfollowErrors += 1;
      updateProgress();
      if (!opts.silent) showToast("Error al dejar de seguir.");
      if (originalBtn) originalBtn.disabled = false;
      return false;
    }

    state.unfollowed += 1;
    removeFromState(userId);
    updateProgress();
    if (!opts.silent) showToast("Listo: dejaste de seguir.");
    animateRemoveCard(cardEl);
    // Espera controlada antes del siguiente
    await sleep(wait);
    return true;
  };

  const massUnfollow = async () => {
    if (state.massRunning) return;
    const targets = currentFiltered(); // usa filtro actual si existe
    if (!targets.length) {
      showToast("No hay usuarios que procesar.");
      return;
    }
    const confirmMsg =
      `Vas a dejar de seguir a ${targets.length} cuenta(s). ` +
      `Esto puede activar límites. ¿Continuar?`;
    if (!confirm(confirmMsg)) return;

    state.massRunning = true;
    state.massAbort = false;
    el.unfollowAllBtn.disabled = true;
    el.unfollowAllBtn.textContent = "Procesando…";

    for (const u of [...targets]) {
      if (state.massAbort) break;
      // Busca la card visible correspondiente
      const card = el.list.querySelector(`.card[data-id="${CSS.escape(u.id)}"]`);
      await handleUnfollowOne(u.id, card, { silent: true });
      // Re-render parcial si hay filtro activo
      // Quitar render completo para no perder botón global
    }

    state.massRunning = false;
    el.unfollowAllBtn.disabled = false;
    el.unfollowAllBtn.textContent = "Dejar de seguir a todos";
    renderList();
    showToast("Proceso masivo terminado.");
  };

  // =======================
  // EVENTOS UI
  // =======================
  el.pauseBtn.addEventListener("click", () => {
    state.running = false;
    setRunning(false);
    showToast("Pausado.");
  });
  el.resumeBtn.addEventListener("click", () => {
    setRunning(true);
    showToast("Reanudando…");
  });
  el.closeBtn.addEventListener("click", () => {
    state.aborted = true;
    setRunning(false);
    panel.remove();
    showToast("Cerrado.");
  });

  el.searchInput.addEventListener("input", () => renderList());
  el.copyBtn.addEventListener("click", copyUsernames);
  el.jsonBtn.addEventListener("click", () =>
    download("usersNotFollowingBack.json",
      JSON.stringify(state.nonFollowers, null, 2),
      "application/json;charset=utf-8")
  );
  el.csvBtn.addEventListener("click", () =>
    download("usersNotFollowingBack.csv",
      toCSV(state.nonFollowers),
      "text/csv;charset=utf-8")
  );
  el.speedRange.addEventListener("input", (e) => {
    state.throttleAvg = Number(e.target.value);
  });

  // Botón global "Dejar de seguir a todos"
  el.unfollowAllBtn.addEventListener("click", massUnfollow);

  // Delegación de eventos para el botón "Dejar de seguir" en cada card
  el.list.addEventListener("click", async (e) => {
    const btn = e.target.closest('[data-action="unfollow-one"]');
    if (!btn) return;
    const card = e.target.closest('.card');
    if (!card) return;
    const userId = card.getAttribute('data-id');
    if (!userId) return;
    // Confirmación individual
    if (!confirm("¿Seguro que quieres dejar de seguir a esta cuenta?")) return;
    await handleUnfollowOne(userId, card, { silent: false });
  });

  // =======================
  // ARRANQUE
  // =======================
  updateProgress();
  renderList();
  mainLoop();
})();
