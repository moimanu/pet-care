const $ = (sel, ctx = document) => ctx.querySelector(sel);
const $$ = (sel, ctx = document) => Array.from(ctx.querySelectorAll(sel));

const EDGES = [
  { from: "node-clients", to: "node-ctrl-1", label: "Pyro5 RPC :9091", bi: true },
  { from: "node-clients", to: "node-ctrl-2", label: "Pyro5 RPC :9092", bi: true },
  { from: "node-clients", to: "node-ctrl-3", label: "Pyro5 RPC :9093", bi: true },
  { from: "node-ctrl-1", to: "node-ctrl-2", label: "Sync P2P", bi: true, vertical: true, dashed: true },
  { from: "node-ctrl-2", to: "node-ctrl-3", label: "Sync P2P", bi: true, vertical: true, dashed: true },
  { from: "node-ctrl-1", to: "node-db-1", label: "SQLite", bi: true },
  { from: "node-ctrl-2", to: "node-db-2", label: "SQLite", bi: true },
  { from: "node-ctrl-3", to: "node-db-3", label: "SQLite", bi: true },
  { from: "node-ctrl-1", to: "node-mqtt", label: "Pub/Sub", bi: true, fromRight: true },
  { from: "node-ctrl-2", to: "node-mqtt", label: "Pub/Sub", bi: true, fromRight: true },
  { from: "node-ctrl-3", to: "node-mqtt", label: "Pub/Sub", bi: true, fromRight: true },
  { from: "node-mqtt", to: "node-sensors", label: "Telemetria / Atuacao", bi: true },
];

const MIDDLEWARE_IDS = [
  "node-ctrl-1", "node-ctrl-2", "node-ctrl-3",
  "node-db-1", "node-db-2", "node-db-3",
];

const board = { x: 0, y: 0, scale: 1 };

const ACTION_TINTS = [
  "rgba(59, 130, 246, 0.16)",
  "rgba(16, 185, 129, 0.16)",
  "rgba(168, 85, 247, 0.16)",
  "rgba(245, 158, 11, 0.16)",
  "rgba(6, 182, 212, 0.16)",
  "rgba(244, 63, 94, 0.16)",
];
let actionColorIdx = -1;
let activeTrace = null;
const seenLogs = new Set();
const logTints = new Map();

function logKey(e) {
  return `${e.ts}|${e.component}|${e.message}`;
}

function applyTransform() {
  const stage = $("#board-stage");
  stage.style.transform = `translate(${board.x}px, ${board.y}px) scale(${board.scale})`;
  const vp = $("#board-viewport");
  vp.style.backgroundPosition = `${board.x}px ${board.y}px`;
  vp.style.backgroundSize = `${24 * board.scale}px ${24 * board.scale}px`;
  $("#zoom-label").textContent = `${Math.round(board.scale * 100)}%`;
}

function positionNodes() {
  $$(".node").forEach((el) => {
    el.style.left = `${Number(el.dataset.x)}px`;
    el.style.top = `${Number(el.dataset.y)}px`;
  });
}

function updateMiddlewareZone() {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  MIDDLEWARE_IDS.forEach((id) => {
    const el = $(`#${id}`);
    const x = Number(el.dataset.x);
    const y = Number(el.dataset.y);
    minX = Math.min(minX, x);
    minY = Math.min(minY, y);
    maxX = Math.max(maxX, x + el.offsetWidth);
    maxY = Math.max(maxY, y + el.offsetHeight);
  });
  const pad = 26;
  const zone = $("#middleware-zone");
  zone.style.left = `${minX - pad}px`;
  zone.style.top = `${minY - pad}px`;
  zone.style.width = `${maxX - minX + pad * 2}px`;
  zone.style.height = `${maxY - minY + pad * 2}px`;
}

function getBox(id) {
  const el = $(`#${id}`);
  const x = Number(el.dataset.x);
  const y = Number(el.dataset.y);
  const w = el.offsetWidth;
  const h = el.offsetHeight;
  return { x, y, w, h, cx: x + w / 2, cy: y + h / 2 };
}

function renderEdges() {
  const svg = $("#flow-edges");
  const defs = svg.querySelector("defs").outerHTML;
  const parts = [defs];

  EDGES.forEach((edge) => {
    const a = getBox(edge.from);
    const b = getBox(edge.to);
    let x1, y1, x2, y2, d;

    if (edge.vertical) {
      const down = b.cy >= a.cy;
      x1 = a.cx;
      y1 = down ? a.y + a.h : a.y;
      x2 = b.cx;
      y2 = down ? b.y : b.y + b.h;
      const midY = (y1 + y2) / 2;
      d = `M ${x1} ${y1} C ${x1} ${midY}, ${x2} ${midY}, ${x2} ${y2}`;
    } else if (edge.fromRight) {
      x1 = a.x + a.w;
      y1 = a.cy;
      x2 = b.x;
      y2 = b.cy;
      const dx = Math.max(60, Math.abs(x2 - x1) * 0.45);
      d = `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;
    } else {
      const right = b.cx >= a.cx;
      x1 = right ? a.x + a.w : a.x;
      y1 = a.cy;
      x2 = right ? b.x : b.x + b.w;
      y2 = b.cy;
      const dx = Math.max(40, Math.abs(x2 - x1) * 0.45);
      const c1x = right ? x1 + dx : x1 - dx;
      const c2x = right ? x2 - dx : x2 + dx;
      d = `M ${x1} ${y1} C ${c1x} ${y1}, ${c2x} ${y2}, ${x2} ${y2}`;
    }

    const mx = (x1 + x2) / 2;
    const my = (y1 + y2) / 2;
    const markerStart = edge.bi ? 'marker-start="url(#arrow)"' : "";
    const cls = edge.dashed ? "edge-path edge-dashed" : "edge-path";
    const labelW = edge.label.length * 6.2 + 12;

    parts.push(
      `<path class="${cls}" d="${d}" marker-end="url(#arrow)" ${markerStart}></path>`,
      `<rect class="edge-label-bg" x="${mx - labelW / 2}" y="${my - 9}" width="${labelW}" height="18" rx="4"></rect>`,
      `<text class="edge-label" x="${mx}" y="${my}">${edge.label}</text>`
    );
  });

  svg.innerHTML = parts.join("");
}

function updateBoardGeometry() {
  updateMiddlewareZone();
  renderEdges();
}

function fitToScreen() {
  const vp = $("#board-viewport");
  const nodes = $$(".node");
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  nodes.forEach((el) => {
    const x = Number(el.dataset.x);
    const y = Number(el.dataset.y);
    minX = Math.min(minX, x);
    minY = Math.min(minY, y);
    maxX = Math.max(maxX, x + el.offsetWidth);
    maxY = Math.max(maxY, y + el.offsetHeight);
  });
  const pad = 48;
  const contentW = maxX - minX + pad * 2;
  const contentH = maxY - minY + pad * 2;
  const scale = Math.min(1, Math.max(0.28, Math.min(vp.clientWidth / contentW, vp.clientHeight / contentH)));
  board.scale = scale;
  board.x = (vp.clientWidth - contentW * scale) / 2 - (minX - pad) * scale;
  board.y = (vp.clientHeight - contentH * scale) / 2 - (minY - pad) * scale;
  applyTransform();
}

function zoomAt(factor, clientX, clientY) {
  const vp = $("#board-viewport");
  const rect = vp.getBoundingClientRect();
  const ox = clientX - rect.left;
  const oy = clientY - rect.top;
  const nextScale = Math.min(1.8, Math.max(0.25, board.scale * factor));
  const ratio = nextScale / board.scale;
  board.x = ox - (ox - board.x) * ratio;
  board.y = oy - (oy - board.y) * ratio;
  board.scale = nextScale;
  applyTransform();
}

function setupBoardInteractions() {
  const vp = $("#board-viewport");
  let panStart = null;
  let dragNode = null;

  vp.addEventListener("pointerdown", (e) => {
    if (e.target.closest("button, input, select, .logs")) return;

    const head = e.target.closest(".node-head");
    if (head) {
      const node = head.closest(".node");
      dragNode = {
        el: node,
        startX: e.clientX,
        startY: e.clientY,
        origX: Number(node.dataset.x),
        origY: Number(node.dataset.y),
      };
      vp.setPointerCapture(e.pointerId);
      return;
    }

    if (e.target.closest(".node")) return;

    panStart = { x: e.clientX - board.x, y: e.clientY - board.y };
    vp.classList.add("panning");
    vp.setPointerCapture(e.pointerId);
  });

  vp.addEventListener("pointermove", (e) => {
    if (dragNode) {
      const dx = (e.clientX - dragNode.startX) / board.scale;
      const dy = (e.clientY - dragNode.startY) / board.scale;
      dragNode.el.dataset.x = Math.round(dragNode.origX + dx);
      dragNode.el.dataset.y = Math.round(dragNode.origY + dy);
      dragNode.el.style.left = `${dragNode.el.dataset.x}px`;
      dragNode.el.style.top = `${dragNode.el.dataset.y}px`;
      updateBoardGeometry();
      return;
    }
    if (panStart) {
      board.x = e.clientX - panStart.x;
      board.y = e.clientY - panStart.y;
      applyTransform();
    }
  });

  const endDrag = () => {
    dragNode = null;
    panStart = null;
    vp.classList.remove("panning");
  };
  vp.addEventListener("pointerup", endDrag);
  vp.addEventListener("pointercancel", endDrag);

  vp.addEventListener(
    "wheel",
    (e) => {
      if (e.target.closest(".logs")) return;
      e.preventDefault();
      zoomAt(e.deltaY < 0 ? 1.1 : 0.9, e.clientX, e.clientY);
    },
    { passive: false }
  );

  $("#btn-zoom-in").addEventListener("click", () => {
    const r = vp.getBoundingClientRect();
    zoomAt(1.15, r.left + r.width / 2, r.top + r.height / 2);
  });
  $("#btn-zoom-out").addEventListener("click", () => {
    const r = vp.getBoundingClientRect();
    zoomAt(0.85, r.left + r.width / 2, r.top + r.height / 2);
  });
  $("#btn-fit").addEventListener("click", fitToScreen);
}

function setBadge(el, text, kind) {
  el.textContent = text;
  el.className = `badge badge-${kind}`;
}

async function postAction(payload) {
  actionColorIdx = (actionColorIdx + 1) % ACTION_TINTS.length;
  activeTrace = {
    tint: ACTION_TINTS[actionColorIdx],
    until: Date.now() + 2500,
  };
  setBadge($("#conn-badge"), "PROCESSANDO", "warn");
  try {
    const res = await fetch("/api/action", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (payload.op === "client_command" && data.command_id) {
      $("#cmd-id-input").value = data.command_id;
    }
  } catch (err) {
    console.error(err);
  }
}

function escapeHtml(str) {
  return String(str).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

function tagNewLogs(logs) {
  const now = Date.now();
  logs.forEach((entry) => {
    const key = logKey(entry);
    if (!seenLogs.has(key)) {
      seenLogs.add(key);
      if (activeTrace && now <= activeTrace.until) {
        logTints.set(key, activeTrace.tint);
      }
    }
  });
}

function renderLogs(containerId, entries) {
  const el = $(containerId);
  const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 24;
  el.innerHTML = entries
    .map((e) => {
      const tint = logTints.get(logKey(e));
      const style = tint ? ` style="background:${tint}"` : "";
      return `<div class="log-line log-${e.level}"${style}><span class="log-ts">${e.ts}</span><span>${escapeHtml(e.message)}</span></div>`;
    })
    .join("");
  if (atBottom) el.scrollTop = el.scrollHeight;
}

function renderState(data) {
  const { state, logs } = data;
  tagNewLogs(logs);
  const c = state.clients;

  setBadge($("#conn-badge"), "ATIVO", "ok");
  if (c.state) {
    $("#m-racao").textContent = `${c.state.racao_gramas}g`;
    $("#m-agua").textContent = `${c.state.agua_ml}ml`;
    $("#m-areia").textContent = c.state.areia_usos;
    $("#m-fuga").textContent = c.state.status_fuga;
    $("#m-alarme").textContent = c.state.alarme_ativo ? "ATIVO" : "DESLIGADO";
    $("#m-hash").textContent = c.majority_hash || "--";
  }

  const hasDiv = (c.divergent || []).length > 0;
  const clientStatus =
    c.quorum_reached === null || c.quorum_reached === undefined
      ? "AGUARDANDO"
      : !c.quorum_reached
        ? "ERRO"
        : hasDiv
          ? "DIVERGENCIA"
          : "ATIVO";
  const clientKind =
    c.quorum_reached === null || c.quorum_reached === undefined
      ? "idle"
      : !c.quorum_reached
        ? "err"
        : hasDiv
          ? "warn"
          : "ok";
  setBadge($("#status-clients"), clientStatus, clientKind);
  $("#rpc-votes").textContent = c.votes || "--";

  state.controllers.forEach((n) => {
    const statusText = !n.alive
      ? "INATIVO"
      : n.byzantine
        ? "BIZANTINO"
        : n.divergent
          ? "DIVERGENTE"
          : n.rpc_ok
            ? "ATIVO"
            : "INICIANDO";
    const badgeKind = !n.alive
      ? "idle"
      : n.byzantine || n.divergent
        ? "err"
        : n.rpc_ok
          ? "ok"
          : "warn";
    const role = n.is_primary ? "LIDER" : "REPLICA";

    $(`#node-ctrl-${n.id}`).classList.toggle("node-byzantine", Boolean(n.alive && n.byzantine));
    $(`#title-ctrl-${n.id}`).textContent = `Controlador ${n.id} (${role})`;
    setBadge($(`#status-ctrl-${n.id}`), statusText, badgeKind);
    $(`#actions-ctrl-${n.id}`).innerHTML = `
      ${
        n.alive
          ? `<button class="btn btn-sm" data-op="controller_stop" data-node="${n.id}">Parar Processo</button>`
          : `<button class="btn btn-sm" data-op="controller_start" data-node="${n.id}">Iniciar Processo</button>`
      }
      ${
        n.alive && !n.is_primary && !n.byzantine
          ? `<button class="btn btn-sm" data-op="rpc_promote" data-node="${n.id}">Promover a Lider</button>`
          : ""
      }
      ${
        n.alive
          ? `<button class="btn btn-sm ${n.byzantine ? "btn-warn" : "btn-danger"}" data-op="controller_byzantine" data-node="${n.id}" data-enabled="${!n.byzantine}">${
              n.byzantine ? "Restaurar Honesto" : "Tornar Bizantino"
            }</button>`
          : ""
      }
    `;
    renderLogs(`#logs-ctrl-${n.id}`, logs.filter((l) => l.component === `controller_${n.id}`));
  });

  const majHash = c.majority_hash;
  state.storage.forEach((s) => {
    const synced = !majHash || s.hash === majHash;
    setBadge($(`#status-db-${s.id}`), synced ? "SINCRONIZADO" : "DIVERGENTE", synced ? "ok" : "warn");
    $(`#hash-db-${s.id}`).textContent = s.hash;
    $(`#meta-db-${s.id}`).textContent =
      `Racao: ${s.state.racao_gramas}g | Agua: ${s.state.agua_ml}ml | Areia: ${s.state.areia_usos} | ${s.state.status_fuga} | Ev: ${s.event_count}`;
    renderLogs(`#logs-db-${s.id}`, logs.filter((l) => l.component === `storage_${s.id}`));
  });

  setBadge($("#status-mqtt"), state.mqtt.connected ? "ATIVO" : "ERRO", state.mqtt.connected ? "ok" : "err");
  setBadge($("#status-sensors"), state.sensors.alive ? "ATIVO" : "INATIVO", state.sensors.alive ? "ok" : "idle");

  renderLogs("#logs-clients", logs.filter((l) => l.component === "clients" || l.component === "rpc"));
  renderLogs("#logs-mqtt", logs.filter((l) => l.component === "mqtt"));
  renderLogs("#logs-sensors", logs.filter((l) => l.component === "sensors"));

  updateBoardGeometry();
}

document.addEventListener("click", (e) => {
  const btn = e.target.closest("[data-op]");
  if (!btn) return;
  const op = btn.dataset.op;

  if (op === "new_uuid") {
    $("#cmd-id-input").value = crypto.randomUUID();
    return;
  }
  if (op === "client_command") {
    postAction({ op, action: btn.dataset.action, command_id: $("#cmd-id-input").value });
    return;
  }
  if (op === "rpc_quorum" || op === "rpc_read" || op === "sensor_toggle_proc") {
    postAction({ op });
    return;
  }
  if (op === "rpc_promote" || op === "controller_stop" || op === "controller_start") {
    postAction({ op, node_id: Number(btn.dataset.node) });
    return;
  }
  if (op === "controller_byzantine") {
    postAction({
      op,
      node_id: Number(btn.dataset.node),
      enabled: btn.dataset.enabled === "true",
    });
    return;
  }
  if (op === "storage_mutate") {
    const nid = Number(btn.dataset.node);
    const field = $(`#field-db-${nid}`).value;
    const rawVal = $(`#val-db-${nid}`).value;
    const value = field === "status_fuga" ? rawVal : Number(rawVal);
    postAction({ op, node_id: nid, field, value });
    return;
  }
  if (op === "mqtt_publish") {
    postAction({ op, topic: $("#mqtt-topic").value, payload: $("#mqtt-payload").value });
    return;
  }
  if (op === "sensor_auto") {
    postAction({ op, enabled: btn.dataset.enabled === "true" });
    return;
  }
  if (op === "sensor_publish") {
    const sensor = btn.dataset.sensor;
    const map = {
      racao: { nivel_gramas: Number($("#inp-racao").value) },
      agua: { nivel_ml: Number($("#inp-agua").value) },
      areia: { usos_acumulados: Number($("#inp-areia").value) },
      fuga: { perimetro: $("#inp-fuga").value },
    };
    postAction({ op, sensor, payload: map[sensor] });
  }
});

positionNodes();
setupBoardInteractions();
updateBoardGeometry();
fitToScreen();
window.addEventListener("resize", updateBoardGeometry);

const ev = new EventSource("/api/events");
ev.onmessage = (msg) => renderState(JSON.parse(msg.data));
ev.onerror = () => setBadge($("#conn-badge"), "ERRO", "err");
