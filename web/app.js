/**
 * PetCare Distributed System - Vanilla Minimal Dashboard
 * Zero emojis, clean state-actions-view separation, SSE-driven.
 */
(() => {
  const $ = (sel, ctx = document) => ctx.querySelector(sel);
  const $$ = (sel, ctx = document) => Array.from(ctx.querySelectorAll(sel));

  // State
  const state = {
    connected: false,
    filterLevel: "ALL",
    logs: [],
    maxLogs: 250,
    pet: {
      racao_gramas: 0.0,
      agua_ml: 0.0,
      areia_usos: 0,
      status_fuga: "--",
      alarme_ativo: false,
      hash: "",
      atualizado_em: "",
    },
    nodes: {
      1: { hash: "--", status: "OK", is_primary: true },
      2: { hash: "--", status: "OK", is_primary: false },
      3: { hash: "--", status: "OK", is_primary: false },
    },
  };

  // View Helpers & DOM elements
  const elConnDot = $("#conn-dot");
  const elConnText = $("#conn-text");
  const elSyncTime = $("#last-sync-time");
  const elFeedback = $("#action-feedback");
  const elLogFeed = $("#log-feed");
  const elLogCount = $("#log-count");

  function showFeedback(message, isError = false) {
    elFeedback.textContent = message;
    elFeedback.className = `feedback-box show ${isError ? "feedback-error" : "feedback-success"}`;
    setTimeout(() => {
      elFeedback.className = "feedback-box";
    }, 4000);
  }

  function renderConnection(status, label) {
    state.connected = status;
    elConnDot.className = `conn-dot ${status ? "online" : "offline"}`;
    elConnText.textContent = label;
  }

  function renderPetState() {
    const p = state.pet;
    $("#val-food").textContent = Number(p.racao_gramas || 0).toFixed(1);
    $("#val-water").textContent = Number(p.agua_ml || 0).toFixed(1);
    $("#val-litter").textContent = String(p.areia_usos ?? 0);

    const elPerimeter = $("#val-perimeter");
    const perimeter = String(p.status_fuga || "--").toUpperCase();
    elPerimeter.textContent = perimeter;
    elPerimeter.className = `badge ${perimeter === "DENTRO" ? "badge-success" : perimeter === "FORA" ? "badge-danger" : "badge-neutral"}`;

    const elAlarm = $("#val-alarm");
    const isAlarm = Boolean(p.alarme_ativo);
    elAlarm.textContent = isAlarm ? "DISPARADO" : "DESARMADO";
    elAlarm.className = `badge ${isAlarm ? "badge-danger" : "badge-neutral"}`;

    if (p.hash) {
      const shortHash = p.hash.slice(0, 14);
      $("#hash-node-1").textContent = `hash: ${shortHash}...`;
      $("#hash-node-2").textContent = `hash: ${shortHash}...`;
      $("#hash-node-3").textContent = `hash: ${shortHash}...`;
    }

    if (p.atualizado_em) {
      elSyncTime.textContent = `Ultima sincronizacao: ${p.atualizado_em.slice(11, 19)}`;
    }
  }

  function renderLogEntry(entry) {
    if (state.filterLevel !== "ALL" && entry.level !== state.filterLevel) {
      return;
    }
    const row = document.createElement("div");
    row.className = "log-entry";
    const ts = entry.timestamp ? entry.timestamp.slice(11, 19) : new Date().toISOString().slice(11, 19);
    const lvl = entry.level || "INFO";
    const src = entry.component || "Sys";

    row.innerHTML = `
      <span class="log-ts">${ts}</span>
      <span class="log-lvl ${lvl}">${lvl}</span>
      <span class="log-src">[${src}]</span>
      <span class="log-msg">${escapeHtml(entry.message || "")}</span>
    `;
    elLogFeed.appendChild(row);
    elLogFeed.scrollTop = elLogFeed.scrollHeight;
  }

  function rebuildLogs() {
    elLogFeed.innerHTML = "";
    state.logs.forEach(renderLogEntry);
    elLogCount.textContent = `${state.logs.length} entradas`;
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  // Network Actions
  async function postCommand(endpoint, payload = {}) {
    try {
      const res = await fetch(`/api/command/${endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok || !data.sucesso) {
        showFeedback(data.mensagem || "Erro na solicitacao", true);
      } else {
        showFeedback(data.mensagem || "Comando executado com sucesso", false);
        if (data.estado) {
          state.pet = Object.assign({}, state.pet, data.estado);
          renderPetState();
        }
      }
      return data;
    } catch (err) {
      showFeedback(`Falha de comunicacao: ${err.message}`, true);
      throw err;
    }
  }

  async function runScenario(scenarioKey, resultEl) {
    resultEl.className = "scenario-res show";
    resultEl.textContent = "Executando cenario...";
    try {
      const res = await fetch(`/api/scenarios/${scenarioKey}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      const data = await res.json();
      resultEl.textContent = JSON.stringify(data, null, 2);
    } catch (err) {
      resultEl.textContent = `Erro: ${err.message}`;
    }
  }

  // SSE Connection
  function setupEventSource() {
    const sse = new EventSource("/api/events");

    sse.onopen = () => {
      renderConnection(true, "SSE Ativo (Tempo Real)");
    };

    sse.onerror = () => {
      renderConnection(false, "SSE Desconectado (Tentando...)");
    };

    sse.addEventListener("state", (evt) => {
      try {
        const payload = JSON.parse(evt.data);
        state.pet = Object.assign({}, state.pet, payload);
        renderPetState();
      } catch (err) {
        console.error("Erro parsing state event", err);
      }
    });

    sse.addEventListener("log", (evt) => {
      try {
        const entry = JSON.parse(evt.data);
        state.logs.push(entry);
        if (state.logs.length > state.maxLogs) {
          state.logs.shift();
        }
        renderLogEntry(entry);
        elLogCount.textContent = `${state.logs.length} entradas`;
      } catch (err) {
        console.error("Erro parsing log event", err);
      }
    });
  }

  // Event Listeners & Binding
  function bindEvents() {
    // Dispense Food
    $("#btn-dispense").addEventListener("click", () => {
      const val = parseFloat($("#input-food-qty").value) || 50.0;
      postCommand("dispense_food", { quantidade: val });
    });

    // Refill Water
    $("#btn-water").addEventListener("click", () => {
      const val = parseFloat($("#input-water-qty").value) || 200.0;
      postCommand("refill_water", { quantidade: val });
    });

    // Reset Litter
    $("#btn-clean-litter").addEventListener("click", () => {
      postCommand("clean_litter");
    });

    // Silence Alarm
    $("#btn-silence").addEventListener("click", () => {
      postCommand("silence_alarm");
    });

    // Clear Logs
    $("#btn-clear-logs").addEventListener("click", () => {
      state.logs = [];
      rebuildLogs();
    });

    // Filter Buttons
    $$(".filter-btn").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        $$(".filter-btn").forEach((b) => b.classList.remove("active"));
        e.target.classList.add("active");
        state.filterLevel = e.target.dataset.filter;
        rebuildLogs();
      });
    });

    // Scenarios
    $("#btn-run-sc1").addEventListener("click", () => runScenario("nominal", $("#res-sc1")));
    $("#btn-run-sc2").addEventListener("click", () => runScenario("failover", $("#res-sc2")));
    $("#btn-run-sc3").addEventListener("click", () => runScenario("byzantine", $("#res-sc3")));
    $("#btn-run-sc4").addEventListener("click", () => runScenario("sanitization", $("#res-sc4")));
    $("#btn-run-sc5").addEventListener("click", () => runScenario("idempotency", $("#res-sc5")));
  }

  // Init
  function init() {
    bindEvents();
    setupEventSource();
    renderPetState();
    if (window.lucide) {
      window.lucide.createIcons();
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

