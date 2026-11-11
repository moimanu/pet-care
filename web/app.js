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
    isPaused: false,
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
      elSyncTime.textContent = `Ultima sincronizacao: ${formatLocalTime(p.atualizado_em)}`;
    }
  }

  function formatLocalTime(isoStr) {
    if (!isoStr) return new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
    const d = new Date(isoStr);
    return !isNaN(d.getTime())
      ? d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false })
      : isoStr.slice(11, 19);
  }

  function normalizeLevel(level) {
    const l = String(level || "INFO").toUpperCase();
    if (l === "WARNING" || l === "WARN") return "WARN";
    if (l === "CRITICAL" || l === "ERROR" || l === "ERR") return "ERROR";
    return l;
  }

  function renderLogEntry(entry) {
    const lvl = normalizeLevel(entry.level);
    if (state.filterLevel !== "ALL" && lvl !== state.filterLevel) {
      return;
    }
    const row = document.createElement("div");
    row.className = "log-entry";
    const ts = formatLocalTime(entry.timestamp);
    const src = entry.component || "Sys";

    if (entry.payload) {
      row.dataset.hasPayload = "true";
      row.dataset.payload = typeof entry.payload === "string" ? entry.payload : JSON.stringify(entry.payload);
    }

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
    const visibleCount = state.filterLevel === "ALL"
      ? state.logs.length
      : state.logs.filter((e) => normalizeLevel(e.level) === state.filterLevel).length;
    elLogCount.textContent = `${visibleCount} entradas`;
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
      if (state.isPaused) return;
      try {
        const payload = JSON.parse(evt.data);
        state.pet = Object.assign({}, state.pet, payload);
        renderPetState();
      } catch (err) {
        console.error("Erro parsing state event", err);
      }
    });

    sse.addEventListener("log", (evt) => {
      if (state.isPaused) return;
      try {
        const entry = JSON.parse(evt.data);
        state.logs.push(entry);
        if (state.logs.length > state.maxLogs) {
          state.logs.shift();
        }
        renderLogEntry(entry);
        elLogCount.textContent = `${state.logs.length} entradas`;

        // Sincroniza estado do painel em tempo real a partir dos logs
        if (entry.payload) {
          const p = entry.payload;
          let changed = false;
          if (p.nivel_gramas !== undefined && p.nivel_gramas !== null) {
            state.pet.racao_gramas = Number(p.nivel_gramas);
            changed = true;
          }
          if (p.nivel_ml !== undefined && p.nivel_ml !== null) {
            state.pet.agua_ml = Number(p.nivel_ml);
            changed = true;
          }
          if (p.usos_acumulados !== undefined && p.usos_acumulados !== null) {
            state.pet.areia_usos = Number(p.usos_acumulados);
            changed = true;
          }
          if (p.perimetro !== undefined && p.perimetro !== null) {
            state.pet.status_fuga = String(p.perimetro);
            changed = true;
          }
          if (p.alarme_ativo !== undefined && p.alarme_ativo !== null) {
            state.pet.alarme_ativo = Boolean(p.alarme_ativo);
            changed = true;
          }
          if (entry.message && entry.message.includes("BUZZER ATIVADO")) {
            state.pet.alarme_ativo = true;
            changed = true;
          }
          if (entry.message && entry.message.includes("Alarme silenciado")) {
            state.pet.alarme_ativo = false;
            changed = true;
          }
          if (entry.timestamp) {
            state.pet.atualizado_em = entry.timestamp;
            changed = true;
          }
          if (changed) {
            renderPetState();
          }
        }
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

    // Pause Toggle
    const btnPause = $("#btn-toggle-pause");
    if (btnPause) {
      btnPause.addEventListener("click", () => {
        state.isPaused = !state.isPaused;
        const label = $("#label-pause-btn");
        if (state.isPaused) {
          if (label) label.textContent = "Retomar";
          btnPause.innerHTML = `<i data-lucide="play" aria-hidden="true"></i><span id="label-pause-btn">Retomar</span>`;
          renderConnection(false, "Pausado");
        } else {
          if (label) label.textContent = "Pausar";
          btnPause.innerHTML = `<i data-lucide="pause" aria-hidden="true"></i><span id="label-pause-btn">Pausar</span>`;
          renderConnection(true, "SSE Ativo (Tempo Real)");
        }
        if (window.lucide) {
          window.lucide.createIcons();
        }
      });
    }

    // Floating Tooltip for Log Payloads on Mouse Move
    let tooltip = $("#floating-tooltip");
    if (!tooltip) {
      tooltip = document.createElement("div");
      tooltip.id = "floating-tooltip";
      tooltip.className = "floating-tooltip";
      document.body.appendChild(tooltip);
    }

    elLogFeed.addEventListener("mousemove", (e) => {
      const row = e.target.closest(".log-entry[data-has-payload='true']");
      if (!row || !row.dataset.payload) {
        tooltip.style.display = "none";
        return;
      }
      try {
        const parsed = JSON.parse(row.dataset.payload);
        const formatted = JSON.stringify(parsed, null, 2);
        tooltip.innerHTML = `<pre><code>${escapeHtml(formatted)}</code></pre>`;
        tooltip.style.display = "block";

        let top = e.clientY + 12;
        let left = e.clientX + 14;

        if (left + tooltip.offsetWidth > window.innerWidth - 12) {
          left = e.clientX - tooltip.offsetWidth - 14;
        }
        if (top + tooltip.offsetHeight > window.innerHeight - 12) {
          top = e.clientY - tooltip.offsetHeight - 14;
        }
        tooltip.style.top = `${Math.max(6, top)}px`;
        tooltip.style.left = `${Math.max(6, left)}px`;
      } catch (err) {
        tooltip.style.display = "none";
      }
    });

    elLogFeed.addEventListener("mouseleave", () => {
      tooltip.style.display = "none";
    });
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

