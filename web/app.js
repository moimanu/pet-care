/**
 * PetCare Distributed System - Dashboard
 * Zero emojis, clean state-actions-view separation, SSE-driven.
 */
(() => {
  const $ = (sel, ctx = document) => ctx.querySelector(sel);

  // State
  const state = {
    connected: false,
    isPaused: false,
    autoScroll: true,
    activeTab: "all",
    pet: {
      racao_gramas: 0.0,
      agua_ml: 0.0,
      areia_usos: 0,
      status_fuga: "--",
      alarme_ativo: false,
      hash: "",
      atualizado_em: "",
    },
  };

  const SOURCE_TAGS = {
    "node-1": { label: "Controlador 1", cls: "tag-node-1" },
    "node-2": { label: "Controlador 2", cls: "tag-node-2" },
    "node-3": { label: "Controlador 3", cls: "tag-node-3" },
    "middleware": { label: "Middleware", cls: "tag-middleware" },
    "iot": { label: "IoT Edge", cls: "tag-iot" },
    "broker": { label: "Broker MQTT", cls: "tag-broker" },
    "web": { label: "Web Server", cls: "tag-web" },
  };

  // View Helpers & DOM elements
  const elConnDot = $("#conn-dot");
  const elConnText = $("#conn-text");
  const elSyncTime = $("#last-sync-time");
  const elFeedback = $("#action-feedback");

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

  function formatLocalTime(isoStr) {
    if (!isoStr) return new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
    const d = new Date(isoStr);
    return !isNaN(d.getTime())
      ? d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false })
      : isoStr.slice(11, 19);
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function appendRawLog(source, rawLine) {
    const term = $("#unified-terminal");
    if (!term) return;

    // Remove placeholder line if present
    const placeholder = term.querySelector(".raw-line[data-src='system']");
    if (placeholder) placeholder.remove();

    const tagInfo = SOURCE_TAGS[source] || { label: source || "Sistema", cls: "tag-generic" };

    const div = document.createElement("div");
    div.className = "raw-line";
    div.setAttribute("data-src", source || "generic");
    if (state.activeTab !== "all" && state.activeTab !== source) {
      div.classList.add("hidden");
    }

    let lvlClass = "";
    if (rawLine.includes("[WARN") || rawLine.includes("WARNING")) {
      lvlClass = "level-warn";
    } else if (rawLine.includes("[ERROR") || rawLine.includes("CRITICAL")) {
      lvlClass = "level-error";
    }

    div.innerHTML = `<span class="raw-tag ${tagInfo.cls}">[${tagInfo.label}]</span><span class="raw-text ${lvlClass}">${escapeHtml(rawLine)}</span>`;
    term.appendChild(div);

    updateLineCount();

    // Prevent memory leaks by capping lines in DOM
    while (term.children.length > 500) {
      term.removeChild(term.firstElementChild);
    }

    if (state.autoScroll) {
      term.scrollTop = term.scrollHeight;
    }
  }

  function updateLineCount() {
    const term = $("#unified-terminal");
    if (!term) return;
    const countEl = $("#term-line-count");
    if (!countEl) return;
    const visibleCount = term.querySelectorAll(".raw-line:not(.hidden)").length;
    countEl.textContent = `${visibleCount} eventos`;
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
      const h1 = $("#hash-node-1");
      const h2 = $("#hash-node-2");
      const h3 = $("#hash-node-3");
      if (h1) h1.textContent = `hash: ${shortHash}...`;
      if (h2) h2.textContent = `hash: ${shortHash}...`;
      if (h3) h3.textContent = `hash: ${shortHash}...`;
    }

    if (p.atualizado_em) {
      elSyncTime.textContent = `Ultima sincronizacao: ${formatLocalTime(p.atualizado_em)}`;
    }
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

    sse.addEventListener("raw_log", (evt) => {
      if (state.isPaused) return;
      try {
        const entry = JSON.parse(evt.data);
        appendRawLog(entry.source, entry.line);
      } catch (err) {
        console.error("Erro parsing raw_log event", err);
      }
    });

    sse.addEventListener("log", (evt) => {
      if (state.isPaused) return;
      try {
        const entry = JSON.parse(evt.data);
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

    // Terminal Tabs
    document.querySelectorAll(".term-tab").forEach((tabBtn) => {
      tabBtn.addEventListener("click", () => {
        document.querySelectorAll(".term-tab").forEach((b) => {
          b.classList.remove("active");
          b.setAttribute("aria-selected", "false");
        });
        tabBtn.classList.add("active");
        tabBtn.setAttribute("aria-selected", "true");
        state.activeTab = tabBtn.getAttribute("data-tab") || "all";

        const term = $("#unified-terminal");
        if (term) {
          term.querySelectorAll(".raw-line").forEach((line) => {
            const src = line.getAttribute("data-src");
            if (state.activeTab === "all" || src === state.activeTab) {
              line.classList.remove("hidden");
            } else {
              line.classList.add("hidden");
            }
          });
          updateLineCount();
          if (state.autoScroll) {
            term.scrollTop = term.scrollHeight;
          }
        }
      });
    });

    // Clear Terminal Button
    const btnClear = $("#btn-clear-term");
    if (btnClear) {
      btnClear.addEventListener("click", () => {
        const term = $("#unified-terminal");
        if (term) {
          term.innerHTML = "";
          updateLineCount();
        }
      });
    }

    // Auto-scroll Button
    const btnScroll = $("#btn-autoscroll");
    if (btnScroll) {
      btnScroll.addEventListener("click", () => {
        state.autoScroll = !state.autoScroll;
        btnScroll.classList.toggle("active", state.autoScroll);
        btnScroll.title = state.autoScroll ? "Auto-scroll ativo" : "Auto-scroll pausado";
        if (state.autoScroll) {
          const term = $("#unified-terminal");
          if (term) term.scrollTop = term.scrollHeight;
        }
      });
    }

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
  }

  // Initialization
  async function init() {
    bindEvents();
    setupEventSource();

    try {
      const res = await fetch("/api/status");
      if (res.ok) {
        const data = await res.json();
        if (data.sucesso && data.estado) {
          state.pet = Object.assign({}, state.pet, data.estado);
          renderPetState();
        }
      }
    } catch (err) {
      console.warn("Falha ao obter status inicial", err);
    }

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
