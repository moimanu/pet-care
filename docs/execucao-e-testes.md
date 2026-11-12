# Guia de Execução, Painel Web e Testes

---

## 1. Pré-requisitos e Instalação

* **Python 3.10+**
* **Docker** (para o broker Mosquitto MQTT)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 2. Inicialização Automática (Recomendado)

Sobe o broker MQTT (Docker), os 3 controladores, o middleware, o simulador IoT e o servidor web em um único comando:

```bash
./scripts/start_system.sh
```

* **Painel Web:** `http://localhost:8080/`
* **Logs em arquivo:** `./logs/` (`controller_1.log`, `controller_2.log`, `controller_3.log`, `middleware.log`, `iot_devices.log`)
* **Encerramento:** Pressione `Ctrl+C` para finalizar todos os processos em segundo plano.

---

## 3. Inicialização Manual (Múltiplos Terminais)

Execute cada comando em um terminal separado a partir da raiz do projeto:

```bash
# Terminal 0: Broker MQTT
docker compose up -d

# Terminal 1: Controlador 1 (Líder Primário)
.venv/bin/python src/entrypoints/run_controller.py --id 1 --primary

# Terminal 2: Controlador 2 (Réplica)
.venv/bin/python src/entrypoints/run_controller.py --id 2

# Terminal 3: Controlador 3 (Réplica — adicione --corrupt para simular falha bizantina via CLI)
.venv/bin/python src/entrypoints/run_controller.py --id 3

# Terminal 4: Middleware Pyro5
.venv/bin/python src/entrypoints/run_middleware.py

# Terminal 5: Simulador de Dispositivos IoT (Sensores e Atuadores)
.venv/bin/python src/entrypoints/run_iot_devices.py

# Terminal 6: Servidor Web e Gateway REST/SSE
.venv/bin/python src/entrypoints/run_web.py
```

---

## 4. Painel Web e Simulação de Falhas (`http://localhost:8080`)

* **Módulo 1 — App Tutor:** Exibe o estado consolidado por quórum (ração, água, areia, perímetro e alarme) e botões de comando remoto (`LIBERAR_RACAO`, `REPOR_AGUA`, `RESETAR_AREIA`, `SILENCIAR_ALARME`).
* **Módulo 2 — Logs em Tempo Real (SSE):** Stream unificado dos logs de todos os processos com filtro por componente.
* **Módulo 3 — Simulação de Falhas e Swagger (`/docs`):**
  * **Crash-Stop (`Derrubar` / `Ligar`):** Simula queda de qualquer controlador para validar o failover automático de liderança e a continuidade de execução.
  * **Falha Bizantina (`Corromper` / `Curar`):** Faz o controlador responder valores falsos para validar o descarte por quórum $2f + 1$ no Middleware.

---

## 5. Testes Automatizados

Executa a suíte completa de testes unitários e de integração (`pytest`):

```bash
.venv/bin/pytest
```

