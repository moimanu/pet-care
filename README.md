# Sistema IoT Distribuído de Pet Care

Trabalho Prático de Sistemas Distribuídos implementando controle autônomo, persistência com semântica *exactly-once*, consenso por quórum bizantino ($2f + 1$), failover transparente e comunicação desacoplada via RPC (Pyro5) e mensageria publish/subscribe (MQTT).

---

## 1. Topologia e Arquitetura do Sistema

```mermaid
flowchart TD
    CLI["Painel Web / Gateway\n(src/entrypoints/run_web.py)"]
    MID["Middleware Pyro5\n(src/entrypoints/run_middleware.py)\n• Quórum Bizantino (2f+1)\n• Failover Transparente\n• Idempotência UUID"]
    
    C1["Controlador 1 (Primário)\n(node_1.db - Porta 9091)"]
    C2["Controlador 2 (Réplica)\n(node_2.db - Porta 9092)"]
    C3["Controlador 3 (Réplica)\n(node_3.db - Porta 9093)"]

    MQTT["Broker MQTT (Mosquitto)\n(Porta 1883 TCP)"]

    IOT["Dispositivos IoT de Borda\n(src/entrypoints/run_iot_devices.py)\n• Sensores: Ração, Água, Areia, Fuga\n• Atuadores: Dispenser, Válvula, Alarme\n• Cache Idempotente de 2s"]

    CLI -->|Pyro5 RPC| MID
    MID -->|Pyro5 RPC| C1
    MID -->|Pyro5 RPC| C2
    MID -->|Pyro5 RPC| C3

    C1 <-->|Pub/Sub MQTT| MQTT
    C2 <-->|Sub MQTT| MQTT
    C3 <-->|Sub MQTT| MQTT

    MQTT <-->|Telemetria & Comandos| IOT
```

### 1.1. Tabela de Portas e Endpoints

| Componente | Protocolo | Endereço / Porta | Identificador Pyro5 |
| :--- | :--- | :--- | :--- |
| **Painel Web / Gateway** | HTTP / SSE | `localhost:8080` | N/A |
| **Broker MQTT** | TCP / MQTT | `localhost:1883` | N/A |
| **Middleware** | TCP / Pyro5 RPC | `localhost:9090` | `petcare.middleware` |
| **Controlador 1** | TCP / Pyro5 RPC | `localhost:9091` | `petcare.controller.1` |
| **Controlador 2** | TCP / Pyro5 RPC | `localhost:9092` | `petcare.controller.2` |
| **Controlador 3** | TCP / Pyro5 RPC | `localhost:9093` | `petcare.controller.3` |

---

## 2. Modelagem de Dados, Tópicos MQTT e Persistência

### 2.1. Taxonomia de Tópicos MQTT
* **Telemetria dos Sensores de Borda (QoS 1):**
  * `petcare/sensores/racao`: `{"nivel_gramas": 180.0, "timestamp": "..."}`
  * `petcare/sensores/agua`: `{"nivel_ml": 420.0, "timestamp": "..."}`
  * `petcare/sensores/areia`: `{"usos_acumulados": 2, "timestamp": "..."}`
  * `petcare/sensores/fuga`: `{"perimetro": "DENTRO", "timestamp": "..."}` (ou `"FORA"`)

* **Comandos para Atuadores de Borda (QoS 2):**
  * `petcare/atuadores/dispenser`: `{"acao": "LIBERAR_RACAO", "quantidade_gramas": 50.0, "command_id": "uuid"}`
  * `petcare/atuadores/valvula`: `{"acao": "REPOR_AGUA", "quantidade_ml": 200.0, "command_id": "uuid"}`
  * `petcare/atuadores/alarme`: `{"acao": "DISPARAR_ALARME", "motivo": "FUGA_DETECTADA", "command_id": "uuid"}` (ou `{"acao": "SILENCIAR_ALARME", "command_id": "uuid"}`)

### 2.2. Esquema Relacional de Cada Réplica SQLite (`node_{id}.db`)
```sql
CREATE TABLE IF NOT EXISTS pet_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    racao_gramas REAL NOT NULL,
    agua_ml REAL NOT NULL,
    areia_usos INTEGER NOT NULL,
    status_fuga TEXT NOT NULL,
    alarme_ativo INTEGER NOT NULL,
    atualizado_em TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_log (
    command_id TEXT PRIMARY KEY,
    tipo_evento TEXT NOT NULL,
    payload TEXT NOT NULL,
    registrado_em TEXT NOT NULL
);
```

---

## 3. Garantias e Propriedades do Sistema Distribuído

1. **Quórum Bizantino Concorrente ($2f + 1, N=3, f=1$):**
   - O Middleware consulta as 3 réplicas em paralelo via `ThreadPoolExecutor` com timeout de $1.0\text{ s}$.
   - O estado retornado é agrupado pelo `state_hash()` (hash SHA-256 canônico que normaliza floats e ignora timestamps voláteis locais).
   - Requer concordância de pelo menos 2 nós ($\ge 2f + 1$). Se um nó divergir (ex: `--corrupt`), a resposta errônea é descartada e um alerta explícito de segurança é registrado.
2. **Failover Transparente de Liderança:**
   - Comandos são direcionados ao Nó Primário.
   - Em caso de queda ou indisponibilidade, o Middleware promove automaticamente a próxima réplica saudável (`set_primary(True)`), executa o comando e replica aos nós ativos com o mesmo `command_id`.
3. **Idempotência e Semântica Exactly-Once:**
   - Todo comando manual ou autônomo carrega um `command_id` UUID único.
   - Os controladores registram o ID na tabela `event_log`; retransmissões são detectadas e não causam efeito duplicado.
   - O simulador de atuadores mantém um cache em janela deslizante de $2\text{ s}$ para absorver retransmissões de rede na borda.
4. **Regras Autônomas e Proteção de Borda:**
   - Se `racao_gramas < 50.0g`: emite `LIBERAR_RACAO` ($+50\text{ g}$).
   - Se `agua_ml < 150.0ml`: emite `REPOR_AGUA` ($+200\text{ ml}$).
   - Se `status_fuga == "FORA"`: emite `DISPARAR_ALARME`.
   - **Prevenção de Triplicação:** Apenas a réplica com flag `--primary` publica comandos autônomos no MQTT.
5. **Sincronização de Boot (Catch-up):**
   - Réplicas que reinicializam após queda consultam seus pares via RPC para clonar o estado mais recente antes de responderem ao quórum.

---

## 4. Pré-requisitos e Instalação

1. **Python 3.10+** (testado em Python 3.10 a 3.14)
2. **Docker** (para execução do Mosquitto MQTT)

Instalação em ambiente virtual:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 5. Como Executar o Sistema

### 5.1. Inicialização Automática Completa (Recomendado)
Um único comando inicializa o broker Docker, os 3 controladores, o middleware, o simulador de dispositivos IoT e o servidor web com o painel unificado:

```bash
./scripts/start_system.sh
```

Acesse no navegador: **http://localhost:8080/**

> **Dica:** Os logs de cada processo em segundo plano ficam gravados em `./logs/` (`controller_1.log`, `controller_2.log`, `controller_3.log`, `middleware.log`, `iot_devices.log`).
> Ao pressionar `Ctrl+C`, todos os processos em segundo plano são finalizados automaticamente.

---

### 5.2. Inicialização Manual Passo a Passo (Múltiplos Terminais)

Abra terminais independentes a partir da raiz do repositório:

#### Terminal 0: Iniciar Broker MQTT
```bash
docker compose up -d
```

#### Terminal 1: Iniciar Controlador 1 (Líder Primário)
```bash
.venv/bin/python src/entrypoints/run_controller.py --id 1 --primary
```

#### Terminal 2: Iniciar Controlador 2 (Réplica Secundária)
```bash
.venv/bin/python src/entrypoints/run_controller.py --id 2
```

#### Terminal 3: Iniciar Controlador 3 (Réplica Secundária)
```bash
.venv/bin/python src/entrypoints/run_controller.py --id 3
```
> *(Para injetar falha bizantina no Nó 3, adicione `--corrupt`).*

#### Terminal 4: Iniciar o Middleware
```bash
.venv/bin/python src/entrypoints/run_middleware.py
```

#### Terminal 5: Iniciar o Simulador de Borda (Sensores e Atuadores)
```bash
.venv/bin/python src/entrypoints/run_iot_devices.py
```

#### Terminal 6: Iniciar o Painel Web (Tutor & Observabilidade)
```bash
.venv/bin/python src/entrypoints/run_web.py
```
Acesse no navegador: **http://localhost:8080/**

**Módulos do Painel Web (Single-Page Application):**
* **Módulo 1 (App Tutor Mobile):** Indicadores de telemetria consolidada em tempo real (ração, água, areia, perímetro de fuga e status da sirene) e ações de controle imediato (dispensar ração, repor água, zerar caixa de areia e silenciar alarme).
* **Módulo 2 (Logs & Observabilidade):** Terminal unificado em tempo real via Server-Sent Events (SSE) com filtros por processo (`Todos`, `Controlador 1..3`, `Middleware`, `IoT Edge`, `Broker MQTT`), auto-scroll e pausa.
* **Módulo 3 (Guia de Testes & Swagger API):** Controle interativo de injeção de falhas nos nós (`Derrubar`/`Ligar` para Crash-Stop e `Corromper`/`Curar` para Falha Bizantina), guia passo a passo dos cenários com cópia rápida de payloads e documentação interativa OpenAPI/Swagger UI embutida (`/docs`).

---

## 6. Testes Automatizados (Pytest)

Executa a suíte de testes unitários e de integração cobrindo entidades, validadores, contratos, repositório SQLite, casos de uso (incluindo quórum bizantino, failover, idempotência e sanitização), adaptadores MQTT/Pyro5/WebGateway, rotas REST/SSE e entrypoints:

```bash
.venv/bin/pytest
```
