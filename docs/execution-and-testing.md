# Guia de Execução e Testes

---

## 1. Pré-requisitos e Instalação

* **Python 3.10+**
* **Docker** e **Docker Compose** (ou broker Mosquitto local ativo na porta `1883`)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 2. Inicialização Automática

O script `scripts/start_system.sh` sobe o container Mosquitto via Docker Compose e inicia o `app.web_server`, que orquestra automaticamente os 3 processos de controladores (`app.controller`) e o processo de dispositivos IoT (`app.sensors`):

```bash
./scripts/start_system.sh
```

Para reiniciar o cluster do zero (removendo bancos SQLite `node_*.db` e logs anteriores em `logs/events.jsonl`):

```bash
./scripts/start_system.sh --reset
```

* **Painel Web Interativo:** `http://localhost:8080/`
* **Logs Estruturados:** `logs/events.jsonl`
* **Encerramento:** Pressione `Ctrl+C` no terminal para encerrar o servidor web, os controladores e o processo de sensores.

---

## 3. Inicialização Manual de Componentes

Caso deseje executar os processos separadamente para depuração individual:

```bash
# 1. Broker MQTT
docker compose up -d mosquitto

# 2. Controladores Pyro5 (portas 9091, 9092 e 9093)
.venv/bin/python -m app.controller --id 1 --primary
.venv/bin/python -m app.controller --id 2
.venv/bin/python -m app.controller --id 3

# 3. Dispositivos IoT de Borda (Sensores e Atuadores)
.venv/bin/python -m app.sensors

# 4. Servidor Web + Cliente RPC (gerencia ou conecta-se aos nos existentes)
.venv/bin/python -m app.web_server
```

---

## 4. Roteiro de Testes e Validação no Painel (`http://localhost:8080`)

O painel web apresenta a arquitetura em tempo real via Server-Sent Events (`/api/events`) e permite injetar comandos e falhas diretamente em cada camada (`POST /api/action`).

### 4.1. Invocação de Métodos via RPC e Idempotência (`Clientes`)

1. No card **Clientes**, clique em **Liberar Ração** (`+50g`), **Repor Água** (`+200ml`), **Resetar Areia** (`0`) ou **Silenciar Alarme**.
2. Verifique nos cards **Armazenamento 1, 2 e 3** que o mesmo hash SHA-256 e contagem de eventos (`Ev`) são replicados instantaneamente nos 3 bancos SQLite.
3. Para testar **idempotência**, mantenha o mesmo `command_id` (preenchido automaticamente após um comando) e clique novamente no mesmo botão de ação: o Líder identificará o UUID no `event_log`, retornará `idempotent: true` e não duplicará a atuação física nem o incremento.

### 4.2. Telemetria MQTT e Atuação Autônoma (`Sensores` e `MQTT`)

1. No card **Sensores**, envie valores individuais (`Ração (g)`, `Água (ml)`, `Areia`) ou ative o ciclo contínuo clicando em **Auto ON**.
2. Para testar a tomada de decisão autônoma crítica, selecione `FORA` no seletor de perímetro e clique em **Fuga**:
   * O sensor publica `{"perimetro": "FORA"}` em `petcare/sensores/fuga` (`QoS 1`).
   * O Líder detecta a violação, ativa `alarme_ativo = True`, publica `DISPARAR_ALARME` em `petcare/atuadores/alarme` (`QoS 2`), replica o estado aos pares e notifica o cliente via callback Pyro5 (`ClientObserver`).

### 4.3. Tolerância a Falhas Crash-Stop e Failover (`Controladores`)

1. Identifique o controlador marcado como `(LIDER)` (por padrão, **Controlador 1**) e clique em **Parar Processo**.
2. Envie um comando pelo card **Clientes** ou publique telemetria pelo card **Sensores**:
   * **Via Telemetria MQTT (Failover P2P):** As réplicas ativas detectam a ausência de líder via `ping()` P2P e o nó ativo de menor ID (`_maybe_elect_self`) assume automaticamente a liderança.
   * **Via Comando RPC (Failover Transparente):** O `ClusterRpcClient` detecta a falha de conexão com o líder antigo, promove a primeira réplica ativa disponível e conclui a execução do método com sucesso (`2/2` votos no quórum).
3. Clique em **Iniciar Processo** no controlador parado: ele executa `_catch_up_from_peers()`, reconhece o novo líder ativo, clona o estado canônico atualizado para seu SQLite e reingressa como `REPLICA`.

### 4.4. Quórum Bizantino ($2f + 1$) e Destituição de Líder Comprometido

1. Em uma réplica (ex.: **Controlador 3**), clique em **Tornar Bizantino** (ou edite diretamente um valor no card **Armazenamento 3** e clique em **Alterar**).
2. Clique em **Verificar Quórum (2f+1)** no card **Clientes**:
   * O cliente consulta `get_state()` nos 3 nós em paralelo.
   * O nó comprometido retorna um hash divergente (`9003.0g` de ração), sendo marcado como `BIZANTINO` / `DIVERGENTE` e descartado pelo quórum majoritário (`2/3` votos válidos).
3. Agora clique em **Tornar Bizantino** no próprio **Controlador Líder**:
   * Ao detectar que o nó divergente possui `is_primary == True`, o `ClusterRpcClient` executa `_demote_and_replace()`, destituindo imediatamente o líder comprometido e promovendo uma réplica íntegra do quórum majoritário.
4. Clique em **Restaurar Honesto** no nó bizantino para que ele desative a adulteração e ressincronize seu banco SQLite com o líder atual.

