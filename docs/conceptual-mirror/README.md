# Guia de Estudos — Pet Care (Conceptual Mirror)

O **Pet Care** é um sistema distribuído de monitoramento e cuidado de pets que combina **borda IoT (MQTT)**, **replicação P2P validada por hash e eleição por mandato (`term`) com tolerância a falhas bizantinas via Pyro5 (quórum $2f+1$)**, **persistência local em SQLite** e um **painel web em tempo real (HTTP + SSE)**.

Para compreender a arquitetura sem se perder na complexidade da orquestração logo no início, a recomendação é estudar **de baixo para cima** (do domínio puro até a camada web).

---

## Fluxo de Estudos Recomendado

### Etapa 1: Fundamentos e Regras de Negócio (sem rede)
Comece entendendo **o que** o sistema gerencia e como ele se configura, sem se preocupar ainda com threads ou rede.

1. **[`__init__.py.md`](./app/__init__.py.md)** e **[`config.py.md`](./app/config.py.md)**: leitura rápida para ver os 3 nós do cluster (`ALL_NODES = (1, 2, 3)`), os parâmetros de quórum (`BYZANTINE_FAULT_LIMIT = 1`, `MIN_QUORUM = 2`), as portas Pyro5 (`BASE_PORT = 9090`, `CLIENT_PORT = 9094`) e web (`WEB_PORT = 8080`), os tópicos MQTT (`TOPIC_SENSORS`, `TOPIC_ACTUATORS`, `TOPIC_SIM_CONTROL`) e as funções de URI/banco (`peer_uri`, `client_uri`, `node_db_path`).
2. **[`domain.py.md`](./app/domain.py.md)** *(ponto de partida principal)*: foque em:
   - Enums de protocolo: `Sensor`, `Perimeter`, `Action`, `TransitionKind` e `RpcError`.
   - `PetState` e `canonical_hash()`: o modelo de estado do pet e como o hash determinístico (ignorando `atualizado_em`) permite comparação de consenso entre réplicas.
   - `SENSOR_SPECS`, `validate_telemetry()`, `is_valid_state()`, `apply_telemetry()`, `apply_command()` e `apply_transition()`: especificação declarativa e funções puras que validam entradas, produzem um novo `PetState` e devolvem comandos de atuadores MQTT.
   - `evaluate_quorum()`: apuração pura do consenso bizantino $2f+1$ (compartilhada entre controladores e cliente), identificando a maioria de hashes, nós divergentes, o mandato (`term`) atual e o `leader_id` íntegro.

### Etapa 2: Infraestrutura Local e Utilitários Compartilhados
Entenda como cada processo persiste dados, emite logs estruturados e compartilha primitivas de rede.

3. **[`storage.py.md`](./app/storage.py.md)**: analise a classe `NodeStorage`. Cada réplica tem seu próprio SQLite com duas tabelas: `pet_state` (estado atual único) e `event_log` (histórico indexado por `command_id`). `save_state()` grava estado e evento na mesma transação para garantir atomicidade e **idempotência** junto a `has_event()`. Note também `update_field_directly()`, usado para simular corrupção manual de dados.
4. **[`event_bus.py.md`](./app/event_bus.py.md)**: barramento simples de logs que grava em `logs/events.jsonl` e notifica *listeners* em memória (usado depois pelo SSE da interface web).
5. **[`infra.py.md`](./app/infra.py.md)**: utilitários compartilhados pelos processos distribuídos: `rpc_proxy()`, `query_cluster_nodes()` (consulta paralela aos controladores reaproveitando estado local quando disponível), `suppress_rpc_errors()`, o par `PushReceiver` / `ObserverRegistry` para notificações push unidirecionais Pyro5, `connect_mqtt()`, `publish_json()` e `install_shutdown_handler()`.

### Etapa 3: A Borda IoT (Sensores e Atuadores)
Veja quem gera os dados e recebe as ordens físicas via MQTT.

6. **[`sensors.py.md`](./app/sensors.py.md)**: analise `IoTDeviceNode`. Observe:
   - Como ele mantém seu estado simulado em `PetState` e publica telemetria periódica em `petcare/sensores/<sensor>` (`step_auto()`).
   - Como ele recebe comandos em `petcare/atuadores/#` (QoS 2) e usa `_is_duplicate()` com cache temporal de 2s (`DEDUP_WINDOW_SECONDS`) para não executar o mesmo comando duas vezes.

### Etapa 4: O Núcleo Distribuído (Replicação, Failover e Consenso)
Com a base consolidada, explore a comunicação entre os 3 controladores e o cliente RPC.

7. **[`controller.py.md`](./app/controller.py.md)**: divida a leitura da classe `PetController` em 4 blocos:
   - **Ciclo de telemetria e comando**: `_on_mqtt_message()` e `execute_command()`, que convergem para `_commit_and_replicate()` (calcula transição determinística, exige quórum P2P via `sync_state()`, persiste no SQLite, publica atuadores MQTT e notifica observadores).
   - **Eleição e Destituição P2P por Mandato (`term`)**: `reconcile_leadership()`, `_propose_leadership()`, `request_vote()`, `commit_term()` e `_catch_up_from_peers()`. Os próprios controladores auditam o quórum $2f+1$, votam um novo mandato (`term + 1`) sem depender do voto de nós bizantinos e rebaixam líderes antigos a `REPLICA`.
   - **Simulação Bizantina e Replicação Determinística**: `_visible_state()`, `set_byzantine()` e `sync_state()`, onde cada réplica recalcula `apply_transition()` localmente e rejeita hashes forjados ou mandatos antigos.
   - **Push para clientes**: `register_observer()`, `unregister_observer()` e `_notify_observers()` apoiados por `ObserverRegistry`.
8. **[`client.py.md`](./app/client.py.md)**: processo independente que conversa com o cluster e expõe seus próprios métodos via Pyro5:
   - **Leitura com quórum**: `get_consensus_state()` consulta os 3 nós em paralelo via `query_cluster_nodes()`, apura o consenso $2f+1$ com `evaluate_quorum()` e aciona `reconcile_leadership()` no cluster caso não haja líder íntegro.
   - **Escrita resiliente com confirmação pós-execução**: `execute_command()` valida o quórum antes e após o RPC, repetindo o mesmo `command_id` no novo líder caso o líder anterior caia ou minta na resposta.
   - **Push + Debounce**: `PushReceiver` e `handle_observer_push()` agrupam notificações em 80ms (`DEBOUNCE_SECONDS`) antes de recalcular o consenso e notificar observadores de UI.

### Etapa 5: Orquestração e Painel Web
Por fim, veja como tudo é inicializado e exposto ao usuário.

9. **[`process_manager.py.md`](./app/process_manager.py.md)**: `ProcessManager` inicia, monitora e encerra os subprocessos reais dos 3 controladores, do simulador de sensores (`app.sensors`) e do cliente RPC (`app.client`).
10. **[`web_server.py.md`](./app/web_server.py.md)**:
    - `AppContext`: usa `ProcessManager`, conversa com o `app.client` via Pyro5, recebe atualizações via `PushReceiver`, determina o líder efetivo pelo mandato/consenso, mantém o cliente MQTT de testes e o stream **SSE** (`/api/events`), além de rotear ações do frontend em `dispatch_action()`.
    - `make_handler()`: servidor HTTP na porta `8080` servindo os arquivos estáticos, `/api/state`, `/api/events` e `/api/action`.

---

## Mapa Mental do Fluxo de Dados

```text
[sensors.py] --(MQTT: sensores/#)--> [controller.py (Líder)] --(Pyro5: sync_state + quorum P2P)--> [Réplicas]
     ^                                     |          |
     |-----(MQTT: atuadores/#)-------------|          |--(SQLite: storage.py)
                                                      |
[web_server.py] <--(Pyro5 Push / SSE)-- [client.py] <-| (Pyro5: get_state / on_push)
```
