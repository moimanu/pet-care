# Guia de Estudos — Pet Care (Conceptual Mirror)

O **Pet Care** é um sistema distribuído de monitoramento e cuidado de pets que combina **borda IoT (MQTT)**, **replicação P2P com tolerância a falhas bizantinas via Pyro5 (quórum $2f+1$)**, **persistência local em SQLite** e um **painel web em tempo real (HTTP + SSE)**.

Para compreender a arquitetura sem se perder na complexidade da orquestração logo no início, a recomendação é estudar **de baixo para cima** (do domínio puro até a camada web).

---

## Fluxo de Estudos Recomendado

### Etapa 1: Fundamentos e Regras de Negócio (sem rede)
Comece entendendo **o que** o sistema gerencia e como ele se configura, sem se preocupar ainda com threads ou rede.

1. **[`__init__.py.md`](./app/__init__.py.md)** e **[`config.py.md`](./app/config.py.md)**: leitura rápida para ver os 3 nós do cluster (`ALL_NODES = (1, 2, 3)`), os parâmetros de quórum (`BYZANTINE_FAULT_LIMIT = 1`, `MIN_QUORUM = 2`), as portas Pyro5 (`BASE_PORT = 9090`, `CLIENT_PORT = 9094`) e web (`WEB_PORT = 8080`), os tópicos MQTT (`TOPIC_SENSORS`, `TOPIC_ACTUATORS`, `TOPIC_SIM_CONTROL`) e as funções de URI/banco (`peer_uri`, `client_uri`, `node_db_path`).
2. **[`domain.py.md`](./app/domain.py.md)** *(ponto de partida principal)*: foque em:
   - Enums de protocolo: `Sensor`, `Perimeter`, `Action` e `RpcError`.
   - `PetState` e `canonical_hash()`: o modelo de estado do pet e como o hash determinístico (ignorando `atualizado_em`) permite comparação de consenso entre réplicas.
   - `SENSOR_SPECS`, `validate_telemetry()`, `apply_telemetry()` e `apply_command()`: especificação declarativa e funções puras que validam entradas, produzem um novo `PetState` e devolvem comandos de atuadores MQTT.

### Etapa 2: Infraestrutura Local e Utilitários Compartilhados
Entenda como cada processo persiste dados, emite logs estruturados e compartilha primitivas de rede.

3. **[`storage.py.md`](./app/storage.py.md)**: analise a classe `NodeStorage`. Cada réplica tem seu próprio SQLite com duas tabelas: `pet_state` (estado atual único) e `event_log` (histórico indexado por `command_id`). `save_state()` grava estado e evento na mesma transação para garantir atomicidade e **idempotência** junto a `has_event()`. Note também `update_field_directly()`, usado para simular corrupção manual de dados.
4. **[`event_bus.py.md`](./app/event_bus.py.md)**: barramento simples de logs que grava em `logs/events.jsonl` e notifica *listeners* em memória (usado depois pelo SSE da interface web).
5. **[`infra.py.md`](./app/infra.py.md)**: utilitários compartilhados pelos processos distribuídos: `rpc_proxy()`, `suppress_rpc_errors()`, o par `PushReceiver` / `ObserverRegistry` para notificações push unidirecionais Pyro5, `connect_mqtt()`, `publish_json()` e `install_shutdown_handler()`.

### Etapa 3: A Borda IoT (Sensores e Atuadores)
Veja quem gera os dados e recebe as ordens físicas via MQTT.

6. **[`sensors.py.md`](./app/sensors.py.md)**: analise `IoTDeviceNode`. Observe:
   - Como ele mantém seu estado simulado em `PetState` e publica telemetria periódica em `petcare/sensores/<sensor>` (`step_auto()`).
   - Como ele recebe comandos em `petcare/atuadores/#` (QoS 2) e usa `_is_duplicate()` com cache temporal de 2s (`DEDUP_WINDOW_SECONDS`) para não executar o mesmo comando duas vezes.

### Etapa 4: O Núcleo Distribuído (Replicação, Failover e Consenso)
Com a base consolidada, explore a comunicação entre os 3 controladores e o cliente RPC.

7. **[`controller.py.md`](./app/controller.py.md)**: divida a leitura da classe `PetController` em 4 blocos:
   - **Ciclo de telemetria e comando**: `_on_mqtt_message()` e `execute_command()`, que convergem para `_commit_and_replicate()` (persiste no SQLite, publica atuadores MQTT, replica P2P via `sync_state()` e notifica observadores).
   - **Sincronização e Eleição**: `_catch_up_from_peers()` (recuperação ao subir) e `_maybe_elect_self()` (failover determinístico pelo menor ID vivo).
   - **Simulação Bizantina**: `_visible_state()` e `set_byzantine()`, que adulteram o estado exposto para provocar divergência de hash.
   - **Push para clientes**: `register_observer()`, `unregister_observer()` e `_notify_observers()` apoiados por `ObserverRegistry`.
8. **[`client.py.md`](./app/client.py.md)**: o outro lado da moeda — processo independente que conversa com o cluster e expõe seus próprios métodos via Pyro5:
   - **Leitura com quórum**: `evaluate_quorum()` e `get_consensus_state()` consultam os 3 nós em paralelo, exigem pelo menos 2 respostas concordantes (quórum $2f+1$ para $f=1$) e substituem o líder (`_demote_and_replace()`) se ele estiver divergente.
   - **Escrita resiliente**: `execute_command()` usa o mesmo `command_id` em caso de falha para fazer failover transparente sem duplicar efeito.
   - **Push + Debounce**: `PushReceiver` e `handle_observer_push()` agrupam notificações em 80ms (`DEBOUNCE_SECONDS`) antes de recalcular o consenso e notificar observadores de UI.

### Etapa 5: Orquestração e Painel Web
Por fim, veja como tudo é inicializado e exposto ao usuário.

9. **[`process_manager.py.md`](./app/process_manager.py.md)**: `ProcessManager` inicia, monitora e encerra os subprocessos reais dos 3 controladores, do simulador de sensores (`app.sensors`) e do cliente RPC (`app.client`).
10. **[`web_server.py.md`](./app/web_server.py.md)**:
    - `AppContext`: usa `ProcessManager`, conversa com o `app.client` via Pyro5, recebe atualizações via `PushReceiver`, mantém o cliente MQTT de testes e o stream **SSE** (`/api/events`), além de rotear ações do frontend em `dispatch_action()`.
    - `make_handler()`: servidor HTTP na porta `8080` servindo os arquivos estáticos, `/api/state`, `/api/events` e `/api/action`.

---

## Mapa Mental do Fluxo de Dados

```text
[sensors.py] --(MQTT: sensores/#)--> [controller.py (Líder)] --(Pyro5: sync_state)--> [Réplicas]
     ^                                     |          |
     |-----(MQTT: atuadores/#)-------------|          |--(SQLite: storage.py)
                                                      |
[web_server.py] <--(Pyro5 Push / SSE)-- [client.py] <-| (Pyro5: get_state / on_push)
```
