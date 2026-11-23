# Guia de Estudos — Pet Care (Conceptual Mirror)

O **Pet Care** é um sistema distribuído de monitoramento e cuidado de pets que combina **borda IoT (MQTT)**, **replicação P2P com tolerância a falhas bizantinas via Pyro5 (quórum $2f+1$)**, **persistência local em SQLite** e um **painel web em tempo real (HTTP + SSE)**.

Para compreender a arquitetura sem se perder na complexidade da orquestração logo no início, a recomendação é estudar **de baixo para cima** (do domínio puro até a camada web).

---

## Fluxo de Estudos Recomendado

### Etapa 1: Fundamentos e Regras de Negócio (sem rede)
Comece entendendo **o que** o sistema gerencia e como ele se configura, sem se preocupar ainda com threads ou rede.

1. **[`__init__.py.md`](./app/__init__.py.md)** e **[`config.py.md`](./app/config.py.md)**: leitura rápida para ver os 3 nós do cluster (`ALL_NODES = (1, 2, 3)`), a porta base Pyro5 (`BASE_PORT = 9090`), o broker MQTT (`localhost:1883`) e como as URIs dos pares são montadas (`peer_uri`).
2. **[`domain.py.md`](./app/domain.py.md)** *(ponto de partida principal)*: foque em:
   - `PetState`: o modelo de estado do pet (ração, água, areia, fuga, alarme).
   - `canonical_hash()`: como o estado gera um hash determinístico (ignorando `atualizado_em`) para permitir comparação de consenso entre réplicas.
   - `validate_telemetry()`, `apply_telemetry()` e `apply_command()`: funções puras que validam entradas, criam um novo `PetState` e devolvem comandos de atuadores MQTT.

### Etapa 2: Infraestrutura Local de cada Nó
Entenda como cada processo persiste dados e emite logs estruturados.

3. **[`storage.py.md`](./app/storage.py.md)**: analise a classe `NodeStorage`. Cada réplica tem seu próprio SQLite com duas tabelas: `pet_state` (estado atual único) e `event_log` (histórico indexado por `command_id` para garantir **idempotência** com `has_event()`/`record_event()`). Note também `update_field_directly()`, usado para simular corrupção manual de dados.
4. **[`event_bus.py.md`](./app/event_bus.py.md)**: barramento simples de logs que grava em `logs/events.jsonl` e notifica *listeners* em memória (usado depois pelo SSE da interface web).

### Etapa 3: A Borda IoT (Sensores e Atuadores)
Veja quem gera os dados e recebe as ordens físicas via MQTT.

5. **[`sensors.py.md`](./app/sensors.py.md)**: analise `IoTDeviceNode`. Observe:
   - Como ele publica telemetria periódica em `petcare/sensores/<sensor>` (`step_auto()`).
   - Como ele recebe comandos em `petcare/atuadores/#` (QoS 2) e usa `_is_duplicate()` com cache temporal de 2s para não executar o mesmo comando duas vezes.

### Etapa 4: O Núcleo Distribuído (Replicação, Failover e Consenso)
Com a base consolidada, explore a comunicação entre os 3 controladores e o cliente RPC.

6. **[`controller.py.md`](./app/controller.py.md)**: divida a leitura da classe `PetController` em 4 blocos:
   - **Ciclo de telemetria e comando**: `_on_mqtt_message()` e `execute_command()` (somente o líder processa, persiste no SQLite, publica atuadores MQTT, replica via `sync_state()` e notifica observadores).
   - **Sincronização e Eleição**: `_catch_up_from_peers()` (recuperação ao subir) e `_maybe_elect_self()` (failover determinístico pelo menor ID vivo).
   - **Simulação Bizantina**: `_visible_state()`, `_forged_state()` e `set_byzantine()`, que adulteram o estado exposto para provocar divergência de hash.
   - **Push para clientes**: `register_observer()` e `_notify_observers()`.
7. **[`rpc_client.py.md`](./app/rpc_client.py.md)**: o outro lado da moeda — como o cliente conversa com o cluster:
   - **Leitura com quórum**: `get_consensus_state()` consulta os 3 nós em paralelo, exige pelo menos 2 respostas concordantes (quórum $2f+1$ para $f=1$) e substitui o líder (`_demote_and_replace()`) se ele estiver divergente.
   - **Escrita resiliente**: `execute_command()` usa o mesmo `command_id` em caso de falha para fazer failover transparente sem duplicar efeito.
   - **Push + Debounce**: `ClientObserver` e `_handle_observer_push()` (agrupam notificações em 80ms antes de recalcular o consenso).

### Etapa 5: Orquestração e Painel Web
Por fim, veja como tudo é inicializado e exposto ao usuário.

8. **[`web_server.py.md`](./app/web_server.py.md)**:
   - `ProcessManager`: sobe e derruba os subprocessos reais dos 3 controladores e do simulador de sensores.
   - `AppContext`: une o `ClusterRpcClient`, o cliente MQTT e o stream **SSE** (`/api/events`), além de rotear ações do frontend em `dispatch_action()`.
   - `make_handler()`: servidor HTTP na porta `8080` servindo os arquivos estáticos e a API `/api/action`.

---

## Mapa Mental do Fluxo de Dados

```text
[sensors.py] --(MQTT: sensores/#)--> [controller.py (Líder)] --(Pyro5: sync_state)--> [Réplicas]
     ^                                     |          |
     |-----(MQTT: atuadores/#)-------------|          |--(SQLite: storage.py)
                                                      |
[web_server.py] <--(SSE / Push)-- [rpc_client.py] <---| (Pyro5: get_state / on_state_changed)
```

