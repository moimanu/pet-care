# Sistema IoT Distribuído de Pet Care

Middleware e sistema distribuído para monitoramento e cuidado autônomo de pets, desenvolvido para o 1º Trabalho Prático de Sistemas Distribuídos. Combina comunicação baseada em objetos via RPC (**Pyro5**), mensageria pub/sub (**MQTT**), persistência replicada (**SQLite**), tolerância a falhas *Crash-Stop* com failover transparente e consenso por quórum bizantino ($2f + 1$).

---

## Documentação

| Documento | Descrição |
| :--- | :--- |
| **[Relatório](docs/report.md)** | Apresentação do tema e soluções para replicação, failover na queda do controlador e segurança por quórum bizantino ($2f + 1$). |
| **[Arquitetura e Modelagem](docs/architecture.md)** | Topologia, portas, objetos remotos Pyro5, contratos de tópicos MQTT, esquema SQLite e estrutura de módulos. |
| **[Execução e Testes](docs/execution-and-testing.md)** | Instalação, inicialização automática e manual, roteiro de validação e injeção de falhas no painel web. |
| **[Especificação do Trabalho](docs/trabalho-pratico.pdf)** | Enunciado original do 1º Trabalho Prático de Sistemas Distribuídos. |

---

## Arquitetura Resumida

* **Dispositivos IoT de Borda (`app/sensors.py`):** Publicam telemetria de ração, água, caixa de areia e perímetro de fuga via MQTT (`QoS 1`) e consomem comandos de atuadores (`QoS 2`) com deduplicação por `command_id`.
* **Cluster de Controladores (`app/controller.py`):** 3 processos independentes (`:9091`, `:9092`, `:9093`) expostos via Pyro5 RPC, com replicação primário-réplica (`sync_state`), catch-up na inicialização e auto-eleição P2P na queda do líder.
* **Persistência Isolada (`app/storage.py`):** 3 bancos SQLite (`node_1.db`, `node_2.db`, `node_3.db`) armazenando o estado canônico (`pet_state`) e o log de idempotência (`event_log`).
* **Cliente RPC e Painel Web (`app/rpc_client.py`, `app/web_server.py`, `web/`):** Servidor HTTP/SSE (`:8080`) e cliente Pyro5 com callback push (`ClientObserver`), leitura por quórum $2f + 1$, destituição automática de líder divergente e failover transparente de comandos.

---

## Início Rápido

### 1. Instalar dependências

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Iniciar o sistema

```bash
./scripts/start_system.sh
```

Para reiniciar limpando bancos SQLite e logs anteriores:

```bash
./scripts/start_system.sh --reset
```

Acesse o fluxograma interativo em **http://localhost:8080/**.
