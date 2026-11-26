# Pet Care Distribuído (Versão Minimalista)

Middleware distribuído em Python simples para monitoramento e controle de ração, desenvolvido para o 1º Trabalho Prático de Sistemas Distribuídos. Combina **MQTT** (`paho-mqtt`), **RPC baseado em objetos** (`Pyro5`) e **persistência replicada** (`SQLite`) com quórum ($\ge 2$ de $3$), eleição de liderança, failover transparente, validação determinística nas réplicas e idempotência.

---

## Arquitetura

- **Broker MQTT ([docker-compose.yml](docker-compose.yml), [mosquitto.conf](mosquitto.conf)):** Mosquitto na porta `1883` intermediando os tópicos `petcare/sensor` e `petcare/atuador`.
- **Sensor ([sensor.py](sensor.py)):** Publica o nível de ração em `petcare/sensor` (`QoS 1`) a cada 5 segundos.
- **Atuador ([atuador.py](atuador.py)):** Assina `petcare/atuador` (`QoS 2`), executa `liberar_racao` e filtra comandos duplicados por `id_cmd`.
- **Controladores ([controlador.py](controlador.py)):** 3 processos independentes (`:9091`, `:9092`, `:9093`) com conexões SQLite persistentes (`no_1.db`, `no_2.db`, `no_3.db`). Realizam eleição de líder por mandato, replicação por quórum ($\ge 2/3$), validação determinística nas réplicas contra valores adulterados, catch-up ao iniciar e registro transacional de idempotência (`comandos`).
- **Cliente RPC ([cliente.py](cliente.py)):** Menu interativo no terminal via Pyro5 com consulta por quórum, geração de `id_cmd` (`uuid4`), confirmação pós-escrita por quórum e failover automático em caso de queda do líder.

---

## Documentação

- **[Especificação do Trabalho](docs/trabalho-pratico.pdf)**
- **[Fluxo de Decisão](docs/fluxo-decisao.md)**

---

## Como Executar

### 1. Instalar dependências

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Iniciar tudo (GNOME Console `kgx` com abas)

```bash
./run.sh
```

Para apagar os bancos SQLite (`no_*.db`) antes de iniciar:

```bash
./run.sh --reset
```

### 3. Execução manual (opcional, 1 processo por terminal)

```bash
docker compose up -d
.venv/bin/python controlador.py 1
.venv/bin/python controlador.py 2
.venv/bin/python controlador.py 3
.venv/bin/python atuador.py
.venv/bin/python sensor.py
.venv/bin/python cliente.py
```
