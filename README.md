# Sistema IoT Distribuído de Pet Care

Middleware e sistema distribuído para monitoramento e cuidado autônomo de pets, desenvolvido para o 1º Trabalho Prático de Sistemas Distribuídos. Combina comunicação baseada em objetos via RPC (**Pyro5**), mensageria pub/sub (**MQTT**), persistência replicada (**SQLite**), tolerância a falhas *Crash-Stop* com failover transparente e consenso por quórum bizantino ($2f + 1$).

---

## Documentação

| Documento | Descrição |
| :--- | :--- |
| **[Relatório Técnico](docs/relatorio.md)** | Apresentação do tema e soluções para replicação, failover na queda do controlador e segurança/quórum bizantino. |
| **[Arquitetura e Modelagem](docs/arquitetura.md)** | Topologia, portas, contratos de tópicos MQTT, esquema SQLite e estrutura de pacotes. |
| **[Execução e Testes](docs/execucao-e-testes.md)** | Instalação, inicialização automática/manual, uso do painel web, injeção de falhas e testes automatizados. |
| **[Especificação do Trabalho](docs/trabalho-pratico.pdf)** | Enunciado original do 1º Trabalho Prático de Sistemas Distribuídos. |

---

## Início Rápido

### 1. Instalar dependências
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Iniciar todo o ecossistema
```bash
./scripts/start_system.sh
```
Acesse o painel em **http://localhost:8080/**

### 3. Executar testes automatizados
```bash
.venv/bin/pytest
```
