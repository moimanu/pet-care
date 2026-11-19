# Relatório Técnico — 1º Trabalho Prático de Sistemas Distribuídos

Respostas diretas aos requisitos especificados em [trabalho-pratico.pdf](trabalho-pratico.pdf).

---

## 1. Tema e Complexidade do Trabalho

**Tema:** Ecossistema IoT Distribuído para Monitoramento e Cuidado Autônomo de Pets (*Smart Pet Care*).

O sistema gerencia 4 grandezas físicas de borda e 3 atuadores em malha fechada, combinando decisões autônomas nos controladores e comandos remotos do tutor via painel web:

| Dispositivo | Tipo | Tópico MQTT | Função / Regra no Sistema |
| :--- | :--- | :--- | :--- |
| Sensor de Ração | Sensor | `petcare/sensores/racao` | Mede peso no comedouro (`0` a `500g`). Se `< 50g`, aciona reposição autônoma (`+50g`). |
| Sensor de Água | Sensor | `petcare/sensores/agua` | Mede volume no bebedouro (`0` a `1000ml`). Se `< 150ml`, aciona reposição autônoma (`+200ml`). |
| Sensor de Areia | Sensor | `petcare/sensores/areia` | Contabiliza usos da caixa sanitária (`>= 0`). |
| Sensor de Perímetro | Sensor | `petcare/sensores/fuga` | Detecta presença (`DENTRO` / `FORA`). Se `FORA`, dispara o alarme imediatamente. |
| Dispenser | Atuador | `petcare/atuadores/dispenser` | Libera ração (`LIBERAR_RACAO`). |
| Válvula Hidráulica | Atuador | `petcare/atuadores/valvula` | Repõe água (`REPOR_AGUA`). |
| Sirene / Alarme | Atuador | `petcare/atuadores/alarme` | Ativa (`DISPARAR_ALARME`) ou silencia (`SILENCIAR_ALARME`) o alerta sonoro. |

---

## 2. Arquitetura do Middleware Descentralizado e Replicação dos Dados

Seguindo fielmente a topologia da Figura 1 da especificação, o **Middleware** é a camada distribuída composta pela **invocação de métodos baseada em objetos (Pyro5 RPC)** entre clientes e controladores, pelos **3 Controladores de Processo** (`Controlador 1`, `Controlador 2`, `Controlador 3`), pela **Sincronização e Tolerância a Falhas P2P** entre os controladores e pela **Replicação nos Armazenamentos SQLite** (`node_1.db`, `node_2.db`, `node_3.db`).

* **Sincronização Completa de Estado Canônico (*Primary-Backup State Transfer*):** Todos os 3 controladores mantêm assinatura ativa em `petcare/sensores/#` (QoS 1), porém apenas o **Controlador Líder (Primário)** processa as transições de estado (seja por telemetria MQTT ou comando RPC do cliente). A cada transição, o Líder grava o novo `PetState` no seu banco SQLite e **replica o snapshot completo do estado canônico (`peer.sync_state(...)`) diretamente para as réplicas via RPC P2P**, eliminando qualquer possibilidade de deriva entre os armazenamentos.
* **Sincronização de Inicialização e Recuperação (*Catch-up* P2P):** Ao iniciar ou ao se recuperar de uma falha (`sync_with_peers()`), o controlador consulta seus pares via RPC para respeitar o líder ativo e clonar o `PetState` canônico mais recente para seu armazenamento local.

---

## 3. Disponibilidade na Queda do Controlador (Failover P2P e Transparente)

Para garantir que o sistema continue atuando na borda e que o cliente sempre obtenha resultado positivo na execução de métodos mesmo se o controlador líder cair (*Crash-Stop*):

1. **Failover P2P Proativo entre Controladores:** Quando o Líder entra em *Crash-Stop* (ou quando as réplicas recebem telemetria MQTT e detectam ausência de um líder saudável ativo entre os pares), a réplica saudável e íntegra disponível assume automaticamente a liderança (`set_primary(True)`), rebaixa os demais pares (`set_primary(False)`) e passa a processar a telemetria MQTT e replicar o estado canônico sem interrupção.
2. **Failover Transparente na Invocação de Métodos:** Quando o cliente (`ClusterClientService`) invoca `execute_command()` e o Líder falha ou atinge timeout, o cliente promove imediatamente a próxima réplica saudável e íntegra, reexecutando o método de forma transparente.
3. **Recuperação Não-Preemptiva:** Quando um nó caído volta a ficar online (`set_offline(False)`), ele detecta o líder ativo via P2P, reingressa como réplica secundária e sincroniza imediatamente seu banco SQLite com o estado canônico do líder.
4. **Idempotência (*Exactly-Once*):** Todo comando possui `command_id` (UUID v4) registrado na tabela `event_log` do Líder e replicado às réplicas via `sync_state`, impedindo reexecuções duplicadas; os atuadores de borda também filtram retransmissões com cache em janela deslizante de 2s.

---

## 4. Segurança: Respostas Divergentes, Invasão e Erros Intermitentes

O sistema resolve falhas maliciosas (nós invadidos/corrompidos) e leituras espúrias em duas camadas:

### 4.1. Consenso por Quórum Bizantino ($2f + 1$) e Destituição de Líder Bizantino
* Para tolerar $f = 1$ controlador comprometido ou com erro intermitente, o cluster opera com $N = 2f + 1 = 3$ réplicas e exige quórum mínimo de **2 respostas idênticas**.
* Em cada leitura (`get_consensus_state`), o `ClusterClientService` consulta `get_state()` diretamente nos 3 controladores em paralelo (`ThreadPoolExecutor`, timeout de `1.0s`) e compara o `state_hash()` SHA-256 canônico de cada resposta.
* **Isolamento e Destituição Automática:** Um nó corrompido (`--corrupt`) tem sua falha isolada sem contaminar os armazenamentos das réplicas honestas via `sync_state`. Se um nó responder valores divergentes do quórum majoritário (2 votos contra 1), sua resposta é descartada com alerta `[SEGURANÇA] Falha Bizantina detectada no Nó X. Resposta descartada.` e, caso o nó comprometido seja o Líder atual, ele é **automaticamente destituído da liderança** em favor de uma réplica íntegra.

### 4.2. Validação Física e Sanitização (`TelemetrySanityValidator`)
* Antes de persistir telemetria ou enviar comandos, o domínio valida tipos e limites físicos estritos:
  * `racao`: `0.0 <= nivel_gramas <= 500.0`
  * `agua`: `0.0 <= nivel_ml <= 1000.0`
  * `areia`: `usos_acumulados >= 0`
  * `fuga`: `perimetro in {"DENTRO", "FORA"}`
* Payloads malformados, negativos ou fora de escala lançam `SanityError` e são descartados antes de alterar o estado.
