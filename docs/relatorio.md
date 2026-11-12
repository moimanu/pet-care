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

## 2. Replicação dos Dados Armazenados

O sistema mantém **3 nós controladores independentes** (`Controlador 1`, `Controlador 2`, `Controlador 3`), cada um com seu próprio banco relacional SQLite em modo WAL (`node_1.db`, `node_2.db`, `node_3.db`).

* **Replicação de Telemetria (Passiva via Pub/Sub):** Os 3 controladores assinam simultaneamente `petcare/sensores/#` (QoS 1) e atualizam seus bancos locais de forma independente.
* **Replicação de Comandos (Ativa via RPC):** O Middleware despacha o comando (`Command` com `command_id` UUID) ao líder primário e o replica via Pyro5 RPC para todas as réplicas secundárias ativas (`MiddlewareService.send_command`).
* **Sincronização de Inicialização (*Catch-up*):** Ao iniciar com banco vazio ou após recuperação, a réplica executa `sync_boot_state()`, consultando os pares via RPC para clonar o estado canônico mais recente.

---

## 3. Disponibilidade na Queda do Controlador (Failover Transparente)

Para garantir que o cliente sempre obtenha resultado positivo na execução de métodos mesmo se o controlador cair (*Crash-Stop*):

1. **Detecção e Failover Automático:** Quando `primary_node.execute_command()` falha ou atinge timeout no Middleware, o loop de failover seleciona imediatamente a próxima réplica saudável.
2. **Promoção de Liderança e Invariante de Líder Único:** O Middleware promove a réplica escolhida (`candidate_node.set_primary(True)`), executa o comando pendente e rebaixa os demais nós (`set_primary(False)`). Apenas o líder ativo publica comandos autônomos no MQTT, evitando triplicação de atuações na borda.
3. **Recuperação Não-Preemptiva:** Quando um nó caído volta a ficar online, ele verifica a existência de um líder ativo e reingressa como réplica secundária sem interromper o tráfego.
4. **Idempotência (*Exactly-Once*):** Todo comando possui `command_id` (UUID v4). Os controladores registram os IDs executados na tabela `event_log` e ignoram duplicatas; os atuadores de borda também filtram retransmissões com cache em janela deslizante de 2s.

---

## 4. Segurança: Respostas Divergentes, Invasão e Erros Intermitentes

O sistema resolve falhas maliciosas (nós invadidos/corrompidos) e leituras espúrias em duas camadas:

### 4.1. Consenso por Quórum Bizantino ($2f + 1$) no Middleware
* Para tolerar $f = 1$ controlador comprometido ou com erro intermitente, o cluster opera com $N = 2f + 1 = 3$ réplicas e exige quórum mínimo de **2 respostas idênticas**.
* Em cada leitura (`get_consensus_state`), o Middleware consulta os 3 controladores em paralelo (`ThreadPoolExecutor`, timeout de `1.0s`).
* Cada estado retornado gera um `state_hash()` SHA-256 canônico (arredonda floats em 2 casas decimais e ignora o campo volátil `atualizado_em`).
* Se um nó for invadido ou injetado com falha bizantina (`--corrupt`, reportando falso estado de fuga/alarme), seu hash diverge do quórum majoritário (2 votos contra 1). O Middleware **descarta a resposta adulterada**, entrega o estado íntegro ao cliente e registra alerta explícito:
  `[SEGURANÇA] Falha Bizantina detectada no Nó X. Resposta descartada.`

### 4.2. Validação Física e Sanitização (`TelemetrySanityValidator`)
* Antes de persistir telemetria ou enviar comandos, o domínio valida tipos e limites físicos estritos:
  * `racao`: `0.0 <= nivel_gramas <= 500.0`
  * `agua`: `0.0 <= nivel_ml <= 1000.0`
  * `areia`: `usos_acumulados >= 0`
  * `fuga`: `perimetro in {"DENTRO", "FORA"}`
* Payloads malformados, negativos ou fora de escala (erros intermitentes de sensores ou ataques de injeção no broker) lançam `SanityError` e são descartados antes de alterar o estado.

