# Relatório - 1º Trabalho Prático de Sistemas Distribuídos

## Sumário

1. [Tema e Funcionamento Geral](#1-tema-e-funcionamento-geral)
2. [Replicação de Dados entre Controladores](#2-replicacao-de-dados-entre-controladores)
3. [Tolerância a Falhas e Continuidade na Queda do Controlador](#3-tolerancia-a-falhas-e-continuidade-na-queda-do-controlador)
4. [Segurança: Respostas Divergentes, Invasão e Erros Intermitentes](#4-seguranca-respostas-divergentes-invasao-e-erros-intermitentes)

---

## 1. Tema e Funcionamento Geral

O trabalho implementa um sistema IoT distribuído para monitoramento e cuidado autônomo de animais domésticos (*Smart Pet Care*). O ambiente integra dispositivos de borda (`app/sensors.py`), um broker MQTT Mosquitto, um cluster de três controladores replicados (`app/controller.py`), um processo cliente RPC dedicado (`app/client.py`) e um servidor web (`app/web_server.py`) que atua como ponte HTTP/SSE para o painel interativo.

Os sensores publicam telemetria no broker MQTT (`QoS 1`) e os atuadores assinam comandos de atuação (`QoS 2`). Os controladores consomem essa telemetria para atualizar o estado do sistema e tomar decisões autônomas, enquanto o cliente RPC monitora o ambiente em tempo real via callbacks remotos (`PushReceiver` em `petcare.client.observer`), apura o quórum e aciona métodos nos controladores via Pyro5 RPC, encaminhando atualizações de consenso ao painel web (`petcare.web.observer`).

| Dispositivo | Tipo | Tópico MQTT | Função e Limites Operacionais |
| :--- | :--- | :--- | :--- |
| **Comedouro (Ração)** | Sensor | `petcare/sensores/racao` | Informa `nivel_gramas` no intervalo `[0.0, 500.0]`. |
| **Bebedouro (Água)** | Sensor | `petcare/sensores/agua` | Informa `nivel_ml` no intervalo `[0.0, 1000.0]`. |
| **Caixa Sanitária (Areia)** | Sensor | `petcare/sensores/areia` | Contabiliza `usos_acumulados` (`int >= 0`). |
| **Monitor de Perímetro (Fuga)** | Sensor | `petcare/sensores/fuga` | Informa `perimetro` (`"DENTRO"` ou `"FORA"`), disparando o alarme se `"FORA"`. |
| **Dispenser de Ração** | Atuador | `petcare/atuadores/dispenser` | Executa `LIBERAR_RACAO` (`+50.0g`, limitado a `500.0g`). |
| **Válvula Hidráulica** | Atuador | `petcare/atuadores/valvula` | Executa `REPOR_AGUA` (`+200.0ml`, limitado a `1000.0ml`). |
| **Sirene / Alarme** | Atuador | `petcare/atuadores/alarme` | Executa `DISPARAR_ALARME` (fuga) ou `SILENCIAR_ALARME` (comando do cliente). |

---

## 2. Replicação de Dados entre Controladores

O cluster opera com três controladores independentes nas portas `9091`, `9092` e `9093`, cada um mantendo seu próprio banco de dados SQLite isolado (`node_1.db`, `node_2.db` e `node_3.db`). Cada banco armazena a tabela `pet_state`, com o estado canônico atual, e a tabela `event_log`, com o registro de todos os comandos e telemetrias processados (`app/storage.py`).

A consistência entre os nós segue o modelo primário-réplica validado por quórum P2P ($2f + 1$). Embora os três controladores assinem os tópicos MQTT de sensores, apenas o controlador Líder do mandato (`term`) atual inicia a transição, calcula o hash canônico resultante e envia a operação (`sync_state()`) para as réplicas secundárias. Cada réplica valida o mandato do remetente, reexecuta deterministicamente a transição sobre seu próprio banco (`apply_transition()` em `app/domain.py`) e só grava se o hash resultante coincidir com o esperado; o Líder persiste e aciona atuadores apenas após atingir maioria mínima de confirmações (`MIN_QUORUM = 2`). Quando um nó inicia ou se recupera de falha, ele executa `_catch_up_from_peers()`, apurando o quórum entre os pares via RPC para clonar o estado canônico e o mandato atual antes de operar.

---

## 3. Tolerância a Falhas e Continuidade na Queda do Controlador

Para garantir que o cliente obtenha um resultado positivo na execução de métodos mesmo se o controlador Líder cair (*Crash-Stop*), o middleware combina eleição P2P por mandato (`term`), failover transparente no cliente e idempotência por identificador único (`command_id` UUID v4).

Os controladores auditam o cluster sob demanda via `reconcile_leadership()` (na chegada de telemetria MQTT ou quando acionados pelo cliente): se o Líder atual estiver inacessível, o nó saudável de menor identificador dentro da maioria propõe um novo mandato (`term + 1`) aos pares via `request_vote()`. Atingido o quórum mínimo (`2/3` ou `2/2` votos), o novo mandato é oficializado via `commit_term()` e o novo Líder assume autonomamente. No lado do cliente (`app/client.py` na porta `9094`), `execute_command()` valida o quórum antes e após a chamada RPC (`timeout = 2.0s`); se o Líder cair ou falhar durante a requisição, o cliente solicita a reconciliação P2P do cluster e reexecuta o método no novo Líder reutilizando o mesmo `command_id`.

Para evitar que uma falha ocorrida logo após a replicação cause execução duplicada na retentativa, todo comando grava seu `command_id` na tabela `event_log`. Se o novo Líder identificar que o UUID já foi processado, retorna sucesso em modo idempotente sem repetir a transição; na borda IoT, os atuadores também descartam comandos repetidos em uma janela deslizante de `2.0s`.

---

## 4. Segurança: Respostas Divergentes, Invasão e Erros Intermitentes

A proteção contra controladores comprometidos ou com falhas intermitentes baseia-se no consenso por quórum bizantino ($2f + 1$) tanto nos controladores quanto no cliente, sem depender de sinalização cooperativa do nó defeituoso. Para tolerar até $f = 1$ controlador respondendo valores falsos, o cluster utiliza $N = 3$ réplicas e exige maioria mínima de 2 hashes idênticos (`PetState.canonical_hash()`, `SHA-256` truncado em 12 caracteres sobre os campos ordenados, excluindo `atualizado_em`).

Na leitura e na confirmação pós-escrita (`get_consensus_state()`), os três controladores são consultados em paralelo via `get_state()`, aceitando-se apenas o estado majoritário (2 ou 3 votos concordantes) e descartando qualquer nó divergente. Caso o próprio controlador Líder passe a responder estado adulterado, dois mecanismos atuam em conjunto:
1. **Bloqueio de contaminação na escrita:** mesmo que o Líder comprometido tente propagar `sync_state()` com hash forjado ou responda sucesso falso ao cliente, as réplicas honestas recalculam a transição localmente (`apply_transition()`) e rejeitam o hash divergente, enquanto o cliente barra a resposta que não coincide com o quórum pós-execução.
2. **Destituição P2P por mandato (`term`):** os controladores íntegros detectam por `evaluate_quorum()` que o Líder atual não pertence mais à maioria saudável e votam um novo mandato (`term + 1`) via `request_vote()`, oficializando a troca via `commit_term()` sem depender do voto do nó comprometido.

Na fronteira com os dispositivos IoT, o módulo de domínio (`validate_telemetry()` em `app/domain.py`) aplica validação estrita de tipos e limites físicos antes de qualquer alteração de estado, descartando leituras corrompidas ou fora de escala vindas do broker MQTT.
