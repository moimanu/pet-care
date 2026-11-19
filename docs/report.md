# Relatório - 1º Trabalho Prático de Sistemas Distribuídos

## Sumário

1. [Tema e Funcionamento Geral](#1-tema-e-funcionamento-geral)
2. [Replicação de Dados entre Controladores](#2-replicacao-de-dados-entre-controladores)
3. [Tolerância a Falhas e Continuidade na Queda do Controlador](#3-tolerancia-a-falhas-e-continuidade-na-queda-do-controlador)
4. [Segurança: Respostas Divergentes, Invasão e Erros Intermitentes](#4-seguranca-respostas-divergentes-invasao-e-erros-intermitentes)

---

## 1. Tema e Funcionamento Geral

O trabalho implementa um sistema IoT distribuído para monitoramento e cuidado autônomo de animais domésticos (*Smart Pet Care*). O ambiente integra dispositivos de borda (`app/sensors.py`), um broker MQTT Mosquitto, um cluster de três controladores replicados (`app/controller.py`) e um cliente web com comunicação baseada em objetos remotos Pyro5 (`app/rpc_client.py` e `app/web_server.py`).

Os sensores publicam telemetria no broker MQTT (`QoS 1`) e os atuadores assinam comandos de atuação (`QoS 2`). Os controladores consomem essa telemetria para atualizar o estado do sistema e tomar decisões autônomas, enquanto o cliente monitora o ambiente em tempo real via callbacks remotos (`ClientObserver`) e aciona métodos diretamente nos controladores via Pyro5 RPC.

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

A consistência entre os nós segue o modelo primário-réplica com transferência de estado via RPC P2P. Embora os três controladores assinem os tópicos MQTT de sensores, apenas o controlador Líder processa as transições de estado, grava a alteração em seu SQLite local e propaga imediatamente o snapshot atualizado e o identificador do evento para as réplicas secundárias através do método remoto `sync_state()`. Quando um nó é iniciado ou se recupera de uma falha, ele executa o procedimento de *catch-up* (`_catch_up_from_peers()`), consultando o Líder ativo via RPC para clonar o estado canônico mais recente antes de entrar em operação.

---

## 3. Tolerância a Falhas e Continuidade na Queda do Controlador

Para garantir que o cliente obtenha um resultado positivo na execução de métodos mesmo se o controlador Líder cair (*Crash-Stop*), o middleware combina failover transparente no cliente, auto-eleição P2P entre réplicas e idempotência por identificador único (`command_id` UUID v4).

Quando o cliente invoca `execute_command()`, o `ClusterRpcClient` localiza o Líder ativo e realiza a chamada RPC com timeout de `2.0s`. Se o Líder estiver inativo ou falhar durante a requisição, o cliente intercepta a exceção, promove automaticamente a próxima réplica saudável à liderança (`_promote_first_available()`) e reexecuta o método com o mesmo `command_id`, entregando o resultado positivo ao usuário sem interromper a operação. De forma complementar, caso o Líder caia e chegue uma nova telemetria via MQTT, as réplicas verificam a ausência de líder ativo via `ping()` P2P e a réplica viva de menor identificador (`_maybe_elect_self()`) assume a liderança automaticamente.

Para evitar que uma falha ocorrida logo após a persistência cause execução duplicada durante a retentativa no novo Líder, todo comando carrega seu `command_id` até a tabela `event_log`. Se o novo Líder identificar que o UUID já foi registrado via replicação anterior, ele retorna sucesso em modo idempotente sem repetir a transição; na borda IoT, os atuadores também descartam comandos repetidos em uma janela deslizante de `2.0s`.

---

## 4. Segurança: Respostas Divergentes, Invasão e Erros Intermitentes

A proteção contra controladores comprometidos ou com falhas intermitentes baseia-se no consenso por quórum bizantino ($2f + 1$). Para tolerar até $f = 1$ controlador respondendo valores falsos, o cluster utiliza $N = 3$ réplicas e exige maioria mínima de 2 respostas idênticas. Cada nó gera um resumo criptográfico determinístico do seu estado (`PetState.canonical_hash()`, com `SHA-256` truncado em 12 caracteres sobre os campos de negócio ordenados).

A cada leitura ou pré-validação de comando (`get_consensus_state()`), o cliente consulta `get_state()` nos três controladores em paralelo. Os hashes retornados são comparados e apenas o estado majoritário (2 ou 3 votos iguais) é aceito, enquanto qualquer resposta divergente é descartada e registrada como falha bizantina. Se o nó divergente for o próprio controlador Líder, o cliente executa `_demote_and_replace()`, destituindo imediatamente o líder comprometido (`set_primary(False)`) e promovendo uma réplica íntegra do quórum majoritário (`set_primary(True)`).

Adicionalmente, um nó em estado bizantino fica impedido de propagar `sync_state()` para não contaminar os bancos SQLite das réplicas honestas. Na fronteira com os dispositivos IoT, o módulo de domínio (`validate_telemetry()` em `app/domain.py`) aplica validação estrita de tipos e limites físicos antes de qualquer alteração de estado, descartando leituras corrompidas ou fora de escala vindas do broker MQTT.
