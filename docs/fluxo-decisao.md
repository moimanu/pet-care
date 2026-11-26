# Árvores de Decisão — Fluxo do Sistema Pet Care

## 1. Inicialização e Catch-up do Controlador (`controlador.py`)

```mermaid
flowchart TD
    A["Controlador(id_no) inicia com mandato = 0, lider_id = None e trava (Lock)"] --> B["Abre no_{id_no}.db, cria tabelas estado/comandos e garante estado inicial (500g)"]
    B --> C["Consulta pares ativos via _executar_nos_pares('consultar_estado', timeout=0.5)"]
    C --> D{"Para cada par: mandato > self.mandato, 0 <= racao <= 500 e hash_estado == calcular_hash(racao, comandos)?"}
    D -- "Sim" --> E["Sob Lock: atualiza mandato, lider_id e persiste via _salvar_estado(racao_par, comandos_par)"]
    D -- "Nao (par desatualizado ou adulterado)" --> F["Ignora resposta do par"]
    E --> G["Conecta ao broker MQTT (:1883) e assina petcare/sensor (QoS 1)"]
    F --> G
```

---

## 2. Fluxo de Telemetria e Eleição Automática (`sensor_atuador.py` -> `controlador.py`)

```mermaid
flowchart TD
    A["Loop do Sensor decrementa sob Lock: racao = max(0, racao - 20)"] --> A1["Publica {'racao': nivel_atual} em petcare/sensor (QoS 1) a cada 5s"]
    A1 --> B["Cada Controlador recebe em _ao_receber_telemetria()"]
    B --> C{"self.lider_id == self.id_no?"}
    C -- "Sim" --> G["Extrai racao (int) do JSON e chama _replicar_por_quorum('telemetria', racao)"]
    C -- "Nao" --> D{"_lider_atual_valido()? (consultar_estado() em timeout 0.5s confirma lider_id == id)"}
    D -- "Sim" --> E["Ignora mensagem (apenas o lider replica)"]
    D -- "Nao" --> D1["Aguarda jitter aleatorio (0.05s a 0.35s) e reavalia estado"]
    D1 -- "Tornou-se lider durante a espera" --> G
    D1 -- "Outro lider valido assumiu" --> E
    D1 -- "Continua sem lider valido" --> F["eleger_lider(): consulta pares via _executar_nos_pares('consultar_estado', 0.5)"]
    F --> F0["Sob Lock: reserva novo_mandato = max(mandato, maior_par) + 1, zera lider_id e le hash_estado"]
    F0 --> F1{"Solicita _executar_nos_pares('votar', ...): pares validam mandato > atual e hash_estado == hash_atual; 1 + votos_pares >= 2?"}
    F1 -- "Nao (< 2 votos)" --> F1A["Imprime 'Eleicao falhou' e encerra"]
    F1 -- "Sim (Quorum >= 2/3)" --> F2{"Revalida sob Lock: mandato nao foi superado e hash_atual == hash_estado?"}
    F2 -- "Nao" --> E
    F2 -- "Sim" --> F3["Define lider_id = id_no e notifica via _sincronizar_pares(novo_mandato, 'lideranca', ...)"]
    F3 --> G
    G --> H["Sob Lock: le _ler_estado() e calcula hash_esperado via calcular_hash(); chama _sincronizar_pares()"]
    H --> I{"Seguidores validam em sincronizar_estado() sob Lock: mandato >= atual, hash_anterior == hash_atual, 0 <= valor <= 500 e hash_esperado?"}
    I -- "Par aprova" --> I1["Seguidor atualiza mandato/lider_id e persiste no SQLite via _salvar_estado()"]
    I -- "Par rejeita" --> I2["Seguidor retorna False (mantem banco intacto)"]
    I1 --> I3{"1 + confirmacoes_pares >= 2 (QUORUM)?"}
    I2 --> I3
    I3 -- "Sim" --> J["Lider persiste no proprio SQLite via _salvar_estado(racao) sob Lock"]
    I3 -- "Nao (Sem quorum / divergencia)" --> K["Aborta escrita no lider, imprime falha e abdica sob Lock se self.lider_id == self.id_no"]
```

---

## 3. Consulta de Quórum e Descoberta de Líder (`cliente.py` — opção `[1]` e base da `[2]`)

```mermaid
flowchart TD
    A["consultar_quorum() consulta consultar_estado() nos nos 1, 2 e 3 (timeout 1.0s)"] --> B{"Ao menos 2 nos responderam (len(respostas) >= 2)?"}
    B -- "Nao" --> B1["Retorna erro: 'Sem quorum (X/3 nos online)'"]
    B -- "Sim" --> C["Conta concordancia do par (racao, hash_estado) entre as respostas"]
    C --> D{"Maior grupo consistente tem votos >= 2?"}
    D -- "Nao (ex.: 2+ nos adulterados com valores distintos)" --> D1["Retorna erro: 'Divergencia de estado/hash entre nos (X/3 concordam)'"]
    D -- "Sim" --> E["Filtra consistentes e nos_vivos ordenados (exclui no divergente/adulterado)"]
    E --> F{"Ha no em consistentes com lider_id == id_no? (escolhe o de maior mandato)"}
    F -- "Sim" --> H["Retorna {'racao', 'lider', 'votos', 'vivos'}, erro=None"]
    F -- "Nao (lider = None)" --> G["tentar_eleger_lider(nos_vivos): chama eleger_lider() (timeout 1.5s) ate um vencer"]
    G --> H
```

---

## 4. Fluxo de Ação para o Atuador, Idempotência e Failover (`cliente.py` -> `controlador.py` -> `sensor_atuador.py`)

```mermaid
flowchart TD
    A["Cliente escolhe [2] Liberar racao -> enviar_liberacao_racao()"] --> B["Gera id_comando unico (UUID v4)"]
    B --> C{"Restam tentativas no loop (ate 3x: for _ in NOS)?"}
    C -- "Nao (3 falhas consecutivas)" --> Z["Exibe [ERRO] Cluster indisponivel"]
    C -- "Sim" --> D["Executa consultar_quorum() (Fluxo 3)"]
    D --> D1{"Retornou erro de quorum/divergencia?"}
    D1 -- "Sim" --> E["Exibe [ERRO] Sem quorum ou Divergencia e encerra"]
    D1 -- "Nao" --> F{"estado['lider'] definido (nao eh None)?"}
    F -- "Nao" --> E1["Exibe [ERRO] Nenhum lider eleito e encerra"]
    F -- "Sim" --> H["Cliente invoca lider.liberar_racao(id_comando) via Pyro5 (timeout 2.0s)"]
    H --> I{"Lider respondeu sem excecao/timeout?"}
    I -- "Nao (Caiu / Timeout)" --> J["Exibe [FAILOVER] Lider caiu e repete tentativa com o mesmo id_comando"]
    J --> C
    I -- "Sim" --> K{"No controlador: self.lider_id == self.id_no?"}
    K -- "Nao" --> L["Retorna ok=False, erro='nao_lider'"]
    K -- "Sim" --> M{"Sob Lock em _ler_estado(): id_comando ja esta em comandos?"}
    M -- "Sim" --> N["Retorna ok=True, idempotente=True (sem republicar no MQTT)"]
    M -- "Nao" --> O["Calcula nova_racao = min(500, racao_atual + 100)"]
    O --> P["Chama _replicar_por_quorum('liberar_racao', nova_racao, id_comando)"]
    P --> Q{"Seguidores validam em sincronizar_estado(): mandato >= atual, idempotencia ou (hash_anterior, valor == min(500, racao + 100) e hash_esperado); 1 + confirmacoes >= 2?"}
    Q -- "Nao" --> R["Retorna ok=False, erro='sem_quorum'"]
    L --> L1["Cliente exibe [FAILOVER] No recusou e chama tentar_eleger_lider(outros_vivos)"]
    R --> L1
    L1 --> C
    Q -- "Sim" --> S["Seguidores e lider gravam nova_racao e id_comando no SQLite via _salvar_estado()"]
    S --> T["Lider publica {'acao': 'liberar_racao', 'id_cmd': id_comando} em petcare/atuador (QoS 2)"]
    T --> U["Atuador recebe em ao_receber_comando()"]
    U --> U1{"id_comando ja esta em comandos_vistos?"}
    U1 -- "Sim" --> U2["Ignora comando duplicado"]
    U1 -- "Nao" --> U3["Registra em comandos_vistos e, se acao == 'liberar_racao', atualiza sob Lock: racao = min(500, racao + 100)"]
    U3 -. "Proximo ciclo de 5s do Sensor publica novo nivel" .-> A_REF["Fluxo 2 (Telemetria)"]
    T --> V["Cliente executa confirmacao pos-escrita via consultar_quorum() (Fluxo 3)"]
    N --> V
    V --> W{"Sem erro, lider segue em estado_confirmado['vivos'] e estado_confirmado['racao'] == resposta['racao']?"}
    W -- "Sim" --> X["Exibe [OK] Racao liberada pelo Lider (com tag 'idempotente' se aplicavel) e encerra"]
    W -- "Nao" --> Y["Exibe [FAILOVER] Resposta do Lider diverge do quorum e tenta proxima iteracao"]
    Y --> C
```
