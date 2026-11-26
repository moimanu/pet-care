import hashlib
import json
import random
import sqlite3
import sys
import threading
import time
import paho.mqtt.client as mqtt
import Pyro5.api

NOS = (1, 2, 3)
QUORUM = 2
TOPICO_SENSOR = "petcare/sensor"
TOPICO_ATUADOR = "petcare/atuador"


def conectar_no(id_no, timeout=0.8):
    # 1. Cria o proxy Pyro5 apontando para a porta do no de destino
    conexao = Pyro5.api.Proxy(f"PYRO:controlador.{id_no}@localhost:{9090 + id_no}")
    # 2. Define o timeout da chamada RPC e retorna a conexao
    conexao._pyroTimeout = timeout
    return conexao


def calcular_hash(racao, comandos):
    # 1. Serializa racao e lista ordenada de comandos de forma deterministica
    dados = json.dumps({"racao": int(racao), "comandos": sorted(comandos)}, sort_keys=True)
    # 2. Calcula e retorna o digest SHA-256 em hexadecimal
    return hashlib.sha256(dados.encode()).hexdigest()


@Pyro5.api.expose
class Controlador:
    def __init__(self, id_no):
        # 1. Inicializa identidade, mandato, lider em memoria e trava de concorrencia
        self.id_no = id_no
        self.mandato = 0
        self.lider_id = None
        self.trava = threading.Lock()

        # 2. Abre o banco SQLite local e garante as tabelas e o estado inicial (500g)
        self.banco = sqlite3.connect(f"no_{id_no}.db", check_same_thread=False)
        with self.banco:
            self.banco.execute("CREATE TABLE IF NOT EXISTS estado (id INTEGER PRIMARY KEY, racao INTEGER)")
            self.banco.execute("CREATE TABLE IF NOT EXISTS comandos (id_cmd TEXT PRIMARY KEY)")
            self.banco.execute("INSERT OR IGNORE INTO estado VALUES (1, 500)")

        # 3. Realiza catch-up inicial consultando pares ativos e validando integridade via hash
        for estado_par in self._executar_nos_pares("consultar_estado", timeout=0.5):
            racao_par = estado_par["racao"]
            comandos_par = estado_par["comandos"]
            hash_par = estado_par["hash_estado"]
            if (
                estado_par["mandato"] > self.mandato
                and 0 <= racao_par <= 500
                and hash_par == calcular_hash(racao_par, comandos_par)
            ):
                with self.trava:
                    self.mandato = estado_par["mandato"]
                    self.lider_id = estado_par["lider_id"]
                    self._salvar_estado(racao_par, comandos_par)

        # 4. Configura e inicia o cliente MQTT para escutar telemetria do sensor
        self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"controlador_{id_no}")
        self.mqtt.on_connect = self._ao_conectar
        self.mqtt.on_message = self._ao_receber_telemetria
        self.mqtt.connect("localhost", 1883, 30)
        self.mqtt.loop_start()

    def _ao_conectar(self, cliente, dados, flags, codigo, propriedades):
        # 1. Assina o topico de telemetria do sensor com QoS 1 ao conectar no broker
        cliente.subscribe(TOPICO_SENSOR, qos=1)

    def _ler_estado(self):
        # 1. Consulta o nivel atual de racao no SQLite
        racao = int(self.banco.execute("SELECT racao FROM estado WHERE id=1").fetchone()[0])
        # 2. Lista todos os IDs de comandos ja processados em ordem
        comandos = [linha[0] for linha in self.banco.execute("SELECT id_cmd FROM comandos ORDER BY id_cmd")]
        # 3. Retorna racao, comandos e o hash correspondente ao estado
        return racao, comandos, calcular_hash(racao, comandos)

    def _salvar_estado(self, racao, comandos=()):
        # 1. Persiste atomicamente a nova racao e os novos IDs de comando no SQLite
        with self.banco:
            self.banco.execute("UPDATE estado SET racao=? WHERE id=1", (int(racao),))
            self.banco.executemany(
                "INSERT OR IGNORE INTO comandos VALUES (?)",
                [(id_comando,) for id_comando in comandos if id_comando],
            )

    def _executar_nos_pares(self, nome_metodo, *argumentos, timeout=0.8):
        # 1. Percorre todos os demais nos do cluster ignorando falhas de rede individuais
        respostas = []
        for id_par in NOS:
            if id_par != self.id_no:
                try:
                    # 2. Invoca o metodo RPC remoto no par e armazena a resposta
                    metodo_remoto = getattr(conectar_no(id_par, timeout), nome_metodo)
                    respostas.append(metodo_remoto(*argumentos))
                except Exception:
                    pass
        return respostas

    def _sincronizar_pares(
        self, mandato, operacao, valor, id_comando=None, hash_anterior=None, hash_esperado=None
    ):
        # 1. Envia a atualizacao de estado para os pares e conta quantos confirmaram
        confirmacoes = self._executar_nos_pares(
            "sincronizar_estado",
            mandato,
            self.id_no,
            operacao,
            valor,
            id_comando,
            hash_anterior,
            hash_esperado,
        )
        return sum(bool(confirmado) for confirmado in confirmacoes)

    def consultar_estado(self):
        # 1. Garante leitura atomica entre memoria (mandato/lider) e disco (SQLite)
        with self.trava:
            racao, comandos, hash_estado = self._ler_estado()
            return {
                "id_no": self.id_no,
                "mandato": self.mandato,
                "lider_id": self.lider_id,
                "racao": racao,
                "comandos": comandos,
                "hash_estado": hash_estado,
            }

    def votar(self, mandato, candidato_id, hash_estado=None):
        # 1. Adquire a trava para impedir dois votos no mesmo mandato
        with self.trava:
            # 2. Rejeita se o mandato nao for estritamente maior ou se o hash divergir
            if mandato <= self.mandato:
                return False
            _, _, hash_atual = self._ler_estado()
            if hash_estado is not None and hash_estado != hash_atual:
                return False

            # 3. Atualiza mandato e registra o voto no candidato
            self.mandato = mandato
            self.lider_id = candidato_id
            return True

    def sincronizar_estado(
        self, mandato, lider_id, operacao, valor, id_comando=None, hash_anterior=None, hash_esperado=None
    ):
        # 1. Serializa validacao e escrita para evitar intercalacao de replicacoes
        with self.trava:
            # 2. Rejeita mensagens de mandatos antigos
            if mandato < self.mandato:
                return False

            # 3. Le o estado atual e verifica idempotencia por id_comando
            racao_atual, comandos_registrados, hash_atual = self._ler_estado()
            if id_comando and id_comando in comandos_registrados:
                return racao_atual == valor

            # 4. Garante que o estado base local bate com o hash_anterior esperado
            if hash_anterior is not None and hash_anterior != hash_atual:
                return False

            # 5. Se for apenas confirmacao de lideranca, atualiza mandato/lider e encerra
            if operacao == "lideranca":
                self.mandato = mandato
                self.lider_id = lider_id
                return True

            # 6. Valida as regras de negocio da operacao (liberar_racao ou telemetria)
            if operacao == "liberar_racao":
                operacao_valida = valor == min(500, racao_atual + 100)
            elif operacao == "telemetria":
                operacao_valida = 0 <= valor <= 500
            else:
                operacao_valida = False

            if not operacao_valida:
                return False

            # 7. Verifica o hash esperado apos aplicar a mudanca
            comandos_apos_operacao = list(comandos_registrados)
            if id_comando:
                comandos_apos_operacao.append(id_comando)
            if hash_esperado is not None and hash_esperado != calcular_hash(valor, comandos_apos_operacao):
                return False

            # 8. Atualiza mandato/lider em memoria e persiste o novo estado no SQLite
            self.mandato = mandato
            self.lider_id = lider_id
            self._salvar_estado(valor, [id_comando])
            return True

    def eleger_lider(self):
        # 1. Descobre o maior mandato conhecido entre os pares ativos
        estados_pares = self._executar_nos_pares("consultar_estado", timeout=0.5)
        maior_mandato_par = max((estado_par["mandato"] for estado_par in estados_pares), default=0)

        # 2. Reserva atomicamente o novo mandato e captura o hash atual antes da votacao
        with self.trava:
            novo_mandato = max(self.mandato, maior_mandato_par) + 1
            self.mandato = novo_mandato
            self.lider_id = None
            _, _, hash_estado = self._ler_estado()

        # 3. Conta o proprio voto e solicita votos aos pares
        respostas_votos = self._executar_nos_pares("votar", novo_mandato, self.id_no, hash_estado)
        votos = 1 + sum(bool(votou) for votou in respostas_votos)
        if votos < QUORUM:
            print(f"[CTRL {self.id_no}] Eleicao falhou (mandato {novo_mandato}, votos {votos}/3)")
            return False

        # 4. Revalida se nao houve alteracao de mandato/estado durante a votacao e assume lideranca
        with self.trava:
            racao_atual, _, hash_atual = self._ler_estado()
            if self.mandato > novo_mandato or hash_atual != hash_estado:
                return False
            self.mandato = novo_mandato
            self.lider_id = self.id_no

        # 5. Notifica os pares sobre a nova lideranca e retorna sucesso
        self._sincronizar_pares(
            novo_mandato,
            "lideranca",
            racao_atual,
            hash_anterior=hash_estado,
            hash_esperado=hash_estado,
        )
        print(f"[CTRL {self.id_no}] Eleito lider (mandato {novo_mandato}, votos {votos}/3)")
        return True

    def _replicar_por_quorum(self, operacao, valor, id_comando=None):
        # 1. Captura snapshot do hash atual e calcula o hash esperado pos-operacao
        with self.trava:
            _, comandos_registrados, hash_anterior = self._ler_estado()
            comandos_apos_operacao = list(comandos_registrados)
            if id_comando and id_comando not in comandos_apos_operacao:
                comandos_apos_operacao.append(id_comando)
            hash_esperado = calcular_hash(valor, comandos_apos_operacao)

        # 2. Replica para os pares e exige confirmacao de quorum (incluindo o proprio no)
        confirmacoes = 1 + self._sincronizar_pares(
            self.mandato, operacao, valor, id_comando, hash_anterior, hash_esperado
        )
        if confirmacoes < QUORUM:
            return False

        # 3. Persiste localmente no SQLite apos confirmar quorum
        with self.trava:
            self._salvar_estado(valor, [id_comando])
        return True

    def liberar_racao(self, id_comando):
        # 1. Apenas o lider atual pode processar comandos de atuacao
        if self.lider_id != self.id_no:
            return {"ok": False, "erro": "nao_lider"}

        # 2. Checa idempotencia e calcula nova quantidade de racao (teto de 500g)
        with self.trava:
            racao_atual, comandos_registrados, _ = self._ler_estado()
            if id_comando and id_comando in comandos_registrados:
                return {"ok": True, "racao": racao_atual, "lider": self.id_no, "idempotente": True}
            nova_racao = min(500, racao_atual + 100)

        # 3. Replica a mudanca por quorum antes de acionar o atuador fisico
        if not self._replicar_por_quorum("liberar_racao", nova_racao, id_comando):
            return {"ok": False, "erro": "sem_quorum"}

        # 4. Publica ordem no topico MQTT do atuador (QoS 2) e retorna resultado
        self.mqtt.publish(
            TOPICO_ATUADOR,
            json.dumps({"acao": "liberar_racao", "id_cmd": id_comando}),
            qos=2,
        )
        print(f"[CTRL {self.id_no}] Racao liberada -> {nova_racao}g ({id_comando})")
        return {"ok": True, "racao": nova_racao, "lider": self.id_no, "idempotente": False}

    def _lider_atual_valido(self):
        # 1. Verifica via RPC se o lider conhecido esta ativo e se reconhece como lider
        if self.lider_id is None:
            return False
        try:
            estado_lider = conectar_no(self.lider_id, 0.5).consultar_estado()
            return estado_lider["lider_id"] == self.lider_id
        except Exception:
            return False

    def _ao_receber_telemetria(self, cliente, dados, mensagem):
        # 1. Se este no nao for o lider, verifica se ha lider valido; se nao houver, tenta eleicao com jitter
        if self.lider_id != self.id_no:
            if self._lider_atual_valido():
                return
            time.sleep(random.uniform(0.05, 0.35))
            # Se ainda nao sou o lider E (ja existe outro lider valido OU tentei me eleger e falhei), encerra
            if self.lider_id != self.id_no and (self._lider_atual_valido() or not self.eleger_lider()):
                return

        # 2. Como lider, extrai o valor de racao da mensagem MQTT e replica por quorum
        racao = int(json.loads(mensagem.payload.decode())["racao"])
        if self._replicar_por_quorum("telemetria", racao):
            print(f"[CTRL {self.id_no}] Telemetria replicada: {racao}g")
        else:
            # 3. Em caso de falha/divergencia, abdica da lideranca somente se ainda for o lider
            print(f"[CTRL {self.id_no}] Falha ao replicar telemetria (sem quorum/divergencia); abdicando lideranca")
            with self.trava:
                if self.lider_id == self.id_no:
                    self.lider_id = None


if __name__ == "__main__":
    # 1. Le o ID do no via argumento, sobe o daemon Pyro5 e entra no loop de requisicoes
    id_no = int(sys.argv[1])
    servidor = Pyro5.api.Daemon(host="localhost", port=9090 + id_no)
    servidor.register(Controlador(id_no), f"controlador.{id_no}")
    print(f"[CTRL {id_no}] Ativo em :{9090 + id_no}")
    servidor.requestLoop()
