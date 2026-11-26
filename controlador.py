import json
import sqlite3
import sys
import threading

import paho.mqtt.client as mqtt
import Pyro5.api

NOS = (1, 2, 3)
QUORUM = 2
TOPICO_SENSOR = "petcare/sensor"
TOPICO_ATUADOR = "petcare/atuador"


def conectar_no(id_no, timeout=0.8):
    conexao = Pyro5.api.Proxy(f"PYRO:controlador.{id_no}@localhost:{9090 + id_no}")
    conexao._pyroTimeout = timeout
    return conexao


@Pyro5.api.expose
class Controlador:
    def __init__(self, id_no):
        self.id_no = id_no
        self.mandato = 0
        self.lider_id = None
        self.trava = threading.Lock()
        self.banco = sqlite3.connect(f"no_{id_no}.db", check_same_thread=False)

        with self.banco:
            self.banco.execute(
                "CREATE TABLE IF NOT EXISTS estado (id INTEGER PRIMARY KEY, racao REAL)"
            )
            self.banco.execute(
                "CREATE TABLE IF NOT EXISTS comandos (id_cmd TEXT PRIMARY KEY)"
            )
            self.banco.execute("INSERT OR IGNORE INTO estado VALUES (1, 500.0)")

        self._sincronizar_ao_iniciar()

        self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"controlador_{id_no}")
        self.mqtt.on_connect = lambda cliente, *_args: cliente.subscribe(TOPICO_SENSOR, qos=1)
        self.mqtt.on_message = self._ao_receber_telemetria
        self.mqtt.connect("localhost", 1883, 30)
        self.mqtt.loop_start()

    def _sincronizar_ao_iniciar(self):
        for id_par in NOS:
            if id_par == self.id_no:
                continue
            try:
                estado_par = conectar_no(id_par, 0.5).consultar_estado()
                if estado_par["mandato"] > self.mandato and 0.0 <= estado_par["racao"] <= 500.0:
                    with self.trava, self.banco:
                        self.mandato = estado_par["mandato"]
                        self.lider_id = estado_par["lider_id"]
                        self.banco.execute(
                            "UPDATE estado SET racao=? WHERE id=1", (estado_par["racao"],)
                        )
            except Exception:
                pass

    def _contar_pares(self, acao):
        total = 0
        for id_par in NOS:
            if id_par != self.id_no:
                try:
                    total += bool(acao(conectar_no(id_par)))
                except Exception:
                    pass
        return total

    def _ler_racao(self):
        return self.banco.execute("SELECT racao FROM estado WHERE id=1").fetchone()[0]

    def _comando_ja_executado(self, id_comando):
        if not id_comando:
            return False
        linha = self.banco.execute(
            "SELECT 1 FROM comandos WHERE id_cmd=?", (id_comando,)
        ).fetchone()
        return linha is not None

    def consultar_estado(self):
        with self.trava:
            return {
                "id_no": self.id_no,
                "mandato": self.mandato,
                "lider_id": self.lider_id,
                "racao": self._ler_racao(),
            }

    def votar(self, mandato, candidato_id):
        with self.trava:
            if mandato <= self.mandato:
                return False
            self.mandato = mandato
            self.lider_id = candidato_id
            return True

    def sincronizar_estado(self, mandato, lider_id, operacao, valor, id_comando=None):
        with self.trava:
            if mandato < self.mandato:
                return False
            if self._comando_ja_executado(id_comando):
                return self._ler_racao() == valor

            if operacao == "lideranca":
                self.mandato = mandato
                self.lider_id = lider_id
                return True

            if operacao == "liberar_racao":
                esperado = min(500.0, round(self._ler_racao() + 100.0, 1))
                if valor != esperado:
                    return False
            elif operacao == "telemetria":
                if not (0.0 <= valor <= 500.0):
                    return False
            else:
                return False

            self.mandato = mandato
            self.lider_id = lider_id
            with self.banco:
                self.banco.execute("UPDATE estado SET racao=? WHERE id=1", (valor,))
                if id_comando:
                    self.banco.execute(
                        "INSERT OR IGNORE INTO comandos VALUES (?)", (id_comando,)
                    )
            return True

    def eleger_lider(self):
        novo_mandato = self.mandato + 1
        votos = 1 + self._contar_pares(lambda par: par.votar(novo_mandato, self.id_no))
        if votos < QUORUM:
            return False

        with self.trava:
            self.mandato = novo_mandato
            self.lider_id = self.id_no
            racao = self._ler_racao()

        self._contar_pares(
            lambda par: par.sincronizar_estado(
                novo_mandato, self.id_no, "lideranca", racao, None
            )
        )
        print(f"[CTRL {self.id_no}] Eleito lider (mandato {novo_mandato}, votos {votos}/3)")
        return True

    def _replicar_por_quorum(self, operacao, valor, id_comando=None):
        confirmacoes = 1 + self._contar_pares(
            lambda par: par.sincronizar_estado(
                self.mandato, self.id_no, operacao, valor, id_comando
            )
        )
        if confirmacoes < QUORUM:
            return False

        with self.trava, self.banco:
            self.banco.execute("UPDATE estado SET racao=? WHERE id=1", (valor,))
            if id_comando:
                self.banco.execute(
                    "INSERT OR IGNORE INTO comandos VALUES (?)", (id_comando,)
                )
        return True

    def liberar_racao(self, id_comando):
        if self.lider_id != self.id_no:
            return {"ok": False, "erro": "nao_lider"}

        with self.trava:
            if self._comando_ja_executado(id_comando):
                return {
                    "ok": True,
                    "racao": self._ler_racao(),
                    "lider": self.id_no,
                    "idempotente": True,
                }
            nova_racao = min(500.0, round(self._ler_racao() + 100.0, 1))

        if not self._replicar_por_quorum("liberar_racao", nova_racao, id_comando):
            return {"ok": False, "erro": "sem_quorum"}

        self.mqtt.publish(
            TOPICO_ATUADOR,
            json.dumps({"acao": "liberar_racao", "qtd": 100, "id_cmd": id_comando}),
            qos=2,
        )
        print(f"[CTRL {self.id_no}] Racao liberada -> {nova_racao}g ({id_comando[:8]})")
        return {
            "ok": True,
            "racao": nova_racao,
            "lider": self.id_no,
            "idempotente": False,
        }

    def _ao_receber_telemetria(self, cliente, dados, mensagem):  # 'dados' (userdata) é exigido pelo paho-mqtt, mesmo sem uso
        if self.lider_id != self.id_no:
            try:
                if (
                    self.lider_id is not None
                    and conectar_no(self.lider_id, 0.5).consultar_estado()
                ):
                    return
            except Exception:
                pass
            if not self.eleger_lider():
                return

        racao = float(json.loads(mensagem.payload.decode())["racao"])
        if self._replicar_por_quorum("telemetria", racao):
            print(f"[CTRL {self.id_no}] Telemetria replicada: {racao}g")


if __name__ == "__main__":
    id_no = int(sys.argv[1])
    servidor = Pyro5.api.Daemon(host="localhost", port=9090 + id_no)
    servidor.register(Controlador(id_no), f"controlador.{id_no}")
    print(f"[CTRL {id_no}] Ativo em :{9090 + id_no}")
    servidor.requestLoop()
