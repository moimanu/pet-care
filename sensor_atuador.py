import json
import threading
import time
import paho.mqtt.client as mqtt

TOPICO_SENSOR = "petcare/sensor"
TOPICO_ATUADOR = "petcare/atuador"

racao = 500
trava = threading.Lock()  # Protege 'racao' entre a thread principal (sensor) e a thread do MQTT (atuador)
comandos_vistos = set()


def ao_conectar(cliente, dados, flags, codigo, propriedades):
    cliente.subscribe(TOPICO_ATUADOR, qos=2)


def ao_receber_comando(cliente, dados, mensagem):  # 'dados' (userdata) é exigido pelo paho-mqtt
    global racao
    comando = json.loads(mensagem.payload.decode())
    id_comando = comando.get("id_cmd")
    if id_comando in comandos_vistos:
        print(f"[ATUADOR] Comando duplicado ignorado ({id_comando})")
        return
    if id_comando:
        comandos_vistos.add(id_comando)
    if comando.get("acao") == "liberar_racao":
        with trava:  # Evita condicao de corrida caso o loop do sensor esteja descontando racao simultaneamente
            racao = min(500, racao + 100)
            nivel_atual = racao
        print(f"[ATUADOR] Liberando 100g de racao! Nivel atual: {nivel_atual}g")


cliente_mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "petcare_sensor_atuador")
cliente_mqtt.on_connect = ao_conectar
cliente_mqtt.on_message = ao_receber_comando
cliente_mqtt.connect("localhost", 1883, 30)
cliente_mqtt.loop_start()

print(f"[ATUADOR] Aguardando comandos em {TOPICO_ATUADOR}...")
print(f"[SENSOR] Publicando nivel de racao em {TOPICO_SENSOR}...")

try:
    while True:
        with trava:  # Evita condicao de corrida caso o callback do atuador esteja somando racao simultaneamente
            racao = max(0, racao - 20)
            nivel_atual = racao
        cliente_mqtt.publish(TOPICO_SENSOR, json.dumps({"racao": nivel_atual}), qos=1)
        print(f"[SENSOR] Nivel enviado: {nivel_atual}g")
        time.sleep(5)
except KeyboardInterrupt:
    cliente_mqtt.loop_stop()
    cliente_mqtt.disconnect()
