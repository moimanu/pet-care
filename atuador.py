import json
import paho.mqtt.client as mqtt

BROKER_HOST = "localhost"
BROKER_PORTA = 1883
TOPICO_ATUADOR = "petcare/atuador"

comandos_vistos = set()


def ao_receber_comando(cliente, dados, mensagem):  # 'dados' (userdata) é exigido pelo paho-mqtt, mesmo sem uso
    comando = json.loads(mensagem.payload.decode())
    id_comando = comando.get("id_cmd")
    if id_comando and id_comando in comandos_vistos:
        print(f"[ATUADOR] Comando duplicado ignorado ({id_comando[:8]})")
        return
    if id_comando:
        comandos_vistos.add(id_comando)
    if comando.get("acao") == "liberar_racao":
        print(f"[ATUADOR] Liberando {comando.get('qtd', 100)}g de racao!")


cliente_mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "petcare_atuador")
cliente_mqtt.on_connect = lambda cliente, *_args: cliente.subscribe(TOPICO_ATUADOR, qos=2)
cliente_mqtt.on_message = ao_receber_comando
cliente_mqtt.connect(BROKER_HOST, BROKER_PORTA, 30)

print(f"[ATUADOR] Aguardando comandos em {TOPICO_ATUADOR}...")
try:
    cliente_mqtt.loop_forever()
except KeyboardInterrupt:
    pass
