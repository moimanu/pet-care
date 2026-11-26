import json, time
import paho.mqtt.client as mqtt

BROKER_HOST = "localhost"
BROKER_PORTA = 1883
TOPICO_SENSOR = "petcare/sensor"
INTERVALO_SEGUNDOS = 5

cliente_mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "petcare_sensor")
cliente_mqtt.connect(BROKER_HOST, BROKER_PORTA, 30)
cliente_mqtt.loop_start()

racao = 500.0
print(f"[SENSOR] Publicando nivel de racao em {TOPICO_SENSOR}...")

try:
    while True:
        racao = max(0.0, racao - 20.0)
        cliente_mqtt.publish(TOPICO_SENSOR, json.dumps({"racao": racao}), qos=1)
        print(f"[SENSOR] Nivel enviado: {racao}g")
        time.sleep(INTERVALO_SEGUNDOS)
except KeyboardInterrupt:
    cliente_mqtt.loop_stop()
