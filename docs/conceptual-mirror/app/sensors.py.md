# app/sensors.py

Processo independente que simula a borda IoT: publica telemetria periódica/manual em `petcare/sensores/<sensor>` (QoS 1) e executa comandos de atuadores recebidos em `petcare/atuadores/#` (QoS 2) com deduplicação temporal.

```python
"""Processo de borda IoT: publica telemetria MQTT (QoS 1) e executa atuadores (QoS 2)."""  # Docstring descrevendo o papel do processo simulador IoT

from contextlib import suppress  # Ignora erros de validação ao atualizar o estado espelho em telemetrias manuais
import json  # Decodifica payloads recebidos via MQTT
import random  # Gera variações aleatórias de consumo no ciclo automático
import sys  # Usado para encerrar o processo no handler de shutdown
import time  # Fornece relógio monotônico para deduplicação e sleep do loop principal

from app.config import MQTT_PORT, TOPIC_ACTUATORS, TOPIC_SENSORS, TOPIC_SIM_CONTROL  # Porta MQTT e tópicos do protocolo
from app.domain import (  # Importa constantes, enums e funções puras do domínio
    Action, FOOD_STEP_GRAMS, MAX_FOOD_GRAMS, MAX_WATER_ML,  # Ações e limites físicos dos reservatórios
    PetState, Sensor, WATER_STEP_ML, apply_telemetry,  # Estado local simulado, enum Sensor e transição de telemetria
)  # Fecha importação de app.domain
from app.event_bus import emit_event  # Registra eventos do simulador IoT no barramento de logs
from app.infra import connect_mqtt, install_shutdown_handler, publish_json  # Utilitários de conexão MQTT, publicação e sinais

DEDUP_WINDOW_SECONDS = 2.0  # Janela temporal (2s) para descartar comandos duplicados pelo command_id
AUTO_LOOP_INTERVAL_SECONDS = 4.0  # Intervalo (4s) entre publicações automáticas de consumo


class IoTDeviceNode:  # Simula os sensores e atuadores físicos conectados ao broker Mosquitto
    """Simula os sensores e atuadores fisicos conectados ao broker MQTT."""  # Docstring da classe IoTDeviceNode

    def __init__(self) -> None:  # Inicializa estado simulado, cache de deduplicação e conexão MQTT
        self.running = True  # Flag que mantém o loop principal em execução
        self.auto_loop = False  # Controla se o consumo automático periódico está ativo
        self.state = PetState()  # Estado físico simulado mantido em memória pela borda IoT
        self._seen_commands: dict[str, float] = {}  # Cache {command_id: timestamp} para deduplicação de atuadores
        self.mqtt = connect_mqtt("petcare_iot_edge", on_connect=self._on_connect, on_message=self._on_message)  # Conecta ao broker MQTT com callbacks
        emit_event("sensors", f"Dispositivos IoT conectados ao broker MQTT :{MQTT_PORT}")  # Registra que a borda IoT está online

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:  # Callback executado ao conectar/reconectar no broker MQTT
        client.subscribe(f"{TOPIC_ACTUATORS}/#", qos=2)  # Assina comandos de atuadores com QoS 2 (exactly-once no transporte)
        client.subscribe(TOPIC_SIM_CONTROL, qos=1)  # Assina tópico de controle do simulador (auto/manual) com QoS 1
        emit_event("sensors", f"Inscrito em {TOPIC_ACTUATORS}/# (QoS 2)")  # Loga confirmação da inscrição

    def _is_duplicate(self, command_id: str) -> bool:  # Verifica se um command_id já foi executado recentemente
        """Descarta comandos repetidos dentro da janela de deduplicacao."""  # Docstring explicando o filtro temporal
        now = time.monotonic()  # Obtém instante monotônico atual (imune a ajustes de relógio)
        self._seen_commands = {cid: ts for cid, ts in self._seen_commands.items() if now - ts < DEDUP_WINDOW_SECONDS}  # Expurga entradas expiradas (> 2s)
        if not command_id:  # Comandos sem command_id não são deduplicados
            return False  # Permite execução
        if command_id in self._seen_commands:  # Se o ID ainda está dentro da janela de 2s
            return True  # Indica que é duplicado e deve ser descartado
        self._seen_commands[command_id] = now  # Registra o novo command_id com o instante atual
        return False  # Primeira ocorrência: permite execução

    def _on_message(self, client, userdata, msg) -> None:  # Processa mensagens recebidas de atuadores ou de controle do simulador
        raw = msg.payload.decode("utf-8", errors="replace")  # Decodifica o payload binário MQTT para string UTF-8
        try:  # Tenta fazer parse do JSON recebido
            data = json.loads(raw)  # Converte string JSON para dicionário Python
        except json.JSONDecodeError:  # Se o payload não for JSON válido
            emit_event("sensors", f"Payload invalido recebido em {msg.topic}: {raw}", "ERROR")  # Loga erro e descarta a mensagem
            return  # Encerra processamento desta mensagem

        if msg.topic == TOPIC_SIM_CONTROL:  # Verifica se a mensagem é de controle do simulador (vinda do painel web)
            self._handle_control(data)  # Delega ao tratador de controle
            return  # Encerra aqui sem tratar como atuador

        command_id = str(data.get("command_id", ""))  # Extrai o command_id enviado pelo controlador líder
        if self._is_duplicate(command_id):  # Checa deduplicação antes de atuar fisicamente
            emit_event("sensors", f"Atuador filtrou comando duplicado {command_id[:8]}", "WARN")  # Loga descarte de duplicata
            return  # Não executa o atuador novamente

        action = data.get("acao")  # Lê o nome da ação enviada ao atuador
        match action:  # Atualiza o estado físico simulado de acordo com a ação
            case Action.LIBERAR_RACAO:  # Se for liberação de ração
                self.state.racao_gramas = min(MAX_FOOD_GRAMS, self.state.racao_gramas + float(data.get("quantidade_gramas", FOOD_STEP_GRAMS)))  # Incrementa ração até o limite máximo
            case Action.REPOR_AGUA:  # Se for reposição de água
                self.state.agua_ml = min(MAX_WATER_ML, self.state.agua_ml + float(data.get("quantidade_ml", WATER_STEP_ML)))  # Incrementa água até o limite máximo
            case Action.DISPARAR_ALARME:  # Se for disparo de alarme
                self.state.alarme_ativo = True  # Liga o alarme simulado
            case Action.SILENCIAR_ALARME:  # Se for silenciamento de alarme
                self.state.alarme_ativo = False  # Desliga o alarme simulado
        emit_event("sensors", f"Atuador executou {action} ({msg.topic})")  # Loga a execução física simulada

    def _handle_control(self, data: dict) -> None:  # Trata comandos de controle enviados pelo painel via TOPIC_SIM_CONTROL
        match data.get("kind"):  # Identifica o tipo de controle (`auto` ou `telemetry`)
            case "auto":  # Liga ou pausa o ciclo automático de consumo
                self.auto_loop = bool(data.get("enabled"))  # Atualiza a flag auto_loop
                emit_event("sensors", f"Ciclo automatico {'ativado' if self.auto_loop else 'pausado'}")  # Loga mudança de modo
            case "telemetry":  # Dispara publicação manual de uma telemetria específica
                sensor, payload = data["sensor"], data["payload"]  # Extrai nome do sensor e payload a publicar
                topic = self._publish_telemetry(sensor, payload)  # Publica no tópico petcare/sensores/<sensor>
                emit_event("sensors", f"Telemetria publicada em {topic}: {payload}")  # Loga a publicação manual
                with suppress(ValueError, TypeError):  # Ignora erro se o usuário enviou payload inválido de propósito para teste
                    self.state, _ = apply_telemetry(self.state, sensor, payload)  # Mantém o estado local do simulador coerente com a leitura

    def _publish_telemetry(self, sensor: str, payload: dict) -> str:  # Publica leitura de um sensor no broker MQTT com QoS 1
        topic = f"{TOPIC_SENSORS}/{sensor}"  # Monta o tópico específico do sensor
        publish_json(self.mqtt, topic, payload, qos=1)  # Publica JSON no broker com QoS 1 (at-least-once)
        return topic  # Retorna o tópico completo publicado

    def step_auto(self) -> None:  # Executa um passo de consumo simulado e publica ração e água via MQTT
        self.state.racao_gramas = max(0.0, round(self.state.racao_gramas - random.uniform(10.0, 35.0), 1))  # Reduz ração aleatoriamente (10g a 35g) sem ficar negativa
        self.state.agua_ml = max(0.0, round(self.state.agua_ml - random.uniform(20.0, 60.0), 1))  # Reduz água aleatoriamente (20ml a 60ml) sem ficar negativa
        for sensor, payload in (  # Percorre os dois sensores atualizados no passo automático
            (Sensor.RACAO, {"nivel_gramas": self.state.racao_gramas}),  # Monta telemetria de ração
            (Sensor.AGUA, {"nivel_ml": self.state.agua_ml}),  # Monta telemetria de água
        ):  # Fecha tupla de sensores
            topic = self._publish_telemetry(sensor, payload)  # Publica leitura no MQTT
            emit_event("sensors", f"[AUTO] {topic}: {payload}")  # Registra publicação automática no log


def main() -> None:  # Ponto de entrada do processo `python -m app.sensors`
    node = IoTDeviceNode()  # Instancia o nó simulador IoT e conecta ao MQTT

    def shutdown(_signum: int) -> None:  # Callback de encerramento limpo para SIGTERM/SIGINT
        node.running = False  # Interrompe o loop principal
        node.mqtt.loop_stop()  # Para a thread de rede do cliente MQTT
        emit_event("sensors", "Dispositivos IoT encerrados", "WARN")  # Loga encerramento do processo
        sys.exit(0)  # Finaliza o processo com código 0

    install_shutdown_handler(shutdown)  # Registra o handler para SIGTERM e SIGINT
    while node.running:  # Loop contínuo enquanto o processo estiver ativo
        if node.auto_loop:  # Se o modo automático estiver habilitado
            node.step_auto()  # Simula consumo e publica telemetrias
        time.sleep(AUTO_LOOP_INTERVAL_SECONDS)  # Aguarda 4 segundos antes da próxima iteração


if __name__ == "__main__":  # Executa main() apenas quando chamado diretamente como módulo/script
    main()  # Inicia o processo dos dispositivos IoT
```
