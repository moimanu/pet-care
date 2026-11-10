from __future__ import annotations

import json
import logging
from typing import Any, Iterable, Mapping, Optional
import uuid

from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import SanityError
from src.domain.interfaces import IBrokerClient, IControllerNode, IStateRepository
from src.domain.validators import TelemetrySanityValidator

logger = logging.getLogger(__name__)


class ControllerService:
    """Controller use case orchestrating telemetry processing, autonomous edge rules, and RPC."""

    TOPIC_RACAO_ATUADOR = "petcare/atuadores/dispenser"
    TOPIC_AGUA_ATUADOR = "petcare/atuadores/valvula"
    TOPIC_ALARME_ATUADOR = "petcare/atuadores/alarme"

    def __init__(
        self,
        node_id: int,
        repository: IStateRepository,
        broker: IBrokerClient,
        is_primary: bool = False,
        is_corrupt: bool = False,
    ) -> None:
        self.node_id = node_id
        self.repository = repository
        self.broker = broker
        self._is_primary = is_primary
        self.is_corrupt = is_corrupt

    def is_primary(self) -> bool:
        return self._is_primary

    def set_primary(self, primary: bool) -> None:
        self._is_primary = primary

    def get_state(self) -> PetState:
        current = self.repository.get_state()
        if current is None:
            current = PetState(
                racao_gramas=0.0,
                agua_ml=0.0,
                areia_usos=0,
                status_fuga="DENTRO",
                alarme_ativo=False,
                atualizado_em="",
            )

        if self.is_corrupt:
            # Inject Byzantine faulty state for testing consensus robustness
            return PetState(
                racao_gramas=current.racao_gramas,
                agua_ml=current.agua_ml,
                areia_usos=current.areia_usos,
                status_fuga="FORA",
                alarme_ativo=True,
                atualizado_em=current.atualizado_em,
            )

        return current

    def handle_telemetry(self, topic: str, payload: Mapping[str, Any]) -> None:
        sensor = topic.strip().rstrip("/").split("/")[-1]
        try:
            TelemetrySanityValidator.validate(sensor, payload)
        except SanityError as exc:
            logger.warning("Rejected anomalous telemetry on %s: %s", topic, exc)
            return

        current = self.get_state()
        timestamp = str(payload.get("timestamp", ""))

        racao_gramas = current.racao_gramas
        agua_ml = current.agua_ml
        areia_usos = current.areia_usos
        status_fuga = current.status_fuga
        alarme_ativo = current.alarme_ativo

        if sensor == "racao":
            racao_gramas = float(payload["nivel_gramas"])
            if racao_gramas < 50.0 and self.is_primary():
                self.broker.publish(
                    self.TOPIC_RACAO_ATUADOR,
                    {
                        "acao": "LIBERAR_RACAO",
                        "quantidade_gramas": 50.0,
                        "command_id": str(uuid.uuid4()),
                    },
                    qos=2,
                )
        elif sensor == "agua":
            agua_ml = float(payload["nivel_ml"])
            if agua_ml < 150.0 and self.is_primary():
                self.broker.publish(
                    self.TOPIC_AGUA_ATUADOR,
                    {
                        "acao": "REPOR_AGUA",
                        "quantidade_ml": 200.0,
                        "command_id": str(uuid.uuid4()),
                    },
                    qos=2,
                )
        elif sensor == "areia":
            areia_usos = int(payload["usos_acumulados"])
        elif sensor == "fuga":
            status_fuga = str(payload["perimetro"]).strip().upper()
            if status_fuga == "FORA" and not alarme_ativo:
                alarme_ativo = True
                if self.is_primary():
                    self.broker.publish(
                        self.TOPIC_ALARME_ATUADOR,
                        {
                            "acao": "DISPARAR_ALARME",
                            "motivo": "FUGA_DETECTADA",
                            "command_id": str(uuid.uuid4()),
                        },
                        qos=2,
                    )

        new_state = PetState(
            racao_gramas=racao_gramas,
            agua_ml=agua_ml,
            areia_usos=areia_usos,
            status_fuga=status_fuga,
            alarme_ativo=alarme_ativo,
            atualizado_em=timestamp,
        )
        self.repository.save_state(new_state)

    def execute_command(self, command: Command) -> CommandResult:
        """Executes a command idempotently with event recording and actuation."""
        if self.repository.has_event(command.command_id):
            return CommandResult(
                sucesso=True,
                mensagem=f"Comando idempotente ja processado: {command.command_id}",
                estado=self.get_state(),
            )

        current = self.get_state()
        racao = current.racao_gramas
        agua = current.agua_ml
        areia = current.areia_usos
        status_fuga = current.status_fuga
        alarme = current.alarme_ativo
        timestamp = command.timestamp or current.atualizado_em

        acao = command.acao.strip().upper()

        if acao == "LIBERAR_RACAO":
            qty = float(command.parametros.get("quantidade_gramas", 50.0))
            racao = min(500.0, racao + qty)
            if self.is_primary():
                self.broker.publish(
                    self.TOPIC_RACAO_ATUADOR,
                    {
                        "acao": "LIBERAR_RACAO",
                        "quantidade_gramas": qty,
                        "command_id": command.command_id,
                    },
                    qos=2,
                )
        elif acao == "REPOR_AGUA":
            qty = float(command.parametros.get("quantidade_ml", 200.0))
            agua = min(1000.0, agua + qty)
            if self.is_primary():
                self.broker.publish(
                    self.TOPIC_AGUA_ATUADOR,
                    {
                        "acao": "REPOR_AGUA",
                        "quantidade_ml": qty,
                        "command_id": command.command_id,
                    },
                    qos=2,
                )
        elif acao == "RESETAR_AREIA":
            areia = 0
        elif acao == "SILENCIAR_ALARME":
            alarme = False
            if self.is_primary():
                self.broker.publish(
                    self.TOPIC_ALARME_ATUADOR,
                    {
                        "acao": "SILENCIAR_ALARME",
                        "command_id": command.command_id,
                    },
                    qos=2,
                )

        new_state = PetState(
            racao_gramas=racao,
            agua_ml=agua,
            areia_usos=areia,
            status_fuga=status_fuga,
            alarme_ativo=alarme,
            atualizado_em=timestamp,
        )
        self.repository.save_state(new_state)
        self.repository.record_event(
            command_id=command.command_id,
            tipo_evento=acao,
            payload=json.dumps(command.parametros),
            registrado_em=timestamp,
        )

        return CommandResult(
            sucesso=True,
            mensagem=f"Acao {acao} executada com sucesso",
            estado=new_state,
        )

    def sync_boot_state(self, peers: Iterable[Any]) -> None:
        """Clones canonical state from online peers if local repository is uninitialized."""
        if self.repository.get_state() is not None:
            return

        for peer in peers:
            try:
                peer_state = peer.get_state()
                if peer_state is not None:
                    self.repository.save_state(peer_state)
                    logger.info("Node %s synchronized boot state from peer", self.node_id)
                    break
            except Exception as exc:
                logger.warning("Node %s failed to sync state from peer: %s", self.node_id, exc)
