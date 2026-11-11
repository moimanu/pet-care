from __future__ import annotations

from dataclasses import replace
import json
import logging
from typing import Any, Iterable, Mapping, Optional
import uuid

from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import NodeUnreachableError, SanityError
from src.domain.interfaces import BrokerClient, ControllerNode, StateRepository
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
        repository: StateRepository,
        broker: BrokerClient,
        is_primary: bool = False,
        is_corrupt: bool = False,
        is_offline: bool = False,
    ) -> None:
        self.node_id = node_id
        self.repository = repository
        self.broker = broker
        self._is_primary = is_primary
        self.is_corrupt = is_corrupt
        self._is_offline = is_offline

    def is_primary(self) -> bool:
        return self._is_primary

    def set_primary(self, primary: bool) -> None:
        self._is_primary = primary

    def is_corrupted(self) -> bool:
        return self.is_corrupt

    def set_corrupt(self, corrupt: bool) -> None:
        self.is_corrupt = corrupt
        logger.warning(
            "Nó %d: modo bizantino (corrupção de resposta) alterado para %s",
            self.node_id,
            corrupt,
        )

    def is_offline(self) -> bool:
        return self._is_offline

    def set_offline(self, offline: bool) -> None:
        self._is_offline = offline
        status_label = "OFFLINE (crash-stop)" if offline else "ONLINE"
        logger.warning("Nó %d: status de conectividade alterado para %s", self.node_id, status_label)

    def get_info(self) -> dict[str, Any]:
        return {
            "id": self.node_id,
            "primary": self.is_primary(),
            "corrupt": self.is_corrupt,
            "offline": self._is_offline,
        }

    def get_state(self) -> PetState:
        if self._is_offline:
            raise NodeUnreachableError(f"Nó {self.node_id} está OFFLINE (crash-stop simulado)")
        state = self.repository.get_state() or PetState()
        return replace(state, status_fuga="FORA", alarme_ativo=True) if self.is_corrupt else state

    def _actuate(self, topic: str, acao: str, command_id: Optional[str] = None, **extra: Any) -> None:
        if self._is_primary:
            payload = {"acao": acao, "command_id": command_id or str(uuid.uuid4()), **extra}
            self.broker.publish(topic, payload, qos=2)

    def handle_telemetry(self, topic: str, payload: Mapping[str, Any]) -> None:
        if self._is_offline:
            return

        sensor = topic.strip().rstrip("/").split("/")[-1]
        try:
            TelemetrySanityValidator.validate(sensor, payload)
        except SanityError as exc:
            logger.warning("Rejected anomalous telemetry on %s: %s", topic, exc)
            return

        current = self.get_state()
        changes: dict[str, Any] = {"atualizado_em": str(payload.get("timestamp", ""))}

        if sensor == "racao":
            changes["racao_gramas"] = val = float(payload["nivel_gramas"])
            if val < 50.0:
                self._actuate(self.TOPIC_RACAO_ATUADOR, "LIBERAR_RACAO", quantidade_gramas=50.0)
        elif sensor == "agua":
            changes["agua_ml"] = val = float(payload["nivel_ml"])
            if val < 150.0:
                self._actuate(self.TOPIC_AGUA_ATUADOR, "REPOR_AGUA", quantidade_ml=200.0)
        elif sensor == "areia":
            changes["areia_usos"] = int(payload["usos_acumulados"])
        elif sensor == "fuga":
            changes["status_fuga"] = status = str(payload["perimetro"]).strip().upper()
            if status == "FORA" and not current.alarme_ativo:
                changes["alarme_ativo"] = True
                self._actuate(self.TOPIC_ALARME_ATUADOR, "DISPARAR_ALARME", motivo="FUGA_DETECTADA")

        self.repository.save_state(replace(current, **changes))

    def execute_command(self, command: Command) -> CommandResult:
        """Executes a command idempotently with event recording and actuation."""
        if self._is_offline:
            raise NodeUnreachableError(f"Nó {self.node_id} está OFFLINE (crash-stop simulado)")

        if self.repository.has_event(command.command_id):
            logger.warning(
                "Nó %d: Comando duplicado ignorado por idempotência (command_id=%s)",
                self.node_id,
                command.command_id,
            )
            return CommandResult(
                sucesso=True,
                mensagem=f"Comando idempotente ja processado: {command.command_id}",
                estado=self.get_state(),
            )

        current = self.get_state()
        acao = command.acao.strip().upper()
        timestamp = command.timestamp or current.atualizado_em
        changes: dict[str, Any] = {"atualizado_em": timestamp}

        if acao == "LIBERAR_RACAO":
            qty = float(command.parametros.get("quantidade_gramas", 50.0))
            changes["racao_gramas"] = min(TelemetrySanityValidator.MAX_RACAO_GRAMAS, current.racao_gramas + qty)
            self._actuate(self.TOPIC_RACAO_ATUADOR, acao, command.command_id, quantidade_gramas=qty)
        elif acao == "REPOR_AGUA":
            qty = float(command.parametros.get("quantidade_ml", 200.0))
            changes["agua_ml"] = min(TelemetrySanityValidator.MAX_AGUA_ML, current.agua_ml + qty)
            self._actuate(self.TOPIC_AGUA_ATUADOR, acao, command.command_id, quantidade_ml=qty)
        elif acao == "RESETAR_AREIA":
            changes["areia_usos"] = 0
        elif acao == "SILENCIAR_ALARME":
            changes["alarme_ativo"] = False
            self._actuate(self.TOPIC_ALARME_ATUADOR, acao, command.command_id)

        new_state = replace(current, **changes)
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
