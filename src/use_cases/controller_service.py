from __future__ import annotations

from dataclasses import replace
import json
import logging
from typing import Any, Iterable, Mapping, Optional
import uuid

from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import NodeUnreachableError, SanityError
from src.domain.interfaces import BrokerClient, StateRepository
from src.domain.validators import TelemetrySanityValidator

logger = logging.getLogger(__name__)


def _is_healthy(node: Any) -> bool:
    try:
        return not getattr(node, "is_offline", lambda: False)() and not getattr(node, "is_corrupted", lambda: False)()
    except Exception:
        return False


class ControllerService:
    """Controller use case orchestrating telemetry processing, P2P state replication, and failover."""

    TOPIC_RACAO_ATUADOR = "petcare/atuadores/dispenser"
    TOPIC_AGUA_ATUADOR = "petcare/atuadores/valvula"
    TOPIC_ALARME_ATUADOR = "petcare/atuadores/alarme"

    def __init__(
        self, node_id: int, repository: StateRepository, broker: BrokerClient,
        is_primary: bool = False, is_corrupt: bool = False, is_offline: bool = False,
        peers: Optional[Iterable[Any]] = None,
    ) -> None:
        self.node_id, self.repository, self.broker = node_id, repository, broker
        self._is_primary, self.is_corrupt, self._is_offline = is_primary, is_corrupt, is_offline
        self.peers: list[Any] = list(peers or [])

    def is_primary(self) -> bool:
        return self._is_primary

    def set_primary(self, primary: bool) -> None:
        self._is_primary = primary
        if primary:
            for peer in self.peers:
                try:
                    if peer.is_primary():
                        peer.set_primary(False)
                except Exception:
                    pass

    def is_corrupted(self) -> bool:
        return self.is_corrupt

    def set_corrupt(self, corrupt: bool) -> None:
        self.is_corrupt = corrupt
        self._handle_fault_transition(corrupt)

    def is_offline(self) -> bool:
        return self._is_offline

    def set_offline(self, offline: bool) -> None:
        self._is_offline = offline
        self._handle_fault_transition(offline)

    def _handle_fault_transition(self, fault_active: bool) -> None:
        if fault_active and self._is_primary:
            self._is_primary = False
            for peer in self.peers:
                if _is_healthy(peer):
                    try:
                        peer.set_primary(True)
                        break
                    except Exception:
                        pass
        elif not fault_active:
            self.sync_with_peers(force_state_clone=True)

    def get_info(self) -> dict[str, Any]:
        return {"id": self.node_id, "primary": self._is_primary, "corrupt": self.is_corrupt, "offline": self._is_offline}

    def _require_online(self) -> None:
        if self._is_offline:
            raise NodeUnreachableError(f"Nó {self.node_id} está OFFLINE")

    def _internal_state(self) -> PetState:
        return self.repository.get_state() or PetState()

    def get_state(self) -> PetState:
        self._require_online()
        state = self._internal_state()
        return replace(state, status_fuga="FORA", alarme_ativo=True) if self.is_corrupt else state

    def sync_state(
        self, state: PetState, command_id: Optional[str] = None, tipo_evento: str = "", payload: str = ""
    ) -> bool:
        self._require_online()
        self.repository.save_state(state)
        if command_id and not self.repository.has_event(command_id):
            self.repository.record_event(command_id, tipo_evento or "SYNC_STATE", payload or "{}", state.atualizado_em)
        return True

    def _replicate_to_peers(
        self, state: PetState, command_id: Optional[str] = None, tipo_evento: str = "", payload: str = ""
    ) -> None:
        if not (self._is_primary and not self.is_corrupt and not self._is_offline):
            return
        for peer in self.peers:
            try:
                if not getattr(peer, "is_offline", lambda: False)() and hasattr(peer, "sync_state"):
                    peer.sync_state(state, command_id=command_id, tipo_evento=tipo_evento, payload=payload)
            except Exception as exc:
                logger.warning("Nó %d: falha ao replicar estado: %s", self.node_id, exc)

    def _ensure_active_leader(self) -> bool:
        if self._is_primary:
            if self.is_corrupt:
                self.set_corrupt(True)
                return False
            return True
        if not self.peers or self.is_corrupt:
            return False

        healthy_ids: list[int] = []
        for peer in self.peers:
            if not _is_healthy(peer):
                continue
            try:
                if peer.is_primary():
                    return False
                if isinstance(info := getattr(peer, "get_info", lambda: {})(), Mapping) and "id" in info:
                    healthy_ids.append(int(info["id"]))
            except Exception:
                continue

        if all(self.node_id < pid for pid in healthy_ids):
            self.set_primary(True)
            return True
        return False

    def _actuate(self, topic: str, acao: str, command_id: Optional[str] = None, **extra: Any) -> None:
        if self._is_primary and not self.is_corrupt:
            self.broker.publish(topic, {"acao": acao, "command_id": command_id or str(uuid.uuid4()), **extra}, qos=2)

    def handle_telemetry(self, topic: str, payload: Mapping[str, Any]) -> None:
        if self._is_offline:
            return
        sensor = topic.strip().rstrip("/").split("/")[-1]
        try:
            TelemetrySanityValidator.validate(sensor, payload)
        except SanityError as exc:
            logger.warning("Telemetria inválida em %s: %s", topic, exc)
            return

        if not self._ensure_active_leader():
            return

        current = self._internal_state()
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

        new_state = replace(current, **changes)
        self.repository.save_state(new_state)
        self._replicate_to_peers(new_state)

    def execute_command(self, command: Command) -> CommandResult:
        self._require_online()
        if self.repository.has_event(command.command_id):
            return CommandResult(True, f"Comando idempotente ja processado: {command.command_id}", self.get_state())

        current = self._internal_state()
        acao = command.acao.strip().upper()
        ts = command.timestamp or current.atualizado_em
        changes: dict[str, Any] = {"atualizado_em": ts}

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
        payload_json = json.dumps(command.parametros)
        self.repository.save_state(new_state)
        self.repository.record_event(command.command_id, acao, payload_json, ts)
        self._replicate_to_peers(new_state, command.command_id, acao, payload_json)
        return CommandResult(True, f"Acao {acao} executada com sucesso", new_state)

    def sync_with_peers(self, peers: Optional[Iterable[Any]] = None, force_state_clone: bool = False) -> None:
        target_peers = list(peers) if peers is not None else self.peers
        primary_peer = next(
            (p for p in target_peers if _is_healthy(p) and getattr(p, "is_primary", lambda: False)()), None
        )
        if primary_peer and self._is_primary:
            self._is_primary = False
        if not force_state_clone and self.repository.get_state() is not None:
            return
        for peer in ([primary_peer] if primary_peer else []) + [p for p in target_peers if p is not primary_peer]:
            if _is_healthy(peer):
                try:
                    if (peer_state := peer.get_state()) is not None:
                        self.repository.save_state(peer_state)
                        break
                except Exception:
                    continue

    def sync_boot_state(self, peers: Optional[Iterable[Any]] = None) -> None:
        if peers is not None and not self.peers:
            self.peers = list(peers)
        self.sync_with_peers(peers=peers, force_state_clone=False)
