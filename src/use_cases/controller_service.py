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
    """Controller use case orchestrating telemetry processing, P2P state replication, and failover."""

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
        peers: Optional[Iterable[Any]] = None,
    ) -> None:
        self.node_id = node_id
        self.repository = repository
        self.broker = broker
        self._is_primary = is_primary
        self.is_corrupt = is_corrupt
        self._is_offline = is_offline
        self.peers: list[Any] = list(peers or [])

    def is_primary(self) -> bool:
        return self._is_primary

    def set_primary(self, primary: bool) -> None:
        was_primary = self._is_primary
        self._is_primary = primary
        if primary:
            if not was_primary:
                logger.info("Nó %d assumiu a liderança primária do cluster.", self.node_id)
            self._demote_peer_leaders()

    def is_corrupted(self) -> bool:
        return self.is_corrupt

    def set_corrupt(self, corrupt: bool) -> None:
        self.is_corrupt = corrupt
        logger.warning(
            "Nó %d: modo bizantino (corrupção de resposta) alterado para %s",
            self.node_id,
            corrupt,
        )
        if corrupt and self._is_primary:
            logger.warning(
                "[SEGURANÇA] Líder Nó %d entrou em estado bizantino. Destituindo liderança e promovendo réplica íntegra.",
                self.node_id,
            )
            self._is_primary = False
            self._promote_healthy_peer()
        elif not corrupt:
            self.sync_with_peers(force_state_clone=True)

    def is_offline(self) -> bool:
        return self._is_offline

    def set_offline(self, offline: bool) -> None:
        self._is_offline = offline
        status_label = "OFFLINE (crash-stop)" if offline else "ONLINE"
        logger.warning("Nó %d: status de conectividade alterado para %s", self.node_id, status_label)

        if offline and self._is_primary:
            logger.warning(
                "Líder Nó %d entrou em crash-stop. Acionando failover P2P para réplica saudável.",
                self.node_id,
            )
            self._is_primary = False
            self._promote_healthy_peer()
        elif not offline:
            self.sync_with_peers(force_state_clone=True)

    def get_info(self) -> dict[str, Any]:
        return {
            "id": self.node_id,
            "primary": self.is_primary(),
            "corrupt": self.is_corrupt,
            "offline": self._is_offline,
        }

    def _get_internal_state(self) -> PetState:
        """Returns the true persisted state without injecting simulated Byzantine response faults."""
        return self.repository.get_state() or PetState()

    def get_state(self) -> PetState:
        if self._is_offline:
            raise NodeUnreachableError(f"Nó {self.node_id} está OFFLINE (crash-stop simulado)")
        state = self._get_internal_state()
        return replace(state, status_fuga="FORA", alarme_ativo=True) if self.is_corrupt else state

    def sync_state(
        self,
        state: PetState,
        command_id: Optional[str] = None,
        tipo_evento: str = "",
        payload: str = "",
    ) -> bool:
        """Receives and persists canonical state snapshot replicated by the Primary Controller."""
        if self._is_offline:
            raise NodeUnreachableError(f"Nó {self.node_id} está OFFLINE (crash-stop simulado)")

        self.repository.save_state(state)
        if command_id and not self.repository.has_event(command_id):
            self.repository.record_event(
                command_id=command_id,
                tipo_evento=tipo_evento or "SYNC_STATE",
                payload=payload or "{}",
                registrado_em=state.atualizado_em,
            )
            logger.info(
                "Nó %d sincronizou estado canônico do Líder (evento=%s, command_id=%s)",
                self.node_id,
                tipo_evento or "SYNC_STATE",
                command_id,
            )
        else:
            logger.debug("Nó %d sincronizou estado canônico de telemetria do Líder", self.node_id)
        return True

    def _replicate_state_to_peers(
        self,
        state: PetState,
        command_id: Optional[str] = None,
        tipo_evento: str = "",
        payload: str = "",
    ) -> None:
        """Pushes canonical state snapshot from Leader to all healthy peer replicas."""
        if not self._is_primary or self.is_corrupt or self._is_offline:
            return

        for peer in self.peers:
            try:
                if getattr(peer, "is_offline", lambda: False)():
                    continue
                sync_fn = getattr(peer, "sync_state", None)
                if callable(sync_fn):
                    sync_fn(
                        state,
                        command_id=command_id,
                        tipo_evento=tipo_evento,
                        payload=payload,
                    )
            except Exception as exc:
                logger.warning("Nó %d: falha ao replicar estado canônico para par: %s", self.node_id, exc)

    def _demote_peer_leaders(self) -> None:
        """Ensures single-leader invariant by demoting any other peer currently marked as primary."""
        for peer in self.peers:
            try:
                if peer.is_primary():
                    peer.set_primary(False)
            except Exception:
                continue

    def _is_peer_healthy(self, peer: Any) -> bool:
        try:
            is_off = getattr(peer, "is_offline", lambda: False)()
            is_corr = getattr(peer, "is_corrupted", lambda: False)()
            return not is_off and not is_corr
        except Exception:
            return False

    def _promote_healthy_peer(self) -> bool:
        """Promotes the first reachable, online, and uncorrupted peer to primary leader."""
        for peer in self.peers:
            try:
                if self._is_peer_healthy(peer):
                    peer.set_primary(True)
                    return True
            except Exception as exc:
                logger.warning("Falha ao promover par durante failover P2P: %s", exc)
        return False

    def _ensure_active_leader_on_telemetry(self) -> bool:
        """Returns True if this node is the active leader (either already or via proactive P2P election)."""
        if self._is_primary:
            if self.is_corrupt:
                self.set_corrupt(True)
                return False
            return True

        if not self.peers or self.is_corrupt:
            return False

        # Check if an active, healthy leader exists among peers
        healthy_peer_ids: list[int] = []
        for peer in self.peers:
            try:
                if not self._is_peer_healthy(peer):
                    continue
                if peer.is_primary():
                    return False
                info = getattr(peer, "get_info", lambda: {})()
                if isinstance(info, Mapping) and "id" in info:
                    healthy_peer_ids.append(int(info["id"]))
            except Exception:
                continue

        # No healthy primary exists; elect lowest-ID healthy node
        if all(self.node_id < pid for pid in healthy_peer_ids):
            logger.info(
                "Nenhum líder saudável detectado via P2P. Nó %d assumindo liderança proativamente.",
                self.node_id,
            )
            self.set_primary(True)
            return True

        return False

    def _actuate(self, topic: str, acao: str, command_id: Optional[str] = None, **extra: Any) -> None:
        if self._is_primary and not self.is_corrupt:
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

        # Only the active leader processes telemetry directly and replicates canonical state to replicas
        if not self._ensure_active_leader_on_telemetry():
            return

        current = self._get_internal_state()
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
        self._replicate_state_to_peers(new_state)

    def execute_command(self, command: Command) -> CommandResult:
        """Executes a command idempotently on the Leader, persists state, and replicates canonical state to peers."""
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

        current = self._get_internal_state()
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
        payload_json = json.dumps(command.parametros)
        self.repository.save_state(new_state)
        self.repository.record_event(
            command_id=command.command_id,
            tipo_evento=acao,
            payload=payload_json,
            registrado_em=timestamp,
        )

        self._replicate_state_to_peers(
            new_state,
            command_id=command.command_id,
            tipo_evento=acao,
            payload=payload_json,
        )
        logger.info(
            "Nó %d executou comando %s (command_id=%s) e sincronizou estado canônico com as réplicas.",
            self.node_id,
            acao,
            command.command_id,
        )

        return CommandResult(
            sucesso=True,
            mensagem=f"Acao {acao} executada com sucesso",
            estado=new_state,
        )

    def sync_with_peers(
        self,
        peers: Optional[Iterable[Any]] = None,
        force_state_clone: bool = False,
    ) -> None:
        """Synchronizes leadership invariant and clones canonical state from active leader/peers."""
        target_peers = list(peers) if peers is not None else self.peers
        active_primary_peer: Optional[Any] = None

        for peer in target_peers:
            try:
                if not self._is_peer_healthy(peer):
                    continue
                if peer.is_primary():
                    active_primary_peer = peer
                    if self._is_primary:
                        logger.info(
                            "Par ativo já é Líder Primário. Nó %s ingressando como réplica secundária.",
                            self.node_id,
                        )
                        self._is_primary = False
                    break
            except Exception:
                continue

        if not force_state_clone and self.repository.get_state() is not None:
            return

        # Prioritize cloning state from the active primary peer first
        candidates = ([active_primary_peer] if active_primary_peer is not None else []) + [
            p for p in target_peers if p is not active_primary_peer
        ]
        for peer in candidates:
            try:
                if not self._is_peer_healthy(peer):
                    continue
                peer_state = peer.get_state()
                if peer_state is not None:
                    self.repository.save_state(peer_state)
                    logger.info("Nó %s sincronizou estado canônico a partir de par ativo.", self.node_id)
                    break
            except Exception as exc:
                logger.warning("Nó %s falhou ao sincronizar estado de par: %s", self.node_id, exc)

    def sync_boot_state(self, peers: Optional[Iterable[Any]] = None) -> None:
        """Synchronizes state and leadership on controller startup."""
        if peers is not None and not self.peers:
            self.peers = list(peers)
        self.sync_with_peers(peers=peers, force_state_clone=False)
