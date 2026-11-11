from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from typing import Any, Mapping, Optional
import uuid

from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import NodeUnreachableError, QuorumError, SanityError
from src.domain.interfaces import ControllerNode
from src.domain.validators import TelemetrySanityValidator

logger = logging.getLogger(__name__)


class MiddlewareService:
    """Middleware managing 2f+1 Byzantine quorum, failover orchestration, and UUID idempotency."""

    def __init__(
        self,
        nodes: Mapping[int, ControllerNode],
        timeout: float = 1.0,
        quorum_threshold: int = 2,
    ) -> None:
        self.nodes = dict(nodes)
        self.timeout = timeout
        self.quorum_threshold = quorum_threshold

    def get_consensus_state(self) -> PetState:
        """Queries all nodes in parallel and resolves state via Byzantine majority."""
        responses: dict[int, PetState] = {}

        with ThreadPoolExecutor(max_workers=max(1, len(self.nodes))) as executor:
            future_to_id = {executor.submit(n.get_state): nid for nid, n in self.nodes.items()}
            for future in as_completed(future_to_id, timeout=self.timeout + 0.5):
                node_id = future_to_id[future]
                try:
                    responses[node_id] = future.result(timeout=self.timeout)
                except Exception as exc:
                    logger.warning("Nó %s falhou na consulta de estado: %s", node_id, exc)

        groups: dict[str, list[tuple[int, PetState]]] = defaultdict(list)
        for node_id, state in responses.items():
            groups[state.state_hash()].append((node_id, state))

        for s_hash, members in groups.items():
            if len(members) >= self.quorum_threshold:
                for other_hash, divergent_members in groups.items():
                    if other_hash != s_hash:
                        for div_id, _ in divergent_members:
                            logger.error(
                                "[SEGURANÇA] Falha Bizantina detectada no Nó %s. Resposta descartada.",
                                div_id,
                            )
                return members[0][1]

        raise QuorumError(
            f"Quorum Byzantine não atingido: obtidas {len(responses)} respostas, "
            f"nenhum consenso >= {self.quorum_threshold} nós."
        )

    def get_primary_node(self) -> tuple[int, ControllerNode]:
        """Finds active primary node or falls back to first responsive node."""
        for node_id, node in self.nodes.items():
            try:
                if node.is_primary():
                    return node_id, node
            except Exception:
                continue

        try:
            return next(iter(self.nodes.items()))
        except StopIteration:
            raise NodeUnreachableError("Nenhum nó controlador disponível no Middleware")

    def send_command(
        self,
        acao: str,
        parametros: Optional[dict[str, Any]] = None,
        command_id: Optional[str] = None,
    ) -> CommandResult:
        """Dispatches an idempotent command to primary and replicates to peers."""
        params = parametros or {}
        cid = str(command_id) if command_id else str(uuid.uuid4())
        command = Command(command_id=cid, acao=acao, parametros=params)

        primary_id, primary_node = self.get_primary_node()
        result: Optional[CommandResult] = None
        executed_primary_id = primary_id

        try:
            result = primary_node.execute_command(command)
        except Exception as exc:
            logger.warning(
                "Nó Primário %s inalcançável durante comando: %s. Iniciando failover transparente.",
                primary_id,
                exc,
            )
            for candidate_id, candidate_node in self.nodes.items():
                if candidate_id == primary_id:
                    continue
                try:
                    candidate_node.set_primary(True)
                    result = candidate_node.execute_command(command)
                    executed_primary_id = candidate_id
                    logger.info("Nó %s promovido com sucesso a novo Líder Primário.", candidate_id)
                    break
                except Exception as cand_exc:
                    logger.warning("Falha ao promover nó candidato %s: %s", candidate_id, cand_exc)

            if result is None:
                raise NodeUnreachableError("Todos os nós controladores falharam ao executar comando")

        # Replicate to remaining healthy nodes
        for node_id, node in self.nodes.items():
            if node_id != executed_primary_id:
                try:
                    node.execute_command(command)
                except Exception as exc:
                    logger.warning("Falha na replicacao do comando para o nó %s: %s", node_id, exc)

        return result

    def dispensar_racao(
        self, quantidade_gramas: float = 50.0, command_id: Optional[str] = None
    ) -> CommandResult:
        qty = float(quantidade_gramas)
        if qty <= 0 or qty > TelemetrySanityValidator.MAX_RACAO_GRAMAS:
            raise SanityError(
                f"Quantidade de racao invalida: {qty}g. Permitido: 0.1 a {TelemetrySanityValidator.MAX_RACAO_GRAMAS}g"
            )
        return self.send_command("LIBERAR_RACAO", {"quantidade_gramas": qty}, command_id=command_id)

    def repor_agua(
        self, quantidade_ml: float = 200.0, command_id: Optional[str] = None
    ) -> CommandResult:
        qty = float(quantidade_ml)
        if qty <= 0 or qty > TelemetrySanityValidator.MAX_AGUA_ML:
            raise SanityError(
                f"Quantidade de agua invalida: {qty}ml. Permitido: 0.1 a {TelemetrySanityValidator.MAX_AGUA_ML}ml"
            )
        return self.send_command("REPOR_AGUA", {"quantidade_ml": qty}, command_id=command_id)

    def resetar_areia(self, command_id: Optional[str] = None) -> CommandResult:
        return self.send_command("RESETAR_AREIA", command_id=command_id)

    def silenciar_alarme(self, command_id: Optional[str] = None) -> CommandResult:
        return self.send_command("SILENCIAR_ALARME", command_id=command_id)

    def get_nodes_info(self) -> list[dict[str, Any]]:
        """Queries topology and status of all configured controller nodes."""
        result = []
        for nid, node in sorted(self.nodes.items()):
            try:
                if hasattr(node, "get_info"):
                    info = node.get_info()
                else:
                    info = {
                        "id": nid,
                        "primary": node.is_primary(),
                        "corrupt": False,
                        "offline": False,
                    }
                result.append(info)
            except Exception as exc:
                result.append({
                    "id": nid,
                    "primary": False,
                    "corrupt": False,
                    "offline": True,
                    "error": str(exc),
                })
        return result

    def set_node_corrupt(self, node_id: int, corrupt: bool) -> bool:
        """Injects or clears Byzantine fault condition on target node."""
        node = self.nodes.get(node_id)
        if not node:
            return False
        if hasattr(node, "set_corrupt"):
            node.set_corrupt(corrupt)
            logger.warning("Middleware alterou falha bizantina no Nó %d para %s", node_id, corrupt)
            return True
        return False

    def set_node_offline(self, node_id: int, offline: bool) -> bool:
        """Simulates crash-stop or recovery on target node."""
        node = self.nodes.get(node_id)
        if not node:
            return False
        if hasattr(node, "set_offline"):
            node.set_offline(offline)
            logger.warning("Middleware alterou status offline no Nó %d para %s", node_id, offline)
            return True
        return False
