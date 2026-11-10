from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from typing import Any, Mapping, Optional
import uuid

from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import NodeUnreachableError, QuorumError
from src.domain.interfaces import IControllerNode

logger = logging.getLogger(__name__)


class MiddlewareService:
    """Middleware managing 2f+1 Byzantine quorum, failover orchestration, and UUID idempotency."""

    def __init__(
        self,
        nodes: Mapping[int, IControllerNode],
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
            future_to_id = {
                executor.submit(node.get_state): node_id
                for node_id, node in self.nodes.items()
            }
            for future in as_completed(future_to_id, timeout=self.timeout + 0.5):
                node_id = future_to_id[future]
                try:
                    state = future.result(timeout=self.timeout)
                    responses[node_id] = state
                except Exception as exc:
                    logger.warning("Nó %s falhou na consulta de estado: %s", node_id, exc)

        # Group by canonical state hash
        hash_to_nodes: dict[str, list[int]] = {}
        hash_to_state: dict[str, PetState] = {}

        for node_id, state in responses.items():
            s_hash = state.state_hash()
            hash_to_nodes.setdefault(s_hash, []).append(node_id)
            hash_to_state[s_hash] = state

        for s_hash, node_ids in hash_to_nodes.items():
            if len(node_ids) >= self.quorum_threshold:
                # Identify and alert Byzantine divergent nodes
                for other_hash, divergent_nodes in hash_to_nodes.items():
                    if other_hash != s_hash:
                        for div_id in divergent_nodes:
                            logger.error(
                                "[SEGURANÇA] Falha Bizantina detectada no Nó %s. Resposta descartada.",
                                div_id,
                            )
                return hash_to_state[s_hash]

        raise QuorumError(
            f"Quorum Byzantine não atingido: obtidas {len(responses)} respostas, "
            f"nenhum consenso >= {self.quorum_threshold} nós."
        )
