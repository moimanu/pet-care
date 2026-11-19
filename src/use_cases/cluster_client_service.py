from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
import json
import logging
from typing import Any, Mapping, Optional
import uuid

from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import NodeUnreachableError, QuorumError, SanityError
from src.domain.interfaces import ControllerNode
from src.domain.validators import TelemetrySanityValidator

logger = logging.getLogger(__name__)


class ClusterClientService:
    """Client-side RPC coordinator for direct method invocation, 2f+1 Byzantine quorum, and failover."""

    def __init__(
        self,
        nodes: Mapping[int, ControllerNode],
        timeout: float = 1.0,
        quorum_threshold: int = 2,
    ) -> None:
        self.nodes = dict(nodes)
        self.timeout = timeout
        self.quorum_threshold = quorum_threshold
        self._current_primary_id: Optional[int] = None

    def get_consensus_state(self) -> PetState:
        """Queries all controllers in parallel, resolves 2f+1 majority, and demotes any Byzantine leader."""
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
                honest_ids = [nid for nid, _ in members]
                for other_hash, divergent_members in groups.items():
                    if other_hash != s_hash:
                        for div_id, _ in divergent_members:
                            logger.error(
                                "[SEGURANÇA] Falha Bizantina detectada no Nó %s. Resposta descartada.",
                                div_id,
                            )
                            self._demote_byzantine_leader_if_needed(div_id, honest_ids)
                return members[0][1]

        raise QuorumError(
            f"Quorum Byzantine não atingido: obtidas {len(responses)} respostas, "
            f"nenhum consenso >= {self.quorum_threshold} nós."
        )

    def _demote_byzantine_leader_if_needed(self, divergent_id: int, honest_ids: list[int]) -> None:
        """Demotes a divergent node if it holds primary leadership and promotes an honest replica."""
        div_node = self.nodes.get(divergent_id)
        if div_node is None:
            return
        try:
            if div_node.is_primary():
                div_node.set_primary(False)
                for honest_id in sorted(honest_ids):
                    honest_node = self.nodes.get(honest_id)
                    if honest_node is not None and self._is_node_healthy(honest_node):
                        honest_node.set_primary(True)
                        self._current_primary_id = honest_id
                        logger.warning(
                            "[SEGURANÇA] Líder Bizantino Nó %s destituído pelo quórum. Nó %s promovido a novo Líder.",
                            divergent_id,
                            honest_id,
                        )
                        break
        except Exception as exc:
            logger.warning("Falha ao destituir líder bizantino Nó %s: %s", divergent_id, exc)

    def _is_node_healthy(self, node: ControllerNode) -> bool:
        try:
            is_off = getattr(node, "is_offline", lambda: False)()
            is_corr = getattr(node, "is_corrupted", lambda: False)()
            return not is_off and not is_corr
        except Exception:
            return False

    def get_primary_node(self) -> tuple[int, ControllerNode]:
        """Finds active healthy primary node or promotes the first healthy responsive replica."""
        if self._current_primary_id is not None and self._current_primary_id in self.nodes:
            active_node = self.nodes[self._current_primary_id]
            try:
                if active_node.is_primary() and self._is_node_healthy(active_node):
                    return self._current_primary_id, active_node
            except Exception:
                pass

        for node_id, node in sorted(self.nodes.items()):
            try:
                if node.is_primary() and self._is_node_healthy(node):
                    self._current_primary_id = node_id
                    return node_id, node
            except Exception:
                continue

        # If current primary is offline or corrupted, promote first healthy replica
        for node_id, node in sorted(self.nodes.items()):
            try:
                if self._is_node_healthy(node):
                    node.set_primary(True)
                    self._current_primary_id = node_id
                    self._demote_others(exclude_id=node_id)
                    return node_id, node
            except Exception:
                continue

        for node_id, node in sorted(self.nodes.items()):
            try:
                if node.is_primary():
                    self._current_primary_id = node_id
                    return node_id, node
            except Exception:
                continue

        try:
            return next(iter(self.nodes.items()))
        except StopIteration:
            raise NodeUnreachableError("Nenhum nó controlador disponível no cluster")

    def _demote_others(self, exclude_id: int) -> None:
        for other_id, other_node in self.nodes.items():
            if other_id != exclude_id:
                try:
                    if other_node.is_primary():
                        other_node.set_primary(False)
                except Exception:
                    pass

    def send_command(
        self,
        acao: str,
        parametros: Optional[dict[str, Any]] = None,
        command_id: Optional[str] = None,
    ) -> CommandResult:
        """Invokes an idempotent command directly on the Primary Controller with transparent failover."""
        params = parametros or {}
        cid = str(command_id) if command_id else str(uuid.uuid4())
        command = Command(command_id=cid, acao=acao, parametros=params)

        primary_id, primary_node = self.get_primary_node()
        result: Optional[CommandResult] = None
        executed_primary_id = primary_id

        try:
            result = primary_node.execute_command(command)
            logger.info(
                "Método RPC %s invocado com sucesso no Líder Nó %s (command_id=%s)",
                acao,
                executed_primary_id,
                cid,
            )
        except Exception as exc:
            logger.warning(
                "Nó Primário %s inalcançável durante invocação de método: %s. Iniciando failover transparente.",
                primary_id,
                exc,
            )
            for candidate_id, candidate_node in sorted(self.nodes.items()):
                if candidate_id == primary_id:
                    continue
                try:
                    if not self._is_node_healthy(candidate_node):
                        continue
                    candidate_node.set_primary(True)
                    result = candidate_node.execute_command(command)
                    executed_primary_id = candidate_id
                    self._current_primary_id = candidate_id
                    logger.info("Nó %s promovido com sucesso a novo Líder Primário.", candidate_id)
                    self._demote_others(exclude_id=candidate_id)
                    break
                except Exception as cand_exc:
                    logger.warning("Falha ao promover nó candidato %s: %s", candidate_id, cand_exc)

            if result is None:
                raise NodeUnreachableError("Todos os nós controladores falharam ao executar o método")

        # If the leader node is an isolated mock without P2P peers configured, ensure state sync fallback
        executed_node = self.nodes.get(executed_primary_id)
        has_p2p_peers = bool(getattr(executed_node, "peers", None))
        if not has_p2p_peers and result.estado is not None:
            payload_json = json.dumps(params)
            for node_id, node in self.nodes.items():
                if node_id != executed_primary_id:
                    try:
                        sync_fn = getattr(node, "sync_state", None)
                        if callable(sync_fn):
                            sync_fn(
                                result.estado,
                                command_id=command.command_id,
                                tipo_evento=acao,
                                payload=payload_json,
                            )
                    except Exception as exc:
                        logger.warning("Falha na sincronização de estado para o nó %s: %s", node_id, exc)

        return result

    def get_pet_status(self) -> dict[str, Any]:
        """Returns serialized consensus pet state dictionary for client consumption."""
        try:
            state = self.get_consensus_state()
            data = asdict(state)
            data["hash"] = state.state_hash()
            return {"sucesso": True, "estado": data}
        except Exception as exc:
            logger.warning("Falha ao obter estado por quórum: %s", exc)
            return {"sucesso": False, "mensagem": str(exc), "estado": None}

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
                result.append(node.get_info())
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
        """Injects or clears Byzantine fault condition on target controller node."""
        node = self.nodes.get(node_id)
        if not node:
            return False
        was_primary = False
        try:
            was_primary = node.is_primary()
        except Exception:
            pass

        node.set_corrupt(corrupt)
        logger.warning("Status bizantino no Nó %d alterado para %s", node_id, corrupt)

        if corrupt and was_primary:
            if self._current_primary_id == node_id:
                self._current_primary_id = None
            # Ensure a healthy replica is promoted if the node didn't already promote a peer via P2P
            if self._find_active_primary_id(exclude=node_id) is None:
                for cand_id, cand_node in sorted(self.nodes.items()):
                    if cand_id != node_id and self._is_node_healthy(cand_node):
                        try:
                            cand_node.set_primary(True)
                            self._current_primary_id = cand_id
                            break
                        except Exception:
                            continue
        return True

    def set_node_offline(self, node_id: int, offline: bool) -> bool:
        """Simulates crash-stop or recovery on target controller node."""
        node = self.nodes.get(node_id)
        if not node:
            return False
        was_primary = False
        try:
            was_primary = node.is_primary()
        except Exception:
            pass

        node.set_offline(offline)
        logger.warning("Status offline no Nó %d alterado para %s", node_id, offline)

        if not offline:
            active_leader_id = self._find_active_primary_id(exclude=node_id)
            if active_leader_id is not None:
                try:
                    node.set_primary(False)
                    active_leader = self.nodes[active_leader_id]
                    sync_fn = getattr(node, "sync_state", None)
                    if callable(sync_fn):
                        sync_fn(active_leader.get_state())
                    logger.info(
                        "Nó %d recuperado como Réplica secundária e sincronizado com o Líder Nó %d",
                        node_id,
                        active_leader_id,
                    )
                except Exception as exc:
                    logger.warning("Falha ao sincronizar nó %d após recuperação: %s", node_id, exc)
            elif node.is_primary():
                self._current_primary_id = node_id
        else:
            if self._current_primary_id == node_id:
                self._current_primary_id = None
            if was_primary and self._find_active_primary_id(exclude=node_id) is None:
                for cand_id, cand_node in sorted(self.nodes.items()):
                    if cand_id != node_id and self._is_node_healthy(cand_node):
                        try:
                            cand_node.set_primary(True)
                            self._current_primary_id = cand_id
                            break
                        except Exception:
                            continue

        return True

    def _find_active_primary_id(self, exclude: Optional[int] = None) -> Optional[int]:
        """Finds any active responsive node that is currently primary, excluding specified node."""
        for nid, n in sorted(self.nodes.items()):
            if exclude is not None and nid == exclude:
                continue
            try:
                if n.is_primary() and self._is_node_healthy(n):
                    return nid
            except Exception:
                continue
        return None

