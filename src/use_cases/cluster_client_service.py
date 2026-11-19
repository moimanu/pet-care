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


def _is_healthy(node: Any) -> bool:
    try:
        return not getattr(node, "is_offline", lambda: False)() and not getattr(node, "is_corrupted", lambda: False)()
    except Exception:
        return False


class ClusterClientService:
    """Client-side RPC coordinator for direct method invocation, 2f+1 Byzantine quorum, and failover."""

    def __init__(self, nodes: Mapping[int, ControllerNode], timeout: float = 1.0, quorum_threshold: int = 2) -> None:
        self.nodes, self.timeout, self.quorum_threshold = dict(nodes), timeout, quorum_threshold
        self._current_primary_id: Optional[int] = None

    def get_consensus_state(self) -> PetState:
        responses: dict[int, PetState] = {}
        with ThreadPoolExecutor(max_workers=max(1, len(self.nodes))) as pool:
            futures = {pool.submit(n.get_state): nid for nid, n in self.nodes.items()}
            for fut in as_completed(futures, timeout=self.timeout + 0.5):
                try:
                    responses[futures[fut]] = fut.result(timeout=self.timeout)
                except Exception as exc:
                    logger.warning("Nó %s falhou na consulta de estado: %s", futures[fut], exc)

        groups: dict[str, list[tuple[int, PetState]]] = defaultdict(list)
        for nid, state in responses.items():
            groups[state.state_hash()].append((nid, state))

        for s_hash, members in groups.items():
            if len(members) >= self.quorum_threshold:
                honest_ids = [nid for nid, _ in members]
                for other_hash, div_members in groups.items():
                    if other_hash != s_hash:
                        for div_id, _ in div_members:
                            logger.error("[SEGURANÇA] Falha Bizantina no Nó %s. Resposta descartada.", div_id)
                            self._demote_byzantine_leader(div_id, honest_ids)
                return members[0][1]

        raise QuorumError(f"Quorum Byzantine não atingido: {len(responses)} respostas, mínimo {self.quorum_threshold}.")

    def _demote_byzantine_leader(self, div_id: int, honest_ids: list[int]) -> None:
        if (div_node := self.nodes.get(div_id)) and self._was_primary(div_node):
            try:
                div_node.set_primary(False)
                self._promote_first_healthy(candidates=honest_ids)
            except Exception as exc:
                logger.warning("Falha ao destituir líder bizantino Nó %s: %s", div_id, exc)

    def _promote_first_healthy(
        self, exclude: Optional[int] = None, candidates: Optional[list[int]] = None
    ) -> Optional[tuple[int, ControllerNode]]:
        for nid in sorted(candidates if candidates is not None else self.nodes.keys()):
            if nid != exclude and (node := self.nodes.get(nid)) and _is_healthy(node):
                try:
                    node.set_primary(True)
                    self._current_primary_id = nid
                    self._demote_others(nid)
                    return nid, node
                except Exception:
                    continue
        return None

    def _find_active_primary(self, exclude: Optional[int] = None) -> Optional[tuple[int, ControllerNode]]:
        for nid, node in sorted(self.nodes.items()):
            if nid != exclude and _is_healthy(node) and self._was_primary(node):
                return nid, node
        return None

    def get_primary_node(self) -> tuple[int, ControllerNode]:
        if (cid := self._current_primary_id) in self.nodes and _is_healthy(n := self.nodes[cid]) and self._was_primary(n):
            return cid, n
        if found := (self._find_active_primary() or self._promote_first_healthy()):
            self._current_primary_id = found[0]
            return found
        for nid, node in sorted(self.nodes.items()):
            if self._was_primary(node):
                self._current_primary_id = nid
                return nid, node
        if self.nodes:
            return next(iter(self.nodes.items()))
        raise NodeUnreachableError("Nenhum nó controlador disponível no cluster")

    def _demote_others(self, exclude_id: int) -> None:
        for nid, node in self.nodes.items():
            if nid != exclude_id and self._was_primary(node):
                try:
                    node.set_primary(False)
                except Exception:
                    pass

    def send_command(
        self, acao: str, parametros: Optional[dict[str, Any]] = None, command_id: Optional[str] = None
    ) -> CommandResult:
        params = parametros or {}
        cmd = Command(command_id=str(command_id or uuid.uuid4()), acao=acao, parametros=params)
        primary_id, primary_node = self.get_primary_node()
        result: Optional[CommandResult] = None
        exec_id = primary_id

        try:
            result = primary_node.execute_command(cmd)
        except Exception as exc:
            logger.warning("Nó Primário %s falhou (%s). Acionando failover.", primary_id, exc)
            for cand_id, cand_node in sorted(self.nodes.items()):
                if cand_id != primary_id and _is_healthy(cand_node):
                    try:
                        cand_node.set_primary(True)
                        result = cand_node.execute_command(cmd)
                        exec_id = self._current_primary_id = cand_id
                        self._demote_others(cand_id)
                        break
                    except Exception:
                        continue
            if result is None:
                raise NodeUnreachableError("Todos os nós controladores falharam ao executar o método")

        if not getattr(self.nodes.get(exec_id), "peers", None) and result.estado is not None:
            payload_json = json.dumps(params)
            for nid, node in self.nodes.items():
                if nid != exec_id and hasattr(node, "sync_state"):
                    try:
                        node.sync_state(result.estado, command_id=cmd.command_id, tipo_evento=acao, payload=payload_json)
                    except Exception:
                        pass
        return result

    def get_pet_status(self) -> dict[str, Any]:
        try:
            state = self.get_consensus_state()
            return {"sucesso": True, "estado": {**asdict(state), "hash": state.state_hash()}}
        except Exception as exc:
            return {"sucesso": False, "mensagem": str(exc), "estado": None}

    def dispensar_racao(self, quantidade_gramas: float = 50.0, command_id: Optional[str] = None) -> CommandResult:
        if not (0 < (qty := float(quantidade_gramas)) <= TelemetrySanityValidator.MAX_RACAO_GRAMAS):
            raise SanityError(f"Quantidade de racao invalida: {qty}g.")
        return self.send_command("LIBERAR_RACAO", {"quantidade_gramas": qty}, command_id=command_id)

    def repor_agua(self, quantidade_ml: float = 200.0, command_id: Optional[str] = None) -> CommandResult:
        if not (0 < (qty := float(quantidade_ml)) <= TelemetrySanityValidator.MAX_AGUA_ML):
            raise SanityError(f"Quantidade de agua invalida: {qty}ml.")
        return self.send_command("REPOR_AGUA", {"quantidade_ml": qty}, command_id=command_id)

    def resetar_areia(self, command_id: Optional[str] = None) -> CommandResult:
        return self.send_command("RESETAR_AREIA", command_id=command_id)

    def silenciar_alarme(self, command_id: Optional[str] = None) -> CommandResult:
        return self.send_command("SILENCIAR_ALARME", command_id=command_id)

    def get_nodes_info(self) -> list[dict[str, Any]]:
        info_list = []
        for nid, node in sorted(self.nodes.items()):
            try:
                info_list.append(node.get_info())
            except Exception as exc:
                info_list.append({"id": nid, "primary": False, "corrupt": False, "offline": True, "error": str(exc)})
        return info_list

    def _was_primary(self, node: ControllerNode) -> bool:
        try:
            return bool(node.is_primary())
        except Exception:
            return False

    def _ensure_leader_after_fault(self, faulted_id: int, was_primary: bool) -> None:
        if self._current_primary_id == faulted_id:
            self._current_primary_id = None
        if was_primary and not self._find_active_primary(exclude=faulted_id):
            self._promote_first_healthy(exclude=faulted_id)

    def set_node_corrupt(self, node_id: int, corrupt: bool) -> bool:
        if not (node := self.nodes.get(node_id)):
            return False
        was_primary = self._was_primary(node)
        node.set_corrupt(corrupt)
        if corrupt:
            self._ensure_leader_after_fault(node_id, was_primary)
        return True

    def set_node_offline(self, node_id: int, offline: bool) -> bool:
        if not (node := self.nodes.get(node_id)):
            return False
        was_primary = self._was_primary(node)
        node.set_offline(offline)
        if offline:
            self._ensure_leader_after_fault(node_id, was_primary)
        elif active := self._find_active_primary(exclude=node_id):
            try:
                node.set_primary(False)
                if hasattr(node, "sync_state"):
                    node.sync_state(active[1].get_state())
            except Exception:
                pass
        elif self._was_primary(node):
            self._current_primary_id = node_id
        return True
