from __future__ import annotations

import logging
import os
from typing import Any, Callable

from src.adapters.sqlite_repository import SqliteRepository
from src.domain.entities import PetState
from src.use_cases.controller_service import ControllerService
from src.use_cases.middleware_service import MiddlewareService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (Audit) %(message)s")
logger = logging.getLogger("AuditSuite")


class AuditInMemoryBroker:
    """In-memory broker capturing published messages and routing to subscriptions."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any], int]] = []
        self.subscriptions: list[tuple[str, Callable[[str, dict[str, Any]], None]]] = []

    def publish(self, topic: str, payload: dict[str, Any], qos: int = 1) -> None:
        self.published.append((topic, payload, qos))
        for pattern, callback in self.subscriptions:
            callback(topic, payload)

    def subscribe(self, topic: str, callback: Callable[[str, dict[str, Any]], None], qos: int = 1) -> None:
        self.subscriptions.append((topic, callback))


def run_scenario_1_nominal(tmpdir: str) -> dict[str, Any]:
    """Cenário 1: Replicação Nominal across 3 nodes with 1 single edge actuation."""
    broker = AuditInMemoryBroker()

    db1 = os.path.join(tmpdir, "node_1.db")
    db2 = os.path.join(tmpdir, "node_2.db")
    db3 = os.path.join(tmpdir, "node_3.db")

    repo1 = SqliteRepository(db1)
    repo2 = SqliteRepository(db2)
    repo3 = SqliteRepository(db3)

    c1 = ControllerService(node_id=1, repository=repo1, broker=broker, is_primary=True)
    c2 = ControllerService(node_id=2, repository=repo2, broker=broker, is_primary=False)
    c3 = ControllerService(node_id=3, repository=repo3, broker=broker, is_primary=False)

    # 1. Telemetry broadcast to all replicas
    telemetry_samples = [
        ("petcare/sensores/racao", {"nivel_gramas": 180.0, "timestamp": "2026-11-10T15:00:00Z"}),
        ("petcare/sensores/agua", {"nivel_ml": 420.0, "timestamp": "2026-11-10T15:00:00Z"}),
        ("petcare/sensores/areia", {"usos_acumulados": 2, "timestamp": "2026-11-10T15:00:00Z"}),
        ("petcare/sensores/fuga", {"perimetro": "DENTRO", "timestamp": "2026-11-10T15:00:00Z"}),
    ]

    for topic, payload in telemetry_samples:
        c1.handle_telemetry(topic, payload)
        c2.handle_telemetry(topic, payload)
        c3.handle_telemetry(topic, payload)

    # 2. Dispatch manual command via Middleware
    middleware = MiddlewareService({1: c1, 2: c2, 3: c3})
    result = middleware.dispensar_racao(50.0)

    # 3. Assertions:
    # Databases finish identical
    s1 = repo1.get_state()
    s2 = repo2.get_state()
    s3 = repo3.get_state()
    states_identical = (s1 is not None and s2 is not None and s3 is not None and
                        s1.state_hash() == s2.state_hash() == s3.state_hash())

    # Exactly 1 actuation published to edge dispenser
    actuations = [p for p in broker.published if "atuadores/dispenser" in p[0]]
    actuations_count = len(actuations)

    success = result.sucesso and states_identical and (actuations_count == 1)
    return {
        "success": success,
        "states_identical": states_identical,
        "actuations_count": actuations_count,
        "state_hash": s1.state_hash() if s1 else None,
    }


def run_scenario_4_sanitization(tmpdir: str) -> dict[str, Any]:
    """Cenário 4: Sanitização de Borda intercepting anomalous telemetry."""
    broker = AuditInMemoryBroker()
    db = os.path.join(tmpdir, "node_1.db")
    repo = SqliteRepository(db)
    controller = ControllerService(node_id=1, repository=repo, broker=broker, is_primary=True)

    # Establish baseline state
    baseline_telemetry = ("petcare/sensores/agua", {"nivel_ml": 400.0, "timestamp": "2026-11-10T15:00:00Z"})
    controller.handle_telemetry(*baseline_telemetry)

    saved_state = repo.get_state()
    assert saved_state is not None and saved_state.agua_ml == 400.0

    # Publish anomalous negative water level
    anomalous_telemetry = ("petcare/sensores/agua", {"nivel_ml": -150.0, "timestamp": "2026-11-10T15:01:00Z"})
    controller.handle_telemetry(*anomalous_telemetry)

    current_state = repo.get_state()
    db_unmodified = (current_state is not None and current_state.agua_ml == 400.0)

    return {
        "success": db_unmodified,
        "anomaly_rejected": True,
        "db_unmodified": db_unmodified,
    }


def run_scenario_2_failover(tmpdir: str) -> dict[str, Any]:
    """Cenário 2: Failover Crash-Stop with automatic promotion and boot catch-up."""
    broker = AuditInMemoryBroker()

    db1 = os.path.join(tmpdir, "node_1.db")
    db2 = os.path.join(tmpdir, "node_2.db")
    db3 = os.path.join(tmpdir, "node_3.db")

    repo1 = SqliteRepository(db1)
    repo2 = SqliteRepository(db2)
    repo3 = SqliteRepository(db3)

    c1 = ControllerService(node_id=1, repository=repo1, broker=broker, is_primary=True)
    c2 = ControllerService(node_id=2, repository=repo2, broker=broker, is_primary=False)
    c3 = ControllerService(node_id=3, repository=repo3, broker=broker, is_primary=False)

    # Base initial state
    init_state = PetState(100.0, 200.0, 1, "DENTRO", False, "2026-11-10T15:00:00Z")
    repo1.save_state(init_state)
    repo2.save_state(init_state)
    repo3.save_state(init_state)

    # 1. Simulate abrupt crash on Node 1
    class CrashedNodeWrapper:
        def __init__(self, node: ControllerService):
            self.node = node

        def get_state(self):
            from src.domain.exceptions import NodeUnreachableError
            raise NodeUnreachableError("Node 1 process killed (SIGKILL)")

        def execute_command(self, cmd):
            from src.domain.exceptions import NodeUnreachableError
            raise NodeUnreachableError("Node 1 process killed (SIGKILL)")

        def is_primary(self):
            return True

        def set_primary(self, primary):
            self.node.set_primary(primary)

    c1_crashed = CrashedNodeWrapper(c1)
    middleware = MiddlewareService({1: c1_crashed, 2: c2, 3: c3})

    # 2. Client dispatches command through middleware
    res = middleware.repor_agua(200.0)
    assert res.sucesso is True
    assert c2.is_primary() is True  # Node 2 promoted

    # 3. Node 1 recovers and performs boot catch-up sync
    # Create fresh recovered DB file to simulate wiped/lagged storage requiring catch-up
    db1_recovered = os.path.join(tmpdir, "node_1_recovered.db")
    repo1_recovered = SqliteRepository(db1_recovered)
    c1_recovered = ControllerService(node_id=1, repository=repo1_recovered, broker=broker, is_primary=False)
    c1_recovered.sync_boot_state([c2, c3])

    s1_rec = repo1_recovered.get_state()
    s2 = repo2.get_state()
    s3 = repo3.get_state()

    recovered_node_synced = (s1_rec is not None and s2 is not None and
                             s1_rec.state_hash() == s2.state_hash() == s3.state_hash())

    success = res.sucesso and c2.is_primary() and recovered_node_synced
    return {
        "success": success,
        "failover_promoted": c2.is_primary(),
        "recovered_node_synced": recovered_node_synced,
    }


def run_scenario_3_byzantine(tmpdir: str) -> dict[str, Any]:
    """Cenário 3: Falha Bizantina with 2x1 majority discarding corrupted replica."""
    broker = AuditInMemoryBroker()

    db1 = os.path.join(tmpdir, "node_1.db")
    db2 = os.path.join(tmpdir, "node_2.db")
    db3 = os.path.join(tmpdir, "node_3.db")

    repo1 = SqliteRepository(db1)
    repo2 = SqliteRepository(db2)
    repo3 = SqliteRepository(db3)

    c1 = ControllerService(node_id=1, repository=repo1, broker=broker, is_primary=True)
    c2 = ControllerService(node_id=2, repository=repo2, broker=broker, is_primary=False)
    # Node 3 is corrupt
    c3 = ControllerService(node_id=3, repository=repo3, broker=broker, is_primary=False, is_corrupt=True)

    telemetry = [
        ("petcare/sensores/racao", {"nivel_gramas": 200.0, "timestamp": "2026-11-10T15:00:00Z"}),
        ("petcare/sensores/agua", {"nivel_ml": 500.0, "timestamp": "2026-11-10T15:00:00Z"}),
        ("petcare/sensores/areia", {"usos_acumulados": 0, "timestamp": "2026-11-10T15:00:00Z"}),
        ("petcare/sensores/fuga", {"perimetro": "DENTRO", "timestamp": "2026-11-10T15:00:00Z"}),
    ]

    for topic, payload in telemetry:
        c1.handle_telemetry(topic, payload)
        c2.handle_telemetry(topic, payload)
        c3.handle_telemetry(topic, payload)

    # Node 3 will return corrupted state status_fuga="FORA", alarme_ativo=True
    middleware = MiddlewareService({1: c1, 2: c2, 3: c3})
    consensus_state = middleware.get_consensus_state()

    byzantine_detected = (consensus_state.status_fuga == "DENTRO" and
                          consensus_state.alarme_ativo is False and
                          c3.get_state().status_fuga == "FORA")

    return {
        "success": byzantine_detected,
        "byzantine_detected": byzantine_detected,
        "consensus_status_fuga": consensus_state.status_fuga,
    }


def run_all_scenarios(tmpdir: Optional[str] = None) -> bool:
    """Executes all 4 experimental evaluation scenarios sequentially."""
    import tempfile
    import sys

    print("================================================================================")
    print("        SUÍTE DE AUDITORIA E VALIDAÇÃO DISTRIBUÍDA - SISTEMA PET CARE          ")
    print("================================================================================")

    all_passed = True
    context_dir = tmpdir or tempfile.mkdtemp(prefix="petcare_audit_")

    scenarios = [
        ("Cenário 1: Replicação Nominal (3 Nós + Idempotência na Borda)", run_scenario_1_nominal),
        ("Cenário 2: Failover Crash-Stop (Promoção Transparente & Boot Catch-up)", run_scenario_2_failover),
        ("Cenário 3: Falha Bizantina (Tolerância 2f+1 & Descarte por Maioria)", run_scenario_3_byzantine),
        ("Cenário 4: Sanitização de Borda (Filtro Físico & Proteção de Estado)", run_scenario_4_sanitization),
    ]

    for title, scenario_fn in scenarios:
        try:
            res = scenario_fn(context_dir)
            if res.get("success"):
                print(f" [PASS] {title}")
            else:
                print(f" [FAIL] {title} -> Detalhes: {res}")
                all_passed = False
        except Exception as exc:
            print(f" [ERROR] {title} -> Exceção: {exc}")
            all_passed = False

    print("================================================================================")
    print(f" RESULTADO FINAL: {'TODOS OS 4 CENÁRIOS FORAM VALIDADOS COM SUCESSO!' if all_passed else 'FALHA EM UM OU MAIS CENÁRIOS'}")
    print("================================================================================")
    return all_passed


if __name__ == "__main__":
    import sys
    success = run_all_scenarios()
    sys.exit(0 if success else 1)


