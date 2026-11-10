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
