import tempfile

from scripts.run_audit_suite import (
    run_scenario_1_nominal,
    run_scenario_2_failover,
    run_scenario_3_byzantine,
    run_scenario_4_sanitization,
)


def test_scenario_1_nominal_replication():
    with tempfile.TemporaryDirectory() as tmpdir:
        res = run_scenario_1_nominal(tmpdir)
        assert res["success"] is True
        assert res["states_identical"] is True
        assert res["actuations_count"] == 1


def test_scenario_4_edge_sanitization():
    with tempfile.TemporaryDirectory() as tmpdir:
        res = run_scenario_4_sanitization(tmpdir)
        assert res["success"] is True
        assert res["anomaly_rejected"] is True
        assert res["db_unmodified"] is True


def test_scenario_2_crash_stop_failover():
    with tempfile.TemporaryDirectory() as tmpdir:
        res = run_scenario_2_failover(tmpdir)
        assert res["success"] is True
        assert res["failover_promoted"] is True
        assert res["recovered_node_synced"] is True


def test_scenario_3_byzantine_fault():
    with tempfile.TemporaryDirectory() as tmpdir:
        res = run_scenario_3_byzantine(tmpdir)
        assert res["success"] is True
        assert res["byzantine_detected"] is True
        assert res["consensus_status_fuga"] == "DENTRO"
