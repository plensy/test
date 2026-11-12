"""Tests automatisés de l'orchestrateur local (`orchestration/local_dag.py`).

Volontairement légers (aucune dépendance à Spark/AWS) pour tourner vite en
CI : ils valident la logique de retry/backoff et de court-circuit des
dépendances via un job factice (`tests/fixtures/always_fail.py`), pas la
pipeline de données elle-même (couverte par des tests d'intégration séparés,
plus lourds, exécutés manuellement — cf. rapport §5.3).
"""
from __future__ import annotations

import sys
from pathlib import Path

import orchestration.local_dag as dag

FIXTURE = str(Path(__file__).parent / "fixtures" / "always_fail.py")


def test_retryable_step_fails_after_max_attempts():
    """Une étape retryable qui échoue toujours doit épuiser ses tentatives."""
    step = dag.Step(
        "fake_transient", [sys.executable, FIXTURE],
        max_attempts=3, backoff_seconds=0.1,
    )
    result = dag.run_step(step)
    assert result["status"] == "FAILED"
    assert len(result["attempts"]) == 3


def test_non_retryable_step_fails_on_first_attempt():
    """Un échec métier (ex: gate qualité) ne doit jamais être retenté."""
    step = dag.Step(
        "fake_quality_gate", [sys.executable, FIXTURE],
        max_attempts=3, retry_on_failure=False,
    )
    result = dag.run_step(step)
    assert result["status"] == "FAILED"
    assert len(result["attempts"]) == 1


def test_dag_short_circuits_dependents_on_failure(monkeypatch, tmp_path):
    """Si une étape échoue, ses dépendantes doivent être SKIPPED et le DAG
    doit remonter un code de sortie non nul (déclencherait l'alerte)."""
    monkeypatch.setattr(dag, "RUNS_DIR", tmp_path)
    monkeypatch.setattr(dag, "STEPS", [
        dag.Step("A_ok", [sys.executable, "-c", "print('A ok')"]),
        dag.Step("B_fail", [sys.executable, FIXTURE], depends_on=["A_ok"],
                 retry_on_failure=False),
        dag.Step("C_depend_on_B", [sys.executable, "-c", "print('jamais exécuté')"],
                 depends_on=["B_fail"]),
    ])

    rc = dag.main()

    assert rc == 1
    reports = list(tmp_path.glob("run_*.json"))
    assert len(reports) == 1
