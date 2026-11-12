"""Orchestrateur local — rejoue le même graphe de dépendances que la
définition Step Functions (`state_machine.asl.json`), en environnement de
développement, sans toucher AWS.

Chaque étape est exécutée comme un sous-processus isolé (comme un job Glue
distinct), avec retry/backoff sur échec, et court-circuit des étapes en
aval si une dépendance échoue — même sémantique que Retry/Catch en ASL.
Un rapport d'exécution JSON est écrit (durée, tentatives, statut par
étape) : c'est la source du monitoring en local (section 6 du rapport).

Usage :
    python -m orchestration.local_dag
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from config import settings

RUNS_DIR = settings.PROJECT_ROOT / "orchestration" / "_runs"


@dataclass
class Step:
    name: str
    cmd: list[str]
    depends_on: list[str] = field(default_factory=list)
    max_attempts: int = 3
    backoff_seconds: float = 5.0
    retry_on_failure: bool = True  # False pour un échec métier (ex: qualité)


# Même graphe que orchestration/state_machine.asl.json :
#   IngestBronze (parallèle logique, exécuté séquentiellement en local)
#   -> SilverWeatherDaily -> SilverTrips -> QualityGate
#   -> [GoldDatamarts, MLDemandForecast] (parallèle)
STEPS: list[Step] = [
    Step("bronze_taxi", [sys.executable, "-m", "src.bronze.ingest", "--dataset", "taxi"]),
    Step("bronze_zones", [sys.executable, "-m", "src.bronze.ingest", "--dataset", "zones"]),
    Step("bronze_weather", [sys.executable, "-m", "src.bronze.ingest", "--dataset", "weather"]),
    Step("silver_weather_daily", [sys.executable, "-m", "src.silver.build_weather_daily"],
         depends_on=["bronze_weather"]),
    Step("silver_trips", [sys.executable, "-m", "src.silver.build_trips"],
         depends_on=["bronze_taxi", "bronze_zones", "silver_weather_daily"]),
    Step("quality_gate", [sys.executable, "-m", "src.silver.quality_checks"],
         depends_on=["silver_trips"], retry_on_failure=False),  # échec métier, pas transitoire
    Step("gold_datamarts", [sys.executable, "-m", "src.gold.build_datamarts"],
         depends_on=["quality_gate"]),
    Step("ml_demand_forecast", [sys.executable, "-m", "src.ml.train_demand_forecast"],
         depends_on=["quality_gate"]),
]


def run_step(step: Step) -> dict:
    """Exécute une étape avec retry/backoff. Retourne son rapport."""
    attempts = []
    for attempt in range(1, step.max_attempts + 1):
        t0 = time.time()
        proc = subprocess.run(step.cmd, capture_output=True, text=True)
        duration = round(time.time() - t0, 1)
        ok = proc.returncode == 0
        attempts.append({"attempt": attempt, "duration_s": duration, "success": ok,
                          "returncode": proc.returncode})
        if ok:
            print(f"  ✓ {step.name} (essai {attempt}/{step.max_attempts}, {duration}s)")
            return {"name": step.name, "status": "SUCCEEDED", "attempts": attempts}
        if not step.retry_on_failure:
            print(f"  ✗ {step.name} — ÉCHEC MÉTIER (pas de retry), voir logs ci-dessous")
            print(proc.stdout[-1500:], proc.stderr[-1500:])
            return {"name": step.name, "status": "FAILED", "attempts": attempts}
        print(f"  ⚠ {step.name} échoué (essai {attempt}/{step.max_attempts}) — "
              f"retry dans {step.backoff_seconds}s")
        if attempt < step.max_attempts:
            time.sleep(step.backoff_seconds)
    print(f"  ✗ {step.name} — échec définitif après {step.max_attempts} tentatives")
    print(proc.stdout[-1500:], proc.stderr[-1500:])
    return {"name": step.name, "status": "FAILED", "attempts": attempts}


def main() -> int:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(timezone.utc)
    print(f"== DAG local : démarrage ({started_at.isoformat()}) ==")

    results: dict[str, dict] = {}
    failed: set[str] = set()

    for step in STEPS:
        blocked_by = [d for d in step.depends_on if d in failed]
        if blocked_by:
            print(f"  ⊘ {step.name} ignoré (dépendance en échec : {blocked_by})")
            results[step.name] = {"name": step.name, "status": "SKIPPED",
                                   "blocked_by": blocked_by}
            failed.add(step.name)
            continue

        print(f"\n▶ {step.name}")
        result = run_step(step)
        results[step.name] = result
        if result["status"] != "SUCCEEDED":
            failed.add(step.name)

    ended_at = datetime.now(timezone.utc)
    success = not failed
    report = {
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_s": round((ended_at - started_at).total_seconds(), 1),
        "success": success,
        "steps": results,
    }
    report_path = RUNS_DIR / f"run_{started_at.strftime('%Y%m%dT%H%M%S')}.json"
    report_path.write_text(json.dumps(report, indent=2))

    print(f"\n== DAG terminé : {'✓ SUCCÈS' if success else '✗ ÉCHEC (' + ', '.join(sorted(failed)) + ')'} ==")
    print(f"   Rapport : {report_path}")

    print("\n== Monitoring : collecte des métriques & alertes ==")
    subprocess.run([sys.executable, "-m", "src.monitoring.check_and_alert"])

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
