"""Collecte des métriques d'observabilité de la pipeline.

Rejoue en local ce que CloudWatch mesurerait en production (durées de job,
nombre de lignes traitées, taux d'erreur qualité) à partir de trois
sources déjà produites par la pipeline :
  - le dernier rapport d'exécution de l'orchestrateur (durées/statuts),
  - le rapport de qualité Great Expectations (silver/_quality),
  - la volumétrie des tables Parquet (comptage par métadonnées, sans
    scan complet — équivalent du "nb de lignes traitées" CloudWatch).
"""
from __future__ import annotations

import json
from pathlib import Path

import pyarrow.dataset as ds

from config import settings

RUNS_DIR = settings.PROJECT_ROOT / "orchestration" / "_runs"
QUALITY_REPORT = Path(settings.SILVER) / "_quality" / "trips_quality_report.json"


def row_count(path: str) -> int | None:
    """Nombre de lignes d'une table Parquet via les métadonnées de fichier
    (pas de scan des données — rapide, adapté à un contrôle fréquent)."""
    try:
        return ds.dataset(path, format="parquet").count_rows()
    except Exception:  # noqa: BLE001 — table absente / pas encore construite
        return None


def latest_run_report() -> dict | None:
    if not RUNS_DIR.exists():
        return None
    runs = sorted(RUNS_DIR.glob("run_*.json"))
    if not runs:
        return None
    return json.loads(runs[-1].read_text())


def quality_report() -> dict | None:
    if not QUALITY_REPORT.exists():
        return None
    return json.loads(QUALITY_REPORT.read_text())


def collect() -> dict:
    """Assemble un instantané de métriques, au format proche d'une mise en
    correspondance CloudWatch (namespace/metric/value)."""
    run = latest_run_report()
    quality = quality_report()

    volumes = {
        "bronze_taxi_rows": row_count(f"{settings.BRONZE}/taxi"),
        "bronze_weather_rows": row_count(f"{settings.BRONZE}/weather"),
        "silver_trips_rows": row_count(f"{settings.SILVER}/trips"),
        "gold_demand_by_zone_rows": row_count(f"{settings.GOLD}/fact_demand_by_zone"),
        "gold_weather_impact_rows": row_count(f"{settings.GOLD}/fact_weather_impact"),
        "gold_tips_rows": row_count(f"{settings.GOLD}/fact_tips"),
        "gold_demand_forecast_rows": row_count(f"{settings.GOLD}/fact_demand_forecast"),
    }

    step_durations = {}
    pipeline_success = None
    pipeline_duration_s = None
    if run:
        pipeline_success = run["success"]
        pipeline_duration_s = run["duration_s"]
        for name, s in run["steps"].items():
            attempts = s.get("attempts", [])
            step_durations[name] = {
                "status": s["status"],
                "attempts": len(attempts),
                "duration_s": sum(a["duration_s"] for a in attempts) if attempts else None,
            }

    rejection_rate_pct = None
    quality_success = None
    quality_pass_ratio = None
    if quality:
        quality_success = quality["success"]
        quality_pass_ratio = quality["expectations_passed"] / max(quality["expectations_total"], 1)

    if volumes["bronze_taxi_rows"] and volumes["silver_trips_rows"] is not None:
        rejection_rate_pct = round(
            (1 - volumes["silver_trips_rows"] / volumes["bronze_taxi_rows"]) * 100, 2
        )

    return {
        "pipeline_success": pipeline_success,
        "pipeline_duration_s": pipeline_duration_s,
        "step_durations": step_durations,
        "quality_success": quality_success,
        "quality_pass_ratio": quality_pass_ratio,
        "rejection_rate_pct": rejection_rate_pct,
        "volumes": volumes,
    }
