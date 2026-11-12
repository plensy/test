"""Point d'entrée monitoring : collecte les métriques, évalue les seuils,
déclenche les alertes. À lancer après un run de la pipeline (dernière étape
de run_local_pipeline.sh, ou job séparé sur planification en production).

Usage :
    python -m src.monitoring.check_and_alert
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from config import settings
from src.monitoring import alerting, metrics

METRICS_PATH = settings.PROJECT_ROOT / "monitoring" / "_metrics" / "latest.json"


def main() -> int:
    print("== Monitoring : collecte des métriques ==")
    m = metrics.collect()
    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    METRICS_PATH.write_text(json.dumps(
        {"collected_at": datetime.now(timezone.utc).isoformat(), **m}, indent=2
    ))

    print(f"  pipeline_success      : {m['pipeline_success']}")
    print(f"  pipeline_duration_s   : {m['pipeline_duration_s']}")
    print(f"  quality_success       : {m['quality_success']}")
    print(f"  rejection_rate_pct    : {m['rejection_rate_pct']}")
    for table, n in m["volumes"].items():
        print(f"  {table:<28}: {n}")

    print("\n== Évaluation des seuils d'alerte ==")
    alerts = alerting.evaluate(m)
    if not alerts:
        print("  ✓ aucun seuil dépassé — pas d'alerte")
    else:
        alerting.dispatch(alerts)

    critical = any(a["severity"] == "CRITICAL" for a in alerts)
    return 1 if critical else 0


if __name__ == "__main__":
    sys.exit(main())
