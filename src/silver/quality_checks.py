"""SILVER — tests de qualité automatisés (Great Expectations).

Ce module est un *gate* de la pipeline : il s'exécute après la construction
de silver/trips et AVANT la couche gold. Si une attente critique échoue, le
job renvoie un code de sortie non nul → l'orchestrateur (Step Functions)
stoppe le flux et déclenche une alerte.

Règles couvertes : complétude (nulls), plages de valeurs (aberrations),
domaines (codes de paiement), fraîcheur/cohérence. Un rapport JSON est écrit
sous silver/_quality pour la traçabilité et le monitoring.

Usage :
    python -m src.silver.quality_checks
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import great_expectations as gx

from config import settings
from src.common.spark import get_spark

# Colonnes dont l'absence de valeur invalide la ligne métier.
NOT_NULL_COLUMNS = [
    "pickup_date", "pickup_hour", "PULocationID", "DOLocationID",
    "pu_borough", "do_borough", "fare_amount", "total_amount",
    "trip_distance", "trip_duration_min",
]

VALID_BOROUGHS = [
    "Manhattan", "Brooklyn", "Queens", "Bronx",
    "Staten Island", "EWR", "Unknown", "N/A",
]

# payment_type : le dictionnaire officiel TLC ne documente que 1..6, mais les
# données récentes contiennent aussi la valeur 0 (~6,6 % des lignes, non
# documentée). On l'accepte dans le domaine et on la mappe en "Unknown"
# (cf. build_trips) plutôt que de rejeter ces lignes — anomalie tracée.
VALID_PAYMENT_TYPES = [0, 1, 2, 3, 4, 5, 6]


def build_suite(validator):
    """Déclare les attentes de qualité sur la table silver/trips."""
    # 1. Complétude
    for col in NOT_NULL_COLUMNS:
        validator.expect_column_values_to_not_be_null(col)

    # 2. Plages de valeurs (valeurs aberrantes déjà filtrées en amont)
    validator.expect_column_values_to_be_between("fare_amount", 0, 1000)
    validator.expect_column_values_to_be_between("trip_distance", 0, 200, strict_min=True)
    validator.expect_column_values_to_be_between("trip_duration_min", 0, 24 * 60, strict_min=True)
    validator.expect_column_values_to_be_between("pickup_hour", 0, 23)
    validator.expect_column_values_to_be_between("passenger_count", 0, 9, mostly=0.99)
    # Plausibilité physique (distance/temps cohérents). Borne basse à 0
    # inclus : une course en embouteillage peut arrondir à 0,0 mph.
    validator.expect_column_values_to_be_between("avg_speed_mph", 0, 80)
    # tip_pct : null autorisé (tarif < seuil ou pourboire > tarif) ; les
    # valeurs présentes sont désormais bornées à [0 ; 100] (nulls ignorés par GE).
    validator.expect_column_values_to_be_between("tip_pct", 0, 100)

    # 3. Domaines de valeurs
    validator.expect_column_values_to_be_in_set("pu_borough", VALID_BOROUGHS)
    validator.expect_column_values_to_be_in_set("do_borough", VALID_BOROUGHS)
    validator.expect_column_values_to_be_in_set("payment_type", VALID_PAYMENT_TYPES)

    # 4. Cohérence temporelle
    validator.expect_column_values_to_be_between(
        "pickup_date",
        min_value="2016-01-01",  # contrainte RGPD : données >= 2016
        max_value=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    )
    return validator


def main() -> int:
    spark = get_spark("silver_quality_checks")
    print(f"== SILVER quality checks (env={settings.ENV}) ==")

    df = spark.read.parquet(f"{settings.SILVER}/trips")

    context = gx.get_context(mode="ephemeral")
    datasource = context.sources.add_or_update_spark("spark_ds")
    asset = datasource.add_dataframe_asset("trips")
    batch_request = asset.build_batch_request(dataframe=df)
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="silver_trips_suite",
    )

    build_suite(validator)
    results = validator.validate()

    # --- Synthèse ---
    stats = results["statistics"]
    n_ok = stats["successful_expectations"]
    n_total = stats["evaluated_expectations"]
    success = results["success"]

    failed = [
        r["expectation_config"]["expectation_type"]
        + " / " + r["expectation_config"]["kwargs"].get("column", "?")
        for r in results["results"] if not r["success"]
    ]

    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "table": "silver/trips",
        "rows": df.count(),
        "expectations_passed": n_ok,
        "expectations_total": n_total,
        "success": success,
        "failed_expectations": failed,
    }

    # Écriture du rapport (traçabilité + monitoring)
    if settings.ENV == "aws":
        import boto3

        boto3.client("s3").put_object(
            Bucket=settings.S3_BUCKET,
            Key="silver/_quality/trips_quality_report.json",
            Body=json.dumps(report, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    else:
        out_dir = Path(settings.SILVER) / "_quality"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "trips_quality_report.json").write_text(json.dumps(report, indent=2))

    print(f"  attentes réussies : {n_ok}/{n_total}")
    if failed:
        print("  ÉCHECS :")
        for f in failed:
            print(f"    ✗ {f}")
    print(f"  résultat global   : {'✓ PASS' if success else '✗ FAIL'}")

    spark.stop()
    # Gate : code de sortie non nul si échec (stoppe l'orchestrateur)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
