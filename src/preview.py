"""Exploration visuelle du data lake : affiche chaque couche (Bronze,
Silver, Gold) avec volumétrie, colonnes et échantillon.

Usage :
    python -m src.preview            # toutes les couches
    python -m src.preview bronze     # une seule couche
    python -m src.preview silver
    python -m src.preview gold
"""
from __future__ import annotations

import sys

from config import settings
from src.common.spark import get_spark


def _title(txt: str) -> None:
    print("\n" + "=" * 78)
    print(f"  {txt}")
    print("=" * 78)


def _show(spark, path: str, label: str, sample: int = 5, cols=None) -> None:
    try:
        df = spark.read.parquet(path)
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠ {label} indisponible ({path}) — as-tu lancé le job ? {e}")
        return
    n = df.count()
    print(f"\n▶ {label}")
    print(f"  chemin  : {path}")
    print(f"  lignes  : {n:,}")
    print(f"  colonnes: {len(df.columns)} -> {', '.join(df.columns)}")
    view = df.select(*cols) if cols else df
    view.show(sample, truncate=20, vertical=False)


def preview_bronze(spark) -> None:
    _title("COUCHE BRONZE — ingestion brute")
    _show(spark, f"{settings.BRONZE}/taxi", "bronze/taxi",
          cols=["tpep_pickup_datetime", "PULocationID", "DOLocationID",
                "trip_distance", "fare_amount", "tip_amount", "year", "month"])
    _show(spark, f"{settings.BRONZE}/zones", "bronze/zones",
          cols=["LocationID", "Borough", "Zone", "service_zone"])
    _show(spark, f"{settings.BRONZE}/weather", "bronze/weather",
          cols=["ID", "DATE", "ELEMENT", "DATA_VALUE", "year"])


def preview_silver(spark) -> None:
    _title("COUCHE SILVER — nettoyée, enrichie, jointe")
    _show(spark, f"{settings.SILVER}/weather_daily", "silver/weather_daily",
          cols=["date", "tmax_c", "tmin_c", "prcp_mm", "is_rain", "is_snow"])
    _show(spark, f"{settings.SILVER}/trips", "silver/trips",
          cols=["pickup_date", "pickup_hour", "pu_borough", "do_borough",
                "trip_distance", "trip_duration_min", "fare_amount",
                "tip_pct", "payment_label", "tavg_c", "is_rain"])


def preview_gold(spark) -> None:
    _title("COUCHE GOLD — datamarts métier (P1/P2/P3)")
    _show(spark, f"{settings.GOLD}/fact_demand_by_zone",
          "gold/fact_demand_by_zone (P1 demande)",
          cols=["pickup_date", "pickup_hour", "pu_borough", "pu_zone",
                "trips_count", "avg_fare", "tavg_c", "is_rain"])
    _show(spark, f"{settings.GOLD}/fact_weather_impact",
          "gold/fact_weather_impact (P2 météo)",
          cols=["pickup_date", "trips_count", "avg_duration_min",
                "tavg_c", "prcp_mm", "weather_condition"])
    _show(spark, f"{settings.GOLD}/fact_tips",
          "gold/fact_tips (P3 pourboires)",
          cols=["pu_borough", "pickup_hour", "is_weekend",
                "weather_condition", "trips_count", "avg_tip_pct"])


LAYERS = {"bronze": preview_bronze, "silver": preview_silver, "gold": preview_gold}


def main() -> int:
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    spark = get_spark("preview")
    targets = LAYERS.keys() if which == "all" else [which]
    for t in targets:
        LAYERS[t](spark)
    spark.stop()
    print("\n" + "=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
