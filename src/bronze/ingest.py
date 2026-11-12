"""Couche BRONZE — ingestion brute, sans transformation de valeur.

Principe médaillon : on stocke les données dans l'état brut de la source.
Les seuls ajouts sont techniques (colonnes de partition + horodatage
d'ingestion pour la traçabilité). Aucune valeur métier n'est modifiée.

Formats et partitionnement :
  - taxi    : Parquet, partitionné par year/month (unité d'ingestion
              incrémentale = 1 fichier mensuel TLC).
  - weather : Parquet, partitionné par year (dérivé de la date brute).
  - zones   : Parquet, non partitionné (table de référence statique,
              rechargée en full à chaque run).

Usage :
    python -m src.bronze.ingest --dataset taxi
    python -m src.bronze.ingest --dataset weather
    python -m src.bronze.ingest --dataset zones
    python -m src.bronze.ingest --dataset all
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

from config import settings
from src.common.spark import get_spark


def ingest_taxi(spark) -> None:
    """Ingestion des fichiers Yellow Taxi mensuels en Bronze."""
    for year, month in settings.TAXI_MONTHS:
        fname = settings.TAXI_FILE_TPL.format(year=year, month=month)
        src = f"{settings.LANDING}/taxi/{fname}"
        df = spark.read.parquet(src)
        n = df.count()
        out = (
            df.withColumn("year", F.lit(year))
            .withColumn("month", F.lit(month))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(fname))
        )
        (
            out.write.mode("overwrite")
            .partitionBy("year", "month")
            .parquet(f"{settings.BRONZE}/taxi")
        )
        print(f"  ✓ taxi {year}-{month:02d} : {n:,} lignes -> bronze/taxi")


def ingest_zones(spark) -> None:
    """Ingestion de la table de correspondance des zones (full reload)."""
    src = f"{settings.LANDING}/zones/taxi_zone_lookup.csv"
    df = spark.read.option("header", True).csv(src)
    n = df.count()
    (
        df.withColumn("_ingested_at", F.current_timestamp())
        .write.mode("overwrite")
        .parquet(f"{settings.BRONZE}/zones")
    )
    print(f"  ✓ zones : {n:,} lignes -> bronze/zones")


def ingest_weather(spark) -> None:
    """Ingestion de la météo GHCN (format long brut conservé tel quel)."""
    src = f"{settings.LANDING}/weather/{settings.WEATHER_STATION}.csv"
    df = spark.read.option("header", True).csv(src)
    # DATE brute = 'YYYYMMDD' -> on dérive uniquement la colonne de partition.
    df = df.withColumn("year", F.substring(F.col("DATE"), 1, 4).cast("int"))
    n = df.count()
    (
        df.withColumn("_ingested_at", F.current_timestamp())
        .write.mode("overwrite")
        .partitionBy("year")
        .parquet(f"{settings.BRONZE}/weather")
    )
    print(f"  ✓ weather : {n:,} lignes (station {settings.WEATHER_STATION}) -> bronze/weather")


DISPATCH = {"taxi": ingest_taxi, "zones": ingest_zones, "weather": ingest_weather}


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingestion Bronze")
    parser.add_argument(
        "--dataset", required=True, choices=[*DISPATCH.keys(), "all"]
    )
    args = parser.parse_args()

    spark = get_spark(f"bronze_ingest_{args.dataset}")
    print(f"== BRONZE ingestion : {args.dataset} (env={settings.ENV}) ==")
    targets = DISPATCH.keys() if args.dataset == "all" else [args.dataset]
    for t in targets:
        DISPATCH[t](spark)
    spark.stop()
    print("== Bronze terminé ==")
    return 0


if __name__ == "__main__":
    sys.exit(main())
