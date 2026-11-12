"""GOLD — datamarts agrégés répondant aux 3 problématiques métier.

  P1  fact_demand_by_zone   : demande (nb courses) par zone/date/heure,
                              enrichie météo → base de prévision + service API.
  P2  fact_weather_impact   : agrégats quotidiens vs météo → impact pluie/
                              neige/température sur volumes et durées.
  P3  fact_tips             : comportement de pourboire par segment
                              (borough, heure, météo, paiement).

RGPD — k-anonymat : les cellules d'agrégat exposées comptant moins de
K_ANON courses sont supprimées, de sorte qu'aucune ligne publiée ne
distingue moins de K_ANON trajets.

Sortie : gold/<datamart> (Parquet, prêt pour Athena / API / dashboard).
"""
from __future__ import annotations

import sys

from pyspark.sql import functions as F

from config import settings
from src.common.spark import get_spark

K_ANON = 5


def build_demand_by_zone(trips):
    """P1 — demande par (date, heure, zone de prise en charge)."""
    daily_weather = trips.groupBy("pickup_date").agg(
        F.first("tavg_c").alias("tavg_c"),
        F.first("prcp_mm").alias("prcp_mm"),
        F.first("is_rain").alias("is_rain"),
        F.first("is_snow").alias("is_snow"),
    )
    agg = (
        trips.groupBy(
            "pickup_date", "pickup_hour", "pickup_dow", "is_weekend",
            "PULocationID", "pu_borough", "pu_zone",
        )
        .agg(
            F.count("*").alias("trips_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
        )
        .join(daily_weather, "pickup_date", "left")
        .filter(F.col("trips_count") >= K_ANON)  # k-anonymat
    )
    return agg


def build_weather_impact(trips):
    """P2 — agrégats quotidiens confrontés à la météo."""
    return (
        trips.groupBy("pickup_date").agg(
            F.count("*").alias("trips_count"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
            F.round(F.avg("avg_speed_mph"), 1).alias("avg_speed_mph"),
            F.first("tavg_c").alias("tavg_c"),
            F.first("tmax_c").alias("tmax_c"),
            F.first("tmin_c").alias("tmin_c"),
            F.first("prcp_mm").alias("prcp_mm"),
            F.first("snow_mm").alias("snow_mm"),
            F.first("is_rain").alias("is_rain"),
            F.first("is_snow").alias("is_snow"),
        )
        .withColumn(
            "weather_condition",
            F.when(F.col("is_snow") == 1, "Neige")
            .when(F.col("is_rain") == 1, "Pluie")
            .otherwise("Sec"),
        )
        .orderBy("pickup_date")
    )


def build_tips(trips):
    """P3 — pourboire par segment métier.

    Le pourboire n'est enregistré que pour les paiements par carte ; on
    restreint donc l'analyse à payment_label = 'Credit card'.
    """
    return (
        trips.filter(F.col("payment_label") == "Credit card")
        .withColumn(
            "weather_condition",
            F.when(F.col("is_snow") == 1, "Neige")
            .when(F.col("is_rain") == 1, "Pluie")
            .otherwise("Sec"),
        )
        .groupBy("pu_borough", "pickup_hour", "is_weekend", "weather_condition")
        .agg(
            F.count("*").alias("trips_count"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip_amount"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        )
        .filter(F.col("trips_count") >= K_ANON)  # k-anonymat
    )


def main() -> int:
    spark = get_spark("gold_datamarts")
    print(f"== GOLD datamarts (env={settings.ENV}) ==")

    trips = spark.read.parquet(f"{settings.SILVER}/trips")

    datamarts = {
        "fact_demand_by_zone": build_demand_by_zone(trips),
        "fact_weather_impact": build_weather_impact(trips),
        "fact_tips": build_tips(trips),
    }
    for name, df in datamarts.items():
        n = df.count()
        df.write.mode("overwrite").parquet(f"{settings.GOLD}/{name}")
        print(f"  ✓ {name} : {n:,} lignes -> gold/{name}")

    spark.stop()
    print("== Gold terminé ==")
    return 0


if __name__ == "__main__":
    sys.exit(main())
