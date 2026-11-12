"""SILVER — courses taxi nettoyées, enrichies et jointes.

Étapes :
  1. Nettoyage / règles de qualité (nulls, doublons, valeurs aberrantes).
  2. Typage et features métier (durée, %pourboire, calendrier).
  3. Jointures : zones (PU + DO) et météo quotidienne (par date de pickup).
  4. RGPD by-design : granularité zone (pas de GPS), aucun identifiant direct.

Sortie : silver/trips (partitionné year/month), + un rapport de rejets
pour la traçabilité qualité.
"""
from __future__ import annotations

import sys

from pyspark.sql import functions as F

from config import settings
from src.common.spark import get_spark

# Bornes de plausibilité (valeurs aberrantes rejetées).
MAX_TRIP_MINUTES = 24 * 60      # 24 h
MAX_TRIP_DISTANCE_MI = 200.0    # trajet intra-NYC réaliste
MAX_FARE = 1000.0
MAX_SPEED_MPH = 80.0            # au-delà : distance/temps corrompus (impossible en ville)
MIN_FARE_FOR_TIP = 3.0         # tarif minimum NYC 2024 ; en-deçà, tip_pct non fiable

PAYMENT_LABELS = {
    1: "Credit card", 2: "Cash", 3: "No charge",
    4: "Dispute", 5: "Unknown", 6: "Voided trip",
}


def main() -> int:
    spark = get_spark("silver_trips")
    print(f"== SILVER trips (env={settings.ENV}) ==")

    trips = spark.read.parquet(f"{settings.BRONZE}/taxi")
    zones = spark.read.parquet(f"{settings.BRONZE}/zones")
    weather = spark.read.parquet(f"{settings.SILVER}/weather_daily")

    n_raw = trips.count()

    # --- 1. Périmètre temporel : pickup dans un mois réellement ingéré ---
    allowed_ym = [y * 100 + m for (y, m) in settings.TAXI_MONTHS]
    trips = trips.withColumn(
        "_pickup_ym",
        F.year("tpep_pickup_datetime") * 100 + F.month("tpep_pickup_datetime"),
    )
    trips = trips.filter(F.col("_pickup_ym").isin(allowed_ym))

    # --- 2. Features de durée (nécessaires aux règles de qualité) ---
    trips = trips.withColumn(
        "trip_duration_min",
        F.round(
            (F.unix_timestamp("tpep_dropoff_datetime")
             - F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
            2,
        ),
    )
    # Vitesse moyenne — calculée ici car elle sert de règle de qualité
    # (une vitesse impossible révèle une distance ou une durée corrompue).
    trips = trips.withColumn(
        "avg_speed_mph",
        F.when(F.col("trip_duration_min") > 0,
               F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60.0), 1))
        .otherwise(None),
    )

    # --- 3. Règles de qualité : rejet des lignes aberrantes ---
    clean = trips.filter(
        (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        & (F.col("trip_duration_min") <= MAX_TRIP_MINUTES)
        & (F.col("trip_distance") > 0) & (F.col("trip_distance") <= MAX_TRIP_DISTANCE_MI)
        & (F.col("fare_amount") >= 0) & (F.col("fare_amount") <= MAX_FARE)
        & (F.col("total_amount") >= 0)
        & F.col("PULocationID").isNotNull() & F.col("DOLocationID").isNotNull()
        # Plausibilité physique : écarte les enregistrements distance/temps corrompus.
        & (F.col("avg_speed_mph") <= MAX_SPEED_MPH)
    )

    # --- 4. Déduplication (lignes strictement identiques) ---
    clean = clean.dropDuplicates()

    # --- 5. Features métier ---
    clean = (
        clean.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dow", F.dayofweek("tpep_pickup_datetime"))  # 1=dim
        .withColumn("is_weekend", F.col("pickup_dow").isin(1, 7).cast("int"))
        .withColumn(
            "tip_pct",
            # Ratio fiable seulement si (a) le tarif est un dénominateur
            # crédible (>= tarif min NYC) ET (b) le pourboire ne dépasse pas
            # le tarif (au-delà = quasi toujours une erreur de saisie).
            # Sinon null → n'entache pas les moyennes des datamarts gold.
            F.when(
                (F.col("fare_amount") >= MIN_FARE_FOR_TIP)
                & (F.col("tip_amount") <= F.col("fare_amount")),
                F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2),
            ).otherwise(None),
        )
        .withColumn("is_airport", F.col("RatecodeID").isin(2, 3).cast("int"))
        .withColumn(
            "payment_label",
            F.coalesce(
                F.create_map([F.lit(x) for kv in PAYMENT_LABELS.items() for x in kv])
                .getItem(F.col("payment_type")),
                F.lit("Unknown"),
            ),
        )
    )

    # --- 6. Jointures zones (PU puis DO) ---
    zpu = zones.select(
        F.col("LocationID").cast("int").alias("PULocationID"),
        F.col("Borough").alias("pu_borough"),
        F.col("Zone").alias("pu_zone"),
    )
    zdo = zones.select(
        F.col("LocationID").cast("int").alias("DOLocationID"),
        F.col("Borough").alias("do_borough"),
        F.col("Zone").alias("do_zone"),
    )
    clean = clean.join(F.broadcast(zpu), "PULocationID", "left") \
                 .join(F.broadcast(zdo), "DOLocationID", "left")

    # --- 7. Jointure météo par date de pickup ---
    clean = clean.join(F.broadcast(weather), clean.pickup_date == weather.date, "left") \
                 .drop("date")

    # --- 8. Sélection finale (schéma silver stable) ---
    silver = clean.select(
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "pickup_date", "pickup_hour", "pickup_dow", "is_weekend",
        "PULocationID", "pu_borough", "pu_zone",
        "DOLocationID", "do_borough", "do_zone",
        "passenger_count", "trip_distance", "trip_duration_min", "avg_speed_mph",
        "RatecodeID", "is_airport",
        "payment_type", "payment_label",
        "fare_amount", "tip_amount", "tip_pct", "tolls_amount", "total_amount",
        "congestion_surcharge",
        "tmax_c", "tmin_c", "tavg_c", "prcp_mm", "snow_mm", "is_rain", "is_snow",
        "year", "month",
    )

    n_clean = silver.count()
    (
        silver.write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(f"{settings.SILVER}/trips")
    )

    rejected = n_raw - n_clean
    print(f"  brut       : {n_raw:,} lignes")
    print(f"  silver     : {n_clean:,} lignes")
    print(f"  rejetées   : {rejected:,} ({rejected/n_raw*100:.2f} %)")
    print("  ✓ -> silver/trips")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
