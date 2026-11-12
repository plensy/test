"""Audit critique des couches Silver et Gold.

But : chercher activement ce qui pourrait être MAL nettoyé, plutôt que de
confirmer que tout va bien. Chaque test affiche PASS/FAIL avec la mesure.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from config import settings
from src.common.spark import get_spark

spark = get_spark("audit")
FAILS = []


def check(name: str, ok: bool, detail: str = "") -> None:
    tag = "\033[1;32mPASS\033[0m" if ok else "\033[1;31mFAIL\033[0m"
    print(f"  [{tag}] {name}" + (f"  → {detail}" if detail else ""))
    if not ok:
        FAILS.append(name)


trips = spark.read.parquet(f"{settings.SILVER}/trips")
n = trips.count()

print("\n=== SILVER / trips — nettoyage ===")

# Update of a comment 
crit = ["pickup_date", "PULocationID", "DOLocationID", "pu_borough",
        "do_borough", "fare_amount", "total_amount", "trip_distance",
        "trip_duration_min"]
nulls = trips.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in crit]).collect()[0].asDict()
worst = max(nulls.values())
check("aucun null dans les colonnes critiques", worst == 0, f"max nulls = {worst} ({nulls})")

# 2. Valeurs aberrantes qui auraient dû être filtrées
n_neg_fare = trips.filter(F.col("fare_amount") < 0).count()
check("aucun fare_amount négatif", n_neg_fare == 0, f"{n_neg_fare} lignes")

n_bad_dur = trips.filter((F.col("trip_duration_min") <= 0) | (F.col("trip_duration_min") > 24 * 60)).count()
check("durée dans ]0 ; 1440] min", n_bad_dur == 0, f"{n_bad_dur} hors bornes")

n_bad_dist = trips.filter((F.col("trip_distance") <= 0) | (F.col("trip_distance") > 200)).count()
check("distance dans ]0 ; 200] mi", n_bad_dist == 0, f"{n_bad_dist} hors bornes")

n_bad_time = trips.filter(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")).count()
check("dropoff strictement après pickup", n_bad_time == 0, f"{n_bad_time} incohérentes")

# 3. Doublons
n_dups = n - trips.dropDuplicates().count()
check("aucun doublon strict", n_dups == 0, f"{n_dups} doublons")

# 4. Fuite hors périmètre (mois non ingérés)
allowed = [y * 100 + m for (y, m) in settings.TAXI_MONTHS]
n_leak = trips.withColumn("ym", F.year("pickup_date") * 100 + F.month("pickup_date")) \
              .filter(~F.col("ym").isin(allowed)).count()
check("aucune course hors des mois du périmètre", n_leak == 0, f"{n_leak} hors périmètre")

# 5. Cohérence de la feature durée vs timestamps (recalcul indépendant)
recomputed = trips.withColumn(
    "dur_check",
    F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0, 2),
)
n_dur_mismatch = recomputed.filter(F.abs(F.col("dur_check") - F.col("trip_duration_min")) > 0.01).count()
check("trip_duration_min cohérent avec les timestamps", n_dur_mismatch == 0, f"{n_dur_mismatch} écarts")

# 6. Cohérence is_weekend
n_we_mismatch = trips.filter(
    (F.col("is_weekend") == 1) & (~F.col("pickup_dow").isin(1, 7))
    | (F.col("is_weekend") == 0) & (F.col("pickup_dow").isin(1, 7))
).count()
check("is_weekend cohérent avec pickup_dow", n_we_mismatch == 0, f"{n_we_mismatch} écarts")

# 7. Zones réellement résolues (pas juste non-null : libellé présent)
n_zone_unresolved = trips.filter(F.col("pu_zone").isNull() | F.col("do_zone").isNull()).count()
check("zones PU/DO résolues (libellé)", n_zone_unresolved == 0, f"{n_zone_unresolved} non résolues")

# --- Signaux d'alerte (pas des échecs stricts mais à connaître) ---
print("\n=== SILVER — signaux à surveiller (non bloquants) ===")
n_null_pax = trips.filter(F.col("passenger_count").isNull()).count()
print(f"  ℹ passenger_count null : {n_null_pax:,} ({n_null_pax/n*100:.2f} %)")
mx_speed = trips.agg(F.max("avg_speed_mph")).collect()[0][0]
n_fast = trips.filter(F.col("avg_speed_mph") > 100).count()
print(f"  ℹ avg_speed_mph max = {mx_speed} ; > 100 mph : {n_fast:,} lignes")
mx_tip = trips.agg(F.max("tip_pct")).collect()[0][0]
n_hitip = trips.filter(F.col("tip_pct") > 100).count()
print(f"  ℹ tip_pct max = {mx_tip} ; > 100 % : {n_hitip:,} lignes")

# --- GOLD ---
print("\n=== GOLD — datamarts ===")
dz = spark.read.parquet(f"{settings.GOLD}/fact_demand_by_zone")
tips = spark.read.parquet(f"{settings.GOLD}/fact_tips")
wi = spark.read.parquet(f"{settings.GOLD}/fact_weather_impact")

check("fact_demand_by_zone : k-anonymat k>=5 respecté",
      dz.filter(F.col("trips_count") < 5).count() == 0,
      f"{dz.filter(F.col('trips_count') < 5).count()} cellules < 5")
check("fact_tips : k-anonymat k>=5 respecté",
      tips.filter(F.col("trips_count") < 5).count() == 0,
      f"{tips.filter(F.col('trips_count') < 5).count()} cellules < 5")

# Réconciliation : somme des courses gold (avant k-anon) ≈ silver
# On revérifie la cohérence d'agrégat sur fact_weather_impact (pas de k-anon)
sum_wi = wi.agg(F.sum("trips_count")).collect()[0][0]
check("fact_weather_impact : total = total silver (pas de perte)",
      sum_wi == n, f"gold={sum_wi:,} vs silver={n:,}")

check("fact_weather_impact : 91 jours (jan+fév+mars 2024)",
      wi.count() == 91, f"{wi.count()} jours")

# Nulls dans dimensions gold
n_dz_null = dz.filter(F.col("pu_borough").isNull() | F.col("pickup_date").isNull()).count()
check("fact_demand_by_zone : dimensions sans null", n_dz_null == 0, f"{n_dz_null} nulls")

print("\n" + "=" * 60)
if FAILS:
    print(f"\033[1;31m✗ {len(FAILS)} test(s) en échec : {FAILS}\033[0m")
else:
    print("\033[1;32m✓ Tous les tests d'audit sont au vert.\033[0m")
spark.stop()
