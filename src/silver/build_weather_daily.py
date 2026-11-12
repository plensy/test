"""SILVER — météo quotidienne (format long GHCN -> large exploitable).

Le brut GHCN est au format long : une ligne par (date, élément). On :
  1. filtre les éléments utiles (TMAX, TMIN, PRCP, SNOW, SNWD) ;
  2. restreint aux années du périmètre ;
  3. pivote en une ligne par jour ;
  4. convertit les unités (GHCN stocke en dixièmes) ;
  5. dérive des indicateurs métier (pluie / neige).

Sortie : silver/weather_daily (une ligne par date).
"""
from __future__ import annotations

import sys

from pyspark.sql import functions as F

from config import settings
from src.common.spark import get_spark


def main() -> int:
    spark = get_spark("silver_weather_daily")
    print(f"== SILVER weather_daily (env={settings.ENV}) ==")

    df = spark.read.parquet(f"{settings.BRONZE}/weather")

    df = (
        df.filter(F.col("ELEMENT").isin(settings.WEATHER_ELEMENTS))
        .filter(F.col("year").isin(settings.WEATHER_YEARS))
        .withColumn("date", F.to_date(F.col("DATE"), "yyyyMMdd"))
        .withColumn("value", F.col("DATA_VALUE").cast("double"))
    )

    # Pivot : une colonne par élément météo.
    wide = (
        df.groupBy("date")
        .pivot("ELEMENT", settings.WEATHER_ELEMENTS)
        .agg(F.first("value"))
    )

    # Conversion d'unités GHCN -> unités physiques.
    #   TMAX/TMIN : dixièmes de °C  | PRCP : dixièmes de mm  | SNOW/SNWD : mm
    daily = (
        wide.withColumn("tmax_c", F.round(F.col("TMAX") / 10.0, 1))
        .withColumn("tmin_c", F.round(F.col("TMIN") / 10.0, 1))
        .withColumn("tavg_c", F.round((F.col("TMAX") + F.col("TMIN")) / 20.0, 1))
        .withColumn("prcp_mm", F.round(F.col("PRCP") / 10.0, 1))
        .withColumn("snow_mm", F.col("SNOW"))
        .withColumn("snow_depth_mm", F.col("SNWD"))
        .withColumn("is_rain", (F.coalesce(F.col("PRCP"), F.lit(0)) > 0).cast("int"))
        .withColumn("is_snow", (F.coalesce(F.col("SNOW"), F.lit(0)) > 0).cast("int"))
        .select(
            "date", "tmax_c", "tmin_c", "tavg_c",
            "prcp_mm", "snow_mm", "snow_depth_mm", "is_rain", "is_snow",
        )
    )

    n = daily.count()
    daily.write.mode("overwrite").parquet(f"{settings.SILVER}/weather_daily")
    print(f"  ✓ weather_daily : {n:,} jours -> silver/weather_daily")

    daily.orderBy("date").show(5)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
