"""Accès aux datamarts gold pour l'API et le dashboard.

Les datamarts sont petits (agrégats k-anonymisés) : on les charge en mémoire
avec pandas au démarrage. Cela reproduit le choix d'architecture AWS —
servir des agrégats pré-matérialisés (Lambda/API Gateway) plutôt que
d'interroger Athena en direct à chaque requête (latence trop élevée).
"""
from __future__ import annotations

from functools import lru_cache

import pandas as pd

from config import settings


@lru_cache(maxsize=None)
def load(datamart: str) -> pd.DataFrame:
    """Charge (et met en cache) un datamart gold en DataFrame pandas."""
    path = f"{settings.GOLD}/{datamart}"
    # pandas + pyarrow lisent un répertoire Parquet partitionné par Spark.
    return pd.read_parquet(path, engine="pyarrow")


DATAMARTS = [
    "fact_demand_by_zone",
    "fact_weather_impact",
    "fact_tips",
    "fact_demand_forecast",
]
