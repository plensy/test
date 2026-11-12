"""Fabrique de session Spark partagée par tous les jobs.

En local : Spark autonome (master local[*]).
Sur AWS Glue : la session est fournie par le runtime Glue ; ce module
renvoie alors la session active existante.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from config import settings

# En mode local, `spark.driver.memory` passé via .config() est ignoré car la
# JVM du driver est déjà démarrée. On le fixe donc avant tout démarrage de JVM
# via PYSPARK_SUBMIT_ARGS (les jobs appellent get_spark en tout premier).
if settings.ENV == "local" and "PYSPARK_SUBMIT_ARGS" not in os.environ:
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--driver-memory 6g --conf spark.driver.maxResultSize=2g pyspark-shell"
    )


def get_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "America/New_York")
        # Évite un partitionnement excessif de petits fichiers en dev local.
        .config("spark.sql.shuffle.partitions", "8")
        # Overwrite DYNAMIQUE : une écriture n'écrase que les partitions
        # présentes dans le batch (indispensable pour l'ingestion
        # incrémentale mois par mois ; le mode statique par défaut
        # effacerait toute la table à chaque run).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if settings.ENV == "local":
        # La mémoire driver est fixée via PYSPARK_SUBMIT_ARGS ci-dessus.
        builder = builder.master("local[*]").config("spark.ui.enabled", "false")
    return builder.getOrCreate()
