"""Configuration centrale de la pipeline.

Un seul point de vérité pour les chemins et paramètres, avec bascule
entre l'environnement de développement local (système de fichiers) et
AWS (S3). La variable d'environnement PIPELINE_ENV pilote la bascule :

    PIPELINE_ENV=local  -> lit/écrit sous ./data (défaut)
    PIPELINE_ENV=aws    -> lit/écrit sous s3://<S3_BUCKET>/...

Ce même module est importé par les jobs Bronze/Silver/Gold pour que le
code local et le code déployé sur Glue soient rigoureusement identiques.
"""
from __future__ import annotations

import os
from pathlib import Path

# Racine du projet (…/PROJET DATA PIPELINE)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

ENV = os.environ.get("PIPELINE_ENV", "local")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "nyc-pipeline-datalake")  # à créer au déploiement

# --- Racines du data lake selon l'environnement ---
if ENV == "aws":
    # AWS Glue utilise son propre connecteur S3 optimisé enregistré sous le
    # schéma "s3://" (contrairement à un Spark/EMR open source classique qui
    # utiliserait "s3a://" via le connecteur Hadoop-AWS).
    _ROOT = f"s3://{S3_BUCKET}"
    LANDING = f"{_ROOT}/_landing"
    BRONZE = f"{_ROOT}/bronze"
    SILVER = f"{_ROOT}/silver"
    GOLD = f"{_ROOT}/gold"
else:
    _ROOT = str(PROJECT_ROOT / "data")
    LANDING = f"{_ROOT}/_landing"
    BRONZE = f"{_ROOT}/bronze"
    SILVER = f"{_ROOT}/silver"
    GOLD = f"{_ROOT}/gold"

# --- Sources publiques ---
# Fichiers Parquet Yellow Taxi servis par le CloudFront officiel de la TLC.
TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_FILE_TPL = "yellow_tripdata_{year}-{month:02d}.parquet"

# Table de correspondance des zones (statique).
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Météo NOAA GHCN-Daily — station Central Park.
# On récupère le fichier par station (16 Mo, historique complet d'une seule
# station) plutôt que le fichier annuel global (1,3 Go, toutes les stations
# du monde) : même donnée utile, coût de transfert et de scan divisé par ~80.
WEATHER_STATION = "USW00094728"  # NY CITY CENTRAL PARK
WEATHER_BASE_URL = "https://noaa-ghcn-pds.s3.amazonaws.com/csv/by_station"
WEATHER_FILE_TPL = "{station}.csv"

# --- Périmètre de données du dev local ---
# On limite volontairement à quelques mois pour un dev peu coûteux ;
# on élargit la fenêtre lors de la phase de recette sur AWS.
# Contrainte RGPD : on n'ingère que des données >= 2016 (plus de GPS précis
# ni d'identifiants directs dans les données TLC récentes).
TAXI_MONTHS = [(2024, 1), (2024, 2), (2024, 3)]
WEATHER_YEARS = [2024]

# Colonnes météo GHCN retenues (le fichier brut est au format long).
WEATHER_ELEMENTS = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD"]
