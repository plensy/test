"""ML (bonus) — prévision de la demande de courses (P1).

Modèle INTÉGRÉ à la pipeline (étape du DAG, pas un notebook) : il lit la
couche silver, entraîne un régresseur, se compare à une baseline simple,
journalise dans MLflow (versionnement), puis écrit ses prédictions en
couche gold où elles sont exposables via l'API / le dashboard.

Cible    : nombre de courses par (borough, date, heure).
Features : calendrier (heure, jour de semaine, week-end, mois) + météo
           (température, pluie, neige) + borough. Aucune donnée identifiante
           (conformité RGPD : features non ré-identifiantes).
Validation : split TEMPOREL (train = janv+févr, test = mars) → pas de fuite.
Baseline : moyenne historique par (borough, heure, jour de semaine).
Modèle   : HistGradientBoostingRegressor (gradient boosting, équivalent
           LightGBM, natif scikit-learn ; portable Glue sans dépendance).

Usage :
    python -m src.ml.train_demand_forecast
"""
from __future__ import annotations

import os
import sys

# MLflow 3.x exige un opt-in pour le file store (déprécié mais suffisant ici).
os.environ.setdefault("MLFLOW_ALLOW_FILE_STORE", "true")

import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

from config import settings
from src.common.spark import get_spark

TRAIN_MONTHS = [1, 2]   # janvier + février
TEST_MONTHS = [3]       # mars
FEATURES = ["pickup_hour", "pickup_dow", "is_weekend", "month",
            "tavg_c", "prcp_mm", "is_rain", "is_snow"]
TARGET = "trips_count"


def _rmse(y_true, y_pred) -> float:
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


def build_training_frame(spark) -> pd.DataFrame:
    """Agrège silver/trips au grain (borough, date, heure) + météo."""
    trips = spark.read.parquet(f"{settings.SILVER}/trips")
    # IMPORTANT : on dérive le mois de la DATE RÉELLE du trajet, pas de la
    # colonne `month` (= mois du fichier source). Sinon une même date/heure
    # peut être scindée entre deux fichiers, et des trajets de fin février
    # présents dans le fichier de mars fausseraient le split train/test.
    trips = trips.withColumn("month", F.month("pickup_date"))
    agg = (
        trips.groupBy("pickup_date", "pickup_hour", "pickup_dow",
                      "is_weekend", "month", "pu_borough")
        .agg(
            F.count("*").alias("trips_count"),
            F.first("tavg_c").alias("tavg_c"),
            F.first("prcp_mm").alias("prcp_mm"),
            F.first("is_rain").alias("is_rain"),
            F.first("is_snow").alias("is_snow"),
        )
    )
    pdf = agg.toPandas()
    pdf["pickup_date"] = pd.to_datetime(pdf["pickup_date"])
    return pdf


def main() -> int:
    spark = get_spark("ml_demand_forecast")
    print(f"== ML prévision de demande (env={settings.ENV}) ==")

    pdf = build_training_frame(spark)

    # Encodage borough en catégories stables (mêmes codes train/test).
    pdf["pu_borough"] = pdf["pu_borough"].astype("category")
    pdf["borough_code"] = pdf["pu_borough"].cat.codes
    feat_cols = FEATURES + ["borough_code"]

    train = pdf[pdf["month"].isin(TRAIN_MONTHS)]
    test = pdf[pdf["month"].isin(TEST_MONTHS)].copy()
    print(f"  train : {len(train):,} lignes ({TRAIN_MONTHS}) | test : {len(test):,} lignes ({TEST_MONTHS})")

    X_train, y_train = train[feat_cols], train[TARGET]
    X_test, y_test = test[feat_cols], test[TARGET]

    # --- Baseline : moyenne par (borough, heure, jour de semaine) ---
    base_tbl = (
        train.groupby(["pu_borough", "pickup_hour", "pickup_dow"], observed=True)[TARGET]
        .mean().rename("baseline_pred").reset_index()
    )
    test = test.merge(base_tbl, on=["pu_borough", "pickup_hour", "pickup_dow"], how="left")
    test["baseline_pred"] = test["baseline_pred"].fillna(train[TARGET].mean())
    base_mae = mean_absolute_error(y_test, test["baseline_pred"])
    base_rmse = _rmse(y_test, test["baseline_pred"])

    # --- Modèle ---
    experiment_name = "nyc_demand_forecast"
    if settings.ENV == "aws":
        # MLflow ne supporte pas s3:// comme URI de TRACKING (métadonnées) —
        # seulement comme emplacement d'ARTEFACTS. Sur un worker Glue, le
        # disque local est éphémère (effacé en fin de job) : les métadonnées
        # de run n'y survivent pas, mais ce n'est pas grave — ce qui compte
        # (le modèle entraîné + ses métriques) est explicitement stocké sur
        # S3 via artifact_location. Pas de serveur MLflow permanent à payer
        # (cf. rapport §7.5).
        mlflow.set_tracking_uri("file:/tmp/mlflow")
        client = mlflow.tracking.MlflowClient()
        exp = client.get_experiment_by_name(experiment_name)
        if exp is None:
            client.create_experiment(
                experiment_name, artifact_location=f"s3://{settings.S3_BUCKET}/mlruns-artifacts"
            )
    else:
        # as_uri() encode correctement les espaces du chemin (ex. "PROJET DATA…").
        mlflow.set_tracking_uri((settings.PROJECT_ROOT / "mlruns").as_uri())
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name="hist_gradient_boosting"):
        # borough_code est une catégorie (pas un entier ordonné) : sans le
        # déclarer, le modèle interprète l'ordre arbitraire des codes comme
        # une relation d'ordre (Bronx < Brooklyn < ...), ce qui dégrade les
        # coupures d'arbre pour tous les boroughs sauf les deux extrêmes.
        categorical_mask = [c == "borough_code" for c in feat_cols]
        model = HistGradientBoostingRegressor(
            max_iter=300, learning_rate=0.08, max_depth=8,
            l2_regularization=1.0, random_state=42,
            categorical_features=categorical_mask,
        )
        model.fit(X_train, y_train)
        test["predicted"] = np.clip(model.predict(X_test), 0, None).round().astype(int)

        model_mae = mean_absolute_error(y_test, test["predicted"])
        model_rmse = _rmse(y_test, test["predicted"])
        improvement = (base_mae - model_mae) / base_mae * 100

        mlflow.log_params({
            "model": "HistGradientBoostingRegressor",
            "max_iter": 300, "learning_rate": 0.08, "max_depth": 8,
            "features": ",".join(feat_cols),
            "train_months": TRAIN_MONTHS, "test_months": TEST_MONTHS,
        })
        mlflow.log_metrics({
            "baseline_mae": base_mae, "baseline_rmse": base_rmse,
            "model_mae": model_mae, "model_rmse": model_rmse,
            "mae_improvement_pct": improvement,
        })
        # serialization_format="cloudpickle" : le sérialiseur par défaut
        # (skops) refuse par sécurité certains objets internes créés par
        # categorical_features. On entraîne et recharge nous-mêmes ce
        # modèle (pas de données externes non fiables) : pickle est adapté.
        mlflow.sklearn.log_model(model, name="model", serialization_format="cloudpickle")

    print("\n  --- Performance (test = mars) ---")
    print(f"  Baseline  : MAE={base_mae:8.1f}  RMSE={base_rmse:8.1f}")
    print(f"  Modèle    : MAE={model_mae:8.1f}  RMSE={model_rmse:8.1f}")
    print(f"  Gain MAE  : {improvement:+.1f} % vs baseline")

    # --- Écriture des prédictions en gold ---
    out = test[["pickup_date", "pickup_hour", "pu_borough",
                "trips_count", "predicted", "baseline_pred"]].copy()
    out.columns = ["pickup_date", "pickup_hour", "pu_borough",
                   "actual", "predicted", "baseline"]
    out["baseline"] = out["baseline"].round().astype(int)
    out["pickup_date"] = out["pickup_date"].dt.date
    (
        spark.createDataFrame(out)
        .write.mode("overwrite").parquet(f"{settings.GOLD}/fact_demand_forecast")
    )
    print(f"\n  ✓ {len(out):,} prédictions -> gold/fact_demand_forecast")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
