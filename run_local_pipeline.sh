#!/usr/bin/env bash
#
# Exécution locale de bout en bout de la pipeline médaillon.
# Chaque étape correspond à un futur état du DAG Step Functions ; l'ordre
# et la gestion d'échec (set -e) reproduisent les dépendances du workflow.
#
# Usage :
#   ./run_local_pipeline.sh              # pipeline complète
#   SKIP_DOWNLOAD=1 ./run_local_pipeline.sh   # si le landing est déjà là
#
set -euo pipefail
cd "$(dirname "$0")"

step() { echo -e "\n\033[1;36m===> $1\033[0m"; }

if [[ "${SKIP_DOWNLOAD:-0}" != "1" ]]; then
  step "0/5  Récupération des sources -> landing"
  python3 -m src.bronze.download_landing
fi

step "1/5  BRONZE  — ingestion brute"
python3 -m src.bronze.ingest --dataset all

step "2/5  SILVER  — météo quotidienne"
python3 -m src.silver.build_weather_daily

step "3/5  SILVER  — courses nettoyées + jointures"
python3 -m src.silver.build_trips

step "4/5  QUALITÉ — gate Great Expectations"
python3 -m src.silver.quality_checks

step "5/6  GOLD    — datamarts métier"
python3 -m src.gold.build_datamarts

step "6/7  ML      — prévision de demande (bonus) -> gold"
python3 -m src.ml.train_demand_forecast

step "7/7  MONITORING — métriques & alertes"
python3 -m src.monitoring.check_and_alert || true  # ne bloque pas le run ; l'alerte est le signal

echo -e "\n\033[1;32m✓ Pipeline terminée avec succès.\033[0m"
echo "   Explorer les résultats : python3 -m src.preview [bronze|silver|gold]"
