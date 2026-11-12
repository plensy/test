"""Évaluation des seuils d'alerte + dispatch des notifications.

En local : console + fichier journal (monitoring/_alerts/alerts.log) +
webhook Slack optionnel (si SLACK_WEBHOOK_URL est défini). Sur AWS, le même
jeu de règles serait implémenté en alarmes CloudWatch sur les métriques
correspondantes, publiant sur le topic SNS de la state machine (cf. rapport
§6 pour la table de correspondance).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import requests

from config import settings

ALERTS_LOG = settings.PROJECT_ROOT / "monitoring" / "_alerts" / "alerts.log"
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")

# Seuils par défaut — mêmes valeurs que celles documentées pour les alarmes
# CloudWatch équivalentes (rapport §6).
DEFAULT_THRESHOLDS = {
    "max_rejection_rate_pct": 5.0,     # taux de rejet silver au-delà = anormal
    "max_step_duration_s": 240.0,      # SLA large par étape (marge vs ~90s observé)
    "min_gold_rows": 1,                # une table gold vide = pipeline cassée
}


def evaluate(metrics: dict, thresholds: dict | None = None) -> list[dict]:
    """Applique les règles d'alerte aux métriques et renvoie la liste des
    alertes déclenchées (sévérité CRITICAL ou WARNING)."""
    th = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    alerts: list[dict] = []

    if metrics.get("quality_success") is False:
        alerts.append({
            "severity": "CRITICAL",
            "rule": "quality_gate_failed",
            "message": "Le gate qualité (Great Expectations) a échoué — "
                       "des données invalides auraient atteint la couche gold.",
        })

    if metrics.get("pipeline_success") is False:
        alerts.append({
            "severity": "CRITICAL",
            "rule": "pipeline_failed",
            "message": "La dernière exécution de la pipeline a échoué.",
        })

    rr = metrics.get("rejection_rate_pct")
    if rr is not None and rr > th["max_rejection_rate_pct"]:
        alerts.append({
            "severity": "WARNING",
            "rule": "high_rejection_rate",
            "message": f"Taux de rejet silver anormalement élevé : {rr}% "
                       f"(seuil {th['max_rejection_rate_pct']}%).",
        })

    for step, s in metrics.get("step_durations", {}).items():
        dur = s.get("duration_s")
        if dur is not None and dur > th["max_step_duration_s"]:
            alerts.append({
                "severity": "WARNING",
                "rule": "step_duration_sla_breach",
                "message": f"Étape '{step}' anormalement longue : {dur}s "
                           f"(seuil {th['max_step_duration_s']}s).",
            })

    for table, n in metrics.get("volumes", {}).items():
        if table.startswith("gold_") and n is not None and n < th["min_gold_rows"]:
            alerts.append({
                "severity": "CRITICAL",
                "rule": "empty_gold_table",
                "message": f"Table gold vide ou quasi vide : {table} ({n} lignes).",
            })

    return alerts


def _append_log(alert: dict) -> None:
    ALERTS_LOG.parent.mkdir(parents=True, exist_ok=True)
    entry = {"ts": datetime.now(timezone.utc).isoformat(), **alert}
    with open(ALERTS_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def _notify_slack(alert: dict) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    icon = "🔴" if alert["severity"] == "CRITICAL" else "🟠"
    payload = {"text": f"{icon} *{alert['severity']}* — `{alert['rule']}`\n{alert['message']}"}
    try:
        requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
    except requests.RequestException as e:  # noqa: BLE001
        print(f"  ⚠ échec d'envoi Slack : {e}")


def dispatch(alerts: list[dict]) -> None:
    """Envoie chaque alerte sur tous les canaux configurés (console, fichier,
    Slack si SLACK_WEBHOOK_URL défini) — équivalent des canaux SNS (email/Slack)."""
    for alert in alerts:
        icon = "🔴" if alert["severity"] == "CRITICAL" else "🟠"
        print(f"  {icon} [{alert['severity']}] {alert['rule']} — {alert['message']}")
        _append_log(alert)
        _notify_slack(alert)
