"""API REST — exposition des datamarts gold (couche Gold).

Documentée automatiquement en OpenAPI/Swagger (FastAPI) : la doc interactive
est servie sur /docs et le schéma OpenAPI sur /openapi.json.

Problématiques exposées :
  P1 demande        : /demand/hourly, /forecast
  P2 impact météo   : /weather/impact
  P3 pourboires     : /tips/by-borough

RGPD : l'API n'expose QUE des agrégats k-anonymisés (k>=5) — aucune course
individuelle, aucune donnée identifiante. Un contrôle d'accès optionnel par
clé d'API (en-tête X-API-Key) est fourni et activable via la variable
d'environnement API_KEY ; chaque requête est journalisée.

Lancer :
    uvicorn api.main:app --reload
    → http://127.0.0.1:8000/docs
"""
from __future__ import annotations

import logging
import os
import time

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request

from api import data

logging.basicConfig(level=logging.INFO, format="%(asctime)s [API] %(message)s")
logger = logging.getLogger("nyc-api")

app = FastAPI(
    title="NYC Taxi Datamarts API",
    version="1.0.0",
    description=(
        "API REST exposant les datamarts gold de la pipeline NYC Taxi "
        "(demande, impact météo, pourboires, prévision ML). "
        "Données agrégées et k-anonymisées (RGPD)."
    ),
)

API_KEY = os.environ.get("API_KEY")  # si défini, l'auth par clé est exigée


async def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """Contrôle d'accès optionnel : actif seulement si API_KEY est défini."""
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Clé d'API invalide ou absente.")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    logger.info("%s %s -> %s (%.0f ms)", request.method, request.url.path,
                response.status_code, (time.time() - start) * 1000)
    return response


@app.get("/", tags=["Général"])
def root():
    """Point d'entrée : liste des endpoints métier."""
    return {
        "service": "NYC Taxi Datamarts API",
        "version": "1.0.0",
        "endpoints": {
            "P1_demande_horaire": "/demand/hourly?borough=Manhattan",
            "P1_prevision": "/forecast?borough=Manhattan",
            "P2_impact_meteo": "/weather/impact",
            "P3_pourboires": "/tips/by-borough",
            "documentation": "/docs",
        },
    }


@app.get("/health", tags=["Général"])
def health():
    """Sonde de disponibilité (utilisée par le monitoring)."""
    return {"status": "ok", "datamarts": data.DATAMARTS}


@app.get("/demand/hourly", tags=["P1 Demande"], dependencies=[Depends(require_api_key)])
def demand_hourly(borough: str | None = Query(default=None, description="Filtrer par borough")):
    """P1 — nombre total de courses par heure (option : un borough)."""
    df = data.load("fact_demand_by_zone")
    if borough:
        df = df[df["pu_borough"] == borough]
        if df.empty:
            raise HTTPException(404, f"Borough inconnu : {borough}")
    out = (
        df.groupby("pickup_hour")["trips_count"].sum()
        .reset_index().rename(columns={"trips_count": "total_trips"})
        .sort_values("pickup_hour")
    )
    return {"borough": borough or "ALL", "data": out.to_dict(orient="records")}


@app.get("/weather/impact", tags=["P2 Météo"], dependencies=[Depends(require_api_key)])
def weather_impact():
    """P2 — demande et durée moyennes par condition météo."""
    df = data.load("fact_weather_impact")
    out = (
        df.groupby("weather_condition")
        .agg(jours=("pickup_date", "count"),
             courses_moy_jour=("trips_count", "mean"),
             duree_moy_min=("avg_duration_min", "mean"))
        .round(1).reset_index()
    )
    return {"data": out.to_dict(orient="records")}


@app.get("/tips/by-borough", tags=["P3 Pourboires"], dependencies=[Depends(require_api_key)])
def tips_by_borough():
    """P3 — pourboire moyen (%) par borough, pondéré par le volume."""
    df = data.load("fact_tips")
    # Moyenne pondérée par le nombre de courses (plus robuste que moy. de moy.).
    df = df.assign(w=df["avg_tip_pct"] * df["trips_count"])
    out = (
        df.groupby("pu_borough")
        .agg(tip_pct_moyen=("w", "sum"), courses=("trips_count", "sum"))
    )
    out["tip_pct_moyen"] = (out["tip_pct_moyen"] / out["courses"]).round(2)
    out = out.reset_index().sort_values("tip_pct_moyen", ascending=False)
    return {"data": out.to_dict(orient="records")}


@app.get("/forecast", tags=["P1 Demande"], dependencies=[Depends(require_api_key)])
def forecast(
    borough: str = Query(default="Manhattan", description="Borough à prévoir"),
    limit: int = Query(default=48, ge=1, le=500, description="Nb de points"),
):
    """P1 — prévision ML de la demande vs réel (test = mars)."""
    df = data.load("fact_demand_forecast")
    df = df[df["pu_borough"] == borough]
    if df.empty:
        raise HTTPException(404, f"Aucune prévision pour : {borough}")
    df = df.sort_values(["pickup_date", "pickup_hour"]).head(limit)
    df = df.assign(pickup_date=df["pickup_date"].astype(str))
    return {"borough": borough, "data": df.to_dict(orient="records")}
