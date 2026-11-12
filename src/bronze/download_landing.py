"""Récupération des données sources vers la zone de landing (dev local).

En production AWS, la couche Bronze lit directement les buckets publics
(`nyc-tlc`, `noaa-ghcn-pds`) situés dans la même région us-east-1. En
local, on rejoue ce contrat en miroir : on télécharge les fichiers bruts
dans ./data/_landing, puis les jobs Bronze les ingèrent sans les modifier.

Usage :
    python -m src.bronze.download_landing
"""
from __future__ import annotations

import sys
from pathlib import Path

import requests

from config import settings

CHUNK = 1 << 20  # 1 Mo


def _download(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  ✓ déjà présent : {dest.name} ({dest.stat().st_size/1e6:.1f} Mo)")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  ↓ {url}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(CHUNK):
                f.write(chunk)
        tmp.rename(dest)
    print(f"  ✓ {dest.name} ({dest.stat().st_size/1e6:.1f} Mo)")


def main() -> int:
    landing = Path(settings.LANDING)
    print("== Téléchargement Taxi ==")
    for year, month in settings.TAXI_MONTHS:
        fname = settings.TAXI_FILE_TPL.format(year=year, month=month)
        _download(f"{settings.TAXI_BASE_URL}/{fname}", landing / "taxi" / fname)

    print("== Téléchargement Zones ==")
    _download(settings.ZONES_URL, landing / "zones" / "taxi_zone_lookup.csv")

    print("== Téléchargement Météo ==")
    fname = settings.WEATHER_FILE_TPL.format(station=settings.WEATHER_STATION)
    _download(f"{settings.WEATHER_BASE_URL}/{fname}", landing / "weather" / fname)

    print("\nLanding prêt sous", landing)
    return 0


if __name__ == "__main__":
    sys.exit(main())
