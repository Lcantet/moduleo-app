from __future__ import annotations
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Chargement des variables d'environnement
env_path = os.getenv("DOTENV_PATH")
if env_path:
    load_dotenv(env_path)
else:
    load_dotenv()
API_BASE_URL = os.getenv("API_BASE_URL", "https://mwa-metris.kipaware.fr/api")
API_KEY = os.getenv("MODULEO_API_KEY", "")
SECURITY_CODE = os.getenv("MODULEO_SECURITY_CODE", "")

HEADERS: Dict[str, str] = {
    "Content-Type": "application/json",
    "ApiKey": API_KEY,
    "SecurityCode": SECURITY_CODE,
    "User-Agent": "ModuleoReport/2.0",
}

# Session HTTP avec retry
SESSION = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
SESSION.mount("https://", HTTPAdapter(max_retries=retries))
SESSION.mount("http://", HTTPAdapter(max_retries=retries))

# --- Helpers internes --------------------------------

def _fetch_tempspasses(
    date_min: str,
    date_max: str,
    id_affaire: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Récupère les temps passés depuis l'API entre date_min et date_max (format DD/MM/YYYY).
    """
    url = f"{API_BASE_URL}/cogeo/tempspasse"
    params: Dict[str, Any] = {"dateMin": date_min, "dateMax": date_max, "nbMaxResultat": 10000}
    if id_affaire is not None:
        params["idAffaire"] = id_affaire
    resp = SESSION.get(url, params=params, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()


def _fetch_tempspasses_multi(
    ids: List[int],
    chunk_size: int = 100,
) -> List[Dict[str, Any]]:
    """
    Récupère en batch les détails de pointage pour enrichissement.
    """
    url = f"{API_BASE_URL}/cogeo/tempspasse/multi"
    out: List[Dict[str, Any]] = []
    for i in range(0, len(ids), chunk_size):
        batch = ids[i : i + chunk_size]
        resp = SESSION.get(
            url,
            params={"ids": ",".join(map(str, batch))},
            headers=HEADERS,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            out.extend(data)
    return out


def save_raw_csv(
    pointages: List[Dict[str, Any]],
    yyyymm: str,
) -> str:
    """
    Sauvegarde la liste de pointages bruts en CSV nommé tempspasses_<yyyymm>_raw.csv.
    Retourne le chemin du fichier.
    """
    df = pd.DataFrame(pointages)
    path = f"tempspasses_{yyyymm}_raw.csv"
    df.to_csv(path, index=False)
    return path


def _enrich_csv_with_affaire(
    pointages_path: str,
) -> str:
    """
    Enrichit le CSV brut avec l'identifiant idAffaire via un appel multi-fetch.
    Retourne le chemin du fichier tempspasses_<yyyymm>_affaires.csv.
    """
    df = pd.read_csv(pointages_path)
    id_col = next(
        (c for c in df.columns if c.lower() in ("idtempspasse", "id", "idpointage")),
        df.columns[0],
    )
    if id_col not in ("idTempsPasse", "id", "idPointage"):
        df = df.rename(columns={id_col: "idTempsPasse"})
        id_col = "idTempsPasse"
    ids = df[id_col].dropna().astype(int).tolist()
    details = _fetch_tempspasses_multi(ids)
    map_aff: Dict[int, int] = {}
    for rec in details:
        pid = next(
            (
                int(v)
                for k, v in rec.items()
                if k.lower() in ("idtempspasse", "id", "idpointage") and v is not None
            ),
            None,
        )
        aid = next(
            (
                int(v)
                for k, v in rec.items()
                if k.lower() == "idaffaire" and v is not None
            ),
            None,
        )
        if pid is not None and aid is not None:
            map_aff[pid] = aid
    df["idAffaire"] = df[id_col].map(map_aff)
    yyyymm = pointages_path.split("_")[1]
    path = f"tempspasses_{yyyymm}_affaires.csv"
    df.to_csv(path, index=False)
    return path


def _export_unique_and_missing(
    csv_aff: str,
    yyyymm: str,
) -> List[int]:
    """
    Extrait les idAffaire uniques (hors exceptions) et sauvegarde deux fichiers :
    - unique_affaires_<yyyymm>.csv
    - sans_affaire_<yyyymm>.csv
    Retourne la liste des ids uniques.
    """
    df = pd.read_csv(csv_aff)
    raw_ids = [int(x) for x in df["idAffaire"].dropna()]
    unique_ids = sorted([i for i in set(raw_ids) if i not in (29966, 35659, 32207)])
    df_unique = pd.DataFrame({"idAffaire": unique_ids})
    csv_unique = f"unique_affaires_{yyyymm}.csv"
    df_unique.to_csv(csv_unique, index=False)
    df_missing = df[df["idAffaire"].isna()]
    csv_missing = f"sans_affaire_{yyyymm}.csv"
    df_missing.to_csv(csv_missing, index=False)
    return unique_ids

# --- Fonctions publiques pour Streamlit ----------------

def fetch_raw_tempspasses(
    date_start: str,
    date_end: str,
) -> str:
    """
    Récupère les pointages bruts entre date_start et date_end (format DD/MM/YYYY)
    et les exporte en CSV. Retourne le chemin du fichier CSV généré.
    """
    pointages = _fetch_tempspasses(date_start, date_end)
    # On déduit la période yyyyMM depuis date_start
    dt = datetime.strptime(date_start, "%d/%m/%Y")
    yyyymm = dt.strftime('%Y%m')
    return save_raw_csv(pointages, yyyymm)


def enrich_with_affaire(
    raw_csv_path: str,
) -> str:
    """
    Enrichit le CSV brut avec les identifiants `idAffaire`.
    Retourne le chemin du CSV enrichi.
    """
    return _enrich_csv_with_affaire(raw_csv_path)


def export_unique_affaires(
    enriched_csv_path: str,
) -> str:
    """
    Extrait et sauvegarde la liste unique des `idAffaire`.
    Retourne le chemin du fichier unique.
    """
    # Extrait la période yyyyMM depuis le nom du fichier enrichi
    yyyymm = enriched_csv_path.split('_')[1]
    _export_unique_and_missing(enriched_csv_path, yyyymm)
    return f"unique_affaires_{yyyymm}.csv"

if __name__ == "__main__":
    # Exemple d'utilisation en ligne de commande
    import argparse

    parser = argparse.ArgumentParser(description="Import et traitement des temps passés.")
    parser.add_argument("date_start", type=str, help="Date de début (DD/MM/YYYY)")
    parser.add_argument("date_end", type=str, help="Date de fin (DD/MM/YYYY)")
    args = parser.parse_args()

    raw = fetch_raw_tempspasses(args.date_start, args.date_end)
    enriched = enrich_with_affaire(raw)
    unique_csv = export_unique_affaires(enriched)
    print(f"Généré : {raw}, {enriched}, {unique_csv}")
