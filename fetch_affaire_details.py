from __future__ import annotations
import os
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dateutil import parser

# --- Chargement des variables d'environnement ---
load_dotenv()
API_BASE_URL = os.getenv("API_BASE_URL", "https://mwa-metris.kipaware.fr/api")
API_KEY = os.getenv("MODULEO_API_KEY", "")
SECURITY_CODE = os.getenv("MODULEO_SECURITY_CODE", "")

HEADERS: Dict[str, str] = {
    "Content-Type": "application/json",
    "ApiKey": API_KEY,
    "SecurityCode": SECURITY_CODE,
    "User-Agent": "ModuleoReport/FetchAffaireDetails"
}
SESSION = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
SESSION.mount("https://", HTTPAdapter(max_retries=retries))
SESSION.mount("http://", HTTPAdapter(max_retries=retries))

# --- Mappings ---
ETAT_MAPPING: Dict[int, str] = {
    4: "Creee", 8: "EnAttente", 7: "Acceptee",
    1: "Production", 5: "Terminee", 9: "Suspendue",
    2: "Cloturee", 6: "Annulee",
}

def _map_etat(val: Any) -> Any:
    try:
        return ETAT_MAPPING.get(int(val), val)
    except (TypeError, ValueError):
        return val

try:
    _services_df = pd.read_csv(os.getenv("SERVICES_CSV", "services.csv"))
    SERVICE_MAPPING: Dict[int, str] = dict(
        zip(_services_df["IdService"].astype(int), _services_df["Nom"].astype(str))
    )
except Exception:
    SERVICE_MAPPING = {}

def _map_service(val: Any) -> Any:
    try:
        return SERVICE_MAPPING.get(int(val), val)
    except (TypeError, ValueError):
        return val

try:
    _collab_df = pd.read_excel(os.getenv("COLLABS_FILE", "Utilisateurs Moduleo.xlsx"))
    COLLAB_MAPPING: Dict[int, str] = dict(
        zip(_collab_df["Id"].astype(int), _collab_df["Nom complet"].astype(str))
    )
except Exception:
    COLLAB_MAPPING = {}

def _map_collaborateur(val: Any) -> Any:
    try:
        return COLLAB_MAPPING.get(int(val), val)
    except (TypeError, ValueError):
        return val

# --- API calls ---
def fetch_affaires_multi(ids: List[int], chunk_size: int = 100) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}/cogeo/affaire/multi"
    out: List[Dict[str, Any]] = []
    for i in range(0, len(ids), chunk_size):
        batch = ids[i:i+chunk_size]
        resp = SESSION.get(url, params={"ids": ",".join(map(str, batch))}, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            out.extend(data)
    return out

# --- Core functions ---
def save_affaire_details(
    affaire_ids: List[int],
    yyyymm: str,
    date_end: str,
) -> str:
    dt_end = parser.parse(date_end, dayfirst=True)

    details = fetch_affaires_multi(affaire_ids)
    rows: List[Dict[str, Any]] = []

    for rec in details:
        aid = next((int(v) for k, v in rec.items() if k.lower() == "idaffaire" and v is not None), None)
        if aid is None:
            continue

        date_cloture_str = rec.get("DateCloture") or rec.get("dateCloture")
        try:
            date_cloture_dt = parser.parse(date_cloture_str) if date_cloture_str else None
        except (ValueError, TypeError):
            date_cloture_dt = None

        etat_mapped = _map_etat(rec.get("Etat"))

        # Logique : une affaire Cloturée avec une DateCloture > date_end → bascule en Production
        if etat_mapped == "Cloturee" and date_cloture_dt and date_cloture_dt.date() > dt_end.date():
            etat_mapped = "Production"

        rows.append({
            "idAffaire": aid,
            "Numero": rec.get("Numero"),
            "Etat": etat_mapped,
            "Objet": rec.get("Objet"),
            "Service": _map_service(rec.get("IdService")),
            "Collaborateur": _map_collaborateur(rec.get("IdActeurEnCharge")),
            "DateCloture": date_cloture_str or "",
        })

    df = pd.DataFrame(rows)
    interm_file = f"affaires_acteur_service_{yyyymm}.csv"
    df.to_csv(interm_file, sep=';', decimal=',', index=False)
    return interm_file


def merge_with_prixventecollab(
    details_file: str,
    yyyymm: str,
) -> str:
    df_details = pd.read_csv(details_file, sep=';', decimal=',')
    prix_file = f"prixventecollab_affaires_{yyyymm}.csv"
    df_prix = pd.read_csv(prix_file, sep=None, engine='python', decimal=',')

    merged = df_details.merge(df_prix, on="idAffaire", how="inner")

    combined_file = f"affaires_combinees_{yyyymm}.csv"
    merged.to_csv(combined_file, sep=';', decimal=',', index=False)
    return combined_file


# --- Fonctions publiques pour Streamlit ---
def fetch_and_save_details(
    date_start: str,
    date_end: str,
) -> str:
    dt = parser.parse(date_start, dayfirst=True)
    yyyymm = dt.strftime("%Y%m")
    unique_csv = f"unique_affaires_{yyyymm}.csv"
    df_u = pd.read_csv(unique_csv, sep=';', decimal=',')
    affaire_ids = df_u["idAffaire"].dropna().astype(int).tolist()
    return save_affaire_details(affaire_ids, yyyymm, date_end)


def fetch_and_merge_details(
    date_start: str,
    date_end: str,
) -> str:
    dt = parser.parse(date_start, dayfirst=True)
    yyyymm = dt.strftime("%Y%m")
    details_file = fetch_and_save_details(date_start, date_end)
    return merge_with_prixventecollab(details_file, yyyymm)


def main(
    date_start: str,
    date_end: str,
) -> dict[str, str]:
    details = fetch_and_save_details(date_start, date_end)
    combined = merge_with_prixventecollab(details, parser.parse(date_start, dayfirst=True).strftime("%Y%m"))
    return {"details": details, "combined": combined}


if __name__ == "__main__":
    import argparse

    parser_ = argparse.ArgumentParser(
        description="Récupérer et combiner les détails des affaires"
    )
    parser_.add_argument("--date-min", dest="date_min", help="JJ/MM/AAAA", required=False)
    parser_.add_argument("--date-max", dest="date_max", help="JJ/MM/AAAA", required=False)
    args = parser_.parse_args()

    if args.date_min and args.date_max:
        dmin, dmax = args.date_min, args.date_max
    else:
        ref = datetime.today().replace(day=1) - pd.Timedelta(days=1)
        first_prev = ref.replace(day=1)
        dmin = first_prev.strftime("%d/%m/%Y")
        dmax = ref.strftime("%d/%m/%Y")

    res = main(dmin, dmax)
    print(f"Généré détails: {res['details']}, fusion: {res['combined']}")
