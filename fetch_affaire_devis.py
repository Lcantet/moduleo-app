from __future__ import annotations
import os
from datetime import date, timedelta, datetime
from typing import Any, Dict, List

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Chargement des variables d'environnement ---
load_dotenv()
API_BASE_URL = os.getenv("API_BASE_URL", "https://mwa-metris.kipaware.fr/api")
API_KEY = os.getenv("MODULEO_API_KEY", "")
SECURITY_CODE = os.getenv("MODULEO_SECURITY_CODE", "")

HEADERS: Dict[str, str] = {
    "Content-Type": "application/json",
    "ApiKey": API_KEY,
    "SecurityCode": SECURITY_CODE,
    "User-Agent": "ModuleoReport/FetchAffaireDevis"
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

# --- Helpers ---
def _previous_month_yyyymm(ref: date | None = None) -> str:
    ref = ref or date.today()
    first_current = ref.replace(day=1)
    last_prev = first_current - timedelta(days=1)
    return last_prev.strftime("%Y%m")

# --- API calls ---
def fetch_affaire_devis(id_affaire: int) -> List[int]:
    url = f"{API_BASE_URL}/cogeo/affaire/{id_affaire}/devis"
    resp = SESSION.get(url, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json() or []
    ids: List[int] = []
    for item in data:
        if isinstance(item, (int, float)) or (isinstance(item, str) and item.isdigit()):
            try:
                ids.append(int(item))
            except (TypeError, ValueError):
                pass
        elif isinstance(item, dict):
            did = item.get('idDevis') or item.get('IdDevis')
            try:
                ids.append(int(did))
            except (TypeError, ValueError):
                pass
    return ids


def fetch_devis_multi(ids: List[int], chunk_size: int = 100) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}/cogeo/devis/multi"
    result: List[Dict[str, Any]] = []
    for i in range(0, len(ids), chunk_size):
        batch = ids[i : i + chunk_size]
        resp = SESSION.get(url, params={"ids": ",".join(map(str, batch))}, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json() or []
        if isinstance(data, list):
            result.extend(data)
    return result

# --- Core orchestration ---
def update_affaires_combinees_with_devis(
    affaire_ids: List[int],
    yyyymm: str,
) -> str:
    """
    Pour chaque idAffaire :
      - récupérer idDevis,
      - filtrer devis commandés (etat == 0),
      - sommer MontantTotalHT,
      - fusionner dans affaires_combinees_{yyyymm}.csv
    Retourne le chemin du fichier mis à jour.
    """
    combined_file = f"affaires_combinees_{yyyymm}.csv"
    # Collecte des devis
    id_to_aff: Dict[int, int] = {}
    all_devis_ids: List[int] = []
    for aid in affaire_ids:
        devis_ids = fetch_affaire_devis(aid)
        for did in devis_ids:
            id_to_aff[did] = aid
            all_devis_ids.append(did)
    all_devis_ids = list(set(all_devis_ids))

    # Récupération détails et filtrage
    details = fetch_devis_multi(all_devis_ids)
    records: List[Dict[str, Any]] = []
    for rec in details:
        etat = rec.get('etat') or rec.get('Etat')
        try:
            if int(etat) != 0:
                continue
        except (TypeError, ValueError):
            continue
        did = rec.get('idDevis') or rec.get('IdDevis')
        try:
            did_int = int(did)
        except (TypeError, ValueError):
            continue
        montant = rec.get('montantTotalHT') or rec.get('MontantTotalHT') or 0
        try:
            montant_val = float(montant)
        except (TypeError, ValueError):
            montant_val = 0.0
        aid = id_to_aff.get(did_int)
        if aid is not None:
            records.append({'idAffaire': aid, 'MontantTotalHT': montant_val})

    df_devis = pd.DataFrame(records)

    # Lecture et fusion
    df_combined = pd.read_csv(combined_file, sep=';', decimal=',')
    # Supprimer l'ancienne colonne si elle existe
    if 'MontantTotalHT' in df_combined.columns:
        df_combined = df_combined.drop(columns=['MontantTotalHT'])
    if df_devis.empty:
        df_combined['MontantTotalHT'] = 0.0
    else:
        df_sum = df_devis.groupby('idAffaire')['MontantTotalHT'].sum().reset_index()
        df_combined = df_combined.merge(df_sum, on='idAffaire', how='left')
        df_combined['MontantTotalHT'] = df_combined['MontantTotalHT'].fillna(0.0)
    df_combined.to_csv(combined_file, sep=';', decimal=',', index=False)
    return combined_file

# --- Fonctions publiques pour Streamlit ---

def fetch_and_update_devis(
    date_start: str,
    date_end: str,
) -> str:
    """
    Lit unique_affaires_{yyyymm}.csv et met à jour
    affaires_combinees_{yyyymm}.csv avec MontantTotalHT.
    """
    dt = datetime.strptime(date_start, "%d/%m/%Y")
    yyyymm = dt.strftime("%Y%m")
    unique_csv = f"unique_affaires_{yyyymm}.csv"
    df_u = pd.read_csv(unique_csv, sep=';', decimal=',')
    affaire_ids = df_u['idAffaire'].dropna().astype(int).tolist()
    return update_affaires_combinees_with_devis(affaire_ids, yyyymm)


def main(
    date_start: str,
    date_end: str,
) -> Dict[str, str]:
    """
    Point d'entrée pour Streamlit : retourne chemin du fichier mis à jour.
    """
    updated = fetch_and_update_devis(date_start, date_end)
    return {"updated": updated}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Intégrer les devis dans le fichier fusionné"
    )
    parser.add_argument("--date-min", help="JJ/MM/AAAA", required=False)
    parser.add_argument("--date-max", help="JJ/MM/AAAA", required=False)
    args = parser.parse_args()

    if args.date_min and args.date_max:
        ds, de = args.date_min, args.date_max
    else:
        ref = date.today()
        fm = ref.replace(day=1) - timedelta(days=1)
        ds = de = fm.strftime("%d/%m/%Y")
    res = main(ds, de)
    print(f"Fichier devis mis à jour : {res['updated']}")
