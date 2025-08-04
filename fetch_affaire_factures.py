# fetch_affaire_factures.py
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
API_BASE_URL   = os.getenv("API_BASE_URL", "https://mwa-metris.kipaware.fr/api")
API_KEY        = os.getenv("MODULEO_API_KEY", "")
SECURITY_CODE  = os.getenv("MODULEO_SECURITY_CODE", "")

HEADERS: Dict[str, str] = {
    "Content-Type":    "application/json",
    "ApiKey":          API_KEY,
    "SecurityCode":    SECURITY_CODE,
    "User-Agent":      "ModuleoReport/FetchAffaireFactures"
}
SESSION = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
SESSION.mount("https://", HTTPAdapter(max_retries=retries))
SESSION.mount("http://",  HTTPAdapter(max_retries=retries))


def fetch_affaire_facture_ids(id_affaire: int) -> List[int]:
    url = f"{API_BASE_URL}/cogeo/affaire/{id_affaire}/factures"
    resp = SESSION.get(url, headers=HEADERS)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    data = resp.json() or []
    ids: List[int] = []
    for item in data:
        if isinstance(item, dict):
            fid = item.get("idFacture") or item.get("IdFacture")
            try: ids.append(int(fid))
            except: pass
        elif isinstance(item, (int, float)) or (isinstance(item, str) and item.isdigit()):
            try: ids.append(int(item))
            except: pass
    return ids


def fetch_facture_multi(ids: List[int], chunk_size: int = 100) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}/cogeo/facture/multi"
    out: List[Dict[str,Any]] = []
    for i in range(0, len(ids), chunk_size):
        batch = ids[i : i + chunk_size]
        resp  = SESSION.get(url, params={"ids": ",".join(map(str, batch))}, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json() or []
        if isinstance(data, list):
            out.extend(data)
    return out


def update_affaires_combinees_with_factures(
    affaire_ids: List[int],
    yyyymm: str,
) -> str:
    combined_file = f"affaires_combinees_{yyyymm}.csv"

    # 1) collecter tous les idFacture
    id_to_aff: Dict[int,int] = {}
    all_ids: List[int] = []
    for aid in affaire_ids:
        for fid in fetch_affaire_facture_ids(aid):
            id_to_aff[fid] = aid
            all_ids.append(fid)
    all_ids = list(set(all_ids))

    # 2) récupérer détails
    details = fetch_facture_multi(all_ids)
    records: List[Dict[str,Any]] = []
    for rec in details:
        fid = rec.get("idFacture") or rec.get("IdFacture")
        try:
            fid_i = int(fid)
        except:
            continue

        montant_ht = rec.get("MontantTotalHT", 0.0)
        try: montant = float(montant_ht)
        except: montant = 0.0

        # Date d'émission de la facture
        date_em = rec.get("DateEmission") or rec.get("dateEmission") or ""
        aid     = id_to_aff.get(fid_i)
        if aid is not None:
            records.append({
                "idAffaire":          aid,
                "MontantFacturesHT":  montant,
                "DateEmission":       date_em,
            })

    # 3) agrégations
    if records:
        df = pd.DataFrame(records)
        df_sum  = df.groupby("idAffaire")["MontantFacturesHT"].sum().reset_index()
        df_date = df.groupby("idAffaire")["DateEmission"].max().reset_index()
    else:
        df_sum  = pd.DataFrame(columns=["idAffaire","MontantFacturesHT"])
        df_date = pd.DataFrame(columns=["idAffaire","DateEmission"])

    # 4) fusion avec le CSV combiné
    df_comb = pd.read_csv(combined_file, sep=";", decimal=",")

    # supprimer anciennes colonnes
    for col in ("MontantFacturesHT","DateEmission_Facture"):
        if col in df_comb.columns:
            df_comb = df_comb.drop(columns=[col])

    df_merged = (
        df_comb
        .merge(df_sum, on="idAffaire", how="left")
        .merge(df_date.rename(columns={"DateEmission":"DateEmission_Facture"}),
               on="idAffaire", how="left")
    )
    df_merged["MontantFacturesHT"]    = df_merged["MontantFacturesHT"]   .fillna(0.0)
    df_merged["DateEmission_Facture"] = df_merged["DateEmission_Facture"].fillna("")

    df_merged.to_csv(combined_file, sep=";", decimal=",", index=False)
    return combined_file


def fetch_and_update_factures(date_start: str, date_end: str) -> str:
    dt         = datetime.strptime(date_start, "%d/%m/%Y")
    yyyymm     = dt.strftime("%Y%m")
    unique_csv = f"unique_affaires_{yyyymm}.csv"
    df_u       = pd.read_csv(unique_csv, sep=";", decimal=",")
    affaire_ids= df_u["idAffaire"].astype(int).tolist()
    return update_affaires_combinees_with_factures(affaire_ids, yyyymm)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Intégrer les factures dans le fichier fusionné"
    )
    parser.add_argument("--date-min", help="JJ/MM/AAAA", required=False)
    parser.add_argument("--date-max", help="JJ/MM/AAAA", required=False)
    args = parser.parse_args()

    if args.date_min and args.date_max:
        ds, de = args.date_min, args.date_max
    else:
        ref    = date.today()
        first  = ref.replace(day=1)
        prev   = first - timedelta(days=1)
        ds = de= prev.strftime("%d/%m/%Y")
    updated = fetch_and_update_factures(ds, de)
    print(f"Fichier factures mis à jour : {updated}")
