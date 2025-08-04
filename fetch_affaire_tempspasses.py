from __future__ import annotations
import os
from datetime import date, timedelta, datetime
from typing import Any, Dict, List, Union, Optional

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
    "User-Agent": "ModuleoReport/FetchAffaireTempspasses"
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

# --- Helpers internes --------------------------------

def _previous_month_period(ref: date | None = None) -> tuple[str, str]:
    ref = ref or date.today()
    first_current = ref.replace(day=1)
    last_prev = first_current - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    fmt = "%d/%m/%Y"
    return first_prev.strftime(fmt), last_prev.strftime(fmt)


def fetch_affaire_tempspasses(
    id_affaire: int,
    date_min: str,
    date_max: str,
) -> list[Union[int, Dict[str, Any]]]:
    url = f"{API_BASE_URL}/cogeo/affaire/{id_affaire}/tempspasses"
    params = {"dateMin": date_min, "dateMax": date_max, "nbMaxResultat": 10000}
    resp = SESSION.get(url, params=params, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()


def fetch_tempspasses_multi(
    ids: list[int],
    chunk_size: int = 100,
) -> list[Dict[str, Any]]:
    url = f"{API_BASE_URL}/cogeo/tempspasse/multi"
    out: list[Dict[str, Any]] = []
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


def save_affaires_tempspasses(
    affaire_ids: list[int],
    date_min: str,
    date_max: str,
    yyyymm: str,
) -> str:
    all_records: list[Dict[str, Any]] = []
    for aid in affaire_ids:
        try:
            records = fetch_affaire_tempspasses(aid, date_min, date_max)
            for rec in records:
                rec_dict = rec if isinstance(rec, dict) else {"idTempsPasse": rec}
                rec_dict["idAffaire"] = aid
                all_records.append(rec_dict)
        except Exception as e:
            print(f"Erreur récupération pour affaire {aid}: {e}")
    df = pd.DataFrame(all_records)
    path = f"tempspasses_affaires_{yyyymm}.csv"
    df.to_csv(path, index=False)
    return path


def calc_prixventecollab(
    csv_aff: str,
    date_max: str,
) -> str:
    """
    Lit le CSV des temps passés, récupère PrixVenteCollaborateur et Date pour chaque pointage,
    filtre ceux dont Date > date_max, puis agrège par affaire.
    Retourne le chemin du fichier agrégé.
    """
    # Parser date_max
    try:
        dt_end = datetime.strptime(date_max, "%d/%m/%Y")
    except ValueError:
        dt_end = datetime.fromisoformat(date_max)

    # Lecture CSV brut par affaire
    df = pd.read_csv(csv_aff)
    t_ids = df["idTempsPasse"].dropna().astype(int).tolist()
    details = fetch_tempspasses_multi(t_ids)

    records: list[Dict[str, Any]] = []
    for rec in details:
        # ID pointage
        pid = next(
            (int(v) for k, v in rec.items() if k.lower() in ("idtempspasse", "id", "idpointage") and v is not None),
            None,
        )
        # Prix
        pvc = rec.get("PrixVenteCollaborateur")
        try:
            pvc = float(pvc) if pvc is not None else 0.0
        except (TypeError, ValueError):
            pvc = 0.0
        # Date du pointage
        date_str: Optional[str] = rec.get("Date")
        date_dt: Optional[datetime] = None
        if date_str:
            try:
                date_dt = datetime.fromisoformat(date_str)
            except ValueError:
                try:
                    date_dt = datetime.strptime(date_str, "%d/%m/%Y")
                except ValueError:
                    date_dt = None
        # Filtrer si date du pointage > date_max
        if date_dt and date_dt > dt_end:
            continue
        records.append({"idTempsPasse": pid, "PrixVenteCollaborateur": pvc})

    # Merge et agrégation
    df_det = pd.DataFrame(records)
    df_full = df.merge(df_det, on="idTempsPasse", how="left")
    df_sum = df_full.groupby("idAffaire")["PrixVenteCollaborateur"].sum().reset_index()

    out = csv_aff.replace("tempspasses_affaires_", "prixventecollab_affaires_")
    df_sum.to_csv(out, index=False)
    return out

# --- Fonctions publiques pour Streamlit ----------------

def fetch_and_export_affaires(
    date_start: str,
    date_end: str,
) -> str:
    """
    Lit unique_affaires_<yyyymm>.csv et exporte les temps passés par affaire.
    Retourne le chemin du CSV généré.
    """
    dt = datetime.strptime(date_start, "%d/%m/%Y")
    yyyymm = dt.strftime("%Y%m")
    unique_csv = f"unique_affaires_{yyyymm}.csv"
    df_u = pd.read_csv(unique_csv)
    affaire_ids = df_u["idAffaire"].dropna().astype(int).tolist()
    return save_affaires_tempspasses(affaire_ids, date_start, date_end, yyyymm)


def calculate_prix_for_period(
    date_start: str,
    date_end: str,
) -> str:
    """
    Exécute export des temps passés par affaire puis calcule le PrixVenteCollaborateur
    en excluant ceux post-datés, retourne le chemin du fichier.
    """
    csv_aff = fetch_and_export_affaires(date_start, date_end)
    return calc_prixventecollab(csv_aff, date_end)


def main(
    date_start: str,
    date_end: str,
) -> dict[str, str]:
    """
    Point d'entrée principal : retourne chemins des CSV générés.
    """
    csv_aff = fetch_and_export_affaires(date_start, date_end)
    prix_csv = calc_prixventecollab(csv_aff, date_end)
    return {"affaires": csv_aff, "prix": prix_csv}

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Récupérer temps passés par affaire et calculer PrixVenteCollaborateur"
    )
    parser.add_argument("--date-min", help="Date min JJ/MM/YYYY", required=False)
    parser.add_argument("--date-max", help="Date max JJ/MM/YYYY", required=False)
    args = parser.parse_args()

    if args.date_min and args.date_max:
        dmin, dmax = args.date_min, args.date_max
    else:
        dmin, dmax = _previous_month_period()

    res = main(dmin, dmax)
    print(f"Généré : {res['affaires']}, {res['prix']}")