import os
import json
from dotenv import load_dotenv
import streamlit as st
import bcrypt
from datetime import date, timedelta

# Import des modules métier
from import_tempspasses import fetch_raw_tempspasses, enrich_with_affaire, export_unique_affaires
from fetch_affaire_tempspasses import fetch_and_export_affaires, calculate_prix_for_period
from fetch_affaire_details import fetch_and_save_details, fetch_and_merge_details
from fetch_affaire_devis import fetch_and_update_devis
from fetch_affaire_factures import fetch_and_update_factures

# --- 1. Chargement des variables d'environnement ---
load_dotenv()
auth_json = os.getenv("USERS_HASH")
if not auth_json:
    st.error("🔴 La variable d'environnement USERS_HASH n'est pas définie.")
    st.stop()
users = json.loads(auth_json)

# --- 2. Initialisation de l'état d'authentification ---
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.name = None

# --- 3. Authentification ---
if not st.session_state.authenticated:
    st.sidebar.title("🔒 Connexion")
    with st.sidebar.form(key='login_form'):
        username_input = st.text_input("Nom d'utilisateur")
        password_input = st.text_input("Mot de passe", type="password")
        submitted = st.form_submit_button("Se connecter")
        if submitted:
            user = users.get(username_input)
            if user and bcrypt.checkpw(password_input.encode(), user['password'].encode()):
                st.session_state.authenticated = True
                st.session_state.name = user.get('name')
            else:
                st.error("❌ Nom d'utilisateur ou mot de passe incorrect")
    st.stop()

# --- 4. Utilisateur authentifié et déconnexion ---
st.sidebar.write(f"Bienvenue, **{st.session_state.name}** ! 🎉")
if st.sidebar.button("Se déconnecter"):
    st.session_state.authenticated = False
    st.experimental_rerun()

# --- 5. Configuration de la page ---
st.set_page_config(page_title="Moduleo Report - Pipeline Complet", layout="wide")
st.title("Moduleo Report - Exécution Automatique du Pipeline")

# --- 6. Sélection de la période (mois précédent par défaut) ---
today = date.today()
first_current = today.replace(day=1)
last_prev = first_current - timedelta(days=1)
first_prev = last_prev.replace(day=1)

st.sidebar.header("Paramètres de la période")
start_dt = st.sidebar.date_input("Date de début", value=first_prev)
end_dt = st.sidebar.date_input("Date de fin", value=last_prev)

# Format JJ/MM/AAAA pour affichage et fonctions métier
date_start = start_dt.strftime("%d/%m/%Y")
date_end = end_dt.strftime("%d/%m/%Y")
st.sidebar.write(f"Période : **{date_start}** → **{date_end}**")

# --- 7. Exécution du pipeline ---
if st.sidebar.button("🚀 Exécuter tout"):
    st.markdown(f"### Résultats pour la période du **{date_start}** au **{date_end}**")

    # 1. Import des temps passés
    try:
        raw_csv = fetch_raw_tempspasses(date_start, date_end)
        st.success(f"CSV brut généré : `{raw_csv}`")
    except Exception as e:
        st.error(f"Erreur import temps passés : {e}")
        st.stop()

    # 2. Enrichissement avec affaires
    try:
        enriched_csv = enrich_with_affaire(raw_csv)
        st.success(f"CSV enrichi : `{enriched_csv}`")
    except Exception as e:
        st.error(f"Erreur enrichissement : {e}")
        st.stop()

    # 3. Export des affaires uniques
    try:
        unique_csv = export_unique_affaires(enriched_csv)
        st.success(f"CSV affaires uniques : `{unique_csv}`")
    except Exception as e:
        st.error(f"Erreur export unique affaires : {e}")
        st.stop()

    # 4. Temps passés par affaire
    try:
        affaires_csv = fetch_and_export_affaires(date_start, date_end)
        st.success(f"CSV temps passés par affaire : `{affaires_csv}`")
    except Exception as e:
        st.error(f"Erreur export temps passés par affaire : {e}")
        st.stop()

    # 5. Calcul PrixVenteCollaborateur
    try:
        prix_csv = calculate_prix_for_period(date_start, date_end)
        st.success(f"CSV PrixVenteCollaborateur : `{prix_csv}`")
    except Exception as e:
        st.error(f"Erreur calcul PrixVenteCollaborateur : {e}")
        st.stop()

    # 6. Sauvegarde des détails des affaires
    try:
        details_csv = fetch_and_save_details(date_start, date_end)
        st.success(f"CSV détails affaires : `{details_csv}`")
    except Exception as e:
        st.error(f"Erreur sauvegarde détails : {e}")
        st.stop()

    # 7. Fusion détails & prix
    try:
        combined_csv = fetch_and_merge_details(date_start, date_end)
        st.success(f"CSV affaires combinées : `{combined_csv}`")
    except Exception as e:
        st.error(f"Erreur fusion détails & prix : {e}")
        st.stop()

    # 8. Intégration des devis
    try:
        devis_csv = fetch_and_update_devis(date_start, date_end)
        st.success(f"CSV devis intégrés : `{devis_csv}`")
    except Exception as e:
        st.error(f"Erreur intégration devis : {e}")
        st.stop()

    # 9. Intégration des factures
    try:
        factures_csv = fetch_and_update_factures(date_start, date_end)
        st.success(f"CSV factures intégrées : `{factures_csv}`")
    except Exception as e:
        st.error(f"Erreur intégration factures : {e}")
        st.stop()