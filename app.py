"""
Application Streamlit Moduleo Report - Version Refactorisée
"""
import os
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import bcrypt
from datetime import date, timedelta

# Import des nouveaux modules
from config import config, load_users
from utils import validate_date_range, setup_logging, format_file_size
from pipeline import ModuleoPipeline


def init_session_state():
    """Initialise l'état de la session"""
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.name = None
    
    if 'pipeline_results' not in st.session_state:
        st.session_state.pipeline_results = {}


def authenticate_user():
    """Gère l'authentification utilisateur"""
    try:
        users = load_users()
    except ValueError as e:
        st.error(f"🔴 {e}")
        st.stop()
    
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
                st.rerun()
            else:
                st.error("❌ Nom d'utilisateur ou mot de passe incorrect")


def show_user_info():
    """Affiche les informations utilisateur et bouton déconnexion"""
    st.sidebar.write(f"Bienvenue, **{st.session_state.name}** ! 🎉")
    
    if st.sidebar.button("Se déconnecter"):
        st.session_state.authenticated = False
        st.session_state.name = None
        st.rerun()


def get_date_range():
    """Interface de sélection de période"""
    st.sidebar.header("📅 Paramètres de la période")
    
    # Période par défaut : mois précédent
    today = date.today()
    first_current = today.replace(day=1)
    last_prev = first_current - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    
    # Interface en colonnes pour un meilleur layout
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_dt = st.date_input("Début", value=first_prev, key="start_date")
    with col2:
        end_dt = st.date_input("Fin", value=last_prev, key="end_date")
    
    # Validation
    if not validate_date_range(start_dt, end_dt):
        st.stop()
    
    # Format JJ/MM/AAAA
    date_start = start_dt.strftime("%d/%m/%Y")
    date_end = end_dt.strftime("%d/%m/%Y")
    
    st.sidebar.info(f"📊 Période : **{date_start}** → **{date_end}**")
    
    return date_start, date_end


def inject_dashboard_data(html_content: str, csv_path: str) -> str:
    """Injecte les données CSV dans le HTML du dashboard"""
    try:
        df = pd.read_csv(csv_path, delimiter=config.CSV_DELIMITER)
        csv_data = df.to_csv(index=False, sep=config.CSV_DELIMITER)
        
        # Lire le CSV temps passés
        temps_passes_csv = "tempspasses_202507_affaires.csv"
        temps_passes_data = ""
        if os.path.exists(temps_passes_csv):
            with open(temps_passes_csv, "r", encoding="utf-8") as f:
                temps_passes_data = f.read()
        
        # Lire le CSV services pour correspondance IdService -> Nom
        services_data = ""
        if os.path.exists("services.csv"):
            with open("services.csv", "r", encoding="utf-8") as f:
                services_data = f.read()
        
        # Injection des trois CSV
        csv_injection = f"""
        <script>
        window.csvData = `{csv_data}`;
        window.tempsPassesData = `{temps_passes_data}`;
        window.servicesData = `{services_data}`;
        </script>
        """
        
        # Modification du chargement pour utiliser les données injectées
        html_content = html_content.replace(
            "loadCSVFromURL('http://localhost:8001/dashboard_data.csv');",
            """Papa.parse(window.csvData, {
                header: true,
                delimiter: ';',
                skipEmptyLines: true,
                complete: results => {
                    rawData = results.data;
                    if (!rawData.length) { messageEl.innerText = 'Aucune donnée trouvée.'; return; }
                    messageEl.innerText = `Données chargées: ${rawData.length} lignes`;
                    buildFilters(rawData);
                    updateDashboard();
                }
            });"""
        )
        
        # Injection avant la fermeture du head
        html_content = html_content.replace("</head>", csv_injection + "</head>")
        
        return html_content
        
    except Exception as e:
        st.error(f"Erreur injection dashboard: {e}")
        return html_content


def display_dashboard(csv_path: str):
    """Affiche le dashboard intégré"""
    dashboard_path = config.DASHBOARD_FILE
    
    if not os.path.exists(dashboard_path):
        st.error(f"❌ Fichier {dashboard_path} non trouvé")
        return
    
    try:
        with open(dashboard_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        
        # Injection des données
        html_content = inject_dashboard_data(html_content, csv_path)
        
        # Affichage du dashboard
        st.markdown("### 📊 Dashboard Moduléo")
        components.html(html_content, height=config.DASHBOARD_HEIGHT, scrolling=True)
        
    except Exception as e:
        st.error(f"Erreur affichage dashboard: {e}")


def show_pipeline_summary(pipeline: ModuleoPipeline):
    """Affiche un résumé du pipeline exécuté"""
    summary = pipeline.get_summary()
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📋 Résumé")
    st.sidebar.metric("Étapes", summary['steps_count'])
    st.sidebar.metric("Résultats", summary['results_count'])
    
    if summary['final_csv'] and os.path.exists(summary['final_csv']):
        file_size = os.path.getsize(summary['final_csv'])
        st.sidebar.metric("Taille fichier", format_file_size(file_size))


def main():
    """Fonction principale"""
    # Configuration de la page
    st.set_page_config(
        page_title="Moduleo Report - Pipeline Complet", 
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialisation
    init_session_state()
    logger = setup_logging()
    
    # Validation de la config
    try:
        config.validate()
    except ValueError as e:
        st.error(f"🔴 Configuration invalide: {e}")
        st.stop()
    
    # Authentification
    if not st.session_state.authenticated:
        authenticate_user()
        st.stop()
    
    # Interface utilisateur connecté
    show_user_info()
    
    # Titre et description
    st.title("🏢 Moduleo Report - Pipeline Automatisé")
    st.markdown("""
    Cette application génère automatiquement les rapports Moduleo en exécutant 
    l'ensemble du pipeline de traitement des données.
    """)
    
    # Sélection de la période
    date_start, date_end = get_date_range()
    
    # Bouton d'exécution principal
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        execute_pipeline = st.button(
            "🚀 Exécuter le Pipeline Complet",
            type="primary",
            use_container_width=True
        )
    
    if execute_pipeline:
        logger.info(f"Démarrage pipeline par {st.session_state.name}")
        
        # Conteneur pour les logs (collapsible)
        with st.expander("📋 Logs du traitement", expanded=True):
            st.markdown(f"### 🔄 Traitement pour la période **{date_start}** → **{date_end}**")
            
            # Création et exécution du pipeline
            pipeline = ModuleoPipeline(date_start, date_end)
            
            if pipeline.run_pipeline(show_progress=True):
                # Succès du pipeline
                st.success(f"✅ **Pipeline terminé avec succès !** Période: {date_start} → {date_end}")
                
                # Préparation des données pour le dashboard
                if pipeline.prepare_dashboard_data():
                    st.info("📊 Données dashboard préparées")
                
                # Stockage des résultats dans la session
                st.session_state.pipeline_results = {
                    'csv_path': pipeline.get_final_csv(),
                    'date_range': f"{date_start} → {date_end}",
                    'summary': pipeline.get_summary()
                }
                
                # Affichage du résumé
                show_pipeline_summary(pipeline)
                
            else:
                st.error("❌ Le pipeline a échoué. Consultez les logs pour plus de détails.")
                logger.error(f"Échec pipeline pour {st.session_state.name}")
    
    # Affichage du dashboard si des données sont disponibles
    if st.session_state.pipeline_results and 'csv_path' in st.session_state.pipeline_results:
        csv_path = st.session_state.pipeline_results['csv_path']
        
        if csv_path and os.path.exists(csv_path):
            st.markdown("---")
            
            # Bouton de téléchargement
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                with open(csv_path, "rb") as f:
                    st.download_button(
                        label="📥 Télécharger le fichier CSV final",
                        data=f,
                        file_name=os.path.basename(csv_path),
                        mime="text/csv",
                        use_container_width=True
                    )
            
            # Dashboard
            display_dashboard(csv_path)


if __name__ == "__main__":
    main()