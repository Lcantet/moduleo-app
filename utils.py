"""
Utilitaires pour l'application Moduleo Report
"""
import logging
import traceback
import streamlit as st
from datetime import date
from typing import Optional, Callable, Any


def setup_logging() -> logging.Logger:
    """Configure le système de logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('moduleo_app.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def safe_execute(func: Callable, error_message: str, logger: logging.Logger, 
                 show_in_ui: bool = True) -> tuple[bool, Optional[Any]]:
    """
    Exécute une fonction de manière sécurisée avec gestion d'erreurs
    
    Args:
        func: Fonction à exécuter
        error_message: Message d'erreur à afficher
        logger: Logger à utiliser
        show_in_ui: Si True, affiche l'erreur dans l'UI Streamlit
    
    Returns:
        (success: bool, result: Any)
    """
    try:
        result = func()
        return True, result
    except Exception as e:
        logger.error(f"{error_message}: {e}")
        logger.error(traceback.format_exc())
        
        if show_in_ui:
            st.error(f"{error_message}: {e}")
        
        return False, None


def validate_date_range(start_dt: date, end_dt: date) -> bool:
    """
    Valide une plage de dates
    
    Args:
        start_dt: Date de début
        end_dt: Date de fin
    
    Returns:
        True si valide, False sinon
    """
    if start_dt > end_dt:
        st.error("❌ La date de début doit être antérieure à la date de fin")
        return False
    
    days_diff = (end_dt - start_dt).days
    
    if days_diff > 365:
        st.warning(f"⚠️ Période très longue ({days_diff} jours), cela peut prendre du temps")
    elif days_diff == 0:
        st.info("ℹ️ Traitement d'une seule journée")
    
    return True


def format_file_size(size_bytes: int) -> str:
    """Formate une taille de fichier en format lisible"""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"


class ProgressTracker:
    """Classe pour gérer les barres de progression Streamlit"""
    
    def __init__(self, total_steps: int):
        self.total_steps = total_steps
        self.current_step = 0
        self.progress_bar = st.progress(0)
        self.status_text = st.empty()
    
    def update(self, step_name: str):
        """Met à jour la progression"""
        self.current_step += 1
        progress = self.current_step / self.total_steps
        self.progress_bar.progress(progress)
        self.status_text.text(f'Étape {self.current_step}/{self.total_steps}: {step_name}')
    
    def complete(self):
        """Marque la progression comme terminée"""
        self.progress_bar.progress(1.0)
        self.status_text.text('✅ Traitement terminé')
    
    def error(self, error_message: str):
        """Affiche une erreur"""
        self.status_text.text(f'❌ Erreur: {error_message}')