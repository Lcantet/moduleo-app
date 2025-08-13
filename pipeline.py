"""
Pipeline de traitement des données Moduleo
"""
import os
import shutil
import pandas as pd
import streamlit as st
from typing import Dict, Optional, Callable, Tuple, Any
from dataclasses import dataclass

from config import config
from utils import setup_logging, safe_execute, ProgressTracker

# Import des modules métier
from import_tempspasses import fetch_raw_tempspasses, enrich_with_affaire, export_unique_affaires
from fetch_affaire_tempspasses import fetch_and_export_affaires, calculate_prix_for_period
from fetch_affaire_details import fetch_and_save_details, fetch_and_merge_details
from fetch_affaire_devis import fetch_and_update_devis
from fetch_affaire_factures import fetch_and_update_factures


@dataclass
class PipelineStep:
    """Représente une étape du pipeline"""
    name: str
    function: Callable
    args: tuple = ()
    kwargs: dict = None
    
    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}


class ModuleoPipeline:
    """Pipeline de traitement des données Moduleo"""
    
    def __init__(self, date_start: str, date_end: str):
        self.date_start = date_start
        self.date_end = date_end
        self.results: Dict[str, Any] = {}
        self.logger = setup_logging()
        self.progress_tracker: Optional[ProgressTracker] = None
        
        # Définition des étapes
        self.steps = [
            PipelineStep(
                name="Import des temps passés",
                function=fetch_raw_tempspasses,
                args=(date_start, date_end)
            ),
            PipelineStep(
                name="Enrichissement avec affaires",
                function=self._step_enrich_affaires,
            ),
            PipelineStep(
                name="Export des affaires uniques",
                function=self._step_export_unique,
            ),
            PipelineStep(
                name="Temps passés par affaire",
                function=fetch_and_export_affaires,
                args=(date_start, date_end)
            ),
            PipelineStep(
                name="Calcul PrixVenteCollaborateur",
                function=calculate_prix_for_period,
                args=(date_start, date_end)
            ),
            PipelineStep(
                name="Sauvegarde détails affaires",
                function=fetch_and_save_details,
                args=(date_start, date_end)
            ),
            PipelineStep(
                name="Fusion détails & prix",
                function=fetch_and_merge_details,
                args=(date_start, date_end)
            ),
            PipelineStep(
                name="Intégration des devis",
                function=fetch_and_update_devis,
                args=(date_start, date_end)
            ),
            PipelineStep(
                name="Intégration des factures",
                function=fetch_and_update_factures,
                args=(date_start, date_end)
            ),
        ]
    
    def _step_enrich_affaires(self):
        """Étape d'enrichissement avec les affaires"""
        if 'raw_csv' not in self.results:
            raise ValueError("CSV brut non trouvé")
        return enrich_with_affaire(self.results['raw_csv'])
    
    def _step_export_unique(self):
        """Étape d'export des affaires uniques"""
        if 'enriched_csv' not in self.results:
            raise ValueError("CSV enrichi non trouvé")
        return export_unique_affaires(self.results['enriched_csv'])
    
    def run_step(self, step: PipelineStep) -> bool:
        """
        Exécute une étape du pipeline
        
        Args:
            step: L'étape à exécuter
        
        Returns:
            True si succès, False sinon
        """
        self.logger.info(f"Démarrage étape: {step.name}")
        
        if self.progress_tracker:
            self.progress_tracker.update(step.name)
        
        # Fonction wrapper pour passer les arguments
        def execute_step():
            return step.function(*step.args, **step.kwargs)
        
        success, result = safe_execute(
            func=execute_step,
            error_message=f"Erreur {step.name}",
            logger=self.logger,
            show_in_ui=True
        )
        
        if success:
            # Stockage du résultat avec une clé dérivée du nom
            result_key = step.name.lower().replace(" ", "_").replace("é", "e")
            if step.name == "Import des temps passés":
                result_key = "raw_csv"
            elif step.name == "Enrichissement avec affaires":
                result_key = "enriched_csv"
            elif step.name == "Export des affaires uniques":
                result_key = "unique_csv"
            elif step.name == "Intégration des factures":
                result_key = "final_csv"
            
            self.results[result_key] = result
            
            st.success(f"✅ {step.name}: `{result}`")
            self.logger.info(f"Étape terminée avec succès: {step.name} -> {result}")
            
        return success
    
    def run_pipeline(self, show_progress: bool = True) -> bool:
        """
        Exécute le pipeline complet
        
        Args:
            show_progress: Si True, affiche une barre de progression
        
        Returns:
            True si tout s'est bien passé, False sinon
        """
        self.logger.info(f"Démarrage pipeline pour période: {self.date_start} -> {self.date_end}")
        
        if show_progress:
            self.progress_tracker = ProgressTracker(len(self.steps))
        
        try:
            for step in self.steps:
                if not self.run_step(step):
                    if self.progress_tracker:
                        self.progress_tracker.error(f"Échec à l'étape: {step.name}")
                    self.logger.error(f"Pipeline arrêté à l'étape: {step.name}")
                    return False
            
            if self.progress_tracker:
                self.progress_tracker.complete()
            
            self.logger.info("Pipeline terminé avec succès")
            return True
            
        except Exception as e:
            self.logger.error(f"Erreur critique dans le pipeline: {e}")
            if self.progress_tracker:
                self.progress_tracker.error(str(e))
            st.error(f"Erreur critique: {e}")
            return False
    
    def get_final_csv(self) -> Optional[str]:
        """Retourne le chemin du CSV final"""
        return self.results.get('final_csv')
    
    def prepare_dashboard_data(self) -> bool:
        """Prépare les données pour le dashboard"""
        final_csv = self.get_final_csv()
        if not final_csv or not os.path.exists(final_csv):
            self.logger.error("CSV final non trouvé pour le dashboard")
            return False
        
        try:
            # Copier le CSV avec un nom prévisible
            dashboard_csv = "dashboard_data.csv"
            shutil.copy(final_csv, dashboard_csv)
            self.logger.info(f"Données dashboard préparées: {dashboard_csv}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erreur préparation dashboard: {e}")
            return False
    
    def get_summary(self) -> Dict[str, Any]:
        """Retourne un résumé de l'exécution du pipeline"""
        return {
            'period': f"{self.date_start} -> {self.date_end}",
            'steps_count': len(self.steps),
            'results_count': len(self.results),
            'final_csv': self.get_final_csv(),
            'results': list(self.results.keys())
        }