"""
Configuration centralisée pour l'application Moduleo Report
"""
import os
import json
from dataclasses import dataclass
from typing import Dict, Any
from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv()

@dataclass
class Config:
    """Configuration de l'application"""
    
    # API Configuration
    API_BASE_URL: str
    API_KEY: str
    SECURITY_CODE: str
    
    # UI Configuration
    DASHBOARD_HEIGHT: int = 1200
    CSV_DELIMITER: str = ';'
    
    # File paths
    DASHBOARD_FILE: str = "Dasboard.html"
    SERVICES_FILE: str = "services.csv"
    
    # Pagination et limites
    MAX_RETRIES: int = 5
    BACKOFF_FACTOR: float = 2.0
    TIMEOUT: int = 30
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Crée une instance de Config à partir des variables d'environnement"""
        return cls(
            API_BASE_URL=os.getenv("API_BASE_URL", "https://mwa-metris.kipaware.fr/api"),
            API_KEY=os.getenv("MODULEO_API_KEY", ""),
            SECURITY_CODE=os.getenv("MODULEO_SECURITY_CODE", ""),
        )
    
    def get_api_headers(self) -> Dict[str, str]:
        """Retourne les headers pour les appels API"""
        return {
            "Content-Type": "application/json",
            "ApiKey": self.API_KEY,
            "SecurityCode": self.SECURITY_CODE,
            "User-Agent": "ModuleoReport/2.0",
        }
    
    def validate(self) -> bool:
        """Valide que la configuration est complète"""
        if not self.API_KEY:
            raise ValueError("MODULEO_API_KEY manquant")
        if not self.SECURITY_CODE:
            raise ValueError("MODULEO_SECURITY_CODE manquant")
        return True

def load_users() -> Dict[str, Any]:
    """Charge la configuration des utilisateurs"""
    auth_json = os.getenv("USERS_HASH")
    if not auth_json:
        raise ValueError("La variable d'environnement USERS_HASH n'est pas définie.")
    return json.loads(auth_json)

# Instance globale
config = Config.from_env()