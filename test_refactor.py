"""
Script de test pour la nouvelle architecture
"""
from config import config
from utils import setup_logging
from pipeline import ModuleoPipeline

def test_config():
    """Test de la configuration"""
    print("=== Test Configuration ===")
    print(f"API URL: {config.API_BASE_URL}")
    print(f"Dashboard Height: {config.DASHBOARD_HEIGHT}")
    print(f"Headers: {config.get_api_headers()}")
    
    try:
        config.validate()
        print("✅ Configuration valide")
    except ValueError as e:
        print(f"❌ Configuration invalide: {e}")

def test_pipeline():
    """Test du pipeline (sans exécution réelle)"""
    print("\n=== Test Pipeline ===")
    
    # Dates de test
    date_start = "01/07/2025"
    date_end = "31/07/2025"
    
    pipeline = ModuleoPipeline(date_start, date_end)
    
    print(f"Pipeline créé pour: {date_start} -> {date_end}")
    print(f"Nombre d'étapes: {len(pipeline.steps)}")
    
    for i, step in enumerate(pipeline.steps, 1):
        print(f"  {i}. {step.name}")
    
    # Test du résumé
    summary = pipeline.get_summary()
    print(f"Résumé: {summary}")

def test_logging():
    """Test du système de logging"""
    print("\n=== Test Logging ===")
    
    logger = setup_logging()
    logger.info("Test message INFO")
    logger.warning("Test message WARNING")
    logger.error("Test message ERROR")
    
    print("✅ Messages de log envoyés (vérifiez moduleo_app.log)")

if __name__ == "__main__":
    print("🧪 Test de la nouvelle architecture Moduleo\n")
    
    test_config()
    test_pipeline()
    test_logging()
    
    print("\n✅ Tests terminés !")
    print("📝 Pour utiliser la nouvelle version, lancez: streamlit run app_refactored.py")