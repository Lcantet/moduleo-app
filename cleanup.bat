@echo off
REM Script de nettoyage après refactoring

echo Nettoyage du dossier moduleo-app...

REM Suppression des fichiers de test
if exist test_refactor.py del test_refactor.py

REM Suppression des logs (ils se recréent automatiquement)  
if exist moduleo_app.log del moduleo_app.log

REM Suppression des fichiers temporaires
if exist dashboard_data.csv del dashboard_data.csv

REM Suppression des fichiers Python cache
if exist __pycache__ rmdir /s /q __pycache__

REM Optionnel : Suppression de l'ancienne version (ATTENTION !)
REM if exist app.py ren app.py app_original.py

echo.
echo ✅ Nettoyage terminé !
echo.
echo Fichiers conservés :
echo - app.py (version originale)
echo - app_refactored.py (version améliorée)
echo - config.py, utils.py, pipeline.py (nouveaux modules)
echo - Tous tes CSV et modules métier
echo.
echo Pour utiliser la nouvelle version : streamlit run app_refactored.py
pause