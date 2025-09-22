#!/usr/bin/env python3
"""
Setup script for Credit Card Fraud Detection Pipeline
Automatise l'installation et la configuration du projet
"""

import os
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv
import shutil

def print_step(step_num, description):
    """Print formatted step"""
    print(f"\n{'='*60}")
    print(f"ÉTAPE {step_num}: {description}")
    print(f"{'='*60}")

def run_command(command, description, check=True):
    """Run shell command with error handling"""
    print(f"🚀 {description}")
    print(f"💻 Commande: {command}")
    
    try:
        if isinstance(command, str):
            result = subprocess.run(command, shell=True, check=check, capture_output=True, text=True)
        else:
            result = subprocess.run(command, check=check, capture_output=True, text=True)
        
        if result.stdout:
            print(f"✅ {result.stdout.strip()}")
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur: {e}")
        if e.stderr:
            print(f"💥 Détails: {e.stderr}")
        return False

def check_requirements():
    """Check system requirements"""
    print_step(1, "VÉRIFICATION DES PRÉREQUIS")
    
    requirements = {
        'python': ['python', '--version'],
        'pip': ['pip', '--version'],
        'docker': ['docker', '--version'],
        'docker-compose': ['docker-compose', '--version'],
        'git': ['git', '--version']
    }
    
    all_good = True
    for tool, command in requirements.items():
        if run_command(command, f"Vérification de {tool}", check=False):
            print(f"✅ {tool} est installé")
        else:
            print(f"❌ {tool} n'est pas installé ou accessible")
            all_good = False
    
    return all_good

def setup_python_environment():
    """Setup Python virtual environment"""
    print_step(2, "CONFIGURATION DE L'ENVIRONNEMENT PYTHON")
    
    # Create virtual environment
    if not run_command([sys.executable, '-m', 'venv', 'venv'], "Création de l'environnement virtuel"):
        return False
    
    # Activate virtual environment (platform specific)
    if sys.platform == "win32":
        activate_script = "venv\\Scripts\\activate.bat"
        pip_path = "venv\\Scripts\\pip"
    else:
        activate_script = "source venv/bin/activate"
        pip_path = "venv/bin/pip"
    
    # Install requirements
    if not run_command([pip_path, 'install', '-r', 'requirements.txt'], "Installation des dépendances Python"):
        return False
    
    print("✅ Environnement Python configuré avec succès")
    return True

def setup_environment_file():
    """Create .env file from template"""
    print_step(3, "CONFIGURATION DU FICHIER ENVIRONNEMENT")
    
    env_template = Path('.env.template')
    env_file = Path('.env')
    
    if env_template.exists():
        if not env_file.exists():
            shutil.copy(env_template, env_file)
            print("✅ Fichier .env créé depuis le template")
            print("⚠️  IMPORTANT: Editez le fichier .env avec vos credentials Kaggle")
        else:
            print("ℹ️  Le fichier .env existe déjà")
    else:
        print("❌ Template .env.template non trouvé")
        return False
    
    return True

def setup_directories():
    """Create necessary directories"""
    print_step(4, "CRÉATION DES RÉPERTOIRES")
    
    directories = [
        'logs',
        'notebooks',
        'data/raw',
        'data/processed', 
        'data/archive'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✅ Répertoire créé: {directory}")
    
    return True

def setup_docker():
    """Setup Docker containers"""
    print_step(5, "CONFIGURATION DOCKER")
    
    os.chdir('docker')
    
    # Build custom Airflow image
    if not run_command(['docker', 'build', '-f', 'Dockerfile.airflow', '-t', 'ccf-airflow', '.'], 
                      "Construction de l'image Airflow personnalisée"):
        return False
    
    # Start services
    if not run_command(['docker-compose', 'up', '-d'], "Démarrage des services Docker"):
        return False
    
    os.chdir('..')
    print("✅ Services Docker démarrés")
    return True

def initialize_airflow():
    """Initialize Airflow database and user"""
    print_step(6, "INITIALISATION D'AIRFLOW")
    
    # Wait for containers to be ready
    print("⏳ Attente du démarrage des conteneurs...")
    import time
    time.sleep(30)
    
    # Initialize Airflow database
    if not run_command([
        'docker', 'exec', 'ccf_airflow_webserver', 
        'airflow', 'db', 'init'
    ], "Initialisation de la base de données Airflow"):
        return False
    
    # Create admin user
    if not run_command([
        'docker', 'exec', 'ccf_airflow_webserver',
        'airflow', 'users', 'create',
        '--username', 'admin',
        '--password', 'admin',
        '--firstname', 'Admin',
        '--lastname', 'User',
        '--role', 'Admin',
        '--email', 'admin@ccf.local'
    ], "Création de l'utilisateur admin Airflow"):
        return False
    
    print("✅ Airflow initialisé avec succès")
    print("🌐 Interface web: http://localhost:8080 (admin/admin)")
    return True

def setup_kaggle_config():
    """Guide user through Kaggle setup"""
    print_step(7, "CONFIGURATION KAGGLE")
    
    kaggle_dir = Path.home() / '.kaggle'
    kaggle_file = kaggle_dir / 'kaggle.json'
    
    if kaggle_file.exists():
        print("✅ Configuration Kaggle trouvée")
        return True
    
    print("⚠️  Configuration Kaggle requise:")
    print("1. Aller sur https://www.kaggle.com/account")
    print("2. Scroll vers 'API' section")
    print("3. Cliquer 'Create New API Token'")
    print("4. Télécharger kaggle.json")
    print(f"5. Placer kaggle.json dans: {kaggle_dir}")
    print("6. Relancer ce script")
    
    return False

def run_tests():
    """Run test suite"""
    print_step(8, "EXÉCUTION DES TESTS")
    
    if not run_command(['python', '-m', 'pytest', 'tests/', '-v'], "Exécution des tests unitaires"):
        print("⚠️  Certains tests ont échoué, mais l'installation peut continuer")
    
    return True

def show_final_info():
    """Show final setup information"""
    print_step(9, "INFORMATION FINALE")
    
    print("🎉 Installation terminée avec succès!")
    print("\n📋 SERVICES DISPONIBLES:")
    print("• Airflow Web UI: http://localhost:8080 (admin/admin)")
    print("• PostgreSQL: localhost:5432 (ccf_user/ccf_password)")
    print("• PgAdmin: http://localhost:5050 (admin@ccf.local/admin123)")
    print("• Jupyter: http://localhost:8888 (token: ccf_token)")
    
    print("\n🚀 PROCHAINES ÉTAPES:")
    print("1. Configurer vos credentials Kaggle dans .env")
    print("2. Tester l'ingestion: python scripts/ingest.py")
    print("3. Tester la transformation: python scripts/transform_spark.py")
    print("4. Tester le chargement: python scripts/load_postgres.py")
    print("5. Activer le DAG dans Airflow UI")
    
    print("\n📚 COMMANDES UTILES:")
    print("• Arrêter les services: docker-compose -f docker/docker-compose.yml down")
    print("• Redémarrer les services: docker-compose -f docker/docker-compose.yml up -d")
    print("• Voir les logs: docker-compose -f docker/docker-compose.yml logs -f")

def main():
    """Main setup function"""
    print("🚀 Credit Card Fraud Detection - Setup automatique")
    print("Ce script va configurer tout l'environnement pour vous")
    
    try:
        if not check_requirements():
            print("❌ Prérequis manquants. Veuillez installer les outils requis.")
            return False
        
        if not setup_python_environment():
            print("❌ Échec de la configuration Python")
            return False
        
        if not setup_environment_file():
            print("❌ Échec de la configuration du fichier environnement")
            return False
        
        if not setup_directories():
            print("❌ Échec de la création des répertoires")
            return False
        
        # Check Kaggle config before Docker (saves time if missing)
        if not setup_kaggle_config():
            print("⚠️  Configuration Kaggle requise pour continuer")
            return False
        
        if not setup_docker():
            print("❌ Échec de la configuration Docker")
            return False
        
        if not initialize_airflow():
            print("❌ Échec de l'initialisation Airflow")
            return False
        
        run_tests()  # Non-blocking
        
        show_final_info()
        return True
        
    except KeyboardInterrupt:
        print("\n⏹️  Installation interrompue par l'utilisateur")
        return False
    except Exception as e:
        print(f"\n💥 Erreur inattendue: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
