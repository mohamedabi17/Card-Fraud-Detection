# Credit Card Fraud Detection - Makefile
# Simplifie les commandes de développement et déploiement

.PHONY: help setup start stop clean test ingest transform load pipeline status logs

# Variables
DOCKER_COMPOSE = docker-compose -f docker/docker-compose.yml
PYTHON = python
VENV_ACTIVATE = venv/Scripts/activate  # Windows
# VENV_ACTIVATE = venv/bin/activate    # Linux/Mac

help: ## Affiche l'aide
	@echo "🚀 Credit Card Fraud Detection - Commandes disponibles:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup: ## Installation complète du projet
	@echo "🚀 Installation du projet..."
	$(PYTHON) setup.py

start: ## Démarre tous les services Docker
	@echo "🚀 Démarrage des services..."
	$(DOCKER_COMPOSE) up -d
	@echo "✅ Services démarrés!"
	@echo "📋 Interfaces disponibles:"
	@echo "  • Airflow: http://localhost:8080 (admin/admin)"
	@echo "  • PgAdmin: http://localhost:5050 (admin@ccf.local/admin123)"
	@echo "  • Jupyter: http://localhost:8888 (token: ccf_token)"

stop: ## Arrête tous les services Docker
	@echo "⏹️  Arrêt des services..."
	$(DOCKER_COMPOSE) down
	@echo "✅ Services arrêtés!"

restart: stop start ## Redémarre tous les services

status: ## Affiche le statut des services
	@echo "📊 Statut des services:"
	$(DOCKER_COMPOSE) ps

logs: ## Affiche les logs de tous les services
	$(DOCKER_COMPOSE) logs -f

logs-airflow: ## Affiche les logs Airflow uniquement
	$(DOCKER_COMPOSE) logs -f airflow-webserver airflow-scheduler airflow-worker

build: ## Reconstruit les images Docker
	@echo "🔨 Reconstruction des images..."
	$(DOCKER_COMPOSE) build --no-cache
	@echo "✅ Images reconstruites!"

# Pipeline commands
ingest: ## Execute l'ingestion des données
	@echo "📥 Démarrage de l'ingestion..."
	$(PYTHON) scripts/ingest.py
	@echo "✅ Ingestion terminée!"

transform: ## Execute la transformation PySpark
	@echo "⚙️  Démarrage de la transformation..."
	$(PYTHON) scripts/transform_spark.py
	@echo "✅ Transformation terminée!"

load: ## Execute le chargement PostgreSQL
	@echo "📊 Démarrage du chargement..."
	$(PYTHON) scripts/load_postgres.py
	@echo "✅ Chargement terminé!"

pipeline: ingest transform load ## Execute le pipeline complet
	@echo "🎉 Pipeline complet terminé!"

# Development commands
test: ## Execute les tests unitaires
	@echo "🧪 Exécution des tests..."
	$(PYTHON) -m pytest tests/ -v
	@echo "✅ Tests terminés!"

test-coverage: ## Execute les tests avec couverture
	@echo "🧪 Tests avec couverture..."
	$(PYTHON) -m pytest tests/ --cov=scripts --cov-report=html
	@echo "📊 Rapport de couverture: htmlcov/index.html"

lint: ## Vérifie la qualité du code
	@echo "🔍 Vérification du code..."
	$(PYTHON) -m flake8 scripts/ tests/
	$(PYTHON) -m black --check scripts/ tests/
	@echo "✅ Code vérifié!"

format: ## Formate le code
	@echo "🎨 Formatage du code..."
	$(PYTHON) -m black scripts/ tests/
	@echo "✅ Code formaté!"

# Data commands
download-data: ## Télécharge uniquement les données Kaggle
	@echo "📥 Téléchargement des données..."
	kaggle datasets download -d mlg-ulb/creditcardfraud -p data/raw --unzip
	@echo "✅ Données téléchargées!"

clean-data: ## Nettoie les données temporaires
	@echo "🧹 Nettoyage des données..."
	rm -rf data/processed/*
	rm -rf data/archive/*
	rm -rf logs/*
	@echo "✅ Données nettoyées!"

# Database commands
db-connect: ## Se connecte à PostgreSQL
	docker exec -it ccf_postgres psql -U ccf_user -d ccf_db

db-backup: ## Sauvegarde la base de données
	@echo "💾 Sauvegarde de la base de données..."
	docker exec ccf_postgres pg_dump -U ccf_user ccf_db > backup_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "✅ Sauvegarde créée!"

db-reset: ## Remet à zéro la base de données
	@echo "🔄 Reset de la base de données..."
	docker exec ccf_postgres psql -U ccf_user -d ccf_db -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
	@echo "✅ Base de données remise à zéro!"

# Monitoring commands
monitor: ## Lance le monitoring des ressources
	@echo "📊 Monitoring des ressources..."
	docker stats

airflow-trigger: ## Déclenche manuellement le DAG Airflow
	@echo "🚀 Déclenchement du DAG..."
	docker exec ccf_airflow_webserver airflow dags trigger credit_card_fraud_pipeline
	@echo "✅ DAG déclenché!"

# Cleanup commands
clean: ## Nettoie tous les fichiers temporaires
	@echo "🧹 Nettoyage complet..."
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf *.pyc
	rm -rf .coverage
	@echo "✅ Nettoyage terminé!"

clean-all: stop clean clean-data ## Nettoie tout et arrête les services
	@echo "🧹 Nettoyage complet avec arrêt des services..."
	docker system prune -f
	@echo "✅ Nettoyage complet terminé!"

# Environment commands
env-check: ## Vérifie l'environnement
	@echo "🔍 Vérification de l'environnement..."
	$(PYTHON) --version
	pip list | grep -E "(pyspark|pandas|sqlalchemy|airflow)"
	docker --version
	docker-compose --version
	@echo "✅ Environnement vérifié!"

env-install: ## Installe les dépendances Python
	@echo "📦 Installation des dépendances..."
	pip install -r requirements.txt
	@echo "✅ Dépendances installées!"

# Documentation
docs: ## Génère la documentation
	@echo "📚 Génération de la documentation..."
	# Add documentation generation commands here
	@echo "✅ Documentation générée!"

# Quick start
quick-start: start airflow-trigger ## Démarrage rapide complet
	@echo "🚀 Démarrage rapide terminé!"
	@echo "📋 Vérifiez Airflow: http://localhost:8080"
