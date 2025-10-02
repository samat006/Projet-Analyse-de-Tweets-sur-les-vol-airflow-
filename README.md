# ETL Tweets avec Apache Airflow

## Description
Ce projet implémente un pipeline ETL pour analyser les tweets d'une compagnie aérienne.  
Le pipeline est construit avec **Apache Airflow** et effectue les étapes suivantes :  

1. **Extract** : récupération des données à partir d'un fichier CSV.
2. **Transform** : nettoyage et transformation des données.
3. **Load** : insertion des données transformées dans une base PostgreSQL.
4. **Analyse** : calcul des statistiques de sentiment global (positif/négatif).

---

## Technologies utilisées
- Python 3.10
- Apache Airflow 2.7
- Docker & Docker Compose
- PostgreSQL
- Pandas
- SQLAlchemy

---

## Prérequis
- Docker et Docker Compose installés
- Accès à GitHub pour cloner le projet
- Variables d'environnement pour Airflow et PostgreSQL configurées dans un fichier `.env`

Exemple `.env` :
