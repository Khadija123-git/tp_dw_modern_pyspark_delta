# TP Data Warehouse Moderne – SCD Type 2, Data Vault 2.0 et Architecture Lakehouse avec PySpark & Delta Lake

## Objectifs

Construire un Data Warehouse complet et moderne, en suivant toutes les étapes du pipeline de données :
- Installer PostgreSQL, Python & PySpark
- Connecter PySpark à PostgreSQL avec JDBC
- Appliquer la gestion d'historique (SCD Type 2)
- Mettre en place une architecture Data Vault 2.0
- Implémenter une architecture Lakehouse (Bronze/Silver/Gold)
- Utiliser Delta Lake pour la fiabilité, l’ACIDité et le Time Travel

## Technologies/Stack utilisées

- PostgreSQL (système source OLTP)
- PySpark & Spark SQL
- Delta Lake
- Data Vault 2.0 (modélisation – Hubs, Links, Sats)
- Python 3.x
- Jupyter Notebook (optionnel mais conseillé)
- Power BI (ou autre BI pour visualisation)

## Organisation du dépôt

- `/postgres/` : scripts SQL pour créer les tables sources et charger des jeux de données exemples
- `/data/` : dumps CSV, extraits de données si besoin
- `/notebooks/` : notebooks Jupyter/PySpark pour chaque étape du pipeline
- `/pyspark/` : scripts principaux de traitement par couches (bronze, silver, gold, SCD2, Data Vault, etc.)
- `/delta_lake/` : scripts pour tests ACID/Time Travel, captures d’écran du Lakehouse, etc.
- `/schema/` : diagrammes d’architecture, lakehouse, dataflow, etc.
- `rapport.pdf` : rapport décrivant toutes les étapes, choix techniques, difficultés

## Instructions d’exécution

1. **Installation**
   - Installer PostgreSQL, Python (>=3.8), Java et PySpark (`pip install pyspark`)
   - Installer Delta Lake (`pip install delta-spark`)
   - Configurer les connections (voir `/postgres/` et `/pyspark/connect_pg_to_spark.py`)

2. **Base source**
   - Lancer les scripts SQL de `/postgres/` pour créer et remplir la base avec des exemples

3. **Pipeline PySpark**
   - Exécuter les Notebooks/Scripts dans `/notebooks/` et `/pyspark/` par ordre logique :
     1. Ingestion Bronze (raw)
     2. Transformation Silver (nettoyage, contrôle qualité)
     3. Gestion SCD Type 2 (historisation)
     4. Data Vault 2.0 (modélisation)
     5. Gold Layer (agrégats analytiques)
     6. Utiliser Delta Lake (queries ACID, time travel)

4. **Documentation / Rapport**
   - Voir `rapport.pdf` pour la démarche complète, schémas, explications détaillées et captures d’écran

5. **(Optionnel) Reporting**
   - Extrait ou sample de dataset à charger dans un outil BI (prévu pour un dashboard Power BI, Tableau ou autre)

## Membres du groupe

- Khadija Nachid Idrissi
- Rajae Fdili
- Aya Hamim
