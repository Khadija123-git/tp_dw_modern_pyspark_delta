# ================================================================
# Script : Transformation Bronze → Silver
# ================================================================

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import *

# Configuration
BRONZE_PATH = 'C:/lakehouse/bronze'
SILVER_PATH = 'C:/lakehouse/silver'

# Créer session Spark
builder = SparkSession.builder.appName("Silver Transformation").master("local[*]")
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("="*70)
print("TRANSFORMATION SILVER")
print("="*70)

# Lire Bronze
df_clients = spark.read.format("delta").load(f"{BRONZE_PATH}/clients")

# Nettoyage :
# 1. Supprimer les doublons
# 2. Standardiser les noms (majuscules)
# 3. Valider les emails
df_clients_clean = df_clients \
    .dropDuplicates(['client_id']) \
    .withColumn('nom', upper(col('nom'))) \
    .withColumn('prenom', initcap(col('prenom'))) \
    .filter(col('email').rlike('^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'))

# EXPLICATIONS :
# - dropDuplicates(['client_id']) : supprime les doublons sur client_id
# - upper(col('nom')) : met le nom en MAJUSCULES
# - initcap(col('prenom')) : Met La Première Lettre En Majuscule
# - rlike(...) : filtre avec une regex pour valider le format email

print(f"Clients Bronze : {df_clients.count()}")
print(f"Clients Silver (après nettoyage) : {df_clients_clean.count()}")

# Écrire en Silver
df_clients_clean.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/clients")

print("✓ Silver clients créé")

spark.stop()