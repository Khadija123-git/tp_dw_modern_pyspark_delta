# ================================================================
# Script : Création de la couche Gold
# ================================================================

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import *

SILVER_PATH = 'C:/lakehouse/silver'
GOLD_PATH = 'C:/lakehouse/gold'

builder = SparkSession.builder.appName("Gold Aggregation").master("local[*]")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("="*70)
print("CRÉATION COUCHE GOLD")
print("="*70)

# Lire Silver
df_ventes = spark.read.format("delta").load(f"{SILVER_PATH}/ventes")

# Agrégation : Ventes par jour
ventes_quotidiennes = df_ventes \
    .withColumn('date', to_date(col('date_vente'))) \
    .groupBy('date') \
    .agg(
        count('*').alias('nb_ventes'),
        sum('montant_total').alias('ca_total'),
        avg('montant_total').alias('panier_moyen')
    ) \
    .orderBy('date')

# EXPLICATIONS :
# - to_date(col('date_vente')) : convertit timestamp en date (sans heure)
# - groupBy('date') : regroupe par jour
# - agg(...) : applique des agrégations
#   * count('*') : compte les ventes
#   * sum('montant_total') : somme des montants
#   * avg('montant_total') : moyenne des montants
# - orderBy('date') : trie par date

ventes_quotidiennes.show()

# Écrire en Gold
ventes_quotidiennes.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/ventes_quotidiennes")

print("✓ Gold ventes_quotidiennes créé")

spark.stop()