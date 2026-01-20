# ================================================================
# Script : Génération de rapport final
# ================================================================

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from datetime import datetime

GOLD_PATH = 'C:/lakehouse/gold'

builder = SparkSession.builder.appName("Rapport Final").master("local[*]")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("\n" + "="*70)
print("RAPPORT DATA WAREHOUSE - " + datetime.now().strftime("%Y-%m-%d %H:%M"))
print("="*70 + "\n")

# Lire Gold
df_ventes_quot = spark.read.format("delta").load(f"{GOLD_PATH}/ventes_quotidiennes")

# Statistiques globales
stats = df_ventes_quot.agg(
    sum('nb_ventes').alias('total_ventes'),
    sum('ca_total').alias('ca_global'),
    avg('panier_moyen').alias('panier_moyen_global')
).collect()[0]

print("STATISTIQUES GLOBALES")
print("-"*70)
print(f"Total des ventes : {stats['total_ventes']}")
print(f"Chiffre d'affaires : {stats['ca_global']:.2f} €")
print(f"Panier moyen : {stats['panier_moyen_global']:.2f} €")

print("\n" + "="*70)
print("TOP 5 MEILLEURS JOURS")
print("="*70)
df_ventes_quot.orderBy(col('ca_total').desc()).show(5)

print("\n✓ Rapport généré avec succès")

spark.stop()