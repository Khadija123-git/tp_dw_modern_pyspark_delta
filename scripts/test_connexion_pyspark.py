import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# ================================================================
# Configuration Hadoop (Windows)
# ================================================================
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

print("Bibliothèques importées avec succès")

# ================================================================
# ÉTAPE 2 : Création de la session Spark
# ================================================================
builder = SparkSession.builder \
    .appName("Test Connexion PostgreSQL") \
    .master("local[*]") \
    .config(
        "spark.jars",
        r"C:\Users\LENOVO\Desktop\TP_DataWarehouse\drivers\postgresql-42.7.7.jar"
    )

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Session Spark créée avec succès")
print(f"Version de Spark : {spark.version}")

# ================================================================
# ÉTAPE 3 : Configuration PostgreSQL
# ================================================================
postgres_config = {
    "url": "jdbc:postgresql://localhost:5432/shopstream",
    # shopstream = nom de la base de données

    "dbtable": "test_table",
    # table de test créée dans le schéma public

    "user": "postgres",
    "password": "postgres123",

    "driver": "org.postgresql.Driver"
}

print("Configuration PostgreSQL définie")

# ================================================================
# ÉTAPE 4 : Lecture des données
# ================================================================
print("\nTentative de connexion à PostgreSQL...")

try:
    df = spark.read.format("jdbc").options(**postgres_config).load()

    print("Connexion réussie !")
    print(f"Nombre de lignes lues : {df.count()}")

    print("\nSchéma de la table :")
    df.printSchema()

    print("\nAperçu des données :")
    df.show(5)

    print("\nTEST RÉUSSI : PySpark peut lire PostgreSQL")

except Exception as e:
    print("ERREUR :", e)

# ================================================================
# ÉTAPE 5 : Arrêt de Spark
# ================================================================
spark.stop()
print("Session Spark arrêtée")

