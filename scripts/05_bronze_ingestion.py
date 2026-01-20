from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import current_timestamp, lit

print("✓ Bibliothèques importées")

POSTGRES_CONFIG = {
    'url': 'jdbc:postgresql://localhost:5432/retailpro_dwh',
    'user': 'postgres',
    'password': 'postgres123',
    'driver': 'org.postgresql.Driver'
}

BRONZE_PATH = "file:///C:/Users/LENOVO/Desktop/TP_DataWarehouse/lakehouse/bronze"
print(f"✓ Configuration définie - Bronze path: {BRONZE_PATH}")

def creer_session_spark():
    """
    Crée une session Spark stricte compatible Delta Lake et Windows
    """
    print("\nCréation de la session Spark...")

    builder = SparkSession.builder \
        .appName("Bronze Ingestion") \
        .master("local[*]") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs") \
        .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse") \
        .config("spark.jars", "file:///C:/Users/LENOVO/Desktop/TP_DataWarehouse/drivers/postgresql-42.7.7.jar") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.LocalLogStore") # LA CLEF

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Session Spark créée - Version: {spark.version}")

    # Vérification stricte
    print("Extension Delta:", spark.conf.get("spark.sql.extensions"))
    print("Delta LogStore  :", spark.conf.get("spark.delta.logStore.class"))

    return spark

def lire_table_postgres(spark, nom_table):
    print(f"\nLecture de la table '{nom_table}' depuis PostgreSQL...")
    df = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_CONFIG['url']) \
        .option("dbtable", nom_table) \
        .option("user", POSTGRES_CONFIG['user']) \
        .option("password", POSTGRES_CONFIG['password']) \
        .option("driver", POSTGRES_CONFIG['driver']) \
        .load()
    nb_lignes = df.count()
    print(f"✓ {nb_lignes} lignes lues depuis '{nom_table}'")
    print("Schéma :")
    df.printSchema()
    return df

def ajouter_metadata(df, nom_table_source):
    df_enrichi = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("PostgreSQL")) \
        .withColumn("source_table", lit(nom_table_source))
    print("✓ Métadonnées ajoutées")
    return df_enrichi

def ecrire_bronze_delta(df, nom_table):
    chemin_complet = f"{BRONZE_PATH}/{nom_table}"
    print(f"\nÉcriture Delta Lake : {chemin_complet}")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(chemin_complet)
    print(f"✓ {df.count()} lignes écrites")

def main():
    print("=" * 70)
    print("INGESTION BRONZE : PostgreSQL → Delta Lake")
    print("=" * 70)
    spark = None
    try:
        spark = creer_session_spark()
        tables = ['clients_source', 'produits_source', 'ventes_source']
        for table_source in tables:
            print("\n" + "=" * 70)
            print(f"Traitement : {table_source}")
            print("=" * 70)
            df = lire_table_postgres(spark, table_source)
            df_enrichi = ajouter_metadata(df, table_source)
            nom_bronze = table_source.replace('_source', '')
            ecrire_bronze_delta(df_enrichi, nom_bronze)
        print("\n✓ INGESTION BRONZE TERMINÉE AVEC SUCCÈS")
    except Exception as e:
        print(f"\n✗ ERREUR : {e}")
        import traceback
        traceback.print_exc()
    finally:
        if spark:
            spark.stop()
            print("\n✓ Session Spark arrêtée")

if __name__ == "__main__":
    main()

