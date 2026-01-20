import pyspark
import delta
import psycopg2
import pandas
import pkg_resources  

print("PySpark version:", pyspark.__version__)
print("Delta Lake version:", pkg_resources.get_distribution("delta-spark").version)
print("Psycopg2 version:", psycopg2.__version__)
print("Pandas version:", pandas.__version__)
print("\nToutes les bibliothèques sont installées !")
