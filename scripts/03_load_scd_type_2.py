# ================================================================
# Script : Chargement SCD Type 2 pour dim_client
# Description : Charge et historise les clients depuis la source
# ================================================================

# ================================================================
# PARTIE 1 : IMPORTS
# ================================================================
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime, date

print("Bibliothèques importées")

# ================================================================
# PARTIE 2 : CONFIGURATION DE LA CONNEXION
# ================================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "retailpro_dwh",
    "user": "postgres",
    "password": "postgres123"
}

print(f"Configuration : connexion à {DB_CONFIG['database']} sur {DB_CONFIG['host']}")

# ================================================================
# PARTIE 3 : FONCTION DE CONNEXION (CORRIGÉ UTF-8)
# ================================================================

def creer_connexion():
    """
    Crée une connexion à PostgreSQL
    """
    try:
        conn = psycopg2.connect(
            **DB_CONFIG,
            client_encoding="UTF8"
        )
        print("✓ Connexion à PostgreSQL réussie")
        return conn

    except Exception as e:
        print("✗ Erreur de connexion (détails techniques)")
        print(repr(e))   # ⚠️ IMPORTANT : pas print(e)
        return None

# ================================================================
# PARTIE 4 : LECTURE DES CLIENTS SOURCE
# ================================================================

def lire_clients_source(conn):
    cur = conn.cursor(cursor_factory=DictCursor)

    sql = """
        SELECT client_id, nom, prenom, email, ville, segment
        FROM clients_source
        ORDER BY client_id
    """

    cur.execute(sql)
    clients = cur.fetchall()
    cur.close()

    print(f"✓ {len(clients)} clients lus depuis la source")
    return clients

# ================================================================
# PARTIE 5 : VÉRIFIER EXISTENCE CLIENT
# ================================================================

def client_existe_dans_dimension(conn, client_id):
    cur = conn.cursor(cursor_factory=DictCursor)

    sql = """
        SELECT client_key, client_id, nom, prenom, email, ville, segment, version
        FROM dim_client
        WHERE client_id = %s
          AND est_courant = TRUE
    """

    cur.execute(sql, (client_id,))
    resultat = cur.fetchone()
    cur.close()

    return resultat

# ================================================================
# PARTIE 6 : DÉTECTER LES CHANGEMENTS
# ================================================================

def detecter_changement(source, dim):
    if source["email"] != dim["email"]:
        print("  Changement détecté : email")
        return True

    if source["ville"] != dim["ville"]:
        print("  Changement détecté : ville")
        return True

    if source["segment"] != dim["segment"]:
        print("  Changement détecté : segment")
        return True

    return False

# ================================================================
# PARTIE 7 : INSÉRER NOUVEAU CLIENT
# ================================================================

def inserer_nouveau_client(conn, client):
    cur = conn.cursor()

    sql = """
        INSERT INTO dim_client
        (client_id, nom, prenom, email, ville, segment,
         date_debut, date_fin, est_courant, version)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE, NULL, TRUE, 1)
    """

    cur.execute(sql, (
        client["client_id"],
        client["nom"],
        client["prenom"],
        client["email"],
        client["ville"],
        client["segment"]
    ))

    conn.commit()
    cur.close()

    print(f"✓ Nouveau client inséré : client_id={client['client_id']}")

# ================================================================
# PARTIE 8 : FERMER VERSION COURANTE
# ================================================================

def fermer_version_courante(conn, client_key):
    cur = conn.cursor()

    sql = """
        UPDATE dim_client
        SET date_fin = CURRENT_DATE - INTERVAL '1 day',
            est_courant = FALSE
        WHERE client_key = %s
    """

    cur.execute(sql, (client_key,))
    conn.commit()
    cur.close()

    print(f"✓ Version fermée : client_key={client_key}")

# ================================================================
# PARTIE 9 : CRÉER NOUVELLE VERSION
# ================================================================

def creer_nouvelle_version(conn, client, ancienne_version):
    cur = conn.cursor()
    nouvelle_version = ancienne_version + 1

    sql = """
        INSERT INTO dim_client
        (client_id, nom, prenom, email, ville, segment,
         date_debut, date_fin, est_courant, version)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE, NULL, TRUE, %s)
    """

    cur.execute(sql, (
        client["client_id"],
        client["nom"],
        client["prenom"],
        client["email"],
        client["ville"],
        client["segment"],
        nouvelle_version
    ))

    conn.commit()
    cur.close()

    print(f"✓ Nouvelle version créée : client_id={client['client_id']} version={nouvelle_version}")

# ================================================================
# PARTIE 10 : PROCESSUS PRINCIPAL
# ================================================================

def traiter_scd_type2():
    print("\n" + "=" * 70)
    print("DÉBUT DU PROCESSUS SCD TYPE 2")
    print("=" * 70)

    conn = creer_connexion()
    if conn is None:
        print("Impossible de continuer sans connexion")
        return

    clients_source = lire_clients_source(conn)

    nb_nouveaux = 0
    nb_versions = 0
    nb_inchanges = 0

    for client in clients_source:
        client_id = client["client_id"]
        print(f"\nTraitement du client {client_id}")

        client_dim = client_existe_dans_dimension(conn, client_id)

        if client_dim is None:
            inserer_nouveau_client(conn, client)
            nb_nouveaux += 1
        else:
            if detecter_changement(client, client_dim):
                fermer_version_courante(conn, client_dim["client_key"])
                creer_nouvelle_version(conn, client, client_dim["version"])
                nb_versions += 1
            else:
                print("→ Aucun changement")
                nb_inchanges += 1

    print("\n" + "=" * 70)
    print("RÉSUMÉ DU CHARGEMENT")
    print("=" * 70)
    print(f"Nouveaux clients : {nb_nouveaux}")
    print(f"Nouvelles versions : {nb_versions}")
    print(f"Inchangés : {nb_inchanges}")
    print("=" * 70)

    conn.close()
    print("✓ Connexion fermée")

# ================================================================
# PARTIE 11 : MAIN
# ================================================================

if __name__ == "__main__":
    traiter_scd_type2()
    print("\n✓ Script terminé avec succès")
