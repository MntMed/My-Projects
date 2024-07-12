import pymysql

# Connexion aux bases de données source et destination
conn_source = pymysql.connect(
    host='localhost',
    user='root',
    password='',
    db='devops_db'
)

conn_destination = pymysql.connect(
    host='localhost',
    user='root',
    password='',
    db='devops_db_combin'
)

# Fonction pour exécuter une requête d'insertion
def insert_data(connection, query, data):
    with connection.cursor() as cursor:
        cursor.executemany(query, data)
    connection.commit()


# Fonction pour extraire des données d'une table
def fetch_data(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

# Extraction des données des tables sources
data1_a = fetch_data(conn_source, "SELECT c1t1, c2t1 FROM table1")
data1_b = fetch_data(conn_source, "SELECT c1t2, c2t2 FROM table2")

# Exemple de transformation/combinaison des données extraites
combined_data = []
for row1_a, row1_b in zip(data1_a, data1_b):
    combined_row = (row1_a[0], row1_b[0])  # Adapter selon vos besoins
    combined_data.append(combined_row)

# Insertion des données combinées dans la table de destination
with conn_destination.cursor() as cursor:
    for row in combined_data:
        cursor.execute("INSERT INTO tableCombin(C1,C2) VALUES (%s, %s)", row)
    conn_destination.commit()

# Fermeture des connexions
conn_source.close()
conn_destination.close()
