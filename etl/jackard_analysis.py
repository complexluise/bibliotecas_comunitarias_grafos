import os
import pandas as pd

from neo4j import GraphDatabase
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import jaccard_score
from sklearn.cluster import KMeans


# --- 1. Conexión a la base de datos Neo4j ---
class Neo4jConnection:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def query(self, query):
        with self._driver.session() as session:
            result = session.run(query)
            return [dict(record) for record in result]


# --- 2. Función para obtener los datos de las bibliotecas ---
def fetch_bibliotecas_data(conn):
    query = """
    MATCH (b:Biblioteca)
    OPTIONAL MATCH (b)-[:TIENE_COLECCION]->(c:Coleccion)
    OPTIONAL MATCH (b)-[:OFRECE_SERVICIOS]->(s:Servicios)
    OPTIONAL MATCH (b)-[:OFRECE_ACTIVIDADES]->(a:Actividades)
    OPTIONAL MATCH (b)-[:OFRECE_PUBLICO]->(p:Publico)
    OPTIONAL MATCH (b)-[:PLANEA_IMPLEMENTAR]->(i:ImplementacionKoha)
    RETURN 
        b.ID AS BibliotecaID,
        b.Nombre AS Nombre,
        c.Tiene26_tipo_coleccion_fanzines AS Tiene26_tipo_coleccion_fanzines,
        c.Tiene26_tipo_coleccion_otros AS Tiene26_tipo_coleccion_otros,
        c.Tiene26_tipo_coleccion_didacticos AS Tiene26_tipo_coleccion_didacticos,
        s.Ofrece30_servicios_internet AS Ofrece30_servicios_internet,
        a.Realiza31_actividades_culturales AS Realiza31_actividades_culturales,
        p.Atiende34_poblacion_jovenes AS Atiende34_poblacion_jovenes,
        i.AccesibilidadCatalogoComunidad AS AccesibilidadCatalogoComunidad
    """
    data = conn.query(query)
    return pd.DataFrame(data)


# --- 3. Normalización de los datos ---
def normalize_data(df, method='minmax'):
    features = df.select_dtypes(include=['float64', 'int64']).columns

    if method == 'minmax':
        scaler = MinMaxScaler()
    elif method == 'standard':
        scaler = StandardScaler()

    df[features] = scaler.fit_transform(df[features])
    return df


# --- 4. Cálculo de la Similitud de Jaccard ---
def calculate_jaccard(df):
    # Convertimos los datos a valores binarios para Jaccard
    binary_df = df.fillna(0).astype(int)

    # Lista para almacenar las similitudes de Jaccard
    jaccard_similarities = []

    for i in range(len(binary_df)):
        row_similarities = []
        for j in range(len(binary_df)):
            sim = jaccard_score(binary_df.iloc[i], binary_df.iloc[j], average='macro')
            row_similarities.append(sim)
        jaccard_similarities.append(row_similarities)

    return pd.DataFrame(jaccard_similarities, index=df.index, columns=df.index)


# --- 5. Clustering ---
def cluster_bibliotecas(df, num_clusters=5):
    kmeans = KMeans(n_clusters=num_clusters)
    clusters = kmeans.fit_predict(df)
    return clusters


# --- 6. Pipeline Principal ---
def main():
    # Conexión a la base de datos Neo4j
    conn = Neo4jConnection(
        uri=os.getenv("NEO4J_URI"),
        user=os.getenv("NEO4J_USER"),
        password=os.getenv("NEO4J_PASSWORD"))

    # Obtener datos de las bibliotecas
    df_bibliotecas = fetch_bibliotecas_data(conn)
    print("Datos originales:")
    print(df_bibliotecas.head())

    # Normalizar los datos (se puede cambiar el método a 'standard' si es necesario)
    #df_normalized = normalize_data(df_bibliotecas.copy(), method='minmax')
    #print("\nDatos normalizados:")
    #print(df_normalized.head())

    # Calcular la similitud de Jaccard
    jaccard_similarities = calculate_jaccard(df_bibliotecas)
    print("\nSimilitudes de Jaccard:")
    print(jaccard_similarities.head())

    # Realizar clustering
    clusters = cluster_bibliotecas(jaccard_similarities, num_clusters=3)
    df_bibliotecas['Cluster'] = clusters

    print("\nBibliotecas agrupadas en clusters:")
    print(df_bibliotecas[['BibliotecaID', 'Nombre', 'Cluster']])

    # Cerrar conexión
    conn.close()


if __name__ == "__main__":
    main()
