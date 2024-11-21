import numpy as np

import networkx as nx

from abc import ABC, abstractmethod
from pandas import DataFrame

class SimilarityStrategy(ABC):
    """Abstract base class for similarity calculation strategies."""

    @abstractmethod
    def calculate(self, *args, **kwargs):
        """Abstract method for calculating similarity between two vectors."""
        pass


class GowerSimilarity(SimilarityStrategy):
    def __init__(self, categorical_columns: list = []):
        self.categorical_columns = categorical_columns

    def _get_feature_ranges(self, data):
        """
        Calcula los rangos (max - min) para las columnas numéricas.
        :param data: DataFrame de entrada.
        :return: Diccionario con los rangos de las columnas numéricas.
        """
        numerical_columns = [
            col for col in data.columns if col not in self.categorical_columns
        ]
        return {col: data[col].max() - data[col].min() for col in numerical_columns}

    def _calculate_row_similarity(self, row1, row2, feature_ranges, weights=None):
        """
        Calcula la similaridad de Gower entre dos filas.
        :param row1: Fila 1 como una Serie de pandas.
        :param row2: Fila 2 como una Serie de pandas.
        :param feature_ranges: Diccionario con los rangos de las variables numéricas.
        :param weights: Diccionario de pesos para cada columna (opcional).
        :return: Similaridad de Gower entre las dos filas.
        """
        categorical_mask = np.array([col in self.categorical_columns for col in row1.index])
        values1 = row1.values
        values2 = row2.values

        # Calculate similarities for categorical columns
        cat_similarities = np.equal(values1[categorical_mask], values2[categorical_mask]).astype(float)

        # Calculate similarities for numerical columns
        num_mask = ~categorical_mask
        ranges = np.array([feature_ranges.get(col, 0) for col in row1.index])[num_mask]
        zero_range_mask = (ranges == 0)

        num_values1 = values1[num_mask]
        num_values2 = values2[num_mask]

        num_similarities = np.zeros(num_mask.sum())
        num_similarities[zero_range_mask] = np.equal(num_values1[zero_range_mask], num_values2[zero_range_mask]).astype(float)
        num_similarities[~zero_range_mask] = 1 - np.abs(num_values1[~zero_range_mask] - num_values2[~zero_range_mask]) / ranges[~zero_range_mask]

        # Combine similarities
        similarities = np.zeros(len(row1))
        similarities[categorical_mask] = cat_similarities
        similarities[num_mask] = num_similarities

        if weights:
            weights_array = np.array([weights[col] for col in row1.index])
            gower_similarity = np.sum(similarities * weights_array) / np.sum(weights_array)
        else:
            gower_similarity = np.mean(similarities)

        return gower_similarity

    def calculate(self, data, weights=None):
        """
        Calcula la matriz de distancias de Gower entre todas las filas del DataFrame.
        :param data: DataFrame donde las filas son bibliotecas y las columnas son atributos.
        :param weights: Diccionario de pesos opcional para cada columna.
        :return: DataFrame con la matriz de distancias.
        """
        # Calcula los rangos de las columnas numéricas
        feature_ranges = self._get_feature_ranges(data)

        # Inicializa la matriz de distancias
        n = data.shape[0]
        similarity_matrix = np.zeros((n, n))

        # Itera por todas las combinaciones de filas
        for i in range(n):
            for j in range(i, n):  # Solo calcula para j >= i para aprovechar la simetría
                row1 = data.iloc[i]
                row2 = data.iloc[j]
                similarity = self._calculate_row_similarity(row1, row2, feature_ranges, weights)
                similarity_matrix[i, j] = similarity
                similarity_matrix[j, i] = similarity  # La matriz es simétrica

        # Convierte la matriz en un DataFrame para facilidad de uso
        similarity_df = DataFrame(
            similarity_matrix, index=data.index, columns=data.index
        )

        return similarity_df


class LayerFactory:
    """Fábrica para construir capas de la red múltiple.

    Genera una capa donde los enlaces entre barrios se basan en una métrica
    de distancia y se crean solo si cumplen con el umbral de conexión.

    Atributos:
        similarity_strategy: Objeto de estrategia para calcular distancias.
        threshold (float): Umbral máximo de distancia para crear conexiones.

    Ejemplo:
        >>> datos_barrios = DataFrame({
        ...     'barrio': ['A', 'B', 'C'],
        ...     'poblacion': [1000, 2000, 1500],
        ...     'ingreso': [50000, 60000, 55000]
        ... })
        >>> estrategia = GowerSimilarity()
        >>> fabrica = LayerFactory(estrategia)
        >>> red = fabrica.create_layer(datos_barrios, ['poblacion', 'ingreso'], 'barrio')
    """

    def __init__(self, similarity_strategy):
        self.similarity_strategy = similarity_strategy
        self.similarity_matrix = None
        self.network = None

    def calculate_similarities(self, data, attributes):
        n = len(data)
        self.similarity_matrix = np.zeros((n, n))

        # Calculate ranges for numeric variables
        feature_ranges = {}
        for i, col in enumerate(attributes):
            if i not in self.similarity_strategy.categorical_columns:
                feature_ranges[i] = data[col].max() - data[col].min()
                if feature_ranges[i] == 0:
                    feature_ranges[i] = 1

        # Calculate similarity matrix
        for i in range(n):
            for j in range(i + 1, n):
                similarity = self.similarity_strategy.calculate(
                    data.iloc[i][attributes].values,
                    data.iloc[j][attributes].values,
                    feature_ranges
                )
                similarity = 1 - similarity
                self.similarity_matrix[i, j] = similarity
                self.similarity_matrix[j, i] = similarity

        return self.similarity_matrix

    def create_network(self, threshold, node_labels):
        network = nx.Graph()
        n = len(node_labels)

        mask = self.similarity_matrix >= threshold
        mask = np.triu(mask, k=1)

        edges = np.column_stack(np.where(mask))
        weights = self.similarity_matrix[mask]

        for (i, j), weight in zip(edges, weights):
            network.add_edge(node_labels[i], node_labels[j], weight=weight)

        return network

    def optimize_threshold(self, node_labels, threshold_range=np.arange(0.1, 1.0, 0.1)):
        best_modularity = -1
        best_threshold = None
        best_network = None

        for threshold in threshold_range:
            G = self.create_network(threshold, node_labels)
            if len(G.edges) > 0:
                communities = list(nx.community.greedy_modularity_communities(G))
                modularity = nx.community.modularity(G, communities)

                if modularity > best_modularity:
                    best_modularity = modularity
                    best_threshold = threshold
                    best_network = G

        return best_network, best_threshold, best_modularity

    def create_layer(self, master_table: DataFrame, attributes_list: list, node_column_name: str):
        similarity_matrix = self.calculate_similarities(master_table, attributes_list)
        network, optimal_threshold, modularity = self.optimize_threshold(master_table[node_column_name].values)
        return network

class MultiplexNetwork:
    """Main class that manages the multiplex network and its layers.

    This class handles the creation and management of multiple network layers,
    each representing different neighborhood attributes.

    Attributes:
        master_table (DataFrame): DataFrame containing neighborhood data.
        layers (dict): Dictionary storing the network layers.
    """

    def __init__(self, master_table, node_column_name):
        self.master_table = master_table
        self.node_column_name = node_column_name
        self.layers = {}

    def add_layer(self, layer_name: str, attributes_list: list, similarity_strategy, threshold):
        """Adds a layer to the multiplex network for a list of attributes.

        Args:
            layer_name (str): Name of the layer.
            attributes_list (list): List of attribute column names to form the vector.
            similarity_strategy (SimilarityStrategy): Strategy for calculating similarity.
            threshold (float): Connection threshold.
        """
        layer_factory = LayerFactory(similarity_strategy, threshold)
        layer_graph = layer_factory.create_layer(self.master_table, attributes_list, self.node_column_name)
        self.layers[layer_name] = layer_graph

    def get_layer(self, attribute_column):
        """Retrieves a specific layer from the multiplex network.

        Args:
            attribute_column (str): Name of the attribute column.

        Returns:
            nx.Graph: The requested network layer, or None if not found.
        """
        return self.layers.get(attribute_column, None)

    def get_multiplex_layers(self):
        """Returns all layers in the multiplex network.

        Returns:
            dict: Dictionary containing all network layers.
        """
        return self.layers

def compute_centralities(graph):
    """Calculates centrality measures for each node in the graph."""
    centrality_measures = {
        "degree": nx.degree_centrality(graph),
        "betweenness": nx.betweenness_centrality(graph),
        "closeness": nx.closeness_centrality(graph)
    }
    return centrality_measures