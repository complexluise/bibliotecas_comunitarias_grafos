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
    """Implementation of Gower similarity calculation."""

    @staticmethod
    def calculate(df: DataFrame, feature_ranges=None, weights=None):
        """
        Compute the Gower similarity matrix for the given DataFrame.
        Automatically identifies numerical and categorical columns.
        :param df: pandas DataFrame with data.
        :param feature_ranges: Optional dict of ranges for numerical features, if not provided, they are calculated.
        :param weights: Optional dict of weights for each feature, if not provided, all weights are 1.
        :return: Gower similarity matrix as a NumPy array.
        """
        # Identify numerical and categorical columns
        num_cols = df.select_dtypes(include=[np.number]).columns
        cat_cols = df.select_dtypes(exclude=[np.number]).columns

        # Initialize weights
        if weights:
            weights_array = np.array([weights.get(col, 1) for col in df.columns])
        else:
            weights_array = np.ones(df.shape[1])

        # Compute feature ranges if not provided
        if feature_ranges is None:
            feature_ranges = df[num_cols].max() - df[num_cols].min()
            feature_ranges[feature_ranges == 0] = 1  # Avoid division by zero

        # Convert DataFrame to NumPy arrays
        num_data = df[num_cols].values
        cat_data = df[cat_cols].astype(str).values
        num_ranges = feature_ranges.values

        n = len(df)

        # Compute numerical similarities
        if num_cols.size > 0:
            num_diff = np.abs(num_data[:, None, :] - num_data[None, :, :]) / num_ranges
            num_sim = 1 - num_diff
        else:
            num_sim = np.zeros((n, n, 0))

        # Compute categorical similarities
        if cat_cols.size > 0:
            cat_sim = (cat_data[:, None, :] == cat_data[None, :, :]).astype(float)
        else:
            cat_sim = np.zeros((n, n, 0))

        # Combine similarities
        all_sim = np.concatenate([num_sim, cat_sim], axis=2)

        # Apply weights
        weighted_sim = all_sim * weights_array

        # Compute Gower similarity
        gower_similarity = np.sum(weighted_sim, axis=2) / np.sum(weights_array)

        return gower_similarity


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