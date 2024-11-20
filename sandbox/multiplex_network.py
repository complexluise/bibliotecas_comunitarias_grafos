import numpy as np
import pandas as pd
from scipy.spatial.distance import euclidean
import networkx as nx
from abc import ABC, abstractmethod


class DistanceStrategy(ABC):
    """Abstract base class for distance calculation strategies."""

    @abstractmethod
    def calculate(self, vector1, vector2):
        """Abstract method for calculating distance between two vectors."""
        pass


class GowerDistance:
    def __init__(self, categorical_columns):
        self.categorical_columns = categorical_columns

    def calculate(self, vector1, vector2, feature_ranges=None, weights=None):
        """
        Calculates the Gower Distance between two vectors.
        feature_ranges: dict with the ranges (max-min) of numeric variables.
        weights: dict with weights for each feature (default to 1 for all).
        """
        vector1 = np.array(vector1)
        vector2 = np.array(vector2)
        p = len(vector1)

        # Default weights if not provided
        if weights is None:
            weights = np.ones(p)

        # Identify categorical and numerical masks
        categorical_mask = np.zeros(p, dtype=bool)
        categorical_mask[list(self.categorical_columns)] = True
        numerical_mask = ~categorical_mask

        # Calculate similarities for categorical features
        categorical_similarity = np.where(
            categorical_mask,
            (vector1 == vector2).astype(float),  # 1 if equal, 0 otherwise
            0
        )

        # Calculate similarities for numerical features
        numerical_ranges = np.array([feature_ranges[i] for i in range(p)])
        numerical_similarity = np.where(
            numerical_mask,
            1 - np.abs(vector1 - vector2) / numerical_ranges,
            0
        )

        # Combine similarities
        similarities = categorical_similarity + numerical_similarity

        # Apply weights
        weighted_similarities = similarities * weights
        weighted_sum = weights * (categorical_mask + numerical_mask)

        # Calculate Gower similarity
        gower_similarity = weighted_similarities.sum() / weighted_sum.sum()

        # Convert similarity to distance
        gower_distance = 1 - gower_similarity
        return gower_distance


class EuclidianDistance(DistanceStrategy):
    """Strategy for calculating distance between neighborhoods using Euclidean Distance.

    This class implements a static method to compute distances between vectors
    representing neighborhood attributes.
    """

    @staticmethod
    def calculate(vector1, vector2):
        """Calculates the Euclidean distance between two vectors.

        Args:
            vector1 (array-like): First vector of attributes.
            vector2 (array-like): Second vector of attributes.

        Returns:
            float: The Euclidean distance between the vectors.
        """
        return euclidean(vector1, vector2)


class LayerFactory:
    """Fábrica para construir capas de la red múltiple.

    Genera una capa donde los enlaces entre barrios se basan en una métrica
    de distancia y se crean solo si cumplen con el umbral de conexión.

    Atributos:
        distance_strategy: Objeto de estrategia para calcular distancias.
        threshold (float): Umbral máximo de distancia para crear conexiones.

    Ejemplo:
        >>> datos_barrios = pd.DataFrame({
        ...     'barrio': ['A', 'B', 'C'],
        ...     'poblacion': [1000, 2000, 1500],
        ...     'ingreso': [50000, 60000, 55000]
        ... })
        >>> estrategia = GowerDistance([])
        >>> fabrica = LayerFactory(estrategia)
        >>> red = fabrica.create_layer(datos_barrios, ['poblacion', 'ingreso'], 'barrio')
    """

    def __init__(self, distance_strategy):
        self.distance_strategy = distance_strategy
        self.similarity_matrix = None
        self.network = None

    def calculate_similarities(self, data, attributes):
        n = len(data)
        self.similarity_matrix = np.zeros((n, n))

        # Calculate ranges for numeric variables
        feature_ranges = {}
        for i, col in enumerate(attributes):
            if i not in self.distance_strategy.categorical_columns:
                feature_ranges[i] = data[col].max() - data[col].min()
                if feature_ranges[i] == 0:
                    feature_ranges[i] = 1

        # Calculate similarity matrix
        for i in range(n):
            for j in range(i + 1, n):
                distance = self.distance_strategy.calculate(
                    data.iloc[i][attributes].values,
                    data.iloc[j][attributes].values,
                    feature_ranges
                )
                similarity = 1 - distance
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

    def create_layer(self, barrios_data: pd.DataFrame, attributes_list: list, node_column_name: str):
        similarity_matrix = self.calculate_similarities(barrios_data, attributes_list)
        network, optimal_threshold, modularity = self.optimize_threshold(barrios_data[node_column_name].values)
        return network

class MultiplexNetwork:
    """Main class that manages the multiplex network and its layers.

    This class handles the creation and management of multiple network layers,
    each representing different neighborhood attributes.

    Attributes:
        barrios_data (pd.DataFrame): DataFrame containing neighborhood data.
        layers (dict): Dictionary storing the network layers.
    """

    def __init__(self, barrios_data, node_column_name):
        self.barrios_data = barrios_data
        self.node_column_name = node_column_name
        self.layers = {}

    def add_layer(self, layer_name, attributes_list, distance_strategy, threshold):
        """Adds a layer to the multiplex network for a list of attributes.

        Args:
            layer_name (str): Name of the layer.
            attributes_list (list): List of attribute column names to form the vector.
            distance_strategy (DistanceStrategy): Strategy for calculating distances.
            threshold (float): Connection threshold.
        """
        layer_factory = LayerFactory(distance_strategy, threshold)
        layer_graph = layer_factory.create_layer(self.barrios_data, attributes_list, self.node_column_name)
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