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
    """Factory for building layers of the multiplex network.

    Generates a layer where links between neighborhoods are based on a distance
    metric and are created only if they meet the connection threshold.

    Attributes:
        distance_strategy: Strategy object for calculating distances.
        threshold (float): Maximum distance threshold for creating connections.
    """

    def __init__(self, distance_strategy, threshold):
        self.distance_strategy = distance_strategy
        self.threshold = threshold

    def create_layer(self, barrios_data: pd.DataFrame, attributes_list: list, node_column_name: str):
        """Creates a network layer based on the specified attributes.

        Args:
            barrios_data (pd.DataFrame): DataFrame containing neighborhood data and attributes.
            attributes_list (list): List of attribute column names to form the vector.
            node_column_name (str): Name of the column containing neighborhood names.

        Returns:
            nx.Graph: Graph of the layer with weighted edges.
        """
        layer_graph = nx.Graph()

        for i, barrio_i in barrios_data.iterrows():
            for j, barrio_j in barrios_data.iterrows():
                if i < j:
                    vector1 = barrio_i[attributes_list].values
                    vector2 = barrio_j[attributes_list].values
                    distance = self.distance_strategy.calculate(vector1, vector2)

                    if distance <= self.threshold:
                        layer_graph.add_edge(
                            barrio_i[node_column_name], barrio_j[node_column_name], weight=1 / (1 + distance)
                        )

        return layer_graph


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