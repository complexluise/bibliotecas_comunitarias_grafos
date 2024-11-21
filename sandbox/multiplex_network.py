import numpy as np
import networkx as nx

from abc import ABC, abstractmethod
from pandas import DataFrame, Series


class SimilarityStrategy(ABC):
    """Abstract base class for similarity calculation strategies."""

    @staticmethod
    def identify_column_types(df: DataFrame):
        """
        Identifies numerical and categorical columns in the DataFrame.
        :param df: pandas DataFrame with data.
        :return: Tuple of numerical and categorical column names.
        """
        num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        cat_cols = df.select_dtypes(exclude=[np.number]).columns.tolist()
        return num_cols, cat_cols

    @staticmethod
    def initialize_weights(df: DataFrame, weights=None):
        """
        Initializes weights for the DataFrame columns.
        :param df: pandas DataFrame with data.
        :param weights: Optional dictionary of weights for columns.
        :return: NumPy array of weights.
        """
        if weights:
            return np.array([weights.get(col, 1) for col in df.columns])
        return np.ones(df.shape[1])

    @staticmethod
    def handle_missing_values(df: DataFrame, nan_strategy="ignore"):
        """
        Handles missing values in the DataFrame based on the strategy.
        :param df: pandas DataFrame with data.
        :param nan_strategy: Strategy for handling NaNs: 'ignore', 'impute', 'drop', 'neutral'.
        :return: Processed DataFrame.
        """
        num_cols, cat_cols = SimilarityStrategy.identify_column_types(df)

        if nan_strategy == "impute":
            df[num_cols] = df[num_cols].fillna(df[num_cols].mean())
            df[cat_cols] = df[cat_cols].fillna("missing")
        elif nan_strategy == "drop":
            df = df.dropna(axis=1, how='any')
        elif nan_strategy == "neutral":
            df[num_cols] = np.nan_to_num(df[num_cols], nan=0)
            df[cat_cols] = df[cat_cols].fillna("neutral")

        return df

    @staticmethod
    def normalize_features(df: DataFrame, feature_ranges=None):
        """
        Normalizes numerical features based on their ranges.
        :param df: pandas DataFrame with data.
        :param feature_ranges: Optional dictionary of feature ranges.
        :return: NumPy array of normalized features and feature ranges.
        """
        num_cols, _ = SimilarityStrategy.identify_column_types(df)

        if feature_ranges is None:
            feature_ranges = df[num_cols].max() - df[num_cols].min()
            feature_ranges[feature_ranges == 0] = 1  # Avoid division by zero

        num_data = df[num_cols].values
        num_ranges = feature_ranges.values if isinstance(feature_ranges, Series) else np.array(list(feature_ranges.values()))

        return num_data, num_ranges

    @abstractmethod
    def calculate(self, *args, **kwargs):
        """
        Abstract method for calculating similarity.
        Must be implemented in subclasses.
        """
        pass

class GowerSimilarity(SimilarityStrategy):
    """Implementation of Gower similarity calculation."""

    @staticmethod
    def calculate(df: DataFrame, feature_ranges=None, weights=None, nan_strategy="ignore"):
        """
        Compute the Gower similarity matrix for the given DataFrame.
        Automatically identifies numerical and categorical columns.
        :param df: pandas DataFrame with data.
        :param feature_ranges: Optional dict of ranges for numerical features.
        :param weights: Optional dict of weights for each feature.
        :param nan_strategy: Strategy for handling NaNs: 'ignore', 'impute', 'drop', 'neutral'.
        :return: Gower similarity matrix as a NumPy array.
        """
        df = GowerSimilarity.handle_missing_values(df, nan_strategy)
        num_cols, cat_cols = GowerSimilarity.identify_column_types(df)
        weights_array = GowerSimilarity.initialize_weights(df, weights)

        num_data, num_ranges = GowerSimilarity.normalize_features(df, feature_ranges)
        cat_data = df[cat_cols].astype(str).values if cat_cols else np.empty((len(df), 0))

        n = len(df)

        if num_cols:
            num_diff = np.abs(num_data[:, None, :] - num_data[None, :, :]) / num_ranges
            num_sim = 1 - num_diff
        else:
            num_sim = np.zeros((n, n, 0))

        if cat_cols:
            cat_sim = (cat_data[:, None, :] == cat_data[None, :, :]).astype(float)
        else:
            cat_sim = np.zeros((n, n, 0))

        all_sim = np.concatenate([num_sim, cat_sim], axis=2)
        weighted_sim = all_sim * weights_array

        return np.sum(weighted_sim, axis=2) / np.sum(weights_array)


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

    def __init__(self, similarity_strategy: SimilarityStrategy):
        self.similarity_strategy = similarity_strategy
        self.network = None

    def calculate_similarities(self, data):
        """
        Calculate similarity matrix using the configured similarity strategy

        Args:
            data (DataFrame): Input data containing the attributes

        Returns:
            ndarray: Similarity matrix
        """

        return self.similarity_strategy.calculate(data)

    @staticmethod
    def create_network(similarity_matrix, threshold, node_labels):
        network = nx.Graph()
        n = len(node_labels)

        mask = similarity_matrix >= threshold
        mask = np.triu(mask, k=1)

        edges = np.column_stack(np.where(mask))
        weights = similarity_matrix[mask]

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
        similarity_matrix = self.calculate_similarities(master_table)
        network, optimal_threshold, modularity = self.optimize_threshold(similarity_matrix, master_table[node_column_name].values)
        return network

class MultiplexNetwork:
    """Main class that manages the multiplex network and its layers.

    This class handles the creation and management of multiple network layers,
    each representing different neighborhood attributes.

    Attributes:
        master_table (DataFrame): DataFrame containing neighborhood data.
        layers (dict): Dictionary storing the network layers.
    """

    def __init__(self, master_table, node_column_name: list = []):
        self.master_table = master_table
        self.column_name = node_column_name
        self.layers = {}

    def add_layer(self, layer_name: str, attributes_list: list, similarity_strategy, threshold):
        """Adds a layer to the multiplex network for a list of attributes.

        Args:
            layer_name (str): Name of the layer.
            attributes_list (list): List of attribute column names to form the vector.
            similarity_strategy (SimilarityStrategy): Strategy for calculating similarity.
        """
        layer_factory = LayerFactory(similarity_strategy)
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