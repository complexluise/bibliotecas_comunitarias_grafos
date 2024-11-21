import numpy as np
import networkx as nx

from abc import ABC, abstractmethod
from pandas import DataFrame, Series


class SimilarityStrategy(ABC):
    """Clase base abstracta para estrategias de cálculo de similitud.

    Esta clase define la interfaz para implementar diferentes estrategias
    de cálculo de similitud entre elementos.
    """

    @staticmethod
    def identify_column_types(df: DataFrame):
        """Identifica los tipos de columnas numéricas y categóricas en el DataFrame.

        Args:
            df (DataFrame): DataFrame de pandas con los datos a analizar.

        Returns:
            tuple: Tupla con dos listas (columnas numéricas, columnas categóricas).

        Ejemplo:
            >>> df = pd.DataFrame({'num': [1,2,3], 'cat': ['a','b','c']})
            >>> num_cols, cat_cols = identify_column_types(df)
            >>> print(num_cols)  # ['num']
            >>> print(cat_cols)  # ['cat']
        """
        num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        cat_cols = df.select_dtypes(exclude=[np.number]).columns.tolist()
        return num_cols, cat_cols

    @staticmethod
    def handle_missing_values(df: DataFrame, nan_strategy="ignore"):
        """Maneja los valores faltantes en el DataFrame según la estrategia especificada.

        Args:
            df (DataFrame): DataFrame con los datos.
            nan_strategy (str): Estrategia para manejar NaNs:
                - 'ignore': Mantiene los valores NaN
                - 'impute': Imputa con la media para numéricos y 'missing' para categóricos
                - 'drop': Elimina columnas con NaN
                - 'neutral': Reemplaza con 0 para numéricos y 'neutral' para categóricos

        Returns:
            DataFrame: DataFrame procesado según la estrategia elegida.

        Ejemplo:
            >>> df = pd.DataFrame({'A': [1, np.nan, 3], 'B': ['x', None, 'z']})
            >>> df_processed = handle_missing_values(df, 'impute')
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
    """Implementación del cálculo de similitud de Gower con estrategias de ponderación.

    Esta clase implementa el método de similitud de Gower que permite trabajar
    con datos mixtos (numéricos y categóricos) y diferentes estrategias de
    ponderación de características.

    Métodos:
        compute_entropy: Calcula la entropía de los datos
        calculate: Calcula la matriz de similitud de Gower
    """

    @staticmethod
    def compute_entropy(data, method='fd'):
        """Calcula la entropía de los datos usando estimación basada en histogramas.

        Args:
            data (array): Array de NumPy con los datos.
            method (str): Método para estimar el ancho de los bins ('fd' para Freedman-Diaconis).

        Returns:
            float: Estimación de entropía corregida por sesgo.

        Ejemplo:
            >>> datos = np.array([1, 2, 2, 3, 3, 3, 4])
            >>> entropia = compute_entropy(datos)
        """
        data = data[~np.isnan(data)]  # Remove NaNs

        if len(data) == 0:
            return 0.0

        # Determine optimal number of bins using Freedman-Diaconis rule
        if method == 'fd':
            # Compute Interquartile Range (IQR)
            q75, q25 = np.percentile(data, [75, 25])
            iqr = q75 - q25
            bin_width = 2 * iqr / (len(data) ** (1 / 3))
            if bin_width > 0:
                n_bins = int(np.ceil((np.max(data) - np.min(data)) / bin_width))
            else:
                n_bins = int(np.sqrt(len(data)))  # Fallback to sqrt rule
        else:
            raise ValueError(f"Unknown method: {method}")

        if n_bins <= 0:
            n_bins = 1  # At least one bin

        # Compute histogram
        counts, _ = np.histogram(data, bins=n_bins) # compare with np.histogram(data, bins='fd')

        # Calculate probabilities
        probabilities = counts / counts.sum()

        # Remove zero probabilities to avoid log(0)
        probabilities = probabilities[probabilities > 0]

        # Calculate raw entropy (in nats)
        entropy = -np.sum(probabilities * np.log(probabilities))

        # Apply Miller-Madow bias correction
        n_nonempty_bins = len(probabilities)
        correction = (n_nonempty_bins - 1) / (2 * len(data))
        entropy_corrected = entropy + correction

        return entropy_corrected

    @staticmethod
    def compute_column_entropies(df: DataFrame):
        """
        Compute the entropy of each column in the DataFrame.
        For numerical columns, use histogram-based estimation with bias correction.
        For categorical columns, use category counts.

        :param df: pandas DataFrame with data.
        :return: Dictionary of entropies per column.
        """
        entropies = {}
        num_cols, cat_cols = SimilarityStrategy.identify_column_types(df)

        # Numerical columns
        for col in num_cols:
            data = df[col].values
            entropy_corrected = GowerSimilarity.compute_entropy(data)
            entropies[col] = entropy_corrected

        # Categorical columns
        for col in cat_cols:
            data = df[col].astype(str).values
            # Get counts of unique categories
            unique, counts = np.unique(data, return_counts=True)
            probabilities = counts / counts.sum()
            # Calculate entropy
            entropy = -np.sum(probabilities * np.log(probabilities))
            # Apply Miller-Madow bias correction
            n_nonempty_bins = len(probabilities)
            correction = (n_nonempty_bins - 1) / (2 * len(data))
            entropy_corrected = entropy + correction
            entropies[col] = entropy_corrected

        return entropies

    @staticmethod
    def compute_similarity_entropies(all_sim):
        """
        Compute the entropy over the per-feature similarity distributions.

        :param all_sim: 3D NumPy array of per-feature similarities (n x n x num_features).
        :return: Array of entropies for each feature.
        """
        num_features = all_sim.shape[2]
        entropies = np.zeros(num_features)
        for i in range(num_features):
            # Get the similarity values for this feature, flatten the matrix
            sim_values = all_sim[:, :, i].flatten()
            entropy_corrected = GowerSimilarity.compute_entropy(sim_values)
            entropies[i] = entropy_corrected
        return entropies

    @staticmethod
    def initialize_weights(df: DataFrame, weights=None, weighting_strategy='uniform', all_sim=None):
        """
        Initializes weights for the DataFrame columns based on the specified strategy.
        :param df: pandas DataFrame with data.
        :param weights: Optional dictionary of weights for columns.
        :param weighting_strategy: Strategy for weighting features.
        :param all_sim: Optional 3D NumPy array of per-feature similarities (required for some strategies).
        :return: NumPy array of weights.
        """
        if weights:
            return np.array([weights.get(col, 1) for col in df.columns])

        num_features = df.shape[1]

        if weighting_strategy == 'uniform':
            return np.ones(num_features)
        elif weighting_strategy == 'feature_entropy':
            # Compute entropies of features
            entropies = GowerSimilarity.compute_column_entropies(df)
            entropy_weights = np.array([entropies.get(col, 0) for col in df.columns])
            return entropy_weights
        elif weighting_strategy in ['similarity_entropy', 'inverse_similarity_entropy', 'similarity_entropy_normalized', 'inverse_similarity_entropy_normalized']:
            # Compute entropies over similarity layers
            if all_sim is None:
                raise ValueError("all_sim must be provided when using similarity entropy-based weighting strategies.")
            entropies = GowerSimilarity.compute_similarity_entropies(all_sim)
            if weighting_strategy == 'similarity_entropy':
                return entropies
            elif weighting_strategy == 'inverse_similarity_entropy':
                # Avoid division by zero
                with np.errstate(divide='ignore', invalid='ignore'):
                    inv_entropies = np.where(entropies != 0, 1 / entropies, 0)
                return inv_entropies
            elif weighting_strategy == 'similarity_entropy_normalized':
                total_entropy = np.sum(entropies)
                if total_entropy == 0:
                    return np.ones_like(entropies) / sum(entropies)
                return entropies / total_entropy
            elif weighting_strategy == 'inverse_similarity_entropy_normalized':
                # Avoid division by zero
                with np.errstate(divide='ignore', invalid='ignore'):
                    inv_entropies = np.where(entropies != 0, 1 / entropies, 0)
                total_inverse_entropy = np.sum(inv_entropies)
                if total_inverse_entropy == 0:
                    return np.ones_like(inv_entropies) / sum(inv_entropies)
                return inv_entropies / total_inverse_entropy
        else:
            raise ValueError(f"Unknown weighting strategy: {weighting_strategy}")

    @staticmethod
    def calculate(df: DataFrame, feature_ranges=None, weights=None, nan_strategy="ignore", weighting_strategy='uniform'):
        """
        Compute the Gower similarity matrix for the given DataFrame.
        Automatically identifies numerical and categorical columns.
        :param df: pandas DataFrame with data.
        :param feature_ranges: Optional dict of ranges for numerical features.
        :param weights: Optional dict of weights for each feature.
        :param nan_strategy: Strategy for handling NaNs: 'ignore', 'impute', 'drop', 'neutral'.
        :param weighting_strategy: Strategy for weighting features.
        :return: Gower similarity matrix as a NumPy array.
        """
        df = GowerSimilarity.handle_missing_values(df, nan_strategy)
        num_cols, cat_cols = GowerSimilarity.identify_column_types(df)

        num_data, num_ranges = GowerSimilarity.normalize_features(df, feature_ranges)
        cat_data = df[cat_cols].astype(str).values if cat_cols else np.empty((len(df), 0), dtype=str)

        n = len(df)

        # Compute numerical similarities
        if num_cols:
            num_diff = np.abs(num_data[:, None, :] - num_data[None, :, :]) / num_ranges
            num_sim = 1 - num_diff
        else:
            num_sim = np.zeros((n, n, 0))

        # Compute categorical similarities
        if cat_cols:
            cat_sim = (cat_data[:, None, :] == cat_data[None, :, :]).astype(float)
        else:
            cat_sim = np.zeros((n, n, 0))

        # Concatenate numerical and categorical similarities
        all_sim = np.concatenate([num_sim, cat_sim], axis=2)

        # Compute weights based on the selected strategy
        weights_array = GowerSimilarity.initialize_weights(df, weights, weighting_strategy, all_sim=all_sim)

        # Ensure weights_array is the same length as the number of features
        if weights_array.shape[0] != all_sim.shape[2]:
            raise ValueError("Length of weights_array does not match number of features.")

        # Apply weights to similarities
        weighted_sim = all_sim * weights_array

        # Compute the total weight (sum of weights)
        total_weight = np.sum(weights_array)

        # Handle case where total weight is zero
        if total_weight == 0:
            return np.zeros((n, n))
        else:
            # Sum over features and normalize by total weight
            return np.sum(weighted_sim, axis=2) / total_weight



class LayerFactory:
    """Fábrica para construir capas de la red múltiple.

    Esta clase se encarga de crear las diferentes capas de la red basándose
    en métricas de similitud entre nodos.

    Atributos:
        similarity_strategy (SimilarityStrategy): Estrategia para calcular similitudes.

    Ejemplo:
        >>> factory = LayerFactory(GowerSimilarity())
        >>> layer = factory.create_layer(data, nodes=['A','B','C'], threshold=0.5)
    """

    def __init__(self, similarity_strategy: SimilarityStrategy):
        self.similarity_strategy = similarity_strategy
        self.network = None

    def calculate_similarities(self, data, **kwargs):
        """
        Calculate similarity matrix using the configured similarity strategy

        Args:
            data (DataFrame): Input data containing the attributes
            **kwargs: Configuration parameters for similarity calculation

        Returns:
            ndarray: Similarity matrix
        """

        return self.similarity_strategy.calculate(df=data, **kwargs)

    @staticmethod
    def create_network(similarity_matrix, threshold, node_labels):
        network = nx.Graph()
        n = len(node_labels)

        if threshold is not None:
            mask = similarity_matrix >= threshold
            mask = np.triu(mask, k=1)
        else:
            # Include all edges without thresholding
            mask = np.triu(np.ones_like(similarity_matrix, dtype=bool), k=1)

        edges = np.column_stack(np.where(mask))
        weights = similarity_matrix[mask]

        for (i, j), weight in zip(edges, weights):
            network.add_edge(node_labels[i], node_labels[j], weight=weight)

        return network

    def optimize_threshold(self, similarity_matrix, node_labels, threshold_range=np.arange(0.1, 1.0, 0.1)):
        best_modularity = -1
        best_threshold = None
        best_network = None

        for threshold in threshold_range:
            G = self.create_network(similarity_matrix, threshold, node_labels)
            if len(G.edges) > 0:
                communities = list(nx.community.greedy_modularity_communities(G))
                modularity = nx.community.modularity(G, communities)

                if modularity > best_modularity:
                    best_modularity = modularity
                    best_threshold = threshold
                    best_network = G

        return best_network, best_threshold, best_modularity

    def create_layer(self, master_table: DataFrame, nodes_name: list[str], threshold=None, optimize_threshold=False, **kwargs):
        """
        Create a network layer based on the similarity matrix and threshold.

        Args:
            master_table (DataFrame): The data containing node attributes.
            nodes_name (list[str]): List of node labels.
            threshold (float, optional): Threshold value for creating edges. Defaults to None.
            optimize_threshold (bool, optional): Whether to optimize the threshold. Defaults to False.
            **kwargs: Additional parameters for similarity calculation.

        Returns:
            networkx.Graph: The created network layer.
        """
        similarity_matrix = self.calculate_similarities(data=master_table, **kwargs)

        if optimize_threshold:
            network, optimal_threshold, modularity = self.optimize_threshold(
                similarity_matrix=similarity_matrix,
                node_labels=nodes_name,
            )
            print(f"Optimized Threshold: {optimal_threshold}, Modularity: {modularity}")
        else:
            network = self.create_network(similarity_matrix, threshold, nodes_name)
        return network

class MultiplexNetwork:
    """Gestiona una red multicapa con diferentes tipos de relaciones.

    Esta clase permite crear y gestionar una red donde los nodos pueden estar
    conectados por diferentes tipos de relaciones, cada una representada en
    una capa diferente.

    Atributos:
        master_table (DataFrame): Tabla con los datos de los nodos.
        nodes_name (list): Lista de identificadores de nodos.
        layers (dict): Diccionario con las capas de la red.

    Ejemplo:
        >>> data = pd.DataFrame({'id': ['A','B'], 'valor': [1,2]})
        >>> red = MultiplexNetwork(data, nodes_name=['A','B'])
        >>> red.add_layer('capa1', GowerSimilarity())
    """

    def __init__(self, master_table, nodes_name: list = []):
        self.master_table = master_table
        self.nodes_name = nodes_name
        self.layers = {}

    def add_layer(self, layer_name: str, similarity_strategy: SimilarityStrategy, **kwargs):
        """Adds a layer to the multiplex network for a list of attributes.

        Args:
            layer_name (str): Name of the layer.
            similarity_strategy (SimilarityStrategy): Strategy for calculating similarity.
        """
        layer_factory = LayerFactory(similarity_strategy)
        layer_graph = layer_factory.create_layer(
            master_table=self.master_table,
            nodes_name=self.nodes_name,
            **kwargs
        )
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
    """Calcula medidas de centralidad para cada nodo del grafo.

    Calcula tres medidas de centralidad fundamentales:
    - Grado: Número de conexiones directas
    - Intermediación: Frecuencia con que un nodo está en los caminos más cortos
    - Cercanía: Inverso de la distancia promedio a otros nodos

    Args:
        graph (nx.Graph): Grafo de NetworkX.

    Returns:
        dict: Diccionario con las medidas de centralidad por nodo.

    Ejemplo:
        >>> G = nx.Graph([(1,2), (2,3), (3,1)])
        >>> centralidades = compute_centralities(G)
        >>> print(centralidades['degree'][1])  # Centralidad de grado del nodo 1
    """
    return {
        "degree": nx.degree_centrality(graph),
        "betweenness": nx.betweenness_centrality(graph),
        "closeness": nx.closeness_centrality(graph)
    }