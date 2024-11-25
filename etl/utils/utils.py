import logging
from csv import DictReader
from datetime import datetime
from pandas import get_dummies, concat


def setup_logger(log_filename: str, logger_name: str = None) -> logging.Logger:
    """
    Creates a logger with both file and console handlers

    Args:
        log_filename: Name of the log file to write to
        logger_name: Optional name for the logger, defaults to module name

    Returns:
        Logger instance configured with file and console handlers
    """
    logger = logging.getLogger(logger_name if logger_name else __name__)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # File handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def extract_csv(ruta_archivo: str) -> list[dict]:
    logging.info(f"Reading CSV file from: {ruta_archivo}")
    try:
        with open(ruta_archivo, "r", encoding="utf-8") as archivo:
            lector = DictReader(archivo)
            data = list(lector)
            logging.info(f"Successfully read {len(data)} rows from CSV")
            return data
    except Exception as e:
        logging.error(f"Error reading" f" CSV file: {str(e)}")
        raise


def parsear_fecha(cadena_fecha):
    try:
        return datetime.strptime(cadena_fecha, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def a_bool(valor):
    return valor.lower() in ("sí", "si", "yes", "true", "1")


def a_float(valor):
    try:
        return float(valor)
    except ValueError:
        return None


def a_int(valor):
    try:
        return int(valor)
    except ValueError:
        return None


def one_hot_encode_categories(df, column_name):
    """
    Realiza One-Hot Encoding de una columna que contiene listas de categorías.

    Args:
    df (pd.DataFrame): DataFrame que contiene la columna a procesar.
    column_name (str): Nombre de la columna que contiene las listas de categorías.

    Returns:
    pd.DataFrame: DataFrame con las columnas adicionales correspondientes al One-Hot Encoding.
    """
    # Generar el One-Hot Encoding usando pandas.get_dummies
    categories = get_dummies(df[column_name].explode()).groupby(level=0).sum()
    return concat([df.drop(columns=[column_name]), categories], axis=1)

def normalize_and_clean_nominal_categories(column):
    """
    Normaliza y limpia las variables categoricas nominales en una columna:
    - Convierte todo a minúsculas.
    - Reemplaza espacios por guiones bajos.
    - Remueve puntos y caracteres innecesarios.
    - Limpia espacios o guiones adicionales al inicio o final de las palabras.
    - Convierte en una lista de términos.

    Args:
    column (pd.Series): Columna con las variables categoricas nominales a normalizar.

    Returns:
    pd.Series: Columna normalizada con las variables categoricas nominales .
    """
    return (
        column.str.lower()
        .str.replace(r"[^\w\s,]", "", regex=True)
        .str.replace(" ", "_")
        .str.split(",")
        .apply(lambda lst: [term.strip("_") for term in lst])
    )
