import logging
from csv import DictReader
from datetime import datetime


def setup_logger(log_filename: str) -> logging.Logger:
    """
    Creates a logger with both file and console handlers

    Args:
        log_filename: Name of the log file to write to

    Returns:
        Logger instance configured with file and console handlers
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

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
