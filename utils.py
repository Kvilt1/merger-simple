# utils.py
import json
import logging
from pathlib import Path
from typing import Dict, Any

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ensure_directory(path: Path) -> None:
    """
    Ensures that a directory exists, creating it if necessary.

    Args:
        path (Path): The path to the directory.
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logging.error(f"Failed to create directory {path}: {e}")
        raise

def load_json(path: Path) -> Dict[str, Any]:
    """
    Loads a JSON file from the given path.

    Args:
        path (Path): The path to the JSON file.

    Returns:
        A dictionary containing the JSON data, or an empty dictionary if loading fails.
    """
    if not path.exists():
        logging.error(f"JSON file not found: {path}")
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in {path}: {e}")
        return {}
    except Exception as e:
        logging.error(f"Failed to load JSON from {path}: {e}")
        return {}

def save_json(data: Dict[str, Any], path: Path) -> None:
    """
    Saves a dictionary to a JSON file.

    Args:
        data (Dict[str, Any]): The dictionary to save.
        path (Path): The path where the JSON file will be saved.
    """
    try:
        ensure_directory(path.parent)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Failed to save JSON to {path}: {e}")
        raise
