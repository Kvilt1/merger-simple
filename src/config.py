"""Configuration and utilities for Snapchat media mapper."""

import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any

# Configuration
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
TIMESTAMP_THRESHOLD_SECONDS = 60
QUICKTIME_EPOCH_ADJUSTER = 2082844800

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def ensure_directory(path: Path) -> None:
    """Ensure directory exists."""
    path.mkdir(parents=True, exist_ok=True)

def load_json(path: Path) -> Dict[str, Any]:
    """Load JSON file, return empty dict on error."""
    if not path.exists():
        logger.error(f"JSON file not found: {path}")
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {path}: {e}")
        return {}

def save_json(data: Dict[str, Any], path: Path) -> None:
    """Save dictionary to JSON file."""
    ensure_directory(path.parent)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def format_timestamp(timestamp_ms: int) -> str:
    """Convert millisecond timestamp to ISO format."""
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt.isoformat().replace('+00:00', 'Z')

def sanitize_filename(filename: str) -> str:
    """Remove invalid characters from filename."""
    import re
    return re.sub(r'[\\/*?:"<>|]', "", filename)[:255]