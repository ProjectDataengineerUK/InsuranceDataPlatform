import logging
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)


def download_csv(url: str, dest_path: str, timeout_seconds: int = 60) -> Path:
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        logger.info("dataset already cached at %s, skipping download", dest)
        return dest

    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    dest.write_bytes(response.content)
    logger.info("downloaded dataset from %s to %s", url, dest)
    return dest


def read_csv_safely(csv_path: str, encoding: str = "latin-1", sep: str = ";") -> pd.DataFrame:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Dataset not found at {csv_path}. Run download_csv() first or place "
            "the file manually (see README setup instructions)."
        )
    return pd.read_csv(path, encoding=encoding, sep=sep, low_memory=False)


def apply_column_mapping(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    available = {source: target for source, target in mapping.items() if source in df.columns}
    missing = set(mapping) - set(available)
    if missing:
        logger.warning("columns missing from source dataset, skipped: %s", missing)
    return df.rename(columns=available)[list(available.values())]
