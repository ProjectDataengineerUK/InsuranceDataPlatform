import io
import logging
import zipfile
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)


def download_csv(url: str, dest_path: str, timeout_seconds: int = 60) -> Path:
    # SUSEP e ANS distribuem os datasets como .zip contendo um único CSV —
    # extraímos automaticamente para dest_path continuar apontando pro CSV.
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        logger.info("dataset already cached at %s, skipping download", dest)
        return dest

    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()

    if url.lower().endswith(".zip") or zipfile.is_zipfile(io.BytesIO(response.content)):
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError(f"no CSV file found inside zip archive from {url}")
            dest.write_bytes(archive.read(csv_names[0]))
    else:
        dest.write_bytes(response.content)

    logger.info("downloaded dataset from %s to %s", url, dest)
    return dest


def read_csv_safely(
    csv_path: str,
    encoding: str = "latin-1",
    sep: str = ";",
    sample_rows: int | None = None,
    random_seed: int = 42,
) -> pd.DataFrame:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Dataset not found at {csv_path}. Run download_csv() first or place "
            "the file manually (see README setup instructions)."
        )
    df = pd.read_csv(path, encoding=encoding, sep=sep, low_memory=False)
    if sample_rows is not None and len(df) > sample_rows:
        df = df.sample(n=sample_rows, random_state=random_seed).reset_index(drop=True)
    return df


def apply_column_mapping(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    available = {source: target for source, target in mapping.items() if source in df.columns}
    missing = set(mapping) - set(available)
    if missing:
        logger.warning("columns missing from source dataset, skipped: %s", missing)
    return df.rename(columns=available)[list(available.values())]
