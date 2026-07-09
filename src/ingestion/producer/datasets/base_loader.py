import io
import logging
import os
import threading
import uuid
import zipfile
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# susep/insurer_a/insurer_b/insurer_c rodam em threads concorrentes (main.py)
# apontando pro MESMO dest_path (data/susep_sinistros.csv) — sem isso, as 4
# threads faziam 4 downloads HTTP simultâneos e redundantes do mesmo arquivo
# de ~60MB, e 2 delas quebravam com ChunkedEncodingError/IncompleteRead
# (confirmado em produção). Um lock por dest_path serializa: só a primeira
# thread a chegar baixa de verdade, as outras esperam e reaproveitam o
# arquivo já em disco.
_download_locks: dict[str, threading.Lock] = {}
_download_locks_guard = threading.Lock()


def _lock_for(dest_path: str) -> threading.Lock:
    with _download_locks_guard:
        return _download_locks.setdefault(dest_path, threading.Lock())


def download_csv(url: str, dest_path: str, timeout_seconds: int = 60) -> Path:
    # SUSEP e ANS distribuem os datasets como .zip contendo um único CSV —
    # extraímos automaticamente para dest_path continuar apontando pro CSV.
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)

    with _lock_for(dest_path):
        if dest.exists():
            logger.info("dataset already cached at %s, skipping download", dest)
            return dest
        return _do_download(url, dest, timeout_seconds)


def _do_download(url: str, dest: Path, timeout_seconds: int) -> Path:
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()

    if url.lower().endswith(".zip") or zipfile.is_zipfile(io.BytesIO(response.content)):
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError(f"no CSV file found inside zip archive from {url}")
            content = archive.read(csv_names[0])
    else:
        content = response.content

    # Escrita atômica: se o processo morrer no meio do write_bytes (ex.: job
    # matado pelo timeout do GitHub Actions), um dest parcialmente escrito
    # ficaria em disco e pareceria "válido" (dest.exists() True) pra uma
    # próxima execução — write num temp file no mesmo diretório + os.replace
    # (atômico em POSIX) garante que dest só existe depois de completo.
    tmp_path = dest.parent / f".{dest.name}.{uuid.uuid4().hex}.tmp"
    tmp_path.write_bytes(content)
    os.replace(tmp_path, dest)

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
