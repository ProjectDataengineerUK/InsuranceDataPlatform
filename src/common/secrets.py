import os


def get_secret(scope: str, key: str, env_fallback: str, required: bool = True) -> str | None:
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        dbutils = DBUtils(SparkSession.getActiveSession())
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as exc:
        value = os.environ.get(env_fallback)
        if not value and required:
            raise RuntimeError(
                f"Missing secret '{key}' in scope '{scope}' and no fallback "
                f"env var '{env_fallback}' set"
            ) from exc
        return value
