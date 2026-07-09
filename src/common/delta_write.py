from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame


def append_or_create(df: DataFrame, table_name: str) -> None:
    try:
        df.write.format("delta").mode("append").saveAsTable(table_name)
    except AnalysisException as exc:
        # Duas streams/jobs concorrentes podem ver a tabela ausente ao mesmo
        # tempo e disputar a criação — quem perder a corrida cai aqui e só
        # precisa inserir na tabela que a outra acabou de criar.
        if "already exists" not in str(exc).lower():
            raise
        df.write.format("delta").mode("append").insertInto(table_name)
