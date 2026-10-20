from pyspark.sql import DataFrame, functions as F
from typing import Iterable

def add_record_hash(df: DataFrame, source_columns: Iterable[str], column: str = "_record_hash") -> DataFrame:
    """Add a deterministic SHA-256 hash of the given source columns (nulls included)."""
    row_json = F.to_json(
        F.struct(*[F.col(name) for name in source_columns]),
        options={"ignoreNullFields": "false"},
    )
    return df.withColumn(column, F.sha2(row_json, 256))
