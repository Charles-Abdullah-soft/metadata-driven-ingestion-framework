from pyspark.sql import DataFrame
from typing import Iterable

def count_duplicate_keys(df: DataFrame, key_columns: Iterable[str]) -> int:
    """Number of rows beyond the first for each key (0 means keys are unique)."""
    keys = list(key_columns)
    return df.count() - df.dropDuplicates(keys).count()