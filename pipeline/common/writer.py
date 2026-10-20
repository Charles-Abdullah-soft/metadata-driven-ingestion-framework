from dataclasses import dataclass
from functools import reduce
from operator import and_
from typing import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F

from pipeline.common.catalog import check_table_exists
from pipeline.common.validation import count_duplicate_keys


@dataclass(frozen=True)
class WriteMetrics:
    replay_existing_row_count: int   # key already in target with the same hash: an exact re-delivery
    inserted_row_count: int
    hash_conflict_count: int         # key already in target with a different hash: the source changed
    target_total_row_count: int


def overwrite_table(df: DataFrame, table_fqn: str, overwrite_schema: bool = False) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", str(overwrite_schema).lower())
        .saveAsTable(table_fqn)
    )

def append_table(df: DataFrame, table_fqn: str) -> None:
    df.write.format("delta").mode("append").saveAsTable(table_fqn)

def replace_table(df: DataFrame, table_fqn: str) -> None:
    """Drop and recreate a Delta table, for disposable outputs whose schema can change between runs.

    Use this instead of overwrite with overwriteSchema: a schema change there makes Delta ask the
    Hive metastore to ALTER the table, which fails here (HiveAlterHandler) and leaves the metastore
    with the old columns. Dropping the table deletes its data, so do not use it for tables you keep.
    """
    df.sparkSession.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    create_delta_table(df, table_fqn)

def create_delta_table(
    df: DataFrame,
    table_fqn: str,
) -> None:
    df.write.format("delta").mode("error").saveAsTable(table_fqn)

def _keys_match(merge_keys: Sequence[str]):
    return reduce(and_, (F.col(f"s.`{k}`").eqNullSafe(F.col(f"t.`{k}`")) for k in merge_keys))

def insert_new_rows(
    spark: SparkSession,
    df: DataFrame,
    table_fqn: str,
    merge_keys: Sequence[str],
    hash_column: str,
    allow_duplicates: bool = True,
    allow_hash_conflicts: bool = True,
) -> WriteMetrics:
    """Insert rows whose merge keys aren't in the table yet; existing rows are never changed.

    Creates the table on first write. Keys are compared null-safely, so null keys match each other.
    For keys already in the table, rows with the same hash_column value are replays; rows with a
    different value are hash conflicts (the source changed) and are not written.

    allow_duplicates: if False, raise when df repeats a merge key. If True, every row is written.
    allow_hash_conflicts: if False, raise when any hash conflict is found.
    Both checks raise DataValidationError before anything is written.
    """
    if not merge_keys:
        raise ValueError("insert_new_rows needs at least one merge key")

    if not allow_duplicates:
        duplicates = count_duplicate_keys(df, merge_keys)
        if duplicates:
            raise ValueError(
                f"{duplicates} rows repeat a merge key ({', '.join(merge_keys)}) within the batch "
                f"for {table_fqn}. Nothing was written."
            )

    if not check_table_exists(spark, table_fqn):
        create_delta_table(df, table_fqn)
        total = spark.read.table(table_fqn).count()
        return WriteMetrics(
            replay_existing_row_count=0,
            inserted_row_count=total,
            hash_conflict_count=0,
            target_total_row_count=total,
        )

    source = df.alias("s")
    existing = spark.read.table(table_fqn).alias("t")
    same_hash = F.col(f"s.`{hash_column}`").eqNullSafe(F.col(f"t.`{hash_column}`"))

    replays = source.join(existing, _keys_match(merge_keys) & same_hash, "left_semi").count()
    conflicts = source.join(existing, _keys_match(merge_keys) & ~same_hash, "left_semi").count()

    if conflicts and not allow_hash_conflicts:
        raise ValueError(
            f"{conflicts} rows have merge keys already in {table_fqn} with a different "
            f"{hash_column}; the source data has changed. Nothing was written."
        )

    target = DeltaTable.forName(spark, table_fqn)
    (
        target.alias("t")
        .merge(df.alias("s"), _keys_match(merge_keys))
        .whenNotMatchedInsertAll()
        .execute()
    )
    merge_metrics = target.history(1).collect()[0]["operationMetrics"] or {}

    return WriteMetrics(
        replay_existing_row_count=replays,
        inserted_row_count=int(merge_metrics.get("numTargetRowsInserted", 0)),
        hash_conflict_count=conflicts,
        target_total_row_count=spark.read.table(table_fqn).count(),
    )
