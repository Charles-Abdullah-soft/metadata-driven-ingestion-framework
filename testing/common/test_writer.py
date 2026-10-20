import pytest
from uuid import uuid4

from pipeline.common.writer import WriteMetrics, create_delta_table, append_table, insert_new_rows
from pipeline.common.catalog import (
    check_table_exists,
    get_schema_fqn,
    get_table_fqn,
    ensure_schema_exists,
)
from pipeline.common.metadata import add_record_hash

from pyspark.sql import SparkSession

from chispa.dataframe_comparer import assert_df_equality

CATALOG = "forge"
TEST_SCHEMA = "bronze"

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

@pytest.fixture(scope="session")
def sample_df(spark):
    return spark.createDataFrame(
        [
            ("James", "Jones", 30),
            ("Ben", "Johnson", 12),
            ("Ellie", "Smith", 30),
            ("Anthony", "Naylor", 26),
            ("Dan", None, 30),
            # TODO: inject more edge cases here

        ],
        ["first_name", "last_name", "age"],
    )

def test_create_delta_table(spark: SparkSession, sample_df):
    catalog = "forge"
    schema = "bronze"
    table_fqn = get_table_fqn(catalog, schema, "test_create_delta_table")
    ensure_schema_exists(spark, catalog, schema)

    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        create_delta_table(sample_df, table_fqn)
        df = spark.table(table_fqn)
        assert_df_equality(df, sample_df, ignore_row_order=True)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")

def test_append_table(spark: SparkSession, sample_df):
    catalog = "forge"
    schema = "bronze"

    table_fqn = get_table_fqn(catalog, schema, "test_append_table")
    ensure_schema_exists(spark, catalog, schema)

    try:
        create_delta_table(sample_df, table_fqn)
        append_table(sample_df, table_fqn)
        df = spark.table(table_fqn)
        expected_df = sample_df.union(sample_df)
        assert_df_equality(df, expected_df, ignore_row_order=True)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")


# --------------------------------------------------------------------------------
# insert_new_rows
# --------------------------------------------------------------------------------


@pytest.fixture
def target(spark: SparkSession):
    ensure_schema_exists(spark, CATALOG, TEST_SCHEMA)

    table_fqn = get_table_fqn(
        CATALOG,
        TEST_SCHEMA,
        f"test_insert_{uuid4().hex}",
    )

    yield table_fqn

    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")


def _batch(spark: SparkSession, rows):
    df = spark.createDataFrame(rows, "id string, value string")
    return add_record_hash(df, ["id", "value"])


def _insert(spark, df, table_fqn, **flags):
    return insert_new_rows(spark, df, table_fqn, ["id"], hash_column="_record_hash", **flags)


def test_insert_new_rows_creates_table_on_first_write(spark, target):
    metrics = _insert(spark, _batch(spark, [("1", "a"), ("2", "b")]), target)
    assert metrics == WriteMetrics(
        replay_existing_row_count=0, inserted_row_count=2, hash_conflict_count=0, target_total_row_count=2
    )

def test_insert_new_rows_rerun_is_a_replay(spark, target):
    batch = _batch(spark, [("1", "a"), ("2", "b")])
    _insert(spark, batch, target)
    metrics = _insert(spark, batch, target)
    assert metrics == WriteMetrics(
        replay_existing_row_count=2, inserted_row_count=0, hash_conflict_count=0, target_total_row_count=2
    )

def test_insert_new_rows_inserts_only_new_keys(spark, target):
    _insert(spark, _batch(spark, [("1", "a")]), target)
    metrics = _insert(spark, _batch(spark, [("1", "a"), ("2", "b")]), target)
    assert metrics.inserted_row_count == 1
    assert metrics.replay_existing_row_count == 1
    assert metrics.target_total_row_count == 2

def test_insert_new_rows_counts_hash_conflict_and_keeps_existing_row(spark, target):
    _insert(spark, _batch(spark, [("1", "a")]), target)
    metrics = _insert(spark, _batch(spark, [("1", "changed")]), target)
    assert metrics.hash_conflict_count == 1
    assert metrics.inserted_row_count == 0
    assert [r.value for r in spark.read.table(target).collect()] == ["a"]

def test_insert_new_rows_raises_on_hash_conflict_when_not_allowed(spark, target):
    _insert(spark, _batch(spark, [("1", "a")]), target)
    with pytest.raises(ValueError, match="1 rows have merge keys already in"):
        _insert(spark, _batch(spark, [("1", "changed"), ("2", "b")]), target, allow_hash_conflicts=False)
    # Nothing was written, not even the new key
    assert spark.read.table(target).count() == 1

def test_insert_new_rows_writes_duplicates_when_allowed(spark, target):
    metrics = _insert(spark, _batch(spark, [("1", "a"), ("1", "b")]), target)
    assert metrics.inserted_row_count == 2

def test_insert_new_rows_raises_on_duplicates_when_not_allowed(spark, target):
    with pytest.raises(ValueError, match="1 rows repeat a merge key"):
        _insert(spark, _batch(spark, [("1", "a"), ("1", "b")]), target, allow_duplicates=False)
    assert not check_table_exists(spark, target)

def test_insert_new_rows_matches_null_keys(spark, target):
    batch = _batch(spark, [(None, "a")])
    _insert(spark, batch, target)
    metrics = _insert(spark, batch, target)
    assert metrics.replay_existing_row_count == 1
    assert metrics.inserted_row_count == 0

def test_insert_new_rows_requires_merge_keys(spark, target):
    with pytest.raises(ValueError, match="at least one merge key"):
        insert_new_rows(spark, _batch(spark, [("1", "a")]), target, [], hash_column="_record_hash")

