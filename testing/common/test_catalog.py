import pytest
from pyspark.sql import SparkSession
from pipeline.common.catalog import (
    check_catalog_exists,
    check_schema_exists,
    check_table_exists,

    ensure_schema_exists,
    ensure_catalog_exists,

    assert_catalog_exists,
    assert_tables_exist,
    assert_schema_exists,

    get_table_fqn,
    get_schema_fqn,
)

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

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

CATALOG = "forge"
TEST_SCHEMA = "bronze" # change to test_schema if privallages exist

@pytest.fixture(scope="session")
def test_Schema():
    return f"{CATALOG}.{TEST_SCHEMA}"

requires_forge_privallages = pytest.mark.skipif(
    True, reason="Requires ability to create and destroy catalogs and schemas"
) # change to false if privallages are added

@pytest.fixture
def test_schema(spark: SparkSession):
    """Create a throwaway schema for a test and drop it (and everything in it) afterwards."""
    ensure_schema_exists(spark, CATALOG, TEST_SCHEMA)
    yield TEST_SCHEMA
    spark.sql(f"DROP SCHEMA IF EXISTS {get_schema_fqn(CATALOG, TEST_SCHEMA)} CASCADE")


# --------------------------------------------------------------------------------
# Getters
# --------------------------------------------------------------------------------


def test_get_schema_fqn_databricks(monkeypatch):
    assert get_schema_fqn(CATALOG, "bronze") == "forge.bronze"

def test_get_table_fqn_databricks(monkeypatch):
    assert get_table_fqn(CATALOG, "bronze", "customer") == "forge.bronze.customer"


# --------------------------------------------------------------------------------
# Ensurers
# --------------------------------------------------------------------------------

@requires_forge_privallages
def test_ensure_schema_exists_creates_and_is_idempotent(spark: SparkSession):
    schema = "test_catalog_ensure"
    try:
        ensure_schema_exists(spark, CATALOG, schema)
        assert check_schema_exists(spark, CATALOG, schema)
        # Second call must not raise
        ensure_schema_exists(spark, CATALOG, schema)
        assert check_schema_exists(spark, CATALOG, schema)
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {get_schema_fqn(CATALOG, schema)} CASCADE")

@requires_forge_privallages
def test_ensure_catalog_exists_creates_and_is_idempotent(spark: SparkSession):
    catalog = "test_catalog_ensure"
    try:
        ensure_catalog_exists(spark, catalog)
        assert check_catalog_exists(spark, catalog)
        ensure_catalog_exists(spark, catalog)
        assert check_catalog_exists(spark, catalog)
    finally:
        spark.sql(f"DROP CATALOG IF EXISTS {catalog} CASCADE")


# --------------------------------------------------------------------------------
# Checkers
# --------------------------------------------------------------------------------


def test_check_table_exists(spark: SparkSession, sample_df, test_schema):
    present = get_table_fqn(CATALOG, test_schema, "present")
    missing = get_table_fqn(CATALOG, test_schema, "missing")
    spark.sql(f"DROP TABLE IF EXISTS {present}")
    sample_df.write.format("delta").saveAsTable(present)

    assert check_table_exists(spark, present)
    assert not check_table_exists(spark, missing)

def test_check_schema_exists(spark: SparkSession, test_schema):
    assert check_schema_exists(spark, CATALOG, test_schema)
    assert not check_schema_exists(spark, CATALOG, "no_such_schema_xyz")

def test_check_catalog_exists(spark: SparkSession):
    assert check_catalog_exists(spark, "hive_metastore")
    assert not check_catalog_exists(spark, "no_such_catalog_xyz")

# --------------------------------------------------------------------------------
# Asserters
# --------------------------------------------------------------------------------

def test_assert_tables_exist_passes_when_present(spark: SparkSession, sample_df, test_schema):
    table_fqn = get_table_fqn(CATALOG, TEST_SCHEMA, "present")
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    sample_df.write.format("delta").saveAsTable(table_fqn)
    assert_tables_exist(spark, CATALOG, test_schema, ["present"])
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")

def test_assert_tables_exist_raises_on_missing(spark: SparkSession, sample_df, test_schema):
    sample_df.write.format("delta").saveAsTable(get_table_fqn(CATALOG, test_schema, "present"))

    with pytest.raises(ValueError, match="missing"):
        assert_tables_exist(spark, CATALOG, test_schema, ["present", "missing"])

def test_assert_schema_exists_passes_when_present(spark: SparkSession, test_schema):
    assert_schema_exists(spark, CATALOG, test_schema)

def test_assert_schema_exists_raises_on_missing(spark: SparkSession):
    with pytest.raises(ValueError, match="Missing schema"):
        assert_schema_exists(spark, CATALOG, "no_such_schema_xyz")

def test_assert_catalog_exists_passes_when_present(spark: SparkSession):
    assert_catalog_exists(spark, "hive_metastore")

def test_assert_catalog_exists_raises_on_missing(spark: SparkSession):
    with pytest.raises(ValueError, match="Missing catalog"):
        assert_catalog_exists(spark, "no_such_catalog_xyz")