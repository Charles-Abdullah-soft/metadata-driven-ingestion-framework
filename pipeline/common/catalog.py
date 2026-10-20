from pyspark.sql import SparkSession
from typing import Iterable

# --------------------------------------------------------------------------------
# Ensurers
# --------------------------------------------------------------------------------


def ensure_schema_exists(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create a schema (database) if it's missing."""
    schema_fqn = get_schema_fqn(catalog, schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")

def ensure_catalog_exists(spark: SparkSession, catalog_name: str) -> None:
    """Create a catalog if it's missing. Only works where Unity Catalog is available (Databricks)."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")


# --------------------------------------------------------------------------------
# Getters
# --------------------------------------------------------------------------------


def get_table_fqn(catalog: str, schema: str, table_name: str) -> str:
    return f"{get_schema_fqn(catalog, schema)}.{table_name}"

def get_schema_fqn(catalog: str, schema: str) -> str:
    return f"{catalog}.{schema}"

# --------------------------------------------------------------------------------
# Checkers
# --------------------------------------------------------------------------------

def check_table_exists(spark: SparkSession, table_fqn: str) -> bool:
    return spark.catalog.tableExists(table_fqn)

def check_schema_exists(spark: SparkSession, catalog: str, schema: str) -> bool:
    return schema in [row[0] for row in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()]

def check_catalog_exists(spark: SparkSession, catalog: str) -> bool:
    return catalog in [c.name for c in spark.catalog.listCatalogs()]


# --------------------------------------------------------------------------------
# Finders
# --------------------------------------------------------------------------------

def find_missing_tables(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table_names: Iterable[str],
) -> list[str]:
    table_fqns = [get_table_fqn(catalog, schema, name) for name in table_names]
    return [fqn for fqn in table_fqns if not check_table_exists(spark, fqn)]


# --------------------------------------------------------------------------------
# Asserters
# --------------------------------------------------------------------------------


def assert_tables_exist(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table_names: Iterable[str],
) -> None:
    missing = find_missing_tables(spark, catalog, schema, table_names)
    if missing:
        raise ValueError(f"Missing tables: {', '.join(missing)}")

def assert_table_exists(
    spark: SparkSession,
    table_fqn: str,
) -> None:
    if not check_table_exists(spark, table_fqn):
        raise ValueError(f"Missing table: {table_fqn}")

def assert_schema_exists(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> None:
    if not check_schema_exists(spark, catalog, schema):
        raise ValueError(f"Missing schema: {catalog}.{schema}")

def assert_catalog_exists(
    spark: SparkSession,
    catalog: str,
) -> None:
    if not check_catalog_exists(spark, catalog):
        raise ValueError(f"Missing catalog: {catalog}")

# --------------------------------------------------------------------------------
# Listers
# --------------------------------------------------------------------------------

def list_tables(spark: SparkSession, catalog: str, schema: str) -> list[str]:
    if not check_schema_exists(spark, catalog, schema):
        raise ValueError(f"Schema does not exist: {catalog}.{schema}")
    schema_fqn = get_schema_fqn(catalog, schema)
    return [t.name for t in spark.catalog.listTables(schema_fqn)]

def list_schemas(spark: SparkSession, catalog: str) -> list[str]:
    if not check_catalog_exists(spark, catalog):
        raise ValueError(f"Catalog does not exist: {catalog}")
    return [row.namespace for row in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()]

def list_catalogs(spark: SparkSession) -> list[str]:
    return [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]