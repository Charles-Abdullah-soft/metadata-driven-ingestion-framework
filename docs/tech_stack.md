# Tech Stack Document: Metadata-Driven Ingestion Framework

Owners: Charles Carpenter and Abdullah
Status: Draft v5 (practice project, not production)
Scope: Under 100 GB total for now, designed to scale. Ingestion starts from Unity Catalog Volumes.

## 1. Purpose

This document records the chosen technology stack for a metadata-driven ingestion framework on Databricks. The reasons for each choice, the design decisions, and the open items are in the companion document, "Design Decisions and Approach".

## 2. Stack Summary

### 2.1 Platform and pipeline tools

| Concern | Choice | Role | Status |
|---|---|---|---|
| Platform | Databricks with Unity Catalog | Compute, storage governance, catalogs and schemas | Chosen |
| Source landing | Unity Catalog Volumes | Where raw files are dropped for ingestion | Chosen |
| Ingestion (now) | Batch read from Volumes (`spark.read`) with an explicit schema from metadata | Loads files from Volumes into Bronze | Chosen |
| Ingestion (later) | Auto Loader (cloudFiles) | Incremental file discovery, possible future addition | Deferred |
| Transformation and quality | Lakeflow Declarative Pipelines | Bronze to Silver to Gold tables, expectations and quarantine | Chosen |
| Orchestration | Lakeflow Jobs (Workflows) | Runs pipeline tasks and notebook tasks in dependency order | Chosen. Pipeline task inside a job confirmed |
| Language | PySpark and SQL | Transformations and framework code | Chosen |
| Metadata | YAML in Git, validated by Pydantic, synced to Delta control tables | Single source of truth for pipeline config and rules | Chosen |
| Schema contracts (runtime) | Expected StructType from metadata, compared with the incoming DataFrame schema | Detects missing, extra or changed columns before Bronze is written | Chosen |
| Deployment | Databricks Asset Bundles | Deploys pipelines, jobs and notebooks from Git | Chosen |
| Version control | Git (repository hosting to be confirmed) | Code, metadata YAML, tests | Chosen |

### 2.2 Testing tools

| Concern | Choice | Role | Status |
|---|---|---|---|
| Unit testing | pytest | Runs tests for transformation functions, schema checks and quarantine rules | Chosen, to verify against testing guide |
| Spark test session | Local SparkSession, or Databricks Connect | Runs tests against Spark without a full pipeline run | Chosen, to verify setup for our runtime |
| DataFrame assertions | chispa | Compares output DataFrames and schemas with expected ones | Chosen |
| Synthetic data (volume and structure) | dbdatagen | Generates test source files and tables at chosen volumes | Chosen |
| Synthetic data (realistic values) | Faker, used through dbdatagen | Produces names, emails, addresses and places for test data | Chosen |