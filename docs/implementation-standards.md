# Implementation Standards: Metadata-Driven Ingestion Framework

Owners: Charles Carpenter and Abdullah
Status: Draft v1 (practice project, not production)

## 1. Purpose and Scope

This document sets out how the framework code is written, versioned, reviewed, tested at the unit level, and deployed. It is the working agreement between the two owners, so that either person can pick up any part of the codebase and know what is expected.

## 2. Version Control

### 2.1 Branching model

The project uses GitHub flow. There is one long-lived branch, `main`, which always reflects the latest reviewed and tested code. All work happens on short-lived branches created from `main`, merged back through a pull request, and then deleted.

There is no `develop` branch and no release or hotfix branch structure. Releases are tags on `main`.

### 2.2 Branch naming

Branches use a type prefix, a slash, and a short kebab-case description:

| Prefix | Use for | Example |
|---|---|---|
| `feature/` | New functionality | `feature/volume-batch-reader` |
| `bugfix/` | Fixing incorrect behaviour | `bugfix/schema-nullable-check` |
| `chore/` | Tooling, CI, dependencies, refactoring with no behaviour change | `chore/ci-ruff-setup` |
| `docs/` | Documentation only | `docs/readme-deploy-steps` |
| `hotfix/` | Urgent fix to something broken in dev | `hotfix/null-quarantine-split` |

If a GitHub issue exists, include its number in the branch name (for example `feature/42-volume-batch-reader`). This is encouraged but not required.

### 2.3 Branch protection on `main`

Branch protection is enabled on `main` with the following rules:

- Changes reach `main` only through a pull request. Direct pushes are blocked, including for owners.
- All required GitHub Actions checks must pass before merging.
- At least one approving review is required. Because there are two owners, the author never approves their own pull request.
- The branch must be up to date with `main` before merging.
- Force pushes and branch deletion of `main` are blocked.
- Conversations on the pull request must be resolved before merging.

### 2.4 Commits

Commit messages follow the Conventional Commits format:

```
<type>(<optional scope>): <short summary in the imperative mood>
```

Permitted types: `feat`, `fix`, `docs`, `test`, `chore`, `refactor`, `ci`. The summary is no more than 72 characters and does not end with a full stop. A body is optional and explains why a change was made when that is not obvious from the diff.

Examples:

```
feat(ingest): read batch files from volume with declared schema
fix(schema): report nullability changes as breaking
test(quarantine): cover rows failing multiple rules
```

Commits on a feature branch can be as granular as needed, because the branch is squashed on merge.

### 2.5 Merging

- Pull requests are merged with **squash and merge**, so each pull request becomes one commit on `main`. The squash commit message is taken from the pull request title, which must follow the Conventional Commits format.
- Before merging, the feature branch is rebased onto the latest `main` if it has fallen behind, to keep the history linear.
- The source branch is deleted automatically after merge (enable this in repository settings).

### 2.6 Tagging and versions

Versions follow Semantic Versioning (`MAJOR.MINOR.PATCH`). The project starts at `0.1.0`. A tag is created on `main` when a set of changes is deployed to dev and considered a stable baseline, for example `v0.2.0`. Tags are created by the person who ran the deployment, and the release notes list the merged pull requests since the previous tag, which GitHub can generate automatically.

While the project is in practice mode, the version bumps are informal, but the tags still matter because they record what code was running in dev at a given point.

## 3. Pull Requests

### 3.1 Size

A pull request should contain one feature, one fix, or one logical change. As a guide, a pull request should be reviewable in under 30 minutes. If a feature is larger, split it into several pull requests, each of which leaves `main` in a working state.

### 3.2 Pull request template

Every pull request uses the repository template, which asks for:

- **What changed and why**: a short summary and a link to the issue, if one exists.
- **How it was tested**: the unit tests added or updated, and any manual checks.
- **Metadata impact**: whether YAML metadata or control table definitions changed. If yes, the Metadata Layout document is referenced.
- **Deployment impact**: whether the change affects the Databricks Asset Bundle, jobs, pipelines, or volumes.
- **Checklist**: linting passes locally, unit tests pass locally, and new functions have unit tests.

### 3.3 Review

- Reviewers check correctness, readability, adherence to this document, and test coverage of new functions.
- Reviewers should run the tests locally for any change that affects transformation logic.
- Review comments are resolved by the author, either by changing the code or by replying with the reason for not changing it.

## 4. Repository Layout

The repository has four top-level folders:

- `pipeline/`: framework code, organised by area (for example `pipeline/common/`, `pipeline/ingestion/`).
- `testing/`: unit tests. The folder structure mirrors `pipeline/` exactly, so the tests for a module sit in the same relative location under `testing/`.
- `docs/`: design, standards, testing and decision records.
- `notebooks/`: thin entry points that call the framework.

For example, the tests for `pipeline/common/catalog.py` live in `testing/common/test_catalog.py`. A test file is always named after the module it tests, with a `test_` prefix.

## 5. Coding Standards

### 5.1 Language and tooling

- PySpark version in development and CI matches the Databricks Runtime version, as stated in the Tech Stack Document.
- **ruff** handles linting and formatting. Its configuration lives in `pyproject.toml`, so every contributor and CI run uses the same rules.
- **pre-commit** runs ruff and basic file checks (trailing whitespace, end-of-file newline, YAML validity) before each commit. CI runs the same checks, so nothing relies on local setup alone.
- SQL in Lakeflow Declarative Pipelines is formatted with **sqlfluff**, configured in the repository. This is optional for the first build but recommended once there is more than one SQL file.

### 5.2 Structure of framework code

- Framework logic lives in modules under `pipeline/`. Notebooks and pipeline files are thin entry points that import these modules and call them. They contain configuration reading and a call to the framework, not transformation logic.
- Transformation logic is written as pure functions where possible: they take DataFrames and configuration as arguments and return DataFrames, with no reads or writes inside them. This is what makes unit testing practical.
- Side effects (reading from volumes, writing to tables, writing audit records) are kept in a small number of functions with names that say so, for example `read_source_files` or `write_audit_record`.
- Functions do one thing. If a function needs a comment to explain what its sections do, consider splitting it.

### 5.3 Naming

| Item | Convention | Example |
|---|---|---|
| Python modules, functions, variables | `snake_case` | `build_quarantine_split` |
| Classes | `PascalCase` | `SchemaContract` |
| Constants | `UPPER_SNAKE_CASE` | `DEFAULT_BATCH_SIZE` |
| Tables, columns, and metadata keys | `snake_case` | `source_system`, `ingested_at` |
| Git branches | Prefix and kebab-case, as in 2.2 | `feature/volume-batch-reader` |

Names describe what something is or does. Avoid abbreviations unless they are standard in the data platform (for example `dq` for data quality is acceptable; `tmp2` is not).

### 5.4 Types and documentation

- All framework functions have type hints on parameters and return values. CI runs a type checker (mypy or pyright, to be chosen by the owners) on `pipeline/`.
- Public functions and classes have a docstring in Google style, stating what the function does, its arguments, what it returns, and any exception it raises.
- Private helper functions need a docstring only when their behaviour is not obvious from the name.

### 5.5 Configuration

- No file paths, catalog names, schema names, or volume names are hard-coded in code. They are read from configuration per environment, supplied by the Databricks Asset Bundle target variables.

## 6. Unit Testing Requirement

This section sets the minimum unit testing rule for the project. The full testing approach (functional, integration, and data quality testing, and the test environments) is defined in the Testing Plan.

### 6.1 The rule

**Every function in `pipeline/` has at least one unit test.** This applies to public and private functions alike. A pull request that adds or changes a function without a corresponding unit test does not pass review.

A unit test should confirm that the function returns the correct result for a normal input. Where a function has known edge cases or failure modes, the test suite should also cover at least one of them, but the minimum requirement is a single passing test that exercises the function.

### 6.2 Where tests live

- Tests live under `testing/`, in a folder structure that mirrors `pipeline/`. A function in `pipeline/common/catalog.py` is tested in `testing/common/test_catalog.py`.
- Test function names describe the behaviour being checked, for example `test_schema_contract_reports_missing_column`.
- Tests use pytest. Shared fixtures go in `conftest.py`.

### 6.3 Running tests

- Developers run unit tests locally before opening a pull request.
- CI runs the full unit test suite on every pull request. A failing test blocks the merge.
- Tests must not depend on a live Databricks workspace, on network access, or on the order in which they run.

### 6.4 Checking coverage

Each pull request reports which functions in `pipeline/` have no test. Coverage reporting is informational at first. A numeric coverage gate is not set until the owners agree a realistic target after the first few features are built.

## 7. Documentation and Decision Records

- The Design Decisions and Approach document records the reasons for architectural choices.
- Each significant decision that closes an open item is recorded as a short Architecture Decision Record in `docs/decisions/`, with a number, a date, the decision, and the reason. The format is one file per decision, for example `0003-batch-read-from-volumes.md`.
- The README is kept current with setup, test, and deployment steps. A change that alters any of these steps updates the README in the same pull request.

## 8. Roles and Responsibilities

Both owners review each other's pull requests. Neither merges their own pull request without an approving review. The owner who opens a pull request is responsible for keeping it up to date, resolving review comments, and deleting the branch after merge.

Decisions that change this document are agreed by both owners and recorded in a pull request against this document.
