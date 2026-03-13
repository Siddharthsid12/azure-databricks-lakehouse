# Contributing

## Development Setup

```bash
# Clone and enter project
git clone https://github.com/<your-username>/azure-databricks-lakehouse.git
cd azure-databricks-lakehouse

# Set up environment
source scripts/setup_env.sh dev

# Or manually:
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e ".[dev]"
```

## Running the Pipeline Locally

The entire pipeline can be run locally without Azure or Databricks:

```bash
# Full pipeline run (Bronze → Silver → Quality Gate → Gold)
python scripts/run_pipeline_local.py

# Skip quality gate (for debugging)
python scripts/run_pipeline_local.py --skip-quality-gate

# Keep previous data (don't clean /tmp/lakehouse)
python scripts/run_pipeline_local.py --keep-data
```

After running, inspect the results:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").getOrCreate()

# View Gold layer tables
spark.read.format("delta").load("/tmp/lakehouse/gold/fact_orders").show()
spark.read.format("delta").load("/tmp/lakehouse/gold/dim_customer").show()

# Check quarantined (rejected) records
spark.read.format("delta").load("/tmp/lakehouse/quarantine/orders_quarantine").show()
```

## Running Tests

```bash
# Run all tests with coverage
make test

# Run a specific test file
pytest tests/test_scd.py -v

# Run a specific test class
pytest tests/test_data_quality.py::TestReferentialIntegrity -v

# Quick run without coverage
make test-quick
```

## Code Quality

```bash
# Lint check
make lint

# Auto-format
make format

# Full CI check (lint + tests)
make check
```

## Project Conventions

### Code Structure
- **Notebooks** (`notebooks/`) are thin orchestration layers — they call functions from `src/`
- **Business logic** lives in `src/` and is fully unit-testable
- **No business logic in notebooks** — this ensures testability and reusability

### Naming
- Delta tables: `snake_case` (e.g., `dim_customer`, `fact_orders`)
- Python modules: `snake_case.py`
- Test files: `test_<module>.py`
- Test functions: `test_<what_it_does>`

### Data Quality
- Rules are defined in `config/data_quality_rules.yaml`
- `severity: critical` → blocks pipeline
- `severity: warning` → logs alert, pipeline continues
- Bad records go to quarantine (never silently dropped)

### SCD Type 2
- Changes detected via SHA-256 hash of tracked columns
- `is_current = true` → active record
- `scd_end_date = 9999-12-31` → no expiry (current)
- Historical records have `is_current = false` and `scd_end_date` set to the change date

### Git Workflow
- `main` → production-ready code, deploys via CI/CD
- `develop` → integration branch
- Feature branches: `feature/<description>`
- Hotfix branches: `hotfix/<description>`
