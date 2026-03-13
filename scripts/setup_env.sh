#!/usr/bin/env bash
# =============================================================================
# Environment Setup — Azure Databricks Lakehouse
# =============================================================================
# Usage: source scripts/setup_env.sh [dev|staging|prod]
# =============================================================================

set -euo pipefail

ENV="${1:-dev}"
echo "Setting up environment: $ENV"

# ── Python virtual environment ───────────────────────────────────────────
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

echo "Activating virtual environment..."
source .venv/bin/activate

# ── Install dependencies ─────────────────────────────────────────────────
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
pip install -e ".[dev]"

# ── Environment variables ────────────────────────────────────────────────
export ENVIRONMENT="$ENV"

if [ "$ENV" = "dev" ]; then
    export AZURE_STORAGE_ACCOUNT="stlakehousedev"
    export LAKEHOUSE_LOCAL_MODE="true"
elif [ "$ENV" = "staging" ]; then
    export AZURE_STORAGE_ACCOUNT="stlakehousestg"
    export LAKEHOUSE_LOCAL_MODE="false"
elif [ "$ENV" = "prod" ]; then
    export AZURE_STORAGE_ACCOUNT="stlakehouseprod"
    export LAKEHOUSE_LOCAL_MODE="false"
fi

echo ""
echo "Environment setup complete:"
echo "  Environment:      $ENVIRONMENT"
echo "  Storage Account:  $AZURE_STORAGE_ACCOUNT"
echo "  Local Mode:       ${LAKEHOUSE_LOCAL_MODE:-false}"
echo "  Python:           $(python --version)"
echo "  Pip packages:     $(pip list --format=freeze | wc -l) installed"
echo ""
echo "Quick commands:"
echo "  make test      — Run unit tests"
echo "  make lint      — Run linter"
echo "  make check     — Run lint + tests"
echo ""
