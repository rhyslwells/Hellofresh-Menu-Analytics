#!/bin/bash
# Databricks cluster init script
# Installs Python packages required by the HelloFresh pipeline
# Place this script in DBFS or attach as an init script for the cluster

set -euo pipefail

echo "[init] Starting HelloFresh cluster init script"

# Use python from the environment
PYTHON_BIN=$(which python3 || which python)
if [ -z "$PYTHON_BIN" ]; then
  echo "[init] python not found; exiting"
  exit 1
fi

echo "[init] Using Python: $PYTHON_BIN"

# Upgrade pip
$PYTHON_BIN -m pip install --upgrade pip setuptools wheel

# Install packages
$PYTHON_BIN -m pip install --no-cache-dir requests pandas matplotlib seaborn pyarrow

# Optional: install databricks-cli if you need CLI actions from cluster
# $PYTHON_BIN -m pip install databricks-cli

echo "[init] Package installation complete"

exit 0
