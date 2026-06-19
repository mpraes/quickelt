#!/bin/bash
set -euo pipefail
echo "[quickelt-bootstrap] Starting VM bootstrap with local PostgreSQL..."
sudo apt-get update -y
sudo apt-get install -y python3-pip git
echo "[quickelt-bootstrap] Installing PostgreSQL..."
sudo apt-get install -y postgresql postgresql-contrib
sudo systemctl enable postgresql
sudo systemctl start postgresql
echo "[quickelt-bootstrap] Creating database and user..."
sudo -u postgres psql -c "CREATE USER quickelt WITH PASSWORD '${local_pg_password}';"
sudo -u postgres psql -c "CREATE DATABASE quickelt_db OWNER quickelt;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE quickelt_db TO quickelt;"
echo "[quickelt-bootstrap] PostgreSQL installation complete."
echo "[quickelt-bootstrap] Bootstrap complete."
