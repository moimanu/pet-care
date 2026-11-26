#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [ "${1:-}" = "--reset" ]; then
    rm -f no_*.db
    echo "[run.sh] Bancos SQLite removidos."
fi

docker compose up -d

kgx -T "CTRL 1" -- bash -c ".venv/bin/python controlador.py 1; exec bash" &
sleep 0.4
kgx --tab -T "CTRL 2" -- bash -c ".venv/bin/python controlador.py 2; exec bash" &
kgx --tab -T "CTRL 3" -- bash -c ".venv/bin/python controlador.py 3; exec bash" &
sleep 1
kgx --tab -T "SENSOR/ATUADOR" -- bash -c ".venv/bin/python sensor_atuador.py; exec bash" &
kgx --tab -T "CLIENTE" -- bash -c ".venv/bin/python cliente.py; exec bash" &
