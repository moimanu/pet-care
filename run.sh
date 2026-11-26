#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [ "${1:-}" = "--reset" ]; then
    rm -f no_*.db
    echo "[run.sh] Bancos SQLite removidos."
fi

docker compose up -d

COMANDOS=(
    "CTRL 1:.venv/bin/python controlador.py 1"
    "CTRL 2:.venv/bin/python controlador.py 2"
    "CTRL 3:.venv/bin/python controlador.py 3"
    "ATUADOR:.venv/bin/python atuador.py"
    "SENSOR:.venv/bin/python sensor.py"
    "CLIENTE:.venv/bin/python cliente.py"
)

kgx -T "${COMANDOS[0]%%:*}" -- bash -c "${COMANDOS[0]#*:}; exec bash" &
sleep 0.4

for i in 1 2 3 4 5; do
    [ "$i" -eq 3 ] && sleep 1
    kgx --tab -T "${COMANDOS[$i]%%:*}" -- bash -c "${COMANDOS[$i]#*:}; exec bash" &
done
