#!/usr/bin/env bash
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

RESET=false
for arg in "$@"; do
    case "$arg" in
        --reset|-r)
            RESET=true
            ;;
        --help|-h)
            echo "Uso: $0 [--reset|-r]"
            echo "  --reset, -r   Remove bancos node_*.db e logs antes de iniciar"
            exit 0
            ;;
        *)
            echo "[ERRO] Parametro desconhecido: $arg"
            echo "Uso: $0 [--reset|-r]"
            exit 1
            ;;
    esac
done

PYTHON=".venv/bin/python"
if [ ! -f "$PYTHON" ]; then
    echo "[ERRO] Ambiente .venv nao encontrado. Execute: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
    exit 1
fi

if [ "$RESET" = true ]; then
    echo "Reiniciando estado do zero (removendo node_*.db e logs)..."
    rm -f node_*.db node_*.db-* logs/*.jsonl
fi

if command -v docker &> /dev/null && [ -f "docker-compose.yml" ]; then
    echo "Subindo broker Mosquitto via Docker Compose..."
    docker compose up -d mosquitto
fi

echo "Iniciando sistema Pet Care (http://localhost:8080)..."
exec "$PYTHON" -m app.web_server
