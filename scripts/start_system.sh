# Script para iniciar todos os componentes do sistema em segundo plano

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

PYTHON=".venv/bin/python"

if [ ! -f "$PYTHON" ]; then
    echo "[ERRO] Ambiente virtual .venv não encontrado. Execute 'python3 -m venv .venv && .venv/bin/pip install -r requirements.txt'"
    exit 1
fi

echo "=================================================="
echo "   INICIANDO SISTEMA IOT DISTRIBUÍDO DE PET CARE   "
echo "=================================================="

# 1. Mosquitto
if command -v docker &> /dev/null && [ -f "docker-compose.yml" ]; then
    echo "[1/5] Subindo broker Mosquitto via Docker Compose..."
    docker compose up -d mosquitto
else
    echo "[AVISO] Docker não detectado ou indisponível. Certifique-se de que o broker Mosquitto está ativo na porta 1883."
fi

mkdir -p logs
rm -f logs/middleware.log

echo "[2/5] Iniciando Controlador 1 (Primário) na porta 9091..."
$PYTHON src/entrypoints/run_controller.py --id 1 --primary > logs/controller_1.log 2>&1 &
PID_C1=$!

echo "[3/5] Iniciando Controlador 2 (Réplica) na porta 9092..."
$PYTHON src/entrypoints/run_controller.py --id 2 > logs/controller_2.log 2>&1 &
PID_C2=$!

echo "[4/5] Iniciando Controlador 3 (Réplica) na porta 9093..."
$PYTHON src/entrypoints/run_controller.py --id 3 > logs/controller_3.log 2>&1 &
PID_C3=$!

sleep 2

echo "[5/5] Iniciando Simulador de Dispositivos IoT..."
$PYTHON src/entrypoints/run_iot_devices.py > logs/iot_devices.log 2>&1 &
PID_IOT=$!

cleanup() {
    echo ""
    echo "Encerrando todos os processos em segundo plano..."
    kill $PID_IOT $PID_C3 $PID_C2 $PID_C1 2>/dev/null || true
    echo "Processos encerrados com sucesso."
}

trap cleanup EXIT INT TERM

echo "--------------------------------------------------"
echo "Todos os nós de backend foram iniciados com sucesso!"
echo "Logs disponíveis em ./logs/"
echo "Pressione Ctrl+C para encerrar o sistema."
echo "--------------------------------------------------"

# Executa o servidor Web (Cliente RPC) em foreground
$PYTHON src/entrypoints/run_web.py
