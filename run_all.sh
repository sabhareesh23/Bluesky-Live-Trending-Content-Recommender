#!/usr/bin/env bash
# Starts the whole pipeline with one command:
#   1. Kafka + topics + Kafka UI + Spark   (Docker)
#   2. Producer (Bluesky -> Kafka)          (background, log in logs/producer.log)
#   3. Dashboard                            (http://localhost:8501)
# Press Ctrl+C to stop the producer and the dashboard. Docker keeps running
# (stop it with ./stop_all.sh).
#
# Usage:  ./run_all.sh            English posts only
#         LANGS=all ./run_all.sh  all languages
set -e
cd "$(dirname "$0")"

echo "🐳 1/3 Starting Docker services (Kafka, Kafka UI, Spark) ..."
docker compose up -d

echo "⏳ Waiting for Kafka to be healthy ..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do
  sleep 2
done
echo "✅ Kafka is healthy"

# Python virtual environment
if [ ! -d ".venv" ]; then
  echo "🐍 Creating virtual environment ..."
  python3 -m venv .venv
fi
source .venv/bin/activate
python -m pip install -q -r requirements.txt

mkdir -p logs
echo "📡 2/3 Starting producer (log: logs/producer.log) ..."
python producer/producer.py > logs/producer.log 2>&1 &
PRODUCER_PID=$!

cleanup() {
  echo ""
  echo "🛑 Stopping producer ..."
  kill -INT "$PRODUCER_PID" 2>/dev/null || true
  wait "$PRODUCER_PID" 2>/dev/null || true
  echo "👋 Done. Docker is still running (stop it with ./stop_all.sh)"
}
trap cleanup EXIT

echo "📊 3/3 Starting dashboard -> http://localhost:8501"
echo "   Kafka UI -> http://localhost:8080 | Spark UI -> http://localhost:4040"
echo "   Spark logs: docker logs -f spark | Producer log: tail -f logs/producer.log"
streamlit run dashboard/app.py
