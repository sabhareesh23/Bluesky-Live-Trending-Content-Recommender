#!/usr/bin/env bash
# Stops all Docker services. Data in Kafka and Spark checkpoints is KEPT (volumes).
# To delete everything as well, run:  docker compose down -v
cd "$(dirname "$0")"
pkill -INT -f "producer/producer.py" 2>/dev/null && echo "🛑 Producer stopped"
docker compose down
echo "✅ All Docker services stopped (data kept)"
