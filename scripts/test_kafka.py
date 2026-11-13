"""
Phase 1 check: can Python talk to Kafka?
Sends 5 test messages to 'raw_posts' and reads them back.

Run:  python scripts/test_kafka.py
"""
import json
import time
import uuid

from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = "localhost:9092"
TOPIC = "raw_posts"
run_id = str(uuid.uuid4())[:8]  # tag so we only read back OUR test messages

# --- 1) Send ---
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    acks="all",  # wait until Kafka confirms the message is saved
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
for i in range(5):
    producer.send(TOPIC, {"test": True, "run_id": run_id, "n": i, "ts": time.time()})
producer.flush()
print(f"✅ Sent 5 test messages (run_id={run_id})")

# --- 2) Read back ---
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    group_id=f"phase1-test-{run_id}",
    consumer_timeout_ms=10000,  # stop after 10 s of silence
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)
received = [m.value for m in consumer if m.value.get("run_id") == run_id]
consumer.close()

print(f"✅ Received {len(received)} / 5 messages back")
print("🎉 Phase 1 complete: Kafka works!" if len(received) == 5 else "⚠️ Something is off, check `docker compose ps`")
