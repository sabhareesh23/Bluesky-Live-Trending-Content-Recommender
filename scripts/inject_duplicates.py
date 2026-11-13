"""
Objective 3 demo: send the SAME posts to Kafka again, several times.

It takes the newest real posts from 'raw_posts' and writes each one again
(default 3 extra copies), exactly like a source that delivers a record twice.
Spark must drop all copies -> watch "Duplicates dropped by Spark" on the
dashboard ('♻️ Reliability & duplicates' tab) go up by posts x copies, while the
post counts do not grow.

Run (while producer + Spark are running):
    python scripts/inject_duplicates.py              # 50 posts x 3 copies = 150 duplicates
    python scripts/inject_duplicates.py 20 5         # 20 posts x 5 copies = 100 duplicates
"""
import json
import sys
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

N_POSTS = int(sys.argv[1]) if len(sys.argv) > 1 else 50
COPIES = int(sys.argv[2]) if len(sys.argv) > 2 else 3
SERVER = "localhost:9092"

# 1) read the newest N real posts
consumer = KafkaConsumer(bootstrap_servers=SERVER, enable_auto_commit=False,
                         value_deserializer=lambda b: json.loads(b.decode("utf-8")))
parts = [TopicPartition("raw_posts", p) for p in consumer.partitions_for_topic("raw_posts")]
consumer.assign(parts)
ends = consumer.end_offsets(parts)
for tp in parts:
    consumer.seek(tp, max(0, ends[tp] - N_POSTS))
posts = {}
deadline = time.time() + 10
while len(posts) < N_POSTS * len(parts) and time.time() < deadline:
    for records in consumer.poll(timeout_ms=1000).values():
        for m in records:
            if "post_id" in m.value:
                posts[m.value["post_id"]] = m.value
consumer.close()
posts = sorted(posts.values(), key=lambda p: p["ingest_time_ms"])[-N_POSTS:]
if not posts:
    sys.exit("⚠️ No posts found in raw_posts. Is the producer running?")

# 2) send each one again COPIES times (same post_id = a duplicate)
producer = KafkaProducer(bootstrap_servers=SERVER, acks="all",
                         key_serializer=lambda k: k.encode("utf-8"),
                         value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"))
for _ in range(COPIES):
    for p in posts:
        dup = dict(p, ingest_time_ms=int(time.time() * 1000))   # arrives "now", same ID
        producer.send("raw_posts", key=p["post_id"], value=dup)
producer.flush()
producer.close()

print(f"💉 Injected {len(posts)} posts x {COPIES} copies = {len(posts) * COPIES} duplicates")
print("👀 Dashboard -> '♻️ Reliability & duplicates': 'Duplicates dropped by Spark' should rise by "
      f"{len(posts) * COPIES} within ~10 s, and the post counts should NOT jump.")
