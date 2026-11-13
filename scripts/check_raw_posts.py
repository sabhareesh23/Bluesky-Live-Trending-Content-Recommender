"""
Step 8: check what is inside the Kafka topic 'raw_posts'.

Reads NEW posts for 20 seconds (while the producer runs) and prints:
  - how many posts arrived and posts/sec (throughput)
  - languages breakdown
  - duplicates found
  - ingestion delay: Bluesky time -> our producer time (first part of latency)

Run (in a 2nd Terminal, while producer.py is running):
    python scripts/check_raw_posts.py
"""
import json
import statistics
import time
from collections import Counter

from kafka import KafkaConsumer

DURATION_SEC = 20

consumer = KafkaConsumer(
    "raw_posts",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",           # only NEW posts from now on
    group_id=f"check-{int(time.time())}",  # fresh group each run
    consumer_timeout_ms=3000,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

print(f"👀 Reading 'raw_posts' for {DURATION_SEC} seconds ...")
start = time.time()
count, langs, ids, dupes, delays_ms = 0, Counter(), set(), 0, []
sample = None

while time.time() - start < DURATION_SEC:
    batch = consumer.poll(timeout_ms=1000)
    for records in batch.values():
        for msg in records:
            post = msg.value
            if "post_id" not in post:          # ignore old Phase 1 test messages
                continue
            count += 1
            langs[post["lang"]] += 1
            if post["post_id"] in ids:
                dupes += 1
            ids.add(post["post_id"])
            delays_ms.append(post["ingest_time_ms"] - post["event_time_us"] / 1000)
            sample = sample or post

consumer.close()
elapsed = time.time() - start

print("\n========== RESULT ==========")
print(f"📬 posts received : {count}")
print(f"⚡ throughput     : {count / elapsed:.1f} posts/sec")
print(f"🌍 top languages  : {langs.most_common(5)}")
print(f"🔁 duplicates     : {dupes}")
if delays_ms:
    print(f"⏱️ ingest delay   : median {statistics.median(delays_ms):.0f} ms, "
          f"max {max(delays_ms):.0f} ms  (Bluesky -> our producer)")
if sample:
    print("\n📝 sample post:")
    print(json.dumps(sample, indent=2, ensure_ascii=False)[:800])
if count == 0:
    print("⚠️ No posts. Is producer.py running in the other Terminal?")
