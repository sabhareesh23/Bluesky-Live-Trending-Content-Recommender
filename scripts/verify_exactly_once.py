"""
Prove exactly-once for ONE specific post.

1. Takes one recent real post from 'raw_posts'
2. Sends that SAME post (same post_id) again COPIES times
3. Waits for Spark, then counts how often this post_id appears in:
     raw_posts       (the input log)      -> 1 original + COPIES copies
     enriched_posts  (Spark's output)     -> must be exactly 1

Run (while producer + Spark are running):
    python scripts/verify_exactly_once.py          # 4 extra copies
    python scripts/verify_exactly_once.py 3        # 3 extra copies
"""
import json
import sys
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

SERVER = "localhost:9092"
COPIES = int(sys.argv[1]) if len(sys.argv) > 1 else 4
WAIT_SEC = 45                       # Spark needs a few batches (about 7 s each on a laptop)


def new_consumer():
    return KafkaConsumer(bootstrap_servers=SERVER, enable_auto_commit=False,
                         value_deserializer=lambda b: json.loads(b.decode("utf-8")))


def read_topic(topic, from_offsets=None, back=300):
    """Read messages of a topic, either from given offsets or the last `back` per partition."""
    c = new_consumer()
    parts = [TopicPartition(topic, p) for p in c.partitions_for_topic(topic)]
    c.assign(parts)
    ends = c.end_offsets(parts)
    for tp in parts:
        start = from_offsets[tp] if from_offsets else max(0, ends[tp] - back)
        c.seek(tp, start)
    out = []
    while any(c.position(tp) < ends[tp] for tp in parts):
        for records in c.poll(timeout_ms=1000).values():
            out.extend(m.value for m in records)
    c.close()
    return out


def end_offsets(topic):
    c = new_consumer()
    parts = [TopicPartition(topic, p) for p in c.partitions_for_topic(topic)]
    ends = c.end_offsets(parts)
    c.close()
    return ends


# 1) pick one recent real post that Spark has already processed (it is in enriched_posts)
enriched = [p for p in read_topic("enriched_posts", back=50) if "post_id" in p]
if not enriched:
    sys.exit("⚠️ No processed posts found. Are the producer and Spark running?")
target_id = enriched[-1]["post_id"]
raw = {p["post_id"]: p for p in read_topic("raw_posts", back=500) if "post_id" in p}
if target_id not in raw:
    sys.exit("⚠️ Could not find that post in raw_posts, please run the script again.")
post = raw[target_id]
print(f"🎯 Chosen post: {target_id}")
print(f"   text: {post['text'][:80]!r}")

# remember where both topics end NOW, so we only look at what comes after
raw_start = end_offsets("raw_posts")
enr_start = end_offsets("enriched_posts")

# 2) send the same post again COPIES times
producer = KafkaProducer(bootstrap_servers=SERVER, acks="all",
                         key_serializer=lambda k: k.encode("utf-8"),
                         value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"))
for _ in range(COPIES):
    producer.send("raw_posts", key=target_id, value=dict(post, ingest_time_ms=int(time.time() * 1000)))
producer.flush()
producer.close()
print(f"💉 Sent the same post {COPIES} more times. Waiting {WAIT_SEC} s for Spark ...")
time.sleep(WAIT_SEC)

# 3) count
copies_in_raw = sum(1 for p in read_topic("raw_posts", from_offsets=raw_start) if p.get("post_id") == target_id)
new_in_output = sum(1 for p in read_topic("enriched_posts", from_offsets=enr_start) if p.get("post_id") == target_id)
total_output = sum(1 for p in read_topic("enriched_posts", back=5000) if p.get("post_id") == target_id)

print("\n========== RESULT ==========")
print(f"📥 raw_posts (input):        1 original + {copies_in_raw} new copies")
print(f"📤 enriched_posts (output):  {total_output} time(s) in total, {new_in_output} new after the copies")
if total_output == 1 and new_in_output == 0:
    print("✅ EXACTLY ONCE: the post was delivered", 1 + copies_in_raw, "times but processed only once.")
else:
    print("⚠️ Unexpected result. Check that Spark is running (docker logs -f spark).")
