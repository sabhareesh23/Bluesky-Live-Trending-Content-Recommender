"""
Bluesky -> Kafka producer (version 3)

Reads live posts AND likes from Bluesky Jetstream and sends them to Kafka:
    raw_posts : clean posts (filtered by language)
    raw_likes : likes on the posts we collected (for the "likes per post" business metric)

Reliability:
  1. Auto-reconnect with exponential backoff (1, 2, 4 ... 60 s)
  2. Cursor replay: last event time saved in producer/cursor.txt; after a crash we
     reconnect "from the cursor" and Jetstream re-sends what we missed -> no data loss
  3. Failover across 4 Jetstream servers
  4. acks="all" + retries + flush on exit
  5. Broken messages -> 'dead_letter' topic

Idempotency (objective 3, first layer):
  The source CAN send the same record twice (e.g. the 5-second overlap after a
  cursor replay). We remember every post_id / like_id we already sent, so a
  duplicate is skipped and NEVER stored twice in Kafka. The recent IDs are saved
  to producer/seen_ids.json, so this also works across restarts.

Run:
    python producer/producer.py              # English posts (default)
    LANGS=all python producer/producer.py    # every language (more load)
    FRESH=1 python producer/producer.py      # ignore saved cursor + seen IDs
"""
import json
import os
import time
from collections import deque

import certifi
import websocket
from kafka import KafkaProducer

# ---------------- Settings ----------------
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
TOPIC_POSTS = "raw_posts"
TOPIC_LIKES = "raw_likes"
TOPIC_DLQ = "dead_letter"
LANGS = os.getenv("LANGS", "en")                 # "en", "en,de" or "all"
STATS_EVERY_SEC = 10

JETSTREAM_HOSTS = [
    "jetstream2.us-east.bsky.network",
    "jetstream1.us-east.bsky.network",
    "jetstream1.us-west.bsky.network",
    "jetstream2.us-west.bsky.network",
]
POST_COLLECTION = "app.bsky.feed.post"
LIKE_COLLECTION = "app.bsky.feed.like"

HERE = os.path.dirname(os.path.abspath(__file__))
CURSOR_FILE = os.path.join(HERE, "cursor.txt")
SEEN_FILE = os.path.join(HERE, "seen_ids.json")
SAVE_EVERY_SEC = 5
REPLAY_BUFFER_US = 5 * 1_000_000      # rewind 5 s extra -> creates duplicates on purpose (we skip them)
MAX_REPLAY_SEC = 10 * 60              # never replay more than 10 minutes

POST_MEMORY_SEC = 2 * 60 * 60         # remember our posts for 2 h (to match likes + skip duplicates)
PERSIST_IDS_SEC = 15 * 60             # IDs younger than 15 min are saved to disk (covers any replay)

wanted_langs = None if LANGS == "all" else set(LANGS.split(","))

# ---------------- Kafka producer ----------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    acks="all",
    retries=5,
    linger_ms=20,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)


# ---------------- "Seen" memory (idempotency) ----------------
class SeenIds:
    """Remembers IDs for a limited time. Like a guest list at a door:
    if your name is already ticked, you cannot come in a second time."""

    def __init__(self, ttl_sec):
        self.ttl = ttl_sec
        self.ids = set()
        self.order = deque()            # (time_added, id), oldest first

    def add(self, item_id, t=None):
        t = t or time.time()
        self.ids.add(item_id)
        self.order.append((t, item_id))

    def __contains__(self, item_id):
        return item_id in self.ids

    def expire(self):
        limit = time.time() - self.ttl
        while self.order and self.order[0][0] < limit:
            _, old = self.order.popleft()
            self.ids.discard(old)

    def recent(self, seconds):
        limit = time.time() - seconds
        return [[t, i] for t, i in self.order if t >= limit]


seen_posts = SeenIds(POST_MEMORY_SEC)
seen_likes = SeenIds(PERSIST_IDS_SEC)


# ---------------- Cursor + seen IDs on disk ----------------
def load_state():
    """Returns the cursor to resume from (or None) and loads recent seen IDs."""
    if os.getenv("FRESH") == "1":
        return None
    if os.path.exists(SEEN_FILE):
        try:
            data = json.load(open(SEEN_FILE))
            for t, i in data.get("posts", []):
                seen_posts.add(i, t)
            for t, i in data.get("likes", []):
                seen_likes.add(i, t)
            print(f"🧠 Loaded {len(data.get('posts', []))} recent post IDs and "
                  f"{len(data.get('likes', []))} like IDs (duplicate protection)")
        except (ValueError, OSError):
            pass
    if not os.path.exists(CURSOR_FILE):
        return None
    try:
        saved = int(open(CURSOR_FILE).read().strip())
    except ValueError:
        return None
    oldest_allowed = int((time.time() - MAX_REPLAY_SEC) * 1_000_000)
    return max(saved - REPLAY_BUFFER_US, oldest_allowed)


def save_state():
    if not last_time_us:
        return
    with open(CURSOR_FILE, "w") as f:
        f.write(str(last_time_us))
    tmp = SEEN_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"posts": seen_posts.recent(PERSIST_IDS_SEC),
                   "likes": seen_likes.recent(PERSIST_IDS_SEC)}, f)
    os.replace(tmp, SEEN_FILE)          # atomic: the file is never half-written


def build_url(host, cursor):
    url = (f"wss://{host}/subscribe?wantedCollections={POST_COLLECTION}"
           f"&wantedCollections={LIKE_COLLECTION}")
    if cursor:
        url += f"&cursor={cursor}"
    return url


# ---------------- Cleaning ----------------
def extract_post(event, commit):
    record = commit.get("record", {})
    text = (record.get("text") or "").strip()
    if not text:
        return None
    langs = record.get("langs") or ["unknown"]
    if wanted_langs and not (set(langs) & wanted_langs):
        return None
    hashtags = [
        f["tag"].lower()
        for facet in record.get("facets", [])
        for f in facet.get("features", [])
        if f.get("$type") == "app.bsky.richtext.facet#tag" and f.get("tag")
    ]
    return {
        "post_id": f'{event["did"]}/{commit["rkey"]}',
        "author": event["did"],
        "event_time_us": event["time_us"],
        "created_at": record.get("createdAt"),
        "lang": langs[0],
        "text": text,
        "hashtags": hashtags,
        "ingest_time_ms": int(time.time() * 1000),
    }


def extract_like(event, commit):
    """A like points to a post: at://<author-did>/app.bsky.feed.post/<rkey>"""
    uri = (commit.get("record", {}).get("subject") or {}).get("uri", "")
    parts = uri.replace("at://", "").split("/")
    if len(parts) != 3 or parts[1] != POST_COLLECTION:
        return None
    return {
        "like_id": f'{event["did"]}/{commit["rkey"]}',
        "post_id": f"{parts[0]}/{parts[2]}",
        "event_time_us": event["time_us"],
        "ingest_time_ms": int(time.time() * 1000),
    }


# ---------------- State ----------------
stats = {"received": 0, "posts": 0, "likes": 0, "dup_posts": 0, "dup_likes": 0, "skipped": 0, "bad": 0}
last_print = time.time()
last_save = time.time()
last_time_us = None
delay = 1
stopping = False
host_index = 0


def maybe_print_stats():
    global last_print
    now = time.time()
    if now - last_print >= STATS_EVERY_SEC:
        sec = now - last_print
        print(f"📊 {stats['posts'] / sec:5.1f} posts/s | {stats['likes'] / sec:5.1f} likes/s | "
              f"duplicates skipped: posts={stats['dup_posts']} likes={stats['dup_likes']} | "
              f"received={stats['received']} skipped={stats['skipped']} bad={stats['bad']}")
        for k in stats:
            stats[k] = 0
        seen_posts.expire()
        seen_likes.expire()
        last_print = now


def maybe_save_state():
    global last_save
    if time.time() - last_save >= SAVE_EVERY_SEC:
        save_state()
        last_save = time.time()


# ---------------- WebSocket callbacks ----------------
def on_open(ws):
    global delay
    delay = 1
    print(f"✅ Connected to {JETSTREAM_HOSTS[host_index]} (post languages: {LANGS})")


def handle_event(event):
    if event.get("kind") != "commit":
        return "skip"
    commit = event.get("commit", {})
    if commit.get("operation") != "create":
        return "skip"

    if commit.get("collection") == POST_COLLECTION:
        post = extract_post(event, commit)
        if post is None:
            return "skip"
        if post["post_id"] in seen_posts:                 # idempotency: already stored
            return "dup_posts"
        seen_posts.add(post["post_id"])
        producer.send(TOPIC_POSTS, key=post["post_id"], value=post)
        return "posts"

    if commit.get("collection") == LIKE_COLLECTION:
        like = extract_like(event, commit)
        if like is None or like["post_id"] not in seen_posts:   # only likes on OUR posts
            return "skip"
        if like["like_id"] in seen_likes:
            return "dup_likes"
        seen_likes.add(like["like_id"])
        producer.send(TOPIC_LIKES, key=like["like_id"], value=like)
        return "likes"
    return "skip"


def on_message(ws, message):
    global last_time_us
    stats["received"] += 1
    try:
        event = json.loads(message)
        last_time_us = event.get("time_us", last_time_us)
        result = handle_event(event)
    except Exception as e:
        stats["bad"] += 1
        producer.send(TOPIC_DLQ, key="bad", value={"error": str(e), "raw": message[:2000]})
        return
    stats[result if result in stats else "skipped"] += 1
    maybe_print_stats()
    maybe_save_state()


def on_error(ws, error):
    global stopping
    if isinstance(error, KeyboardInterrupt):
        stopping = True
        return
    print(f"⚠️ WebSocket error: {error}")


def on_close(ws, code, reason):
    print(f"🔌 Connection closed (code={code}, reason={reason})")


# ---------------- Main loop ----------------
if __name__ == "__main__":
    print(f"🚀 Producer starting -> Kafka {KAFKA_SERVER}, topics '{TOPIC_POSTS}' + '{TOPIC_LIKES}'")
    first_cursor = load_state()
    try:
        while True:
            cursor = last_time_us - REPLAY_BUFFER_US if last_time_us else first_cursor
            if cursor:
                behind = time.time() - cursor / 1_000_000
                print(f"🔖 Resuming from cursor ({behind:.0f} s ago) -> replaying missed events")
            ws = websocket.WebSocketApp(
                build_url(JETSTREAM_HOSTS[host_index], cursor),
                on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            ws.run_forever(sslopt={"ca_certs": certifi.where()},
                           ping_interval=30, ping_timeout=10)
            if stopping:
                break
            save_state()
            host_index = (host_index + 1) % len(JETSTREAM_HOSTS)
            print(f"🔄 Reconnecting in {delay}s to {JETSTREAM_HOSTS[host_index]} ...")
            time.sleep(delay)
            delay = min(delay * 2, 60)
    except KeyboardInterrupt:
        pass
    finally:
        print("\n🛑 Stopping producer ...")
        save_state()
        producer.flush()
        producer.close()
        print("👋 Bye (cursor + seen IDs saved)")
