"""
Step 6: peek at live Bluesky posts (no Kafka yet).
Connects to Bluesky Jetstream, prints 5 real posts, then stops.

Run:  python scripts/peek_jetstream.py
"""
import json
from datetime import datetime, timezone

import certifi
import websocket

URL = ("wss://jetstream2.us-east.bsky.network/subscribe"
       "?wantedCollections=app.bsky.feed.post")

# certifi gives Python an up-to-date list of trusted certificates (fixes SSL errors on Mac)
ws = websocket.create_connection(URL, timeout=15, sslopt={"ca_certs": certifi.where()})
print("✅ Connected to Bluesky Jetstream\n")

shown = 0
first_raw = None
while shown < 5:
    event = json.loads(ws.recv())

    # We only want NEW posts (not deletes, not account events)
    if event.get("kind") != "commit":
        continue
    commit = event["commit"]
    if commit.get("operation") != "create":
        continue

    record = commit["record"]
    if first_raw is None:
        first_raw = event

    # Hashtags live inside "facets" with type ...#tag
    hashtags = [
        f["tag"]
        for facet in record.get("facets", [])
        for f in facet.get("features", [])
        if f.get("$type") == "app.bsky.richtext.facet#tag"
    ]

    event_time = datetime.fromtimestamp(event["time_us"] / 1_000_000, tz=timezone.utc)
    shown += 1
    print(f"--- Post {shown} ---")
    print("🕒 time     :", event_time.strftime("%H:%M:%S"), "UTC")
    print("🌍 language :", record.get("langs"))
    print("#️⃣ hashtags :", hashtags or "none")
    print("📝 text     :", record.get("text", "")[:120].replace("\n", " "))
    print()

ws.close()
print("=== Full raw JSON of the first post (this is what Kafka will store) ===")
print(json.dumps(first_raw, indent=2)[:1500])
