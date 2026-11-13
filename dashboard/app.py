"""
Live dashboard (Streamlit) for the Bluesky Trending Content Recommender.

A background thread reads the Kafka topics written by Spark and the page
refreshes every 3 seconds.

Outage recovery (objective 2):
  The reader belongs to the Kafka consumer group "dashboard" and COMMITS its
  position (offset) after every batch it processes. Kafka keeps all messages,
  so when the dashboard is down (simulated with the sidebar button, or really
  stopped and restarted) the data keeps piling up in Kafka. When it comes back
  it continues from its committed offset, catches up the held messages, and
  then continues live. Spark batch numbers are checked for gaps -> proof that
  nothing was lost.

Run:  streamlit run dashboard/app.py      -> http://localhost:8501
"""
import json
import os
import threading
import time
import traceback
from collections import Counter, OrderedDict, deque

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer, TopicPartition

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
GROUP_ID = "dashboard"
LIVE_TOPICS = ["trending", "recommendations", "enriched_posts", "metrics"]
HOURLY_TOPIC = "hourly_metrics"                # compacted: always read from the beginning
REFRESH_SEC = 3
STALE_AFTER_SEC = 20
HEARTBEAT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".heartbeat")
ALL_TOPICS = ["Politics", "Tech", "Sports", "Gaming", "Entertainment",
              "Music", "Science", "Art", "Other"]
TOPIC_EMOJI = {"Politics": "🏛️", "Tech": "💻", "Sports": "⚽", "Gaming": "🎮",
               "Entertainment": "🎬", "Music": "🎵", "Science": "🔬", "Art": "🎨", "Other": "🌐"}

st.set_page_config(page_title="Bluesky Live Trends", page_icon="🦋", layout="wide")


# =====================================================================
# Background Kafka reader (started once per dashboard process)
# =====================================================================
@st.cache_resource
def start_reader():
    state = {
        "trending": None,
        "recommendations": None,
        "posts": OrderedDict(),            # post_id -> post (keyed = no duplicates on screen)
        "metrics": {},                     # batch_id -> metrics message (keyed = idempotent)
        "live_from": {},                   # partition -> first offset that counts as "new" data
        "live_batches": set(),             # Spark batch ids received from the live / held data
        "hourly": {},                      # window_start -> latest hourly message (keyed)
        "kafka_ok": False,
        "error": None,
        "pause_until": 0.0,                # simulated outage
        "phase": "starting",               # starting | live | offline | catching_up
        "recoveries": deque(maxlen=10),    # history of recoveries
        "read_counts": Counter(),          # messages read per topic (for the status panel)
        "bad_messages": 0,                 # messages we could not use (skipped, not fatal)
        "assigned": {},                    # topic -> number of partitions we read
        "lock": threading.Lock(),
    }

    def handle(topic, value, partition=None, offset=None):
        """Process one message. A broken message is skipped, never fatal."""
        try:
            _handle(topic, value)
            # history we re-read for the charts does not count for the gap check,
            # only data from the live point (first start) or committed point (restart) does
            if (topic == "metrics" and isinstance(value, dict) and "batch_id" in value
                    and offset is not None and offset >= state["live_from"].get(partition, 0)):
                state["live_batches"].add(value["batch_id"])
            state["read_counts"][topic] += 1
        except Exception as e:
            state["bad_messages"] += 1
            print(f"[dashboard] skipped a bad {topic} message: {e!r}", flush=True)

    def safe_commit(consumer):
        try:
            consumer.commit()
        except Exception as e:                       # a failed commit must not stop the dashboard
            print(f"[dashboard] commit failed (will retry): {e!r}", flush=True)

    def _handle(topic, value):
        if not isinstance(value, dict):
            raise ValueError("not a JSON object")
        with state["lock"]:
            if topic in ("trending", "recommendations"):
                old = state[topic]
                if old is None or value.get("ts", 0) >= old.get("ts", 0):
                    state[topic] = value
            elif topic == "enriched_posts":
                state["posts"][value["post_id"]] = value
                state["posts"].move_to_end(value["post_id"])
                while len(state["posts"]) > 3000:
                    state["posts"].popitem(last=False)
            elif topic == "metrics":
                state["metrics"][value["batch_id"]] = value
                if len(state["metrics"]) > 900:
                    for old in sorted(state["metrics"])[:100]:
                        del state["metrics"][old]
            elif topic == HOURLY_TOPIC:
                key = value["window_start"]
                old = state["hourly"].get(key)
                if old is None or value.get("updated_ts", 0) >= old.get("updated_ts", 0):
                    state["hourly"][key] = value

    def held_messages(consumer, parts):
        """How many live messages are waiting in Kafka that we have not read yet?"""
        live = [tp for tp in parts if tp.topic in LIVE_TOPICS]
        ends = consumer.end_offsets(live)
        return sum(max(0, ends[tp] - consumer.position(tp)) for tp in live)

    def catch_up(consumer, parts, reason, offline_sec, held=None):
        state["phase"] = "catching_up"
        if held is None:
            held = held_messages(consumer, parts)
        ends = consumer.end_offsets(parts)
        t0 = time.time()
        while any(consumer.position(tp) < ends[tp] for tp in parts):
            for records in consumer.poll(timeout_ms=500, max_records=2000).values():
                for msg in records:
                    handle(msg.topic, msg.value, msg.partition, msg.offset)
            safe_commit(consumer)
        state["recoveries"].appendleft({
            "at": time.time(), "reason": reason, "offline_sec": round(offline_sec, 1),
            "held": held, "catch_up_sec": round(time.time() - t0, 2),
        })
        state["phase"] = "live"

    def run():
        while True:                                          # reconnect forever if Kafka is down
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=KAFKA_SERVER,
                    group_id=GROUP_ID,
                    enable_auto_commit=False,                # we commit ourselves, after processing
                    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                )
                consumer.topics()
                live_parts = [TopicPartition(t, p) for t in LIVE_TOPICS
                              for p in (consumer.partitions_for_topic(t) or [])]
                hourly_parts = [TopicPartition(HOURLY_TOPIC, p)
                                for p in (consumer.partitions_for_topic(HOURLY_TOPIC) or [])]
                consumer.assign(live_parts + hourly_parts)
                state["assigned"] = dict(Counter(tp.topic for tp in live_parts + hourly_parts))
                ends = consumer.end_offsets(live_parts)

                # 1) business metrics: compacted topic, read everything
                if hourly_parts:
                    consumer.seek_to_beginning(*hourly_parts)

                # 2) live topics: continue from the committed offset if we have one
                restarted, held = False, 0
                state["live_from"] = {}
                for tp in live_parts:
                    committed = consumer.committed(tp)
                    if committed is not None:
                        held += max(0, ends[tp] - committed)      # arrived while we were down
                        if tp.topic == "metrics":
                            state["live_from"][tp.partition] = committed
                        # metrics: also reload ~10 min of history so the charts are not empty
                        # (keyed by batch_id, so reading a message twice changes nothing)
                        consumer.seek(tp, max(0, committed - 120) if tp.topic == "metrics" else committed)
                        restarted = True
                    else:                                    # very first start: a little history
                        back = {"metrics": 120, "trending": 3, "recommendations": 3,
                                "enriched_posts": 200}[tp.topic]
                        consumer.seek(tp, max(0, ends[tp] - back))
                        if tp.topic == "metrics":
                            state["live_from"][tp.partition] = ends[tp]

                state["kafka_ok"], state["error"] = True, None
                if restarted:
                    offline = 0.0
                    if os.path.exists(HEARTBEAT_FILE):
                        offline = time.time() - os.path.getmtime(HEARTBEAT_FILE)
                    catch_up(consumer, live_parts + hourly_parts, "dashboard restarted", offline, held)
                state["phase"] = "live"

                while True:
                    # ---- simulated outage: stop reading, Kafka keeps collecting ----
                    if time.time() < state["pause_until"]:
                        state["phase"] = "offline"
                        start = time.time()
                        while time.time() < state["pause_until"]:
                            time.sleep(0.2)
                        catch_up(consumer, live_parts + hourly_parts,
                                 "simulated outage", time.time() - start)
                        continue

                    for records in consumer.poll(timeout_ms=1000).values():
                        for msg in records:
                            handle(msg.topic, msg.value, msg.partition, msg.offset)
                    safe_commit(consumer)                    # "I have processed up to here"
                    with open(HEARTBEAT_FILE, "w") as f:
                        f.write(str(time.time()))
            except Exception as e:
                state["kafka_ok"], state["error"], state["phase"] = False, repr(e), "starting"
                print("[dashboard] reader error, reconnecting in 5 s:\n" + traceback.format_exc(), flush=True)
                time.sleep(5)

    threading.Thread(target=run, daemon=True).start()
    return state


state = start_reader()


def snapshot():
    with state["lock"]:
        return {
            "trending": state["trending"],
            "recommendations": state["recommendations"],
            "posts": list(state["posts"].values()),
            "metrics": [state["metrics"][k] for k in sorted(state["metrics"])],
            "hourly": [state["hourly"][k] for k in sorted(state["hourly"])],
            "recoveries": list(state["recoveries"]),
            "phase": state["phase"],
            "pause_until": state["pause_until"],
        }


# =====================================================================
# Sidebar
# =====================================================================
st.title("🦋 Bluesky Live Trending Content Recommender")
st.caption("Real-time pipeline: Bluesky Jetstream → Kafka → Spark Structured Streaming → Kafka → this dashboard")

interests = st.sidebar.multiselect("🎯 Your interests (for the 'For you' tab)",
                                   ALL_TOPICS, default=["Tech", "Sports", "Entertainment"])
st.sidebar.markdown("---")
st.sidebar.subheader("🧪 Outage test")
outage_sec = st.sidebar.slider("Take the dashboard offline for (seconds)", 2, 10, 5)
if st.sidebar.button("🔌 Simulate dashboard outage"):
    state["pause_until"] = time.time() + outage_sec
st.sidebar.caption("The dashboard stops reading Kafka. Producer + Spark keep running, so data "
                   "piles up in Kafka. Afterwards the dashboard catches up the held messages.")
st.sidebar.markdown("---")
st.sidebar.markdown(f"🔄 Auto-refresh every **{REFRESH_SEC} s**")
with st.sidebar.expander("🩺 Reader status (debug)"):
    st.write("phase:", state["phase"], "| kafka ok:", state["kafka_ok"])
    st.write("partitions read:", state["assigned"])
    st.write("messages read:", dict(state["read_counts"]))
    st.write("bad messages skipped:", state["bad_messages"])
    if state["error"]:
        st.write("last error:", state["error"])


def missing_batches(metrics=None):
    """Spark numbers its batches 0, 1, 2 ... A gap in the batches received since the
    dashboard (re)started would mean lost data. Older history re-read for the charts is
    ignored: it is spread unevenly over 3 partitions, so it naturally has gaps."""
    ids = sorted(state["live_batches"])
    return sum(b - a - 1 for a, b in zip(ids, ids[1:]) if b - a > 1)


@st.fragment(run_every=REFRESH_SEC)
def live_view():
    data = snapshot()
    metrics = data["metrics"]
    now = time.time()
    now_ms = now * 1000

    # ---------------- Status bar ----------------
    if data["phase"] == "offline":
        left = max(0, data["pause_until"] - now)
        st.error(f"🔌 DASHBOARD OFFLINE (simulated), back in {left:.0f} s. Data keeps flowing into "
                 f"Kafka and is being held for us ...")
    elif data["phase"] == "catching_up":
        st.warning("🔄 Catching up on the messages held in Kafka ...")
    elif not state["kafka_ok"]:
        st.error(f"🔴 Kafka unreachable, retrying every 5 s ... ({state['error']})")
    elif not metrics:
        st.info("⏳ Waiting for the first data from Spark ...")
    else:
        age = (now_ms - metrics[-1]["ts"]) / 1000
        recent_posts = sum(m["posts"] for m in metrics[-4:])
        if age > STALE_AFTER_SEC:
            st.error(f"🚨 STREAM STALE: no update from Spark for {age:.0f} s. Is the Spark container running?")
        elif recent_posts == 0:
            st.warning("⚠️ Spark is running but receives NO new posts. Is the producer / Bluesky connection down?")
        else:
            st.success(f"🟢 LIVE · last update {age:.0f} s ago")

    rec = data["recoveries"][0] if data["recoveries"] else None
    if rec and now - rec["at"] < 60 and data["phase"] == "live":
        st.info(f"✅ Recovered from a {rec['offline_sec']} s outage ({rec['reason']}): caught up "
                f"**{rec['held']} held messages** in {rec['catch_up_sec']} s. "
                f"Missing Spark batches: **{missing_batches(metrics)}**, so no data was lost.")

    # ---------------- KPI row ----------------
    last = metrics[-12:]
    tps = [m["throughput_eps"] for m in last if m.get("throughput_eps") is not None]
    p95 = [m["latency_p95_ms"] for m in last if m.get("latency_p95_ms") is not None]
    trending = data["trending"] or {}
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("⚡ Throughput", f"{sum(tps) / len(tps):.1f} posts/s" if tps else "–")
    c2.metric("⏱️ Latency p95", f"{p95[-1] / 1000:.1f} s" if p95 else "–")
    c3.metric("📬 Posts (last ~1 min)", f"{sum(m['posts'] for m in last):,}" if last else "–")
    c4.metric("🔥 Trending terms", len(trending.get("trends", [])))

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🔥 Trending now", "🎁 For you", "💼 Business metrics",
                                            "♻️ Reliability & duplicates", "⚙️ System health"])

    # ---------------- Tab 1: Trending now ----------------
    with tab1:
        trends = trending.get("trends", [])
        if trending.get("warming_up"):
            st.info("⏳ Warming up: Spark has not seen a 'normal' level yet, so 'people normally' is 0 "
                    "and terms are ranked by how many people use them. Real growth scores start "
                    "after ~5 minutes and are fully reliable after ~15 minutes of running.")
        if not trends:
            st.write("No trends yet ...")
        else:
            df = pd.DataFrame(trends[:15])
            df["label"] = df.apply(lambda r: f"{TOPIC_EMOJI.get(r['topic'], '')} {r['term']}", axis=1)
            st.subheader("Top 15 trending terms (score = times above normal)")
            st.bar_chart(df.set_index("label")["score"], horizontal=True)
            cols = [c for c in ["term", "topic", "score", "recent_count", "expected_count", "mentions"]
                    if c in df.columns]
            st.dataframe(df[cols].rename(columns={
                "recent_count": "people (last 5 min)", "expected_count": "people normally"}),
                hide_index=True, width="stretch")

    # ---------------- Tab 2: For you ----------------
    with tab2:
        recs = (data["recommendations"] or {}).get("by_topic", {})
        posts = data["posts"]
        if not interests:
            st.write("👈 Pick some interests in the sidebar.")
        for topic in interests:
            st.subheader(f"{TOPIC_EMOJI.get(topic, '')} {topic}")
            items = recs.get(topic, [])
            if not items:
                st.caption("Nothing trending in this topic right now.")
                continue
            for t in items[:3]:
                st.markdown(f"**{t['term']}** · score {t['score']} · {t['recent_count']} people posting")
                examples = [p for p in reversed(posts) if t["term"] in (p.get("terms") or [])][:2]
                for p in examples:
                    st.markdown(f"> {p['text'][:280]}")

    # ---------------- Tab 3: Business metrics (objective 1) ----------------
    with tab3:
        hourly = data["hourly"]
        if not hourly:
            st.write("No business metrics yet (Spark publishes them every 30 s) ...")
        else:
            h = pd.DataFrame(hourly)
            h["start"] = pd.to_datetime(h["window_start"])
            h["end"] = pd.to_datetime(h["window_end"])
            bucket_h = (h["end"].iloc[0] - h["start"].iloc[0]).total_seconds() / 3600
            bucket_label = "hour" if bucket_h >= 1 else f"{bucket_h * 60:.0f} min"

            total_posts, total_likes = int(h["posts"].sum()), int(h["likes"].sum())
            # time covered: from the first bucket start until now (or the end of the last bucket)
            now_utc = pd.Timestamp.utcnow().tz_localize(None)
            covered_h = max((min(now_utc, h["end"].max()) - h["start"].min()).total_seconds() / 3600,
                            1 / 60)

            st.subheader("📊 Cumulative business metrics (since the pipeline started)")
            b1, b2, b3 = st.columns(3)
            b1.metric("📝 Avg posts per hour", f"{total_posts / covered_h:,.0f}",
                      help="Total posts ÷ hours covered")
            b2.metric("❤️ Avg likes per hour", f"{total_likes / covered_h:,.0f}",
                      help="Likes on the posts we collected ÷ hours covered")
            b3.metric("📈 Avg likes per post", f"{total_likes / total_posts:.2f}" if total_posts else "–",
                      help="Total likes ÷ total posts (engagement)")
            st.caption(f"Based on {total_posts:,} posts and {total_likes:,} likes over "
                       f"{covered_h:.2f} hours · bucket size: {'1 hour' if bucket_label == 'hour' else bucket_label}")

            h["label"] = h["start"].dt.strftime("%d %b %H:%M")
            h["running_avg_posts_per_hour"] = h["posts"].cumsum() / (
                (h["end"] - h["start"].min()).dt.total_seconds() / 3600)
            left, right = st.columns(2)
            with left:
                st.markdown(f"**Posts and likes per {bucket_label}**")
                st.bar_chart(h.set_index("label")[["posts", "likes"]], stack=False)
            with right:
                st.markdown(f"**Likes per post, per {bucket_label} (engagement trend)**")
                st.line_chart(h.set_index("label")[["likes_per_post"]])
            st.markdown("**Running average posts per hour (cumulative)**")
            st.line_chart(h.set_index("label")[["running_avg_posts_per_hour"]])
            st.caption("The latest bucket is still filling up, so its bar grows until the bucket closes. "
                       "Likes per post starts low and rises during the first ~2 hours: likes arrive "
                       "minutes to hours AFTER a post, and we only count likes on posts we collected.")

    # ---------------- Tab 4: Reliability & duplicates (objectives 2 + 3) ----------------
    with tab4:
        st.subheader("🔌 Outage recovery")
        st.markdown(f"Missing Spark batches in memory: **{missing_batches(metrics)}** "
                    "(0 = every batch arrived, nothing was lost)")
        if data["recoveries"]:
            st.dataframe(pd.DataFrame([{
                "time": time.strftime("%H:%M:%S", time.localtime(r["at"])),
                "reason": r["reason"], "offline (s)": r["offline_sec"],
                "held messages caught up": r["held"],
                "catch-up time (s)": r["catch_up_sec"]} for r in data["recoveries"]]),
                hide_index=True, width="stretch")
        else:
            st.caption("No outage yet. Use the sidebar button, or stop and restart the dashboard.")

        st.subheader("♻️ Duplicates (exactly-once counting)")
        if metrics:
            dup_total = sum(m.get("duplicates_dropped", 0) or 0 for m in metrics)
            unique_posts = sum(m["posts"] for m in metrics)
            d1, d2 = st.columns(2)
            d1.metric("🗑️ Duplicates dropped by Spark", f"{dup_total:,}",
                      help="Copies of a post_id already seen in the last 15 minutes")
            d2.metric("✅ Unique posts counted (recent batches)", f"{unique_posts:,}")
            dm = pd.DataFrame(metrics)
            if "duplicates_dropped" in dm:
                dm["time"] = pd.to_datetime(dm["ts"], unit="ms")
                st.bar_chart(dm.set_index("time")[["duplicates_dropped"]].fillna(0))
        st.caption("Layer 1: the producer skips IDs it already sent (see its log). Layer 2: Spark drops "
                   "duplicate post_ids within 15 min. Layer 3: results are keyed, and this dashboard keeps "
                   "one value per key.")

    # ---------------- Tab 5: System health ----------------
    with tab5:
        if not metrics:
            st.write("No metrics yet ...")
        else:
            m = pd.DataFrame(metrics)
            m["time"] = pd.to_datetime(m["ts"], unit="ms")
            m["latency_p95_s"] = m["latency_p95_ms"] / 1000
            m["latency_avg_s"] = m["latency_avg_ms"] / 1000
            left, right = st.columns(2)
            with left:
                st.subheader("⚡ Throughput over time (posts/s)")
                st.line_chart(m.set_index("time")[["throughput_eps"]])
            with right:
                st.subheader("⏱️ Latency over time (seconds)")
                st.line_chart(m.set_index("time")[["latency_avg_s", "latency_p95_s"]])
            st.subheader("📈 Throughput vs latency (each dot = one Spark micro-batch)")
            st.scatter_chart(m.dropna(subset=["throughput_eps", "latency_p95_s"]),
                             x="throughput_eps", y="latency_p95_s")
            st.caption("Expected: as throughput goes up, latency tends to go up too.")


live_view()
