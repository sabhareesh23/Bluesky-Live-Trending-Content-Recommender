"""
Phase 3: the main Spark Structured Streaming job.

Reads live posts from Kafka 'raw_posts' and runs TWO streaming queries:

  Query A "trends"  : counts every hashtag/keyword per 1-minute window,
                      computes a trending score (growth), and writes
                      -> Kafka 'trending'         (top trends overall)
                      -> Kafka 'recommendations'  (top trends per topic = "For you")

  Query B "posts"   : cleans + enriches each post (topic, terms, bad-word flag),
                      measures latency/throughput per micro-batch, and writes
                      -> Kafka 'enriched_posts'   (clean posts, used as examples on the dashboard)
                      -> Kafka 'metrics'          (system health + duplicates dropped)

  Query C "business": posts and likes per hour (objective 1), written
                      -> Kafka 'hourly_metrics'   (one message per hour, key = the hour)

Exactly-once / idempotency (objective 3):
  * every stream is de-duplicated by its ID (post_id / like_id) inside a 15-minute window
  * results are written with a KEY (hour, post_id) and the dashboard keeps only the
    latest value per key -> if Spark ever re-runs a batch after a crash, nothing is
    counted twice

Runs inside the 'spark' Docker container:  docker logs -f spark
"""
import json
import os
import time
from collections import Counter, defaultdict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, LongType, StringType, StructField,
                               StructType)

# =====================================================================
# 0) Settings
# =====================================================================
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/opt/checkpoints")

WINDOW = "1 minute"          # size of one counting bucket
RECENT_MIN = 5               # "now"      = last 5 minutes
BASELINE_MIN = 10            # "normal"   = the 10 minutes before that
MIN_RECENT_AUTHORS = 5       # a trend needs at least 5 DIFFERENT people in the last 5 minutes
TOP_N = 30                   # how many trends to publish
DEDUP_WINDOW = "15 minutes"  # a duplicate arriving within 15 min of the original is dropped
BUSINESS_WINDOW = os.getenv("BUSINESS_WINDOW", "1 hour")   # bucket size for business metrics

spark = (SparkSession.builder
         .appName("bluesky-trending-recommender")
         .config("spark.sql.shuffle.partitions", "6")      # default 200 is too many for our stream
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# =====================================================================
# 1) Word lists
# =====================================================================
# Topics: a post belongs to the FIRST topic whose words appear in it.
TOPICS = {
    "Politics": ["trump", "biden", "election", "congress", "senate", "president", "government",
                 "vote", "voting", "democrat", "republican", "gop", "maga", "minister",
                 "parliament", "policy", "white house", "supreme court"],
    "Tech": ["ai", "llm", "chatgpt", "openai", "claude", "python", "coding", "software",
             "programming", "developer", "tech", "apple", "google", "microsoft", "android",
             "iphone", "linux", "robot", "startup", "crypto", "bitcoin", "data"],
    "Sports": ["football", "soccer", "nba", "nfl", "mlb", "nhl", "basketball", "baseball",
               "tennis", "cricket", "f1", "formula 1", "hockey", "olympics", "league",
               "championship", "touchdown", "goal", "match"],
    "Gaming": ["gaming", "videogame", "playstation", "ps5", "xbox", "nintendo", "switch 2",
               "steam", "gamer", "twitch", "esports", "minecraft", "fortnite", "pokemon"],
    "Entertainment": ["movie", "film", "netflix", "tv show", "series", "actor", "actress",
                      "hollywood", "oscar", "anime", "manga", "trailer", "episode", "celebrity"],
    "Music": ["music", "song", "album", "concert", "spotify", "singer", "band", "rapper",
              "playlist", "tour", "lyrics"],
    "Science": ["science", "nasa", "space", "climate", "research", "physics", "biology",
                "scientist", "vaccine", "health", "medicine"],
    "Art": ["art", "artist", "drawing", "illustration", "painting", "sketch", "commission",
            "digitalart", "fanart", "photography"],
}

# Common words that carry no meaning ("stopwords"). Words shorter than 4 letters are dropped anyway.
STOPWORDS = """
about above after again against also always another anything around back because been before
being best better both came cant come could didn does doesn doing done dont down each even ever
every everyone everything feel first from getting going gonna good great have haven having here
hers just keep know last like little look lots made make many maybe more most much must need never
next nothing okay only other over people pretty really right said same says should shouldn since
some someone something still such sure take than thank thanks that thats their them then there
these they thing things think this those though through time today told very want wanna wasn
were what when where which while will with without wont would wouldn year years your yours yeah
haha lmao post posts please isn aren couldn hasn mean week days actually literally getting
everybody anyone also new into onto upon away youre theyre hes shes heres
tonight morning night talking everywhere tomorrow yesterday already enough seen part start
the and but for you our its his her she who why how now not all any can get got let one yes
omg lol wow hey big old new top via per amp etc watching
are was has had did nah yep yup ive ill youll were details detail english update updates breaking
mon tue wed thu fri sat sun monday tuesday wednesday thursday friday saturday sunday
jan feb mar apr jun jul aug sep sept oct nov dec cst cdt est edt pst pdt mst mdt utc gmt
iembot http https www com bsky app
""".split()

# Simple bad-word filter so the dashboard stays presentable (posts are still kept in raw_posts).
BAD_WORDS = ["fuck", "shit", "bitch", "cunt", "dick", "cock", "pussy", "porn", "nsfw",
             "nude", "nudes", "slut", "whore", "sex", "horny", "onlyfans",
             # disturbing topics we do not want to show as a "recommendation"
             "pedophile", "pedo", "rape", "nazi", "suicide", "kill", "gore"]

# =====================================================================
# 2) Read + parse + clean
# =====================================================================
POST_SCHEMA = StructType([
    StructField("post_id", StringType()),
    StructField("author", StringType()),
    StructField("event_time_us", LongType()),
    StructField("created_at", StringType()),
    StructField("lang", StringType()),
    StructField("text", StringType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("ingest_time_ms", LongType()),
])


def read_raw(topic="raw_posts"):
    """Kafka topic -> streaming DataFrame with a 'value' column."""
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVER)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")      # keep running even if old data was deleted
            .load())


def to_kafka(df, topic):
    """Write a (batch) DataFrame with a 'value' column to a Kafka topic."""
    (df.write.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_SERVER)
       .option("topic", topic)
       .save())


def send_keyed(items, topic):
    """items = [(key, payload), ...] -> Kafka messages WITH a key.
    Same key again = 'newer version of the same thing' (idempotent overwrite)."""
    rows = [(k, json.dumps(v, ensure_ascii=False)) for k, v in items]
    to_kafka(spark.createDataFrame(rows, ["key", "value"]), topic)


def send_json(payload, topic):
    to_kafka(spark.createDataFrame([(json.dumps(payload, ensure_ascii=False),)], ["value"]), topic)


def word_regex(words):
    """['ai','movie'] -> regex that matches the whole word, plural allowed: \\b(ai|movie)s?\\b"""
    return r"\b(" + "|".join(words) + r")s?\b"


def enrich(raw):
    posts = (raw
             .select(F.from_json(F.col("value").cast("string"), POST_SCHEMA).alias("p"))
             .select("p.*")
             .where(F.col("post_id").isNotNull() & F.col("text").isNotNull())
             # OUR clock as event time (avoids the clock-skew problem found in Step 8)
             .withColumn("event_time", (F.col("ingest_time_ms") / 1000).cast("timestamp"))
             # 'en-US' -> 'en'
             .withColumn("lang", F.lower(F.split(F.col("lang"), "-").getItem(0)))
             .withColumn("hashtags", F.coalesce(F.col("hashtags"), F.array().cast("array<string>"))))

    # Exactly-once counting: drop a post whose post_id was already seen in the last 15 min
    # (the producer replays at most 10 min, so every possible source duplicate is covered)
    posts = (posts.withWatermark("event_time", DEDUP_WINDOW)
                  .dropDuplicatesWithinWatermark(["post_id"]))

    content = F.concat_ws(" ", F.lower(F.col("text")), F.array_join(F.col("hashtags"), " "))

    # Topic: chain of WHEN ... OTHERWISE, first matching topic wins
    topic = F.lit("Other")
    for name, words in reversed(list(TOPICS.items())):
        topic = F.when(content.rlike(word_regex(words)), name).otherwise(topic)

    # Terms = hashtags (as "#tag") + NAMES from the text.
    # A "name" = a Capitalised word (Trump, Netflix, Eclipse, Taylor ...). These are
    # proper nouns, so they describe WHAT people talk about. Ordinary lowercase words
    # (similar, expected, highly ...) are ignored because they carry no topic.
    no_links = F.regexp_replace(F.col("text"), r"https?://\S+|@\S+", " ")
    # drop the FIRST word of every sentence (it is capitalised only because it starts the sentence)
    no_starts = F.regexp_replace(no_links, r"(^|[.!?\n]\s*)\S+", "$1")
    words = F.split(no_starts, r"[^\p{L}\p{N}_]+")
    names = F.filter(words, lambda w: w.rlike(r"^\p{Lu}[\p{L}\p{N}]{2,}$"))
    stop = F.array(*[F.lit(w) for w in STOPWORDS + BAD_WORDS])
    keywords = F.filter(F.transform(names, lambda w: F.lower(w)),
                        lambda w: (F.length(w) >= 3) & ~F.array_contains(stop, w))
    tags = F.transform(F.col("hashtags"), lambda h: F.concat(F.lit("#"), h))

    return (posts
            .withColumn("topic", topic)
            .withColumn("is_clean", ~content.rlike(r"\b(" + "|".join(BAD_WORDS) + r")"))
            .withColumn("terms", F.array_distinct(F.concat(tags, keywords))))


LIKE_SCHEMA = StructType([
    StructField("like_id", StringType()),
    StructField("post_id", StringType()),
    StructField("event_time_us", LongType()),
    StructField("ingest_time_ms", LongType()),
])


def parse_likes(raw):
    return (raw
            .select(F.from_json(F.col("value").cast("string"), LIKE_SCHEMA).alias("l"))
            .select("l.*")
            .where(F.col("like_id").isNotNull())
            .withColumn("event_time", (F.col("ingest_time_ms") / 1000).cast("timestamp"))
            .withWatermark("event_time", DEDUP_WINDOW)
            .dropDuplicatesWithinWatermark(["like_id"]))


# =====================================================================
# 3) Query A: trending terms + recommendations
# =====================================================================
history = defaultdict(dict)   # (term, topic) -> {window_start_seconds: (mentions, people)}
first_window = None           # when did we start counting (for the warm-up phase)


def process_trends(batch_df, batch_id):
    global first_window
    rows = (batch_df.where(F.col("authors") >= 2)
            .select(F.col("window.start").alias("ws"), "term", "topic", "mentions", "authors")
            .collect())
    for r in rows:
        history[(r.term, r.topic)][r.ws.timestamp()] = (r["mentions"], r["authors"])
    if not history:
        return

    ref = max(ws for wins in history.values() for ws in wins)           # newest window
    first_window = min(first_window or ref, min(ws for wins in history.values() for ws in wins))
    recent_from = ref - (RECENT_MIN - 1) * 60
    base_from = recent_from - BASELINE_MIN * 60

    # forget windows older than we need (keeps memory small)
    for key in list(history):
        history[key] = {ws: c for ws, c in history[key].items() if ws >= base_from}
        if not history[key]:
            del history[key]

    # add up "recent" and "baseline" numbers per term.
    # We count PEOPLE (different authors), not mentions -> one spam bot posting
    # the same thing 100 times cannot create a fake trend.
    stats = defaultdict(lambda: {"recent": 0, "base": 0, "mentions": 0, "topics": Counter()})
    for (term, topic), wins in history.items():
        for ws, (mentions, people) in wins.items():
            if ws >= recent_from:
                stats[term]["recent"] += people
                stats[term]["mentions"] += mentions
                stats[term]["topics"][topic] += people
            else:
                stats[term]["base"] += people

    # how many baseline minutes do we actually have? (0 during the first 5 minutes)
    base_minutes = max(0, min(BASELINE_MIN, (recent_from - first_window) / 60))
    warming_up = base_minutes < 1

    trends = []
    for term, s in stats.items():
        if s["recent"] < MIN_RECENT_AUTHORS:
            continue
        expected = (s["base"] / base_minutes) * RECENT_MIN if base_minutes >= 1 else 0.0
        score = (s["recent"] + 1) / (expected + 1)          # growth: "how many times above normal"
        trends.append({
            "term": term,
            "topic": s["topics"].most_common(1)[0][0],
            "recent_count": s["recent"],          # people in the last 5 minutes
            "mentions": s["mentions"],
            "expected_count": round(expected, 1),  # people we would normally expect
            "score": round(score, 2),
        })
    trends.sort(key=lambda t: (t["score"], t["recent_count"]), reverse=True)

    now_ms = int(time.time() * 1000)
    send_json({"ts": now_ms, "batch_id": batch_id, "warming_up": warming_up,
               "baseline_minutes": round(base_minutes, 1), "trends": trends[:TOP_N]}, "trending")

    by_topic = defaultdict(list)
    for t in trends:
        if len(by_topic[t["topic"]]) < 5:
            by_topic[t["topic"]].append(t)
    send_json({"ts": now_ms, "batch_id": batch_id, "by_topic": by_topic}, "recommendations")

    print(f"[trends] batch {batch_id}: {len(trends)} trending terms "
          f"(warming_up={warming_up}) top: {[t['term'] for t in trends[:5]]}", flush=True)


# =====================================================================
# 4) Query B: enriched posts + system metrics
# =====================================================================
last_batch_time = None
q_posts = None                # set when the query starts (used to read its progress)
dups_total = 0
last_progress_batch = -1


def duplicates_dropped_last_batch():
    """Spark reports how many rows its de-duplication step threw away."""
    global last_progress_batch
    if q_posts is None or q_posts.lastProgress is None:
        return 0
    prog = q_posts.lastProgress
    if prog.get("batchId") == last_progress_batch:
        return 0                                   # already counted
    last_progress_batch = prog.get("batchId")
    return sum(int(op.get("customMetrics", {}).get("numDroppedDuplicateRows", 0))
               for op in prog.get("stateOperators", []))


def process_posts(batch_df, batch_id):
    global last_batch_time, dups_total
    now = time.time()
    now_ms = int(now * 1000)
    batch_df = batch_df.persist()

    latency = F.lit(now_ms) - F.col("ingest_time_ms")       # producer -> Spark (same clock)
    s = batch_df.agg(F.count("*").alias("n"),
                     F.avg(latency).alias("avg"),
                     F.percentile_approx(latency, 0.95).alias("p95"),
                     F.max(latency).alias("max"),
                     F.avg(F.col("is_clean").cast("int")).alias("clean")).first()

    dups = duplicates_dropped_last_batch()
    dups_total += dups
    seconds = (now - last_batch_time) if last_batch_time else None
    last_batch_time = now
    n = s["n"] or 0

    # clean posts -> examples for the dashboard
    if n:
        to_kafka(batch_df.where("is_clean")
                 .select(F.to_json(F.struct("post_id", "lang", "topic", "text", "terms",
                                            "ingest_time_ms")).alias("value")),
                 "enriched_posts")

    # one metrics message per micro-batch (also when n = 0 -> works as a heartbeat)
    send_json({
        "ts": now_ms,
        "batch_id": batch_id,
        "posts": n,
        "throughput_eps": round(n / seconds, 2) if seconds else None,
        "latency_avg_ms": round(s["avg"]) if s["avg"] is not None else None,
        "latency_p95_ms": s["p95"],
        "latency_max_ms": s["max"],
        "clean_ratio": round(s["clean"], 3) if s["clean"] is not None else None,
        "duplicates_dropped": dups,                # in the previous batch
        "duplicates_dropped_total": dups_total,    # since Spark started
    }, "metrics")
    batch_df.unpersist()

    tp = f"{n / seconds:.1f}" if seconds else "?"
    print(f"[posts]  batch {batch_id}: {n} posts, {tp} posts/s, "
          f"latency avg={s['avg'] and round(s['avg'])} ms p95={s['p95']} ms, "
          f"duplicates dropped={dups} (total {dups_total})", flush=True)


# =====================================================================
# 5) Query C: business metrics per hour (objective 1)
# =====================================================================
hourly = defaultdict(dict)    # window_start_iso -> {"post": n, "like": n, "window_end": iso}


def process_hourly(batch_df, batch_id):
    rows = (batch_df.select(F.col("window.start").alias("ws"), F.col("window.end").alias("we"),
                            "kind", "count").collect())
    touched = set()
    for r in rows:
        key = r.ws.strftime("%Y-%m-%dT%H:%M:%S")
        hourly[key][r.kind] = r["count"]           # assignment, not "+=" -> re-running is harmless
        hourly[key]["window_end"] = r.we.strftime("%Y-%m-%dT%H:%M:%S")
        touched.add(key)
    if not touched:
        return
    items = []
    for key in sorted(touched):
        h = hourly[key]
        posts, likes = h.get("post", 0), h.get("like", 0)
        items.append((key, {
            "window_start": key,
            "window_end": h["window_end"],
            "posts": posts,
            "likes": likes,
            "likes_per_post": round(likes / posts, 3) if posts else None,
            "updated_ts": int(time.time() * 1000),
        }))
    send_keyed(items, "hourly_metrics")
    for k, v in items:
        print(f"[business] {k}: posts={v['posts']} likes={v['likes']} "
              f"likes/post={v['likes_per_post']}", flush=True)


# =====================================================================
# 6) Start all queries
# =====================================================================
if __name__ == "__main__":
    posts = enrich(read_raw("raw_posts"))
    likes = parse_likes(read_raw("raw_likes"))

    term_counts = (posts
                   .select("event_time", "topic", "author", F.explode("terms").alias("term"))
                   .groupBy(F.window("event_time", WINDOW), "term", "topic")
                   .agg(F.count("*").alias("mentions"),
                        F.approx_count_distinct("author").alias("authors")))

    q_trends = (term_counts.writeStream
                .queryName("trends")
                .outputMode("update")
                .foreachBatch(process_trends)
                .option("checkpointLocation", f"{CHECKPOINT_DIR}/trends")
                .trigger(processingTime="10 seconds")
                .start())

    q_posts = (posts.writeStream
               .queryName("posts")
               .outputMode("append")
               .foreachBatch(process_posts)
               .option("checkpointLocation", f"{CHECKPOINT_DIR}/posts")
               .trigger(processingTime="5 seconds")
               .start())

    business = (posts.select("event_time", F.lit("post").alias("kind"))
                .unionByName(likes.select("event_time", F.lit("like").alias("kind")))
                .groupBy(F.window("event_time", BUSINESS_WINDOW), "kind")
                .count())
    q_business = (business.writeStream
                  .queryName("business")
                  .outputMode("update")
                  .foreachBatch(process_hourly)
                  .option("checkpointLocation", f"{CHECKPOINT_DIR}/business")
                  .trigger(processingTime="30 seconds")
                  .start())

    print(f"🚀 Spark job started: 'trends' (10 s), 'posts' (5 s), 'business' (30 s, "
          f"window {BUSINESS_WINDOW})", flush=True)
    spark.streams.awaitAnyTermination()
