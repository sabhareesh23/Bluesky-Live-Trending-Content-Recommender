# Bluesky Live Trending Content Recommender

A real-time data streaming system that reads every public post on Bluesky as it is
written, finds out which topics are suddenly becoming popular, and recommends that
trending content to users based on their interests. The results are shown on a live
dashboard.

Final project for the Data Management module, Winter 2026, SRH University Campus Hamburg.

---

## 1. Business objective

**Recommendation (social media / marketing).** Brands, newsrooms and creators want
to know what people are talking about *right now*, not yesterday. My system spots
topics whose activity is growing fast and recommends them per interest area
(Politics, Tech, Sports, Gaming, Entertainment, Music, Science, Art).

On top of that, the system tracks three cumulative business metrics:

- average posts per hour
- average likes per hour
- average likes per post (engagement)

---

## 2. Data source

**Bluesky Jetstream**: the official public live feed of the Bluesky network.

- Repository: https://github.com/bluesky-social/jetstream
- Endpoint used: `wss://jetstream2.us-east.bsky.network/subscribe`
  (plus three other public instances for failover)
- Protocol: WebSocket (the server keeps pushing events, no polling)
- Collections used: `app.bsky.feed.post` (posts) and `app.bsky.feed.like` (likes)
- No API key needed

This is real, live data. No synthetic data is used anywhere in the project.

---

## 3. Architecture

```
 Bluesky Jetstream (WebSocket, posts + likes)
          |
          v
 Python producer  ---- broken messages ----> Kafka: dead_letter
 (clean, filter, skip duplicates, cursor replay, failover)
          |
          v
 Kafka: raw_posts, raw_likes            (3 partitions each)
          |
          v
 Spark Structured Streaming (3 queries)
   - trends    (every 10 s): term counts per 1-min window -> trending score
   - posts     (every 5 s) : enrich posts, latency/throughput, duplicates dropped
   - business  (every 30 s): posts and likes per hour
          |
          v
 Kafka: trending, recommendations, enriched_posts, metrics, hourly_metrics
          |
          v
 Streamlit dashboard (consumer group "dashboard", refresh every 3 s)
```

Everything except the producer and the dashboard runs in Docker
(Kafka 3.8 in KRaft mode, Kafka UI, Spark 3.5.3).

### Kafka topics

- `raw_posts`: clean English posts from the producer
- `raw_likes`: likes on the posts we collected
- `enriched_posts`: posts after Spark (topic, terms, bad-word filter)
- `trending`: top 30 trends, one snapshot every 10 s
- `recommendations`: top 5 trends per interest area
- `metrics`: throughput, latency and duplicates per Spark batch
- `hourly_metrics`: posts and likes per hour. **Compacted**: Kafka keeps the latest value per hour
- `dead_letter`: messages that could not be parsed

---

## 4. How it works

### 4.1 Producer (`producer/producer.py`)

- Connects to Jetstream and keeps only the fields we need (ID, author, time,
  language, text, hashtags).
- Skips empty or image-only posts and filters by language (`LANGS=en` by default,
  `LANGS=all` for every language).
- Sends a like to Kafka only if it belongs to a post we collected.
- Adds two timestamps: when Bluesky received the post and when we received it.
- Kafka settings: `acks="all"` and retries, so a message only counts as sent once
  Kafka has saved it.

### 4.2 Spark job (`spark/trending_job.py`)

- Uses our own clock (`ingest_time_ms`) as event time, because Bluesky's clock and my
  laptop's clock differ by a few milliseconds (clock skew).
- Removes duplicate posts and likes by their ID within a 15-minute window.
- Assigns each post a topic using keyword lists.
- **Terms** = hashtags plus capitalised names (Trump, Netflix, Eclipse). Ordinary
  words are ignored because they say nothing about the topic.
- **Trending score** = how many *different people* used a term in the last 5 minutes,
  compared to the 10 minutes before:
  `score = (people_recent + 1) / (people_expected + 1)`.
  Counting people instead of posts means one spam account posting 100 times cannot
  create a fake trend. A term needs at least 5 different people to count.
- Posts with offensive words are kept in `raw_posts` but not shown on the dashboard.

### 4.3 Dashboard (`dashboard/app.py`)

Five tabs:

1. **Trending now**: top 15 trends and their scores
2. **For you**: the user picks interests and gets the top trends in those topics, with real example posts
3. **Business metrics**: the three cumulative metrics and per-bucket charts
4. **Reliability & duplicates**: outage recoveries and duplicates dropped
5. **System health**: throughput, latency, and throughput vs latency

A status bar shows LIVE, "no new posts" (producer down), "stream stale" (Spark down)
or "Kafka unreachable".

---

## 5. The three extra objectives

### 5.1 Cumulative business metrics

Spark counts posts and likes per hour and writes one message per hour to the
compacted topic `hourly_metrics` (key = the hour). The dashboard reads the whole
topic and computes:

- average posts per hour = total posts / hours covered
- average likes per hour = total likes / hours covered
- average likes per post = total likes / total posts

For a quick demo the bucket size can be set to 5 minutes:
`BUSINESS_WINDOW="5 minutes" docker compose up -d`.

**Note:** likes per post starts low and rises during the first ~2 hours. Likes arrive
minutes to hours after a post, and only likes on posts we collected are counted, so
the metric needs time to warm up.

### 5.2 Recovery from a dashboard outage (2–10 s)

- The dashboard reads Kafka as consumer group `dashboard` and **commits its offset**
  (its position) after every batch it has processed.
- While the dashboard is down, the producer and Spark keep running and Kafka holds
  the data.
- When the dashboard comes back, it continues from its committed offset, catches up
  the held messages, and then continues live.
- Proof of no data loss: Spark numbers its batches 0, 1, 2, ... and the dashboard
  checks for gaps ("Missing Spark batches: 0").

Two ways to test it:

1. Sidebar → **Simulate dashboard outage** (slider 2–10 s)
2. Stop the dashboard with `Ctrl + C` and start it again

### 5.3 Exactly-once processing of duplicates (idempotency)

The source can deliver the same record several times, for example after a reconnect.
I handle this in three layers:

1. **Producer:** remembers every post and like ID it has sent (saved to
   `producer/seen_ids.json`, so this also works after a restart) and skips repeats.
2. **Spark:** `dropDuplicatesWithinWatermark` on `post_id` / `like_id` with a
   15-minute window, and reports how many rows it dropped.
3. **Outputs are keyed** (by hour, post or batch number), and the dashboard keeps
   one value per key. If Spark re-runs a batch after a crash, nothing is counted twice.

Like a payment button: `post_id` works like an order number. However often the same
order number arrives, it is processed once.

Test: `python scripts/inject_duplicates.py 50 4` re-sends 50 real posts 4 times
(200 duplicates).

---

## 6. Measures

### Throughput

- English posts only: about **20–25 posts/s**
- All languages: about **46 posts/s**
- Likes on our posts: about **8–10 likes/s**

### Latency

- Bluesky → producer: median **147 ms**, max 550 ms (`scripts/check_raw_posts.py`)
- Producer → Spark result: about **3 s average, 5 s p95**, after a short warm-up
- Most of it comes from micro-batching: on my laptop a Spark batch takes 6–12 s,
  longer than the 5 s trigger, so Spark logs "falling behind". When throughput goes
  up, latency goes up (see the throughput vs latency chart).

### Scalability

- Switching from English to all languages raised the load by about **+75%** (from
  ~26 to ~46 posts/s), more than the +20% asked for. The pipeline handled it with no
  errors and no duplicates.
- Every topic has 3 partitions, so the work can be shared by up to 3 parallel
  consumers. With more load I would add partitions, Spark workers and Kafka brokers.

### Reliability

- Auto-reconnect with exponential backoff (1, 2, 4, ... 60 s)
- Cursor replay: after a disconnect the producer resumes from its last bookmark and
  Jetstream re-sends what was missed (max 2 minutes)
- Failover between 4 Jetstream servers
- `acks="all"` plus retries in the producer
- Spark checkpoints: Spark restarts where it stopped
- Dead letter topic for broken messages (0 in about 2.9 million posts)
- Dashboard outage recovery through committed offsets (section 5.2)

### Accuracy

- Duplicates removed in the producer and in Spark (section 5.3)
- `en-US` normalised to `en`; empty posts skipped
- Trends counted by distinct authors (robust against spam)
- Watermark for late data

---

## 7. Results from my test runs

- **Long run:** about 2.87 million posts in `raw_posts`; Spark ran for about 7 hours
  without interruption; `dead_letter` stayed empty.
- **Business metrics** (5-minute buckets, 0.49 h): 28,314 posts and 25,213 likes →
  57,436 posts/hour, 51,146 likes/hour, 0.89 likes per post.
- **Outage:** 4.5 s offline, 94 messages held and caught up in 0.08 s, 0 missing batches.
- **Duplicates:** after injecting 50 posts × 4 copies, Spark dropped exactly 200 and
  the post counts did not change.
- **Unplanned test:** by mistake I had three producers running at the same time, so
  every post arrived three times. Spark dropped about 29,650 duplicates and the counts
  stayed correct. After stopping the extra producers, `check_raw_posts.py` showed 0
  duplicates.
- **Producer layer:** after a reconnect, the producer skipped 87 duplicate posts and 40
  duplicate likes re-sent by Jetstream.

---

## 8. Limitations

- Single Kafka broker with replication factor 1 (laptop setup). In production I would
  use 3 brokers and replication factor 3.
- Spark runs in local mode in one container, so batches are slower than the trigger
  interval on a laptop.
- Duplicates are detected within 15 minutes. That covers every replay from the
  producer (max 2 minutes), but a copy arriving hours later would get through.
- Topics are assigned with keyword lists, not a trained model.
- Replayed posts get our clock time, so after a long outage they land in the current
  minute and can briefly look like a spike.

---

## 9. How to run

### Prerequisites

- Docker Desktop (running)
- Python 3.10+

### Steps

```bash
# 1. start Kafka, topics, Kafka UI and Spark
docker compose up -d
#    (quick demo of business metrics: BUSINESS_WINDOW="5 minutes" docker compose up -d)

# 2. Python environment
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. terminal 1: producer
python producer/producer.py

# 4. terminal 2: Spark logs
docker logs -f spark

# 5. terminal 3: dashboard
streamlit run dashboard/app.py
```

Or everything in one command: `./run_all.sh` (stop with `Ctrl + C` and `./stop_all.sh`).

### URLs

- Dashboard: http://localhost:8501
- Kafka UI: http://localhost:8080
- Spark UI: http://localhost:4040

### Demos

```bash
python scripts/inject_duplicates.py 50 4   # idempotency
python scripts/check_raw_posts.py          # throughput, duplicates, delay in raw_posts
```
Outage: sidebar → **Simulate dashboard outage**.

### Fresh start (deletes Spark state, keeps Kafka data)

```bash
docker compose down
docker volume rm bluesky-trending-recommender_spark_checkpoints
docker compose up -d
```

---

## 10. Project structure

```
bluesky-trending-recommender/
├── docker-compose.yml        Kafka, topic setup, Kafka UI, Spark
├── requirements.txt
├── run_all.sh / stop_all.sh  start / stop everything
├── producer/producer.py      Bluesky -> Kafka
├── spark/trending_job.py     main Spark Structured Streaming job
├── spark/hello_stream.py     first test job (Step 9)
├── dashboard/app.py          Streamlit dashboard
├── scripts/
│   ├── test_kafka.py         check that Python can talk to Kafka
│   ├── peek_jetstream.py     look at 5 live posts
│   ├── check_raw_posts.py    throughput / duplicates / delay in raw_posts
│   └── inject_duplicates.py  idempotency test
└── prompts.txt               AI prompts used (course rule)
```

---

## 11. Use of AI

I used Claude (Anthropic) as an assistant to plan the project, explain concepts
(Kafka, Spark Structured Streaming, watermarks, offsets) and write and debug code.
I ran, tested and debugged everything on my own machine, and made the design
decisions. All prompts are listed in `prompts.txt`, as the course rules require.
