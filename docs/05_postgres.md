# Section 5: PostgreSQL Data Warehouse

## What is a data warehouse and how is it different from a regular database?

You've probably heard of databases before. A regular database — like the one behind a website — is built for **transactions**: creating a user account, placing an order, updating a password. Lots of small, fast reads and writes happening constantly.

A **data warehouse** is built for a completely different purpose: **analysis**. Instead of "update this one row", you're asking questions like "give me the total views per month across all 300 videos for the last 2 years." That's a query that touches millions of rows and needs to be fast at reading, not writing.

PostgreSQL can do both. In this project you're using it as a data warehouse — a place where your YouTube data lands, gets cleaned, and gets aggregated so you can query it for insights.

---

## Schemas — how Bronze, Silver, and Gold live in Postgres

In PostgreSQL, a **schema** is like a folder inside your database. It groups related tables together. Your project uses three schemas, one per layer:

```
youtube_db  (the database)
│
├── bronze    (schema)  ← raw data exactly as it came from the API
│   └── video_stats
│
├── silver    (schema)  ← cleaned and typed data
│   └── video_stats
│
└── gold      (schema)  ← aggregated, business-ready data
    └── video_stats_summary
```

The same data exists in three versions. Each version is progressively more refined and trustworthy. If something breaks in the Silver transformation, you always have Bronze to re-run from.

To create a schema in PostgreSQL:

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

---

## Creating tables

Before you can load any data, you need to define the table structure. This tells Postgres what columns exist and what type of data each one holds.

Here's the Bronze table — it mirrors the JSON output from `video_stats.py` exactly:

```sql
CREATE TABLE IF NOT EXISTS bronze.video_stats (
    video_id        VARCHAR(20),
    title           TEXT,
    published_at    VARCHAR(30),    -- stored as a string in Bronze, typed in Silver
    duration        VARCHAR(20),    -- stored as "PT5M30S" in Bronze, seconds in Silver
    view_count      VARCHAR(20),    -- stored as a string in Bronze, bigint in Silver
    like_count      VARCHAR(20),
    comment_count   VARCHAR(20),
    ingested_at     TIMESTAMP DEFAULT NOW()   -- when the row was loaded
);
```

Notice that in Bronze, everything is stored as text — even numbers. That's intentional. The YouTube API returns `view_count` as a string like `"1400000000"`. In Bronze you don't change anything. You convert it to a proper `BIGINT` in Silver.

The Silver table uses proper types:

```sql
CREATE TABLE IF NOT EXISTS silver.video_stats (
    video_id        VARCHAR(20) PRIMARY KEY,
    title           TEXT,
    published_at    TIMESTAMP,      -- proper timestamp now
    duration_secs   INTEGER,        -- "PT5M30S" converted to 330 seconds
    view_count      BIGINT,         -- proper number now
    like_count      BIGINT,
    comment_count   BIGINT,
    updated_at      TIMESTAMP DEFAULT NOW()
);
```

---

## Connecting Airflow to PostgreSQL

Your Airflow setup already has the database connection defined in `docker-compose.yaml`:

```yaml
AIRFLOW_CONN_POSTGRES_DB_YT_ELT: 'postgresql://username:password@host:port/youtube_db'
```

The connection ID is `postgres_db_yt_elt` (Airflow lowercases the part after `AIRFLOW_CONN_`).

In your DAG code, you use this connection with the `PostgresHook` — Airflow's built-in way to talk to Postgres:

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id='postgres_db_yt_elt')
conn = hook.get_conn()
cursor = conn.cursor()
```

You never write the password in your code. Airflow looks up the connection by ID and handles the credentials for you.

---

## Loading JSON data into Bronze

After `save_to_json()` writes the JSON file, the next task reads that file and inserts every record into `bronze.video_stats`.

```python
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_to_bronze():
    hook = PostgresHook(postgres_conn_id='postgres_db_yt_elt')
    conn = hook.get_conn()
    cursor = conn.cursor()

    with open('/opt/airflow/data/youtube_data_2026-06-21.json') as f:
        records = json.load(f)

    for record in records:
        cursor.execute("""
            INSERT INTO bronze.video_stats
                (video_id, title, published_at, duration, view_count, like_count, comment_count)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s)
        """, (
            record['video_id'],
            record['title'],
            record['published_at'],
            record['duration'],
            record['view_count'],
            record['like_count'],
            record['comment_count'],
        ))

    conn.commit()
    cursor.close()
    conn.close()
```

The `%s` placeholders are important — never build SQL strings with f-strings using user data, because that opens you up to SQL injection. Always use parameterised queries like this.

---

## Inserts, Updates, and Deletes — the Upsert pattern

Here's a problem you'll hit quickly: if you run the pipeline today and again tomorrow, you'll try to insert the same videos again. You don't want duplicates.

The solution is an **upsert** — "insert if new, update if already exists." In PostgreSQL this is done with `ON CONFLICT`:

```sql
INSERT INTO silver.video_stats
    (video_id, title, published_at, duration_secs, view_count, like_count, comment_count)
VALUES
    (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (video_id)
DO UPDATE SET
    view_count    = EXCLUDED.view_count,
    like_count    = EXCLUDED.like_count,
    comment_count = EXCLUDED.comment_count,
    updated_at    = NOW();
```

Breaking this down:
- `ON CONFLICT (video_id)` — if a row with this `video_id` already exists (it's the primary key)...
- `DO UPDATE SET` — ...then update these columns instead of inserting a new row
- `EXCLUDED.view_count` — `EXCLUDED` refers to the new values you were trying to insert

This way, existing videos get their stats refreshed (views go up over time) and new videos get added. No duplicates, no manual checking.

---

## Transformations — Bronze to Silver

The transformation step is just SQL. You read from Bronze, clean and type the data, and write to Silver.

```sql
INSERT INTO silver.video_stats
    (video_id, title, published_at, duration_secs, view_count, like_count, comment_count)
SELECT
    video_id,
    title,
    published_at::TIMESTAMP,                          -- cast string to timestamp
    EXTRACT(EPOCH FROM duration::INTERVAL)::INTEGER,  -- "PT5M30S" → 330 seconds
    view_count::BIGINT,                               -- cast string to number
    like_count::BIGINT,
    comment_count::BIGINT
FROM bronze.video_stats
ON CONFLICT (video_id)
DO UPDATE SET
    view_count    = EXCLUDED.view_count,
    like_count    = EXCLUDED.like_count,
    comment_count = EXCLUDED.comment_count,
    updated_at    = NOW();
```

The key transformations:
- `::TIMESTAMP` casts a string like `"2024-01-15T10:30:00Z"` into a real timestamp
- `EXTRACT(EPOCH FROM duration::INTERVAL)` converts ISO 8601 duration `"PT5M30S"` into seconds (`330`). PostgreSQL understands the `INTERVAL` type natively.
- `::BIGINT` converts string numbers to actual integers you can do maths on

---

## Transformations — Silver to Gold

Gold is where you aggregate. This is what you'd actually query for a dashboard or report:

```sql
CREATE TABLE IF NOT EXISTS gold.video_stats_summary AS
SELECT
    DATE_TRUNC('month', published_at)   AS month,
    COUNT(*)                            AS video_count,
    SUM(view_count)                     AS total_views,
    AVG(view_count)                     AS avg_views,
    MAX(view_count)                     AS max_views,
    SUM(like_count)                     AS total_likes,
    SUM(comment_count)                  AS total_comments
FROM silver.video_stats
GROUP BY DATE_TRUNC('month', published_at)
ORDER BY month;
```

This gives you a monthly summary: how many videos were published, total and average views, etc. This is the kind of table a dashboard would read from directly.

---

## The Data Warehouse DAG

Just like the extraction script became a DAG, the loading and transformation steps become their own DAG. This keeps concerns separate — one DAG extracts and saves JSON, another DAG loads and transforms the data.

```python
with DAG(
    dag_id='load_and_transform',
    schedule='30 14 * * *',   # 30 minutes after the extraction DAG
    catchup=False,
    ...
) as dag:

    load_bronze   = load_to_bronze()
    transform_silver = bronze_to_silver()
    transform_gold   = silver_to_gold()

    load_bronze >> transform_silver >> transform_gold
```

The `>>` operator here explicitly sets the order: Bronze loads first, then Silver transformation runs, then Gold. You can't transform data that hasn't been loaded yet.

---

## DBeaver — a GUI for your database

DBeaver is a desktop app that lets you connect to PostgreSQL and browse your data visually — like having a spreadsheet view of your tables, plus a SQL editor.

**To connect DBeaver to your Postgres container:**

1. Open DBeaver → New Database Connection → PostgreSQL
2. Fill in:
   - **Host:** `localhost`
   - **Port:** `5432`
   - **Database:** `youtube_db`
   - **Username/Password:** from your `.env` file
3. Click Test Connection — if Docker is running it should connect

**What you'll use it for:**
- Checking that data actually landed in Bronze after a DAG run
- Running SQL queries manually to explore the data
- Verifying that Silver transformations worked correctly
- Building and testing SQL before putting it in a DAG

Think of DBeaver as your window into the database. Airflow runs the pipeline, DBeaver lets you look inside and verify everything is correct.

---

## Before you move on — check these off

- [ ] You understand the difference between a database and a data warehouse
- [ ] You know what a schema is and why you have three (bronze, silver, gold)
- [ ] You can write a `CREATE TABLE` statement with appropriate column types
- [ ] You understand why Bronze stores everything as strings and Silver uses proper types
- [ ] You know how to use `PostgresHook` to connect to Postgres from a DAG
- [ ] You understand what an upsert is and how `ON CONFLICT DO UPDATE` works
- [ ] You understand the Bronze → Silver transformation SQL (casting types)
- [ ] You understand the Silver → Gold transformation SQL (aggregating)
- [ ] You can connect DBeaver to your running Postgres container
- [ ] You can query your tables in DBeaver and verify the data looks correct
