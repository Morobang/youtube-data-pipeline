# Section 4: Airflow

## The problem with just running a Python script

Right now, to get fresh YouTube data you have to:

1. Open your terminal
2. Activate your virtual environment
3. Run `python video_stats.py`
4. Hope it doesn't crash
5. Remember to do this again tomorrow

That's fine for experimenting. It's not fine for a real data pipeline. What happens when you forget? What happens when it crashes at 2am and nobody notices? What if one step depends on a previous step finishing successfully first? What if you want to run it every day at 6am automatically?

**Airflow solves all of this.** It's a tool for defining, scheduling, and monitoring workflows. Instead of running scripts manually, you define your pipeline in code and Airflow handles when it runs, retries it if it fails, and gives you a web UI to see everything that's happened.

---

## The core concept: the DAG

Everything in Airflow is built around a **DAG** — a Directed Acyclic Graph.

That's a mouthful. Let's break it down:

- **Graph** — a collection of nodes connected by edges (arrows)
- **Directed** — the arrows only go one way (task A → task B, not back and forth)
- **Acyclic** — there are no loops (you can't have task A depend on task B which depends on task A — that would run forever)

In practice, a DAG is just your **pipeline written in Python**. Each step in your pipeline is a **task**, and the arrows between them define what order things run in.

For your YouTube pipeline, the DAG looks like this:

```
get_playlist_id  →  get_video_ids  →  extract_video_data  →  save_to_json
```

Each box is a task. The arrows mean "don't start this until the previous one finishes successfully."

You define DAGs in Python files and drop them in the `dags/` folder. Airflow's Scheduler watches that folder and picks them up automatically.

---

## How Airflow actually executes your DAG

Here's the sequence of events when a DAG run is triggered:

1. The **Scheduler** notices it's time to run (either by schedule or manual trigger)
2. The Scheduler puts a message into **Redis** — "task X is ready to run"
3. The **Worker** picks up that message from Redis and executes the task
4. The Worker writes the result back to the **Metadata Database** (Postgres) — "task X finished successfully at 06:02:14"
5. The Scheduler sees the result, decides what's next, and puts the next task into Redis
6. The **Webserver** reads from the Metadata Database to show you everything in the UI

This is why your `docker-compose.yaml` has so many services — they're all playing a specific role in this process. Redis is the message queue that the Scheduler and Workers use to pass tasks back and forth. That's the **CeleryExecutor** setup you have.

---

## Your `docker-compose.yaml` — what's actually in there

Looking at your setup, a few things are worth understanding:

**`x-airflow-common` and `&airflow-common`** — these are YAML anchors. Because the Webserver, Scheduler, and Worker all need the same environment variables and volume mounts, instead of copy-pasting that block three times, you define it once under `x-airflow-common` and reference it with `<<: *airflow-common`. It's just a way to avoid repeating yourself in YAML.

**`airflow-init`** — this is a one-off container that runs before everything else. It creates the Airflow database tables, creates your admin user, and sets up folder permissions. It runs once, then exits. The other services have `condition: service_completed_successfully` in their `depends_on` — meaning they wait for `airflow-init` to finish before starting.

**`AIRFLOW_VAR_API_KEY` and `AIRFLOW_VAR_CHANNEL_HANDLE`** — these are Airflow Variables. Instead of your DAG reading from `.env` with `os.getenv()`, it reads from Airflow's own variable store. This is important — it means you access them differently in your DAG code (more on this below).

**`AIRFLOW_CONN_POSTGRES_DB_YT_ELT`** — this is an Airflow Connection. Airflow has a built-in system for storing database credentials so you don't hardcode them anywhere. The connection ID `postgres_db_yt_elt` is how you'll reference this connection in your DAG.

---

## What is a Connection?

A Connection is Airflow's way of storing credentials for external systems — databases, APIs, cloud services. Instead of putting a database password in your Python code, you store it in Airflow and refer to it by a name (the Connection ID).

In your `docker-compose.yaml`:

```yaml
AIRFLOW_CONN_POSTGRES_DB_YT_ELT: 'postgresql://username:password@host:port/database'
```

Setting an environment variable that starts with `AIRFLOW_CONN_` automatically registers it as a Connection inside Airflow. The part after `AIRFLOW_CONN_` becomes the connection ID — but Airflow lowercases it, so `POSTGRES_DB_YT_ELT` becomes `postgres_db_yt_elt`.

In your DAG code you'll use this ID to get a database connection without ever typing a password.

---

## What is a Variable?

A Variable is Airflow's equivalent of an environment variable — a key-value pair you can access from any DAG. Just like Connections, you can register them via environment variables:

```yaml
AIRFLOW_VAR_API_KEY: ${API_KEY}
AIRFLOW_VAR_CHANNEL_HANDLE: ${CHANNEL_HANDLE}
```

In your DAG code, you read them like this:

```python
from airflow.models import Variable

api_key = Variable.get("API_KEY")
channel_handle = Variable.get("CHANNEL_HANDLE")
```

Notice: the environment variable is `AIRFLOW_VAR_API_KEY`, but you access it as `Variable.get("API_KEY")` — Airflow strips the `AIRFLOW_VAR_` prefix automatically.

---

## Refactoring `video_stats.py` for Airflow

Before Airflow, your `video_stats.py` was a standalone script — you ran it directly with `python video_stats.py`. For Airflow, you need to wrap your functions in Airflow **tasks** inside a **DAG**.

The modern way to do this is with the **TaskFlow API** — you use the `@task` decorator on your functions, and Airflow handles the rest.

Here's what the refactored DAG file looks like:

```python
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
import requests
import json
import os

@dag(
    schedule="0 6 * * *",        # run every day at 6am
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["youtube"],
)
def youtube_pipeline():

    @task()
    def get_playlist_id():
        api_key = Variable.get("API_KEY")
        channel_handle = Variable.get("CHANNEL_HANDLE")

        url = (
            f"https://youtube.googleapis.com/youtube/v3/channels"
            f"?part=contentDetails&forHandle={channel_handle}&key={api_key}"
        )
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    @task()
    def get_video_ids(playlist_id):
        api_key = Variable.get("API_KEY")
        max_results = 50
        video_ids = []
        page_token = None
        base_url = (
            f"https://youtube.googleapis.com/youtube/v3/playlistItems"
            f"?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={api_key}"
        )
        while True:
            url = base_url + (f"&pageToken={page_token}" if page_token else "")
            data = requests.get(url).json()
            for item in data.get('items', []):
                video_id = item['contentDetails'].get('videoId')
                if video_id:
                    video_ids.append(video_id)
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        return video_ids

    @task()
    def extract_video_data(video_ids):
        # ... same logic as before ...
        return extracted_data

    @task()
    def save_to_json(data):
        # ... same logic as before ...
        pass

    # Wire up the tasks — this defines the DAG structure
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)

youtube_pipeline()
```

A few things to notice:

**`@dag`** turns a function into a DAG definition. The function contains all the tasks.

**`@task`** turns a regular Python function into an Airflow task. When one task returns a value and you pass it to another task, Airflow automatically knows there's a dependency between them — `get_playlist_id` must finish before `get_video_ids` can start.

**`Variable.get()`** replaces `os.getenv()` because inside a DAG, credentials come from Airflow's variable store, not from a `.env` file on disk.

**`schedule="0 6 * * *"`** is a cron expression. Cron is a standard format for scheduling. `0 6 * * *` means "at minute 0 of hour 6, every day, every month, every day of week" — i.e. 6:00am daily. You'll learn cron syntax as you go — for now just know it's the standard way to express schedules.

**`catchup=False`** — by default, if you set a `start_date` in the past, Airflow will try to run the DAG for every missed day since then. `catchup=False` turns that off. You almost always want this off.

---

## The Airflow UI

Once your stack is running with `docker-compose up -d`, open your browser and go to `http://localhost:8080`. Log in with the username and password you set in your `.env` file (`AIRFLOW_WWW_USER_USERNAME` and `AIRFLOW_WWW_USER_PASSWORD`).

**The DAGs page** — this is the home screen. Every DAG file in your `dags/` folder appears here. New DAGs are paused by default (you can see `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'` in your `docker-compose.yaml`). You have to manually toggle them on the first time.

**Triggering a DAG manually** — click the play button (▶) next to a DAG to trigger it immediately, without waiting for the schedule. You'll use this constantly while developing.

**The Grid view** — when you click on a DAG, the Grid view shows every run and the status of each task within it. Green = success, Red = failed, Yellow = running.

**Task logs** — if a task fails, click on the red square, then click "Log". This shows you the full output from that task — print statements, error tracebacks, everything. This is where you debug.

---

## What happens when a task fails

This is one of Airflow's most valuable features. If `extract_video_data` fails midway through, Airflow doesn't just give up. You can configure it to retry automatically:

```python
@task(retries=3, retry_delay=timedelta(minutes=5))
def extract_video_data(video_ids):
    ...
```

If the task fails, Airflow waits 5 minutes and tries again, up to 3 times. You get an alert (if you configure one), and you can see exactly which run failed and why in the UI. Compare this to a plain script — it fails, nothing happens, and you don't find out until you notice the data is missing.

---

## The `dags/` folder and how Airflow picks up your DAG

Airflow's Scheduler continuously scans the `dags/` folder for Python files. When it finds a new or changed file, it imports it and registers any DAGs defined inside.

In your `docker-compose.yaml`, the `dags/` folder on your laptop is mounted into every Airflow container:

```yaml
volumes:
  - ./dags:/opt/airflow/dags
```

This means you create and edit DAG files on your laptop like any other Python file. The Scheduler picks them up automatically — no restart needed.

One important thing: **the DAG file is imported by the Scheduler, not executed**. Your task functions are only called when a DAG run actually triggers. Keep this in mind — any code at the module level (outside functions) runs every time the Scheduler imports the file, which happens frequently.

---

## Before you move on — check these off

- [ ] You understand why Airflow exists (automation, monitoring, retries, dependencies)
- [ ] You know what a DAG is and what Directed Acyclic Graph actually means
- [ ] You understand the role of each component: Scheduler, Worker, Webserver, Metadata DB, Redis
- [ ] You know the difference between an Airflow Variable and an Airflow Connection
- [ ] You understand why DAGs use `Variable.get()` instead of `os.getenv()`
- [ ] You can read a `@dag` and `@task` decorated function and understand what it defines
- [ ] You know what a cron expression is and what `0 6 * * *` means
- [ ] You know what `catchup=False` does and why you want it
- [ ] You can log into the Airflow UI, find your DAG, toggle it on, and trigger it manually
- [ ] You know how to find task logs when something fails
- [ ] You understand that DAG files go in `dags/` and Airflow picks them up automatically
