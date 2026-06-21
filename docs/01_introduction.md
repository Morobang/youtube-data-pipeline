# Section 1: Introduction

## Before we touch any code, let's talk about what you're actually building

You're building a **data pipeline**. That phrase sounds fancy but it's just a program that automatically moves data from one place to another, cleans it up, and makes it ready to use.

Here's the specific thing you're building: every day, this pipeline wakes up, goes to YouTube, grabs all the stats from a channel (views, likes, comments, video titles, durations), and saves them into a database so you can query and analyse them.

Why would you want that? Because the YouTube website doesn't let you ask questions like *"which of this channel's videos had the fastest growth in the first 48 hours?"* or *"what's the average view count per month over the last year?"*. To answer those you need the raw numbers in a proper database that you can write SQL against. That's what this pipeline gives you.

---

## The pattern this project follows: ELT

You'll hear the term **ELT** throughout this course. It stands for **Extract → Load → Transform**, and it describes the order things happen:

1. **Extract** — pull the raw data out of YouTube using their API
2. **Load** — dump it straight into your database, untouched
3. **Transform** — clean it, reshape it, and aggregate it *inside* the database using SQL

The old pattern was ETL (transform *before* loading). ELT is the modern approach because storage is cheap — it's easier and safer to load everything raw first and transform it later, rather than trying to clean it before you even have it saved anywhere.

---

## The three data layers (Bronze → Silver → Gold)

Inside your database, data flows through three layers. This is called the **Medallion Architecture** and it's a standard pattern used in professional data engineering.

**Bronze — the raw landing zone**

When data comes out of the YouTube API, it goes into Bronze exactly as-is. No cleaning, no changes. If YouTube sends back a duration as `"PT5M30S"` instead of `330` seconds, that's what gets saved. Bronze is your safety net — no matter what breaks downstream, the original data is always here.

**Silver — the clean layer**

This is where you fix things. `"PT5M30S"` becomes `330`. Null values get handled. Duplicates get removed. Dates get proper types instead of strings. Silver data is trustworthy and consistent, but it's still close to the raw shape.

**Gold — the business-ready layer**

This is where you aggregate. Things like *"average views per video this month"*, *"top 10 videos by engagement rate"*, *"week-over-week growth"*. Gold is what a dashboard or report would read from. It's shaped for answering specific questions, not storing raw facts.

The reason for three separate layers is that if something goes wrong in your Silver transformation — say you wrote a bug that accidentally deleted rows — you can always re-run the transformation from Bronze. You never lose the source data.

---

## The tools and why each one exists

Rather than just listing the tools, let's talk about *why* each one is part of this project — because each one solves a specific problem.

**Python** is the glue. It's what you write the extraction scripts in and what Airflow DAGs are written in. Everything connects through Python.

**YouTube Data API** is the official, authorised way to request data from YouTube. You can't just scrape the website — YouTube blocks that. The API gives you structured data in a predictable format.

**requests** is a Python library that handles the actual HTTP communication with the API. When your Python script "calls" the YouTube API, it's using `requests` to send a network request and read the response.

**python-dotenv** loads your API key and other secrets from a `.env` file. This keeps sensitive values out of your code so you can safely share the code on GitHub without accidentally exposing your credentials.

**PostgreSQL** is your data warehouse — the database where Bronze, Silver, and Gold data lives. It's a proper SQL database, not just a file, which means you can query it, join tables, and run aggregations.

**Apache Airflow** is the scheduler. Without it, you'd have to manually run your Python script every day. Airflow lets you define a DAG (Directed Acyclic Graph — basically a workflow) and schedule it to run automatically. It also gives you a web UI to see what ran, what failed, and what's scheduled next.

**Docker** is what makes all of this runnable on your laptop. Airflow and PostgreSQL are complex pieces of software with their own dependencies. Instead of installing them directly (which would be painful and brittle), Docker lets you run them in isolated containers that work the same on any machine.

**SODA** is a data quality tool. After data moves from Silver to Gold, SODA runs checks — things like "does this column have any nulls?", "is the row count above 0?", "is this value within a valid range?". If a check fails, the pipeline stops before bad data reaches Gold.

**pytest** is for testing your Python code. You write tests that verify your functions do what they're supposed to, so you catch bugs before they cause problems in production.

**GitHub Actions** is CI/CD — Continuous Integration / Continuous Deployment. Every time you push code to GitHub, it automatically runs your tests. If the tests pass, it can also deploy automatically. This means you never accidentally push broken code without knowing.

---

## How the project is structured on disk

```
youtube-data-pipeline/
│
├── video_stats.py        ← the extraction script you've already built
├── docs/                 ← these files
├── images/               ← architecture diagram
├── data/                 ← where extracted JSON files are saved (ignored by git)
├── .env                  ← your secrets: API key, database passwords, etc.
├── .env.example          ← a template so others know what variables to fill in
├── .gitignore            ← tells git which files to never commit
└── requirements.txt      ← the Python packages this project depends on
```

As the course progresses you'll add more folders — `dags/` for Airflow, `sql/` for transformations, `tests/` for your test suite, and so on.

---

## What to read next

Work through the sections in order — each one builds on the last.

- [Section 2: Data Extraction Using the API](02_data_extraction.md) — a line-by-line walkthrough of `video_stats.py`
- [Section 3: Docker](03_docker.md) — how to containerise everything and get Airflow running
