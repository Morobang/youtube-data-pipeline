# Section 1: Introduction

## What Is This Project?

This is a **data pipeline** for YouTube. A pipeline is a series of steps that automatically moves data from one place to another and transforms it along the way.

This pipeline:
1. **Extracts** video data from a YouTube channel using the YouTube Data API
2. **Loads** that data into a PostgreSQL database
3. **Transforms** it through three layers (Bronze → Silver → Gold) so it's clean and ready for analysis

---

## Why Build This?

If you want to analyze a YouTube channel — things like "which videos get the most views?", "how fast is this channel growing?", or "what's the average engagement rate?" — you can't just look at the YouTube website. You need the raw numbers in a format you can query and analyze.

This pipeline automates that: every day it fetches fresh data from YouTube and makes it available in a database.

---

## The Three Data Layers

Data passes through three layers inside the database. This is a standard pattern in data engineering called the **Medallion Architecture**:

```
YouTube API
    |
    v
Bronze Layer  — raw data, exactly as it came from the API, nothing changed
    |
    v
Silver Layer  — cleaned, typed, and deduplicated (e.g. "PT5M30S" becomes 330 seconds)
    |
    v
Gold Layer    — aggregated, business-ready (e.g. "average views per month")
    |
    v
Analytics / Dashboards
```

The reason for three layers is safety: if something goes wrong in cleaning or aggregation, the original raw data is always preserved in Bronze.

---

## Tools Used

| Tool | What It Does in This Project |
|---|---|
| **Python** | The language everything is written in |
| **YouTube Data API** | The official way to request YouTube channel/video data programmatically |
| **requests** | A Python library that makes HTTP calls (how Python talks to APIs) |
| **python-dotenv** | Loads secret values (like your API key) from a `.env` file so they never end up in the code |
| **PostgreSQL** | The database where processed data is stored |
| **Apache Airflow** | Schedules the pipeline to run automatically (e.g. every day at midnight) |
| **Docker** | Packages Airflow and PostgreSQL into containers so the setup works the same on any machine |
| **SODA** | Runs data quality checks — makes sure the data looks correct before it reaches Gold |
| **pytest** | Automated tests that verify the code works correctly |
| **GitHub Actions** | Runs tests automatically whenever code is pushed to GitHub |

---

## How the Code Is Organised

```
youtube-data-pipeline/
│
├── video_stats.py        # The extraction script — pulls data from YouTube API
├── docs/                 # Documentation (you are here)
├── images/               # Architecture diagram
├── data/                 # Output JSON files (ignored by git)
├── .env                  # Your secrets — API key etc. (never committed)
├── .env.example          # A template showing which variables are needed
├── .gitignore            # Tells git which files to leave out of version control
└── requirements.txt      # List of Python packages this project needs
```

---

## Where to Go Next

- [Section 2: Data Extraction Using the API](02_data_extraction.md) — a detailed walkthrough of `video_stats.py`
