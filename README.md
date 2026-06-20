# youtube-data-pipeline

An end-to-end ELT pipeline that extracts YouTube channel and video data via the YouTube Data API, loads it into a PostgreSQL data warehouse, and transforms it through Bronze → Silver → Gold layers. Orchestrated with Apache Airflow, containerized with Docker, validated with SODA data quality checks, and deployed with GitHub Actions CI/CD.

![Project Architecture](images/project_architecture.png)

---

## Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Extraction scripts, ELT logic |
| YouTube Data API | Data source |
| PostgreSQL | Data warehouse |
| Apache Airflow | Pipeline orchestration |
| Docker | Containerization |
| SODA | Data quality checks |
| pytest | Unit, integration & E2E testing |
| GitHub Actions | CI/CD automation |

---

## Architecture

Data flows through three warehouse layers:

- **Bronze** — raw data landed as-is from the YouTube API, no transformations
- **Silver** — cleaned, typed, and deduplicated data
- **Gold** — aggregated, business-ready data consumed by downstream analytics or ML models

SODA quality checks run between Silver and Gold to ensure only trustworthy data reaches the serving layer. Airflow DAGs orchestrate every step on a schedule. GitHub Actions runs the test suite and triggers deployments on every push to `main`.

---

## Project Structure

```
youtube-data-pipeline/
│
├── dags/                        # Airflow DAGs
├── extraction/                  # YouTube API extraction scripts
├── sql/
│   ├── bronze/                  # Raw load SQL
│   ├── silver/                  # Cleaning & transformation SQL
│   └── gold/                    # Aggregation & serving layer SQL
├── tests/
│   ├── unit/                    # Unit tests
│   ├── integration/             # Integration tests
│   └── e2e/                     # End-to-end pipeline tests
├── soda/                        # SODA data quality check configs (.yml)
├── images/                      # Architecture diagrams
├── .github/
│   └── workflows/               # GitHub Actions CI/CD pipelines
├── docker-compose.yml           # Spins up Airflow + PostgreSQL
├── requirements.txt
├── .env.example                 # Environment variable template
└── README.md
```

---

## Getting Started

### Prerequisites

- Python 3.11
- Docker Desktop (with WSL2 enabled on Windows)
- Git

### 1. Clone the repo

```bash
git clone https://github.com/Morobang/youtube-data-pipeline.git
cd youtube-data-pipeline
```

### 2. Set up virtual environment

```bash
py -3.11 -m venv .venv
.venv\Scripts\activate       # Windows PowerShell
pip install -r requirements.txt
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Fill in your values in `.env` — YouTube API key, Postgres credentials, Airflow config.

### 4. Spin up the stack

```bash
docker-compose up -d
```

This starts PostgreSQL and Airflow. Access the Airflow UI at `http://localhost:8080`.

### 5. Run tests

```bash
pytest tests/
```

---

## Data Quality

SODA checks are defined in `/soda` and run as part of the Airflow DAG after Silver layer loading. Checks include:

- No nulls on key columns
- Row count thresholds after each load
- Uniqueness constraints on primary keys
- Value range validations on numeric fields

If any check fails, the DAG halts before data reaches the Gold layer.

---

## CI/CD

GitHub Actions runs on every push to `main`:

1. Installs dependencies
2. Runs the full pytest suite (unit → integration → E2E)
3. Runs SODA checks against a test database
4. Deploys if all checks pass

---

## Environment Variables

Copy `.env.example` to `.env` and fill in:

```
YOUTUBE_API_KEY=
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_DB=
POSTGRES_USER=
POSTGRES_PASSWORD=
AIRFLOW_UID=
```

Never commit your `.env` file — it is already in `.gitignore`.

---

## Author

**Morobang Tshigidimisa**
[GitHub](https://github.com/Morobang) · [Portfolio](https://morobangtshigidimisa.vercel.app)