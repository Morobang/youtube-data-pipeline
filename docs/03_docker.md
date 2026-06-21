# Section 3: Docker

## Why Docker?

Picture this: you write code on your Windows laptop. It works perfectly. You send it to a teammate on Mac — it crashes. You deploy it to a Linux server — it crashes again. The error is always something like "wrong Python version", "missing library", or "environment variable not set".

**Docker solves this.** It packages your code *together with everything it needs to run* — the OS layer, Python version, libraries, config — into one self-contained unit. That unit runs identically on any machine.

In this project, Docker is what lets you run **Airflow** and **PostgreSQL** on your Windows laptop without installing them directly — they run inside containers.

---

## The Three Core Concepts

### 1. Image — the blueprint

An image is a read-only snapshot of an environment. Think of it like a recipe or a class in Python — it defines what something looks like but isn't running yet.

```
Python 3.10 image  →  add your requirements.txt  →  your custom image
```

Images are built from a file called a **Dockerfile**.

### 2. Container — the running instance

A container is what you get when you *run* an image. Like creating an object from a class in Python. You can run the same image many times to get many containers, each isolated from the others.

```
Image  →  docker run  →  Container (running, has its own filesystem, network, memory)
```

### 3. Volume — persistent storage

Containers are **throwaway by default** — when you stop and remove one, any data it wrote is gone. Volumes are folders on your actual machine that get mounted *into* the container, so data survives restarts.

This is critical for PostgreSQL: without a volume, your entire database would be wiped every time you restart Docker.

---

## The Dockerfile

A Dockerfile is a plain text file of instructions that tells Docker how to build your image, line by line. Every line creates a **layer**.

```dockerfile
FROM python:3.10-slim
```
Start from an existing base image — Python 3.10 on a minimal Linux install. You never build from absolute scratch; you always extend something that already exists.

```dockerfile
WORKDIR /app
```
Set `/app` as the working directory inside the container. All following commands run from here, and your files will land here.

```dockerfile
COPY requirements.txt .
```
Copy `requirements.txt` from your laptop into the container's `/app` folder. The dot means "current directory" (which is `/app` due to `WORKDIR`).

```dockerfile
RUN pip install -r requirements.txt
```
`RUN` executes a shell command **during the build phase** — this installs all your Python packages into the image so they're permanently baked in.

```dockerfile
COPY . .
```
Copy everything else (your Python scripts) into the container.

```dockerfile
CMD ["python", "video_stats.py"]
```
`CMD` is the default command that runs **when the container starts**. This is the run phase, not the build phase.

### Key distinction

| Instruction | When it runs | Purpose |
|---|---|---|
| `RUN` | Once, at build time | Install packages, set up the environment |
| `CMD` | Every time the container starts | Start your actual application |

### Layer caching

Docker caches each layer. If you change only your Python script and rebuild, Docker reuses the cached pip install layer and only re-runs the `COPY . .` step. This is why `COPY requirements.txt` comes *before* `COPY . .` — you want the slow pip install to be cached unless the requirements actually change.

---

## Building the Image

```bash
docker build -t youtube-pipeline .
```

- `build` — reads the Dockerfile and creates an image
- `-t youtube-pipeline` — tags (names) the image so you can refer to it later
- `.` — look for the Dockerfile in the current directory

---

## Airflow Architecture

Airflow is not a single program — it is four separate processes that must all run at the same time and communicate with each other:

| Component | What it does |
|---|---|
| **Webserver** | The browser UI you open at `http://localhost:8080` |
| **Scheduler** | Watches your DAGs and decides when to trigger them |
| **Worker** | Actually executes the tasks inside a triggered DAG |
| **Metadata Database** | A PostgreSQL database where Airflow stores DAG run history and task state |

Running all four manually on your laptop would be painful. Docker Compose handles all of them together with a single command.

---

## Airflow Directories

When Airflow runs inside Docker, it expects certain folders to exist and be mounted from your laptop:

| Folder | Purpose |
|---|---|
| `dags/` | Your DAG Python files — Airflow watches this folder for pipelines |
| `logs/` | Task execution logs written by the worker |
| `plugins/` | Custom Airflow operators or hooks (optional) |

These are mounted as volumes so you can edit DAGs on your laptop and Airflow sees the changes instantly without rebuilding anything.

---

## The `.env` File with Docker

Your `.env` file works with Docker Compose the same way it works with Python — Docker Compose reads it automatically and makes every variable available inside `docker-compose.yaml` using `${VARIABLE_NAME}`:

```yaml
environment:
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
```

This keeps secrets out of both your Python code and your Docker config. Never hardcode passwords in `docker-compose.yaml`.

---

## docker-compose.yaml

If a Dockerfile defines *one* container, `docker-compose.yaml` defines **multiple containers and how they connect**. Your pipeline needs:

1. Airflow Webserver
2. Airflow Scheduler
3. Airflow Worker
4. Airflow Metadata Database (PostgreSQL)
5. Your YouTube data warehouse (a second PostgreSQL database)

Here is an annotated example of what the file looks like:

```yaml
services:

  postgres:                              # your data warehouse container
    image: postgres:14                   # use the official Postgres image, no Dockerfile needed
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data   # persist the DB on your machine

  airflow-webserver:
    image: apache/airflow:2.9.2
    depends_on:
      - postgres                         # don't start until postgres is running
    ports:
      - "8080:8080"                      # map container port 8080 to your laptop port 8080
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql://user:pass@postgres/airflow
    volumes:
      - ./dags:/opt/airflow/dags         # your DAG files on laptop → inside container

  airflow-scheduler:
    image: apache/airflow:2.9.2
    depends_on:
      - postgres

volumes:
  postgres_data:                         # declare the named volume so Docker manages it
```

### Three concepts to understand

**`ports: "8080:8080"`**
Format is `host_port:container_port`. The container lives on its own internal network — this line punches a hole so your browser on your laptop can reach it.

**`volumes: ./dags:/opt/airflow/dags`**
Format is `host_path:container_path`. Your local `./dags` folder is mounted inside the container at `/opt/airflow/dags`. Edit a file on your laptop → Airflow sees it immediately.

**`depends_on`**
Controls startup order. The webserver won't start until Postgres is ready, because Airflow needs its metadata database to exist before it can boot.

---

## init-multiple-databases.sh

By default, a PostgreSQL container creates one database. Your project needs two:
1. `airflow` — for Airflow's internal metadata (task history, connections, variables)
2. `youtube_db` — your actual YouTube data warehouse

This shell script runs automatically when the PostgreSQL container first starts and creates both databases. You mount it into a special folder that Postgres watches on boot:

```yaml
volumes:
  - ./init-multiple-databases.sh:/docker-entrypoint-initdb.d/init.sh
```

Postgres automatically runs any `.sh` or `.sql` file it finds in `/docker-entrypoint-initdb.d/` on first boot. This only runs once — if the data volume already exists, Postgres skips it.

---

## Essential Docker Commands

### Starting and stopping

```bash
# Start everything defined in docker-compose.yaml (runs in background)
docker-compose up -d

# Stop all containers but keep your volumes and data intact
docker-compose down

# Stop AND delete volumes — this wipes your database completely
docker-compose down -v
```

> Always run `docker-compose down` before shutting your laptop. If you just close the lid, containers freeze mid-state and can corrupt the Postgres data files.

### Checking status and logs

```bash
# See which containers are currently running
docker ps

# See logs for one service
docker-compose logs airflow-webserver

# Follow logs in real time (Ctrl+C to stop)
docker-compose logs -f airflow-scheduler
```

### Entering a container

```bash
# Open a bash shell inside a running container
docker exec -it <container_name> bash

# Example: get a psql prompt inside the postgres container
docker exec -it postgres psql -U admin -d youtube_db
```

### Rebuilding after changes

```bash
# Rebuild images if you changed the Dockerfile or requirements.txt
docker-compose up -d --build
```

---

## Mental Model

```
Your Laptop
│
├── Dockerfile                  defines your custom Python extraction image
├── docker-compose.yaml         defines the full stack (all containers)
├── .env                        secrets — read by Python AND Docker Compose
├── dags/                       mounted into the Airflow containers
├── logs/                       written by the Airflow worker
│
└── Docker Engine (running silently in the background)
    │
    ├── Container: postgres            ← data warehouse + Airflow metadata DB
    ├── Container: airflow-webserver   ← UI at http://localhost:8080
    ├── Container: airflow-scheduler   ← triggers DAG runs on schedule
    └── Container: airflow-worker      ← runs the actual tasks
```

All containers share a **virtual network** that Compose creates automatically. They talk to each other by service name — for example, the Airflow webserver connects to `postgres:5432` using the service name `postgres`, not an IP address.

---

## Section 3 Checklist

- [ ] Understand why Docker exists and what problem it solves
- [ ] Know the difference between an image and a container
- [ ] Know what a volume is and why it matters for Postgres
- [ ] Read and understand every line of the Dockerfile
- [ ] Run `docker build` to build the image
- [ ] Understand the four Airflow components and why each exists
- [ ] Read and understand the `docker-compose.yaml`
- [ ] Run `docker-compose up -d` and verify all containers start
- [ ] Open `http://localhost:8080` and log into the Airflow UI
- [ ] Practice the core docker commands (`ps`, `logs`, `exec`, `down`)
- [ ] Run `docker-compose down` before closing your laptop
