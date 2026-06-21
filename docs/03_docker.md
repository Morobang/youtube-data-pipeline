# Section 3: Docker

## The problem Docker solves

Before we look at any Docker commands or files, let's understand *why* Docker exists. Because if you don't understand the problem, the solution won't make sense.

Imagine you write your pipeline on your Windows laptop. Everything works. You then try to run it on a colleague's Mac — it crashes. Wrong Python version. You fix that, try again — missing library. You fix that, try again — environment variable not found. This is the classic *"works on my machine"* problem, and it's been a nightmare in software for decades.

The root cause is that your code doesn't run in isolation — it depends on the operating system, the Python version, the installed libraries, the environment variables, and dozens of other things that differ between machines.

**Docker's solution** is to package your code *together with everything it needs* into one self-contained unit. That unit — called a **container** — runs identically on any machine that has Docker installed. Windows, Mac, Linux — it doesn't matter. The container brings its own environment with it.

For this project specifically, Docker lets you run **Airflow** and **PostgreSQL** on your Windows laptop without installing either of them directly. They run inside containers, completely isolated from the rest of your system.

---

## Three concepts you need to understand before anything else

### Image — the blueprint

An image is a snapshot of a complete environment, frozen in time. It contains the operating system layer, the runtime (like Python), the libraries, the config — everything. It's read-only, meaning it never changes once built.

You can think of an image the same way you think of a class in Python. A class *defines* what an object looks like, but isn't actually running. An image defines what a container looks like, but isn't running either.

Images are built from a file called a **Dockerfile**.

### Container — the running instance

A container is what you get when you *run* an image. Just like creating an object from a class in Python. The image is the blueprint; the container is the live thing.

You can run the same image multiple times and get multiple containers, each completely isolated from the others — separate filesystem, separate network, separate memory.

```
Image  →  docker run  →  Container (live, running, isolated)
```

Containers are **throwaway by default**. When you stop and delete a container, everything inside it is gone. That leads us to the third concept.

### Volume — saving data outside the container

Since containers are throwaway, you need a way to preserve data that should survive. A **volume** is a folder on your actual laptop that gets "mounted" into the container — meaning the container can read and write to it, but the data lives on your machine, not inside the container.

This is critical for PostgreSQL. Without a volume, every time you restart your Docker setup, your entire database would be wiped. With a volume, the database files live on your laptop and the container just reads them from there.

```
Your laptop: ./postgres_data/    ←→    /var/lib/postgresql/data  (inside container)
```

---

## The Dockerfile — building your own image

A Dockerfile is a plain text file with a list of instructions. Docker reads it top to bottom and executes each instruction to build your image. Every instruction adds a **layer** on top of the previous one.

Here's what each line does:

```dockerfile
FROM python:3.10-slim
```

You always start by extending an existing image. Here you're taking the official Python 3.10 image (which already has Python installed on a minimal Linux system) and building on top of it. You never start from absolute scratch.

```dockerfile
WORKDIR /app
```

This sets the working directory *inside the container* to `/app`. All the commands that follow will run from this directory, and files you copy in will land here. It's the equivalent of `cd /app` — but it also creates the folder if it doesn't exist.

```dockerfile
COPY requirements.txt .
```

This copies `requirements.txt` from your laptop into the container at `/app/requirements.txt`. The `.` means "the current directory inside the container", which is `/app` because of `WORKDIR`. Only `requirements.txt` is copied at this point — not your Python scripts.

```dockerfile
RUN pip install -r requirements.txt
```

`RUN` executes a command during the *build* phase — while the image is being created. This installs all your packages into the image so they're permanently baked in.

```dockerfile
COPY . .
```

Now copy everything else — your Python scripts and any other files — into the container.

```dockerfile
CMD ["python", "video_stats.py"]
```

`CMD` is the command that runs when the *container starts*. This is different from `RUN` — `RUN` happens once at build time, `CMD` happens every time someone runs the container.

**Why does the order matter?**

Notice that `requirements.txt` is copied and packages are installed *before* the rest of the code is copied. This is deliberate. Docker caches each layer — if nothing changed in that layer, it reuses the cache instead of re-running it. Since your Python scripts change far more often than your requirements, you want the slow `pip install` to be cached. If you changed the order and put `COPY . .` first, Docker would re-run pip install on every single code change.

---

## Building the image

Once you have a Dockerfile, you build the image with:

```bash
docker build -t youtube-pipeline .
```

- `build` — tells Docker to read the Dockerfile and create an image
- `-t youtube-pipeline` — gives the image a name (tag) so you can refer to it later
- `.` — "look for the Dockerfile in the current directory"

You only need to rebuild when the Dockerfile changes, or when `requirements.txt` changes. Changing your Python scripts alone doesn't require a rebuild — unless you're copying them with `COPY . .` at build time, in which case yes, you'd rebuild.

---

## Airflow is not one thing — it's four

Before we talk about Docker Compose, you need to understand why we need it. The reason is Airflow.

Most software you run is a single process. Airflow is not. To function, Airflow requires **four separate processes running at the same time**, all communicating with each other:

**The Webserver** is what you open in your browser at `http://localhost:8080`. It shows you the UI — your DAGs, their status, task logs. That's all it does.

**The Scheduler** is a background process that watches your DAG files and decides when to trigger runs. If your DAG is set to run daily at midnight, the Scheduler is what notices when midnight arrives and fires it off.

**The Worker** is what actually *executes* the tasks. When the Scheduler says "run this task now", it hands it to a Worker. The Worker is where your Python code actually runs.

**The Metadata Database** is a PostgreSQL database that ties it all together. The Scheduler writes "task triggered" to it. The Worker writes "task completed" to it. The Webserver reads from it to show you the UI. Without this database, none of the components know what the others are doing.

So to run Airflow, you need four processes plus a database running simultaneously and able to talk to each other. Doing that manually on your laptop would be a nightmare. That's exactly what `docker-compose.yaml` handles.

---

## docker-compose.yaml — running everything together

If a Dockerfile defines *one* container, `docker-compose.yaml` defines an entire **stack of containers** and wires them together. You run the whole thing with one command.

Here's an annotated version of what the file looks like for this project:

```yaml
services:

  postgres:
    image: postgres:14               # use the official Postgres 14 image
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data   # ← persist the database here


  airflow-webserver:
    image: apache/airflow:2.9.2
    depends_on:
      - postgres                     # ← don't start until postgres is up
    ports:
      - "8080:8080"                  # ← your browser → inside the container
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql://user:pass@postgres/airflow
    volumes:
      - ./dags:/opt/airflow/dags     # ← your DAG files, live-synced


  airflow-scheduler:
    image: apache/airflow:2.9.2
    depends_on:
      - postgres


volumes:
  postgres_data:
```

Let's break down the three things in here that people find confusing:

**`ports: "8080:8080"`**

The format is `your_laptop_port:container_port`. The container lives on its own private network and can't be reached from your browser by default. This line punches a hole — requests to port 8080 on your laptop get forwarded into the container's port 8080. That's how you open `localhost:8080` and see the Airflow UI.

**`volumes: ./dags:/opt/airflow/dags`**

The format is `your_laptop_path:container_path`. Your local `./dags` folder and the container's `/opt/airflow/dags` folder are now the same folder — a live, two-way sync. You create or edit a DAG file on your laptop, and Airflow sees it instantly without any rebuild or restart. This is one of the most powerful things about volumes.

**`depends_on: postgres`**

Controls startup order. The webserver cannot start without the metadata database, because it needs to connect to it immediately on boot. `depends_on` makes Docker wait for Postgres to be ready before starting the webserver.

---

## The `.env` file with Docker Compose

Your `.env` file works with Docker Compose the same way it works with Python — Docker Compose reads it automatically. Any variable in `.env` can be referenced in `docker-compose.yaml` using `${VARIABLE_NAME}`:

```yaml
environment:
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
```

This is important. Never hardcode passwords directly in `docker-compose.yaml`. That file gets committed to git. Your `.env` file does not — it's in `.gitignore`.

---

## `init-multiple-databases.sh` — setting up two databases

Here's something that trips people up. By default, a PostgreSQL container creates exactly one database. Your project needs two:

1. `airflow` — Airflow's internal database where it stores task history, connections, and variables
2. `youtube_db` — your actual data warehouse where the pipeline data lands

PostgreSQL has a built-in trick for this. If you place a shell script in `/docker-entrypoint-initdb.d/`, Postgres runs it automatically on **first boot**, before accepting connections. So you write a script that creates both databases, and mount it into that folder:

```yaml
volumes:
  - ./init-multiple-databases.sh:/docker-entrypoint-initdb.d/init.sh
```

One important thing to know: this only runs on the very first startup — when the data volume is being created for the first time. If you've already started Postgres and the data volume exists, it won't run again. To reset and re-run it, you'd need to delete the volume with `docker-compose down -v`.

---

## Docker commands you'll use every day

**Starting the stack:**

```bash
docker-compose up -d
```

The `-d` flag means "detached" — it runs everything in the background so your terminal stays free. Without `-d`, the logs from all containers would stream into your terminal and you couldn't type anything else.

**Stopping the stack:**

```bash
docker-compose down
```

This stops all the containers but keeps your volumes intact — your database data is preserved.

**If you want to wipe everything and start fresh:**

```bash
docker-compose down -v
```

The `-v` flag deletes volumes too. This destroys your database. Only use this when you genuinely want a clean slate.

**Seeing what's running:**

```bash
docker ps
```

Shows every running container — its name, the image it's running, how long it's been up, and which ports are exposed.

**Reading logs:**

```bash
docker-compose logs airflow-scheduler        # all logs so far
docker-compose logs -f airflow-scheduler     # follow in real time (Ctrl+C to stop)
```

Logs are how you debug. When something isn't working, this is the first place to look.

**Getting inside a container:**

```bash
docker exec -it <container_name> bash
```

This opens a terminal *inside* the running container. It's like SSH-ing into the container's Linux environment. Useful when you want to poke around, check file paths, or run a command manually.

**After you change the Dockerfile or `requirements.txt`:**

```bash
docker-compose up -d --build
```

The `--build` flag tells Docker to rebuild the images before starting. Without it, Docker reuses the old cached images even if you changed the Dockerfile.

---

## The one habit to build right now

Always run `docker-compose down` before you close your laptop.

If you just close the lid while containers are running, they get suspended mid-operation. PostgreSQL in particular can end up with partially-written files that get corrupted. When you open your laptop again and try to start Docker, Postgres might refuse to start because the data files are in a bad state.

One command before you stop working, every time.

---

## How it all fits together

Here's the full picture:

```
Your Laptop
│
├── Dockerfile               → builds your Python extraction image
├── docker-compose.yaml      → defines and wires together the full stack
├── .env                     → secrets, read by both Python and Docker Compose
├── dags/                    → mounted live into the Airflow containers
├── logs/                    → written by the Airflow worker
│
└── Docker Engine (running in the background on your machine)
    │
    ├── Container: postgres
    │     └── stores Airflow metadata + your YouTube data warehouse
    │
    ├── Container: airflow-webserver
    │     └── serves the UI at http://localhost:8080
    │
    ├── Container: airflow-scheduler
    │     └── watches DAGs, triggers runs on schedule
    │
    └── Container: airflow-worker
          └── actually runs the tasks in your DAGs
```

All containers share a private **virtual network** that Docker Compose creates automatically. This is why the Airflow webserver can connect to Postgres by just using the hostname `postgres` — Docker resolves that name to the right container automatically. You don't need to deal with IP addresses.

---

## Before you move on — check these off

- [ ] You understand *why* Docker exists (the "works on my machine" problem)
- [ ] You know the difference between an image and a container
- [ ] You know what a volume is and why Postgres needs one
- [ ] You can read every line of the Dockerfile and explain what it does
- [ ] You know why `COPY requirements.txt` comes before `COPY . .`
- [ ] You understand that Airflow is four separate processes, not one
- [ ] You can read the `docker-compose.yaml` and explain `ports`, `volumes`, and `depends_on`
- [ ] You understand why `init-multiple-databases.sh` exists and when it runs
- [ ] You can run `docker-compose up -d` and see all containers start
- [ ] You can open `http://localhost:8080` and log into the Airflow UI
- [ ] You know to run `docker-compose down` before closing your laptop
