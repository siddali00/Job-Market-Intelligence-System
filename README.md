# Job Market Intelligence System

Job listings pipeline with a FastAPI backend and React dashboard. Technical details live in [docs/DATA_PIPELINE.md](docs/DATA_PIPELINE.md).


## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Compose), **or** Python 3.11+ and Node 18+ for the optional local run below  
- In the **repository root**, copy the env file and add secrets:

```powershell
Copy-Item .env.example .env -ErrorAction SilentlyContinue
```

Edit `.env`: set variables as set in env example.

---

## Run everything with Docker (recommended)

From the **repository root**:

```powershell
docker compose up --build -d
```

Logs:

```powershell
docker compose logs -f
```

Stop:

```powershell
docker compose down
```

**URLs**

| Service    | URL                         |
|-----------|-----------------------------|
| Dashboard | http://localhost:3000       |
| API docs  | http://localhost:8000/docs  |
| Prefect UI | http://localhost:4200     |

### Run the pipeline (one command)

With the stack up (`docker compose up …`), run ingestion + transforms **once**:

```powershell
docker compose --profile pipeline run --rm pipeline --once
```

Other modes:

```powershell
docker compose --profile pipeline run --rm pipeline --seed       # Kaggle seed only (Bronze)
docker compose --profile pipeline run --rm pipeline --schedule # register schedule only
docker compose --profile pipeline run --rm pipeline            # default pipeline.py behavior
```

If `docker compose` is not found, try `docker-compose` instead.

---

## Run without Docker (optional)

**One-time setup**

```powershell
Copy-Item .env.example .env
python -m venv env
.\env\Scripts\activate
pip install -r requirements.txt
cd serving\frontend
npm install
cd ..\..
```

You need PostgreSQL reachable at the URL in `.env`. Then open **four terminals** from the repo root (venv activated in each Python terminal):

```powershell
# Terminal 1 — API
.\env\Scripts\activate
uvicorn serving.api.main:app --reload --host 0.0.0.0 --port 8000
```

```powershell
# Terminal 2 — Prefect
.\env\Scripts\activate
prefect server start
```

```powershell
# Terminal 3 — Pipeline (single run)
.\env\Scripts\activate
python pipeline.py --once
```

```powershell
# Terminal 4 — Frontend
cd serving\frontend
npm run dev
```

Same URLs as in the table above.

---
