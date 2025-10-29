# Daily ClickHouse -> S3 exporter

This repository contains a small production-ready Python exporter that queries a ClickHouse
`conversations` table and writes per-agent metrics (average call length and 90th percentile)
to a customer-provided AWS S3 bucket as a CSV file, one file per day.

Files added
- `src/config.py` - configuration dataclass
- `src/services/clickhouse_client.py` - ClickHouse HTTP client (returns CSV bytes)
- `src/services/s3_client.py` - lightweight S3 upload helper
- `scripts/export_daily_agent_stats.py` - main daily exporter script
- `requirements.txt` - Python dependencies
- `.github/workflows/daily-clickhouse-export.yml` - GitHub Actions workflow (runs daily)
- `.env.example` - example environment variables

Quickstart (local)
1. Create a virtualenv and install deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Set required environment variables (or use `.env` with a tool like `direnv`):

```bash
export CLICKHOUSE_URL="http://clickhouse:8123/"
export S3_BUCKET="my-bucket"
# optional: CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE
```

3. Run the exporter (defaults to yesterday UTC):

```bash
python scripts/export_daily_agent_stats.py
```

CI / GitHub Actions

1. Add the following repository secrets: `CLICKHOUSE_URL`, `CLICKHOUSE_USER` (optional), `CLICKHOUSE_PASSWORD` (optional), `CLICKHOUSE_DATABASE` (optional), `S3_BUCKET`, `S3_KEY_PREFIX` (optional), `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`.
2. The workflow `.github/workflows/daily-clickhouse-export.yml` runs daily and uploads the `Main/` folder as an artifact (per your requirement) as well as produced CSVs in `exports/`.

Notes
- The ClickHouse query uses `quantileExact(0.9)` to compute the 90th percentile and `avg()` for the average.
- CSV format uses `FORMAT CSVWithNames` so the first row contains column names.
- The script also writes a local copy to `exports/` so the workflow can upload it as an artifact.
# click-house