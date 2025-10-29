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
ClickHouse connection example

If you want to test the ClickHouse endpoint directly from your shell, avoid pasting secrets into files tracked by git. Use environment variables (or GitHub Secrets for CI).

Example (local shell). Set the URL, user and password as environment variables and run a quick SELECT 1 to validate connectivity:

```bash
export CLICKHOUSE_URL="https://v0tapvvnar.us-east-2.aws.clickhouse.cloud:8443"
export CLICKHOUSE_USER="default"
# set CLICKHOUSE_PASSWORD in your shell (do not commit)
# export CLICKHOUSE_PASSWORD="<your_password>"

# simple health check
curl --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" --data-binary 'SELECT 1' "${CLICKHOUSE_URL}"
```

Run the production query for a date (CSV output). This uses `FORMAT CSVWithNames` so the first row is headers:

```bash
DATE="$(date -u -d 'yesterday' +%F)"  # GNU date; on macOS: date -v -1d +%F
cat <<'SQL' > /tmp/query.sql
SELECT
	agent_id,
	avg(call_duration_sec) AS avg_call_length_sec,
	quantileExact(0.9)(call_duration_sec) AS p90_call_length_sec
FROM conversations
WHERE call_start >= toDateTime('${DATE} 00:00:00')
	AND call_start < toDateTime('${DATE} 00:00:00') + INTERVAL 1 DAY
GROUP BY agent_id
ORDER BY agent_id
FORMAT CSVWithNames
SQL

curl --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" --data-binary @/tmp/query.sql "${CLICKHOUSE_URL}" -o agent_stats_${DATE}.csv
```

Security note

You shared a ClickHouse credential earlier in chat. If that credential is sensitive and has been exposed, rotate it immediately and then update the new value in the GitHub secret. Never commit passwords or secrets to the repository.

GitHub Actions secrets

Add these secrets via the GitHub UI (Settings → Secrets and variables → Actions) or `gh`:

 - CLICKHOUSE_URL
 - CLICKHOUSE_USER
 - CLICKHOUSE_PASSWORD
 - CLICKHOUSE_DATABASE (optional)
 - S3_BUCKET
 - S3_KEY_PREFIX (optional)
 - AWS_ACCESS_KEY_ID
 - AWS_SECRET_ACCESS_KEY
 - AWS_REGION

With these secrets configured, the workflow `.github/workflows/daily-clickhouse-export.yml` will run daily and upload the `Main/` folder and produced `exports/` CSV files as artifacts.

# click-house

S3 / AWS configuration

This exporter writes CSV files into a customer S3 bucket. For your project use the bucket name (not the ARN) in `S3_BUCKET`:

- Bucket name: `my-clickhouse-project` (example)
- AWS Region: `us-east-2` (US East — Ohio)

We recommend using a small prefix to keep customer exports organized. Example prefix used in `.env.example`:

```
S3_KEY_PREFIX=customer-exports/Main
```

Minimal IAM policy (least-privilege)

Create an IAM user or role for CI and attach a policy that allows PutObject to the target bucket (and GetBucketLocation). Replace `my-clickhouse-project` with your bucket name.

```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AllowPutObjects",
			"Effect": "Allow",
			"Action": [
				"s3:PutObject",
				"s3:PutObjectAcl",
				"s3:PutObjectTagging"
			],
			"Resource": "arn:aws:s3:::my-clickhouse-project/*"
		},
		{
			"Sid": "AllowGetBucketLocation",
			"Effect": "Allow",
			"Action": "s3:GetBucketLocation",
			"Resource": "arn:aws:s3:::my-clickhouse-project"
		}
	]
}
```

If you'd like to restrict uploads to a specific prefix, change the object resource to `arn:aws:s3:::my-clickhouse-project/customer-exports/*` and set `S3_KEY_PREFIX` accordingly.

After creating IAM credentials, add them as GitHub repository secrets (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) and set `S3_BUCKET` and `AWS_REGION`.