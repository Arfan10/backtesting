# Delta Exchange → AWS Data Pipeline

## Architecture

```
WebSocket (Delta Exchange)
        │
        ▼
  EC2 (pipeline container)
        │  streams rows
        ▼
  RDS PostgreSQL  ◄──── source of truth (raw tables)
        │
        ▼
  Airflow (EC2, Docker)
        │  triggers on schedule
        ▼
  dbt   (runs inside Airflow)
        │  transforms raw → staging → marts
        ▼
  RDS PostgreSQL (transformed tables)
        │
        ▼
  S3    (logs, exports, backups)
```

## Project Layout

```
delta_pipeline/
├── pipeline/
│   ├── config.py
│   ├── pipeline.py
│   ├── schema.sql
│   ├── requirements.txt
│   ├── Dockerfile
│   └── docker-compose.pipeline.yml   ← local dev only
│
├── airflow/
│   ├── Dockerfile.airflow
│   ├── docker-compose.airflow.yml    ← deploy on Airflow EC2
│   ├── requirements.airflow.txt
│   └── dags/
│       └── delta_dbt_dag.py
│
├── dbt/delta_models/
│   ├── dbt_project.yml
│   ├── profiles.yml                  ← gitignored, holds RDS creds
│   ├── models/
│   │   ├── staging/                  ← clean + cast raw tables
│   │   └── marts/                    ← analytics-ready aggregates
│   └── macros/
│
├── infra/
│   └── setup_aws.sh                  ← RDS + ECR + S3 provisioning guide
│
└── .env.example
```

---

## Step-by-Step Setup

### 1. Local Postgres connection (right now)

Your `POSTGRES_DSN` in `config.py` defaults to:
```
postgresql://postgres:password@localhost:5432/delta_data
```

To fix it for your local machine:
```bash
# Check your local postgres user/password
psql -U postgres -c "\du"

# Then export the correct DSN before running
export POSTGRES_DSN="postgresql://YOUR_USER:YOUR_PASSWORD@localhost:5432/delta_data"
python pipeline.py
```

Or edit `config.py` directly with your actual credentials (don't commit).

---

### 2. Provision AWS Infrastructure

```bash
# Install AWS CLI first: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html
aws configure   # enter Access Key, Secret Key, region (e.g. ap-south-1 for India)

bash infra/setup_aws.sh
```

This script creates:
- S3 bucket for logs/exports
- RDS PostgreSQL 16 instance (db.t3.micro — free tier eligible)
- ECR repository for your Docker image
- Outputs connection strings you paste into .env

---

### 3. Push Pipeline Image to ECR

```bash
# Get your ECR login token (replace REGION and ACCOUNT_ID)
aws ecr get-login-password --region ap-south-1 | \
  docker login --username AWS --password-stdin ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com

# Build and push
docker build -t delta-pipeline ./pipeline/
docker tag delta-pipeline:latest ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com/delta-pipeline:latest
docker push ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com/delta-pipeline:latest
```

---

### 4. Run Pipeline on EC2

SSH into your EC2 instance (pipeline server), then:

```bash
# Install docker + compose
sudo apt update && sudo apt install -y docker.io docker-compose-plugin
sudo usermod -aG docker ubuntu

# Pull and run
aws ecr get-login-password --region ap-south-1 | \
  docker login --username AWS --password-stdin ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com

docker pull ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com/delta-pipeline:latest
docker run -d --restart always \
  -e DELTA_API_KEY=your_key \
  -e DELTA_API_SECRET=your_secret \
  -e POSTGRES_DSN="postgresql://delta:password@YOUR_RDS_ENDPOINT:5432/delta_data" \
  --name delta_pipeline \
  ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com/delta-pipeline:latest
```

---

### 5. Deploy Airflow on EC2

SSH into your Airflow EC2 instance:

```bash
git clone your-repo && cd delta_pipeline/airflow

cp ../.env.example .env
nano .env   # fill in RDS DSN and dbt settings

docker compose -f docker-compose.airflow.yml up -d

# First time only — init the Airflow DB
docker compose -f docker-compose.airflow.yml run airflow-init
```

Airflow UI: http://YOUR_AIRFLOW_EC2_IP:8080
Default login: airflow / airflow (change immediately)

---

### 6. dbt Setup

```bash
cd dbt/delta_models
pip install dbt-postgres

# Edit profiles.yml with your RDS credentials
dbt debug     # test connection
dbt run       # run all models
dbt test      # run schema tests
```

---

## Environment Variables Reference

| Variable | Description |
|---|---|
| `DELTA_API_KEY` | Delta Exchange API key |
| `DELTA_API_SECRET` | Delta Exchange API secret |
| `POSTGRES_DSN` | Full PostgreSQL connection string |
| `AIRFLOW_UID` | Linux UID for Airflow (run: `id -u`) |
| `DBT_PROFILES_DIR` | Path to dbt profiles.yml |