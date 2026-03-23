# ESPyGuard Data Pipeline

## Setup

1. Copy environment files:
```
   cp .env.example .env
   cp dbt/profiles.yml.example dbt/profiles.yml
```

2. Fill in your credentials in `.env` and `dbt/profiles.yml`

3. Generate Fernet key:
```
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

4. Start services:
```
   docker compose up airflow-init
   docker compose up -d
```

5. Install dependencies:
```
   docker compose exec airflow-scheduler pip install dbt-postgres psycopg2-binary boto3
   docker compose exec airflow-webserver pip install dbt-postgres psycopg2-binary boto3
```

6. Open Airflow UI: http://localhost:8080 (admin / admin)
7. Open MinIO Console: http://localhost:9001 (minioadmin / minioadmin)
```

---

## Commit for This
```
chore: add .gitignore files for airflow, webdev, and root
chore: add .env.example and profiles.yml.example safe templates
docs: add README.md with setup instructions