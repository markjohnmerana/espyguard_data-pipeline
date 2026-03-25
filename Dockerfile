FROM apache/airflow:2.9.1

USER root

# Install system dependencies needed by psycopg2 (dbt-postgres uses it)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt for Postgres (Supabase is Postgres-compatible)
RUN pip install --no-cache-dir \
    dbt-core==1.8.2 \
    dbt-postgres==1.8.2

# Verify dbt is available
RUN dbt --version