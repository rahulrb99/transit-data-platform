FROM python:3.11-slim@sha256:f1fd7707d6823c38591aa16ebaa0bc6892b609835a73520ad18a39e2e7454fc5

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

COPY pyproject.toml requirements-runtime.lock README.md ./
COPY ingestion ./ingestion
COPY dashboard ./dashboard
COPY infrastructure ./infrastructure
COPY scripts ./scripts

RUN pip install --no-cache-dir -r requirements-runtime.lock \
    && pip install --no-cache-dir --no-deps . \
    && python -m pip uninstall --yes pip setuptools wheel \
    && useradd --create-home --uid 10001 app \
    && mkdir -p /app/data/raw /app/data/archive/raw /app/data/processed \
    && chown -R app:app /app/data

USER app

EXPOSE 8501

CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
