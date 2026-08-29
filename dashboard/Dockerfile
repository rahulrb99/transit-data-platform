FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY ingestion ./ingestion
COPY dashboard ./dashboard

RUN pip install --no-cache-dir -e .

EXPOSE 8501

CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0"]

