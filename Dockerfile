FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv/app

COPY requirements.txt /srv/app/requirements.txt
RUN pip install --no-cache-dir -r /srv/app/requirements.txt

COPY app /srv/app/app

# All mutable state (parquet files + SQLite metadata) lives under /data so a
# single volume gives full crash-recovery persistence.
ENV DATA_DIR=/data
VOLUME ["/data"]

EXPOSE 8000

# Single-process by design: consistency is guaranteed by in-process locks
# plus SQLite transactions, so do not scale this with multiple workers.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
