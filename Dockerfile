FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1 \
	CONFIG_PATH=/app/config/config.json

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY proto/ ./proto/

HEALTHCHECK --interval=60s --timeout=5s --start-period=90s --retries=3 CMD ["python", "-c", "from pathlib import Path; import os, sys, time; path = Path(os.getenv('LIFECHECK_FILE', '/tmp/nestgraph2.lifecheck')); interval = int(os.getenv('LIFECHECK_INTERVAL_SECONDS', '60')); sys.exit(0 if path.exists() and (time.time() - path.stat().st_mtime) <= int(interval * 2.5) else 1)"]

CMD ["python", "-m", "src.main"]

