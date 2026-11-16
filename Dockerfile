
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --system --uid 10001 app \
    && mkdir -p /data \
    && chown -R app:app /data

COPY --chown=app:app app.py .

USER app

EXPOSE 18883

CMD ["python", "app.py"]
