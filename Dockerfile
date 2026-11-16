FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app.py .

# Create persistent data directory
RUN mkdir -p /data \
    && chown -R 10001:10001 /data \
    && chmod 750 /data

# Run as non-root user
USER 10001:10001

CMD ["python", "app.py"]