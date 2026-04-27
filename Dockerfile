# Use slim image (smaller, faster, fewer vulnerabilities)
FROM python:3.11-slim

# Prevent Python from buffering logs
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (better caching)
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Cloud Run expects port 8080
ENV PORT=8080

# Run with Gunicorn (production WSGI server)
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]
