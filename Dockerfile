FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first so code-only changes don't re-download everything
COPY requirements.txt /app/
RUN pip install --no-cache-dir "urllib3<2.0.0" -r requirements.txt

COPY . /app

CMD ["python", "main.py"]
