FROM python:3.11-slim

WORKDIR /app

# تثبيت ffmpeg الخفيف بدون مكتبات رسومية
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "-u", "main.py"]
