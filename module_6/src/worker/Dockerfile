FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/app

COPY src/worker/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

USER 1000

CMD ["python", "-m", "src.worker.consumer"]