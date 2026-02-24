FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/app

COPY src/web/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

USER 1000

CMD ["python", "-m", "src.web.run"]