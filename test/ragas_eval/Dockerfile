FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY eval_ragas.py /app/eval_ragas.py
COPY rag_eval_samples.json /app/rag_eval_samples.json

ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/eval_ragas.py"]
