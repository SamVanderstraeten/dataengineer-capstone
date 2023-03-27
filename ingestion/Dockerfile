FROM python:slim

USER root

RUN mkdir -p /workspace/ingestion

WORKDIR /workspace/ingestion

COPY requirements.txt /workspace/ingestion

RUN pip install -r requirements.txt

COPY data_ingestion.py /workspace/ingestion

# REMINDER: needs aws credentials on docker run (env vars)
CMD ["python", "data_ingestion.py"]