FROM python:slim

USER root

RUN mkdir -p /workspace/ingestion

WORKDIR /workspace/ingestion

COPY requirements.txt /workspace/ingestion

RUN pip install -r requirements.txt

COPY data_ingestion.py /workspace/ingestion

CMD ["python", "data_ingestion.py"]