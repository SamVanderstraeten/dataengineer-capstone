FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

# copy .jars

USER root

RUN mkdir -p /home/capstone/spark

COPY spark /home/capstone/spark
COPY requirements.txt /home/capstone

WORKDIR /home/capstone

RUN pip install -r requirements.txt
RUN chmod +x /home/capstone/spark/data_transform.py

CMD ["python3", "/home/capstone/spark/data_transform.py"]