FROM apache/airflow:2.10.5

USER root

# Install OpenJDK 17 and procps for PySpark execution
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python requirements and Airflow providers
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    psycopg2-binary==2.9.9 \
    pyyaml==6.0.1 \
    great-expectations==0.17.15 \
    "apache-airflow-providers-postgres<5.0.0" \
    "apache-airflow-providers-apache-spark<5.0.0"
