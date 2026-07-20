FROM bitnami/spark:3.4.1

USER root

# Install wget to fetch JDBC PostgreSQL driver
RUN apt-get update && \
    apt-get install -y wget && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /opt/bitnami/spark/jars/postgresql-42.6.0.jar && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the non-root Spark user
USER 1001
