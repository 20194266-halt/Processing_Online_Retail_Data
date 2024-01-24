FROM apache/airflow:2.8.1

USER root

# Install OpenJDK 17
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Apache Airflow and Apache Spark provider
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark

# Upgrade pip
RUN pip install --upgrade pip

# Install psutil
RUN pip install psutil==5.8.0
