FROM quay.io/astronomer/astro-runtime:12.8.0

USER root


RUN apt update && \ 
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    pip install -r requirements.txt && \
    apt-get clean;

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery==1.5.3 && deactivate

USER astro
