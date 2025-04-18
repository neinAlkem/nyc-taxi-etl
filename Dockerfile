FROM quay.io/astronomer/astro-runtime:12.8.0

USER root


RUN apt update && \ 
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    pip install --np--cache--dir -r requirements.txt && \
    apt-get clean;

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER astro
