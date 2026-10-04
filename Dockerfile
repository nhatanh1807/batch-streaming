FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN mkdir -p /opt/jdbc \
    && curl -L \
        -o /opt/jdbc/postgresql-42.7.8.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.8.jar

ENV SPARK_CLASSPATH=/opt/jdbc/postgresql-42.7.8.jar

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir pyspark==4.1.2

COPY . .

CMD ["python", "main_etl.py"]