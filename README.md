# PyFlink

## Python Virtual Environment

1. Setup virtual environment
    ```bash
    python -m venv .venv
    ```
2. Activate virtual environment
    ```bash
    source .venv/Scripts/activate
    ```
3. Install dependencies
    ```bash
    pip install -vr requirements-dev.txt
    ```

## PyFlink Docker Image

Based on [Apache Flink LTS > Using Flink Python on Docker](https://nightlies.apache.org/flink/flink-docs-lts/docs/deployment/resource-providers/standalone/docker/#using-flink-python-on-docker), create Docker image based on [Dockerfile](Dockerfile)
```bash
docker build --tag pyflink:1.20.2 .
```

## PyFlink Docker Compose Stack

Based on [Spark and Iceberg Quickstart](https://iceberg.apache.org/spark-quickstart/) and [databricks/docker-spark-iceberg](https://github.com/databricks/docker-spark-iceberg/tree/main/flink-example),
1. Start Docker containers via [docker-compose.yml](docker-compose.yml)
    ```bash
    docker compose up -d
    ```
2. Stop Docker containers
    ```bash
    docker compose stop
    ```
3. Remove Docker containers and volumes
    ```bash
    docker compose down --volume
    ```

For Edge, disable automatic HTTPS rerouting, `edge://flags/#edge-automatic-https`
* Apache Flink Dashboard - http://localhost:8081/#/overview
* MinIO Console - http://localhost:9001/login - `admin` / `password`
* Apache Spark Web UI - http://localhost:8080/
* Jupyter Server - http://localhost:8888/

## Kafka

The [docker-compose.yml](docker-compose.yml) was updated to include [apache/kafka](https://hub.docker.com/r/apache/kafka/) as well.
1. Disable Windows path resolution
    ```bash
    export MSYS_NO_PATHCONV=1
    ```
2. Run docker exec to "remote" into the container, alternatively use "Exec" tab on Docker Desktop
    ```bash
    docker exec --workdir /opt/kafka/bin/ -it pyflink-kafka-broker-1 sh
    ```
3. Create topic to begin pushing events into
    ```bash
    ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-topic 
    ```
4. Check topic to check events
    ```bash
    ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning
    ```

### Flink Connector - Kafka

Based on [Apache Kafka Connector](https://nightlies.apache.org/flink/flink-docs-lts/docs/connectors/datastream/kafka/), ensure that you've downloaded the following jars (with `kafka-clients` version chosen based on [flink-connector-kafka/pom.xml](https://github.com/apache/flink-connector-kafka/blob/v3.3/pom.xml#L54))
* [org.apache.flink:flink-connector-kafka:3.3.0-1.20](https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka/3.3.0-1.20)
* [org.apache.kafka:kafka-clients:3.4.0](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/3.4.0)

Use the following command to download the jars in question
```bash
curl -o lib/flink-connector-kafka-3.3.0-1.20.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.20/flink-connector-kafka-3.3.0-1.20.jar
curl -o lib/kafka-clients-3.4.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar
```

The `--jarfile` argument for Flink CLI can be used to upload the jars to the task managers but it may not be relevant
due to mounting the files to the `/opt/flink/lib` which is already part of the Flink classpath.

## PyFlink Job Submission

Instructions below is based on [Submitting PyFlink Jobs](https://nightlies.apache.org/flink/flink-docs-lts/docs/deployment/cli/#submitting-pyflink-jobs) to submit PyFlink job, specifically [word_count.py](word_count.py) which is based on copy from [flink-python/pyflink/examples/datastream/word_count.py](https://github.com/apache/flink/blob/release-1.20/flink-python/pyflink/examples/datastream/word_count.py)
1. Disable Windows path resolution
    ```bash
    export MSYS_NO_PATHCONV=1
    ```
2. Run Flink command to run the script
    ```bash
    docker run -it --rm \
       -v $(pwd):/opt/flink/app \
       -v $(pwd)/lib/flink-connector-kafka-3.3.0-1.20.jar:/opt/flink/lib/flink-connector-kafka-3.3.0-1.20.jar \
       -v $(pwd)/lib/kafka-clients-3.4.0.jar:/opt/flink/lib/kafka-clients-3.4.0.jar \
       pyflink:1.20.2 \
       /opt/flink/bin/flink run \
       --jobmanager http://host.docker.internal:8081 \
       --python /opt/flink/app/word_count.py
    ```

The current code fails with the following exception:
> Caused by: java.lang.ClassNotFoundException: org.apache.flink.connector.kafka.sink.KafkaSink
