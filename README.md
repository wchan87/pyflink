# PyFlink

## PyFlink Docker Image

Based on [Apache Flink v1.20.2 > Using Flink Python on Docker](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/resource-providers/standalone/docker/#using-flink-python-on-docker), create Docker image based on [Dockerfile](Dockerfile)
```bash
docker build --tag pyflink:1.20.2 .
```

## PyFlink Docker Compose Stack

Based on [Spark and Iceberg Quickstart](https://iceberg.apache.org/spark-quickstart/) and [databricks/docker-spark-iceberg](https://github.com/databricks/docker-spark-iceberg/tree/main/flink-example),
1. Start Docker containers via [docker-compose.yml](docker-compose.yml)
    ```bash
    docker compose up -d
    ```
   * 
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

## PyFlink Job Submission

Instructions below is based on [Submitting PyFlink Jobs](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/cli/#submitting-pyflink-jobs) to submit PyFlink job, specifically [word_count.py](word_count.py) which is based on copy from [flink-python/pyflink/examples/datastream/word_count.py](https://github.com/apache/flink/blob/release-1.20/flink-python/pyflink/examples/datastream/word_count.py)
1. Disable Windows path resolution
    ```bash
    export MSYS_NO_PATHCONV=1
    ```
2. Run Flink command to run the script
    ```bash
    docker run -it --rm -d \
    -v $(pwd):/opt/flink/app \
    pyflink:1.20.2 \
    /opt/flink/bin/flink run \
    --jobmanager http://host.docker.internal:8081 \
    --python /opt/flink/app/word_count.py
    ```
