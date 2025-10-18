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
* Hive - `jdbc:hive2://localhost:10000` - `beeline` / (no password)
* [Kafka](#kafka) - `locahost:19092` (from your local machine), `host.docker.internal:9092` (from within any container) or `kafka:9092` (from within container attached to network)

## Kafka

The [docker-compose.yml](docker-compose.yml) was updated to include [apache/kafka](https://hub.docker.com/r/apache/kafka/) as well.
1. Disable Windows path resolution
    ```bash
    export MSYS_NO_PATHCONV=1
    ```
2. Run docker exec to "remote" into the container, alternatively use "Exec" tab on Docker Desktop
    ```bash
    docker exec --workdir /opt/kafka/bin/ -it kafka sh
    ```
3. Create topic to begin pushing events into
    ```bash
    ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic lottery-topic 
    ```
4. Check topic to check events
    ```bash
    ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic lottery-topic --from-beginning
    ```
5. Delete topic to clean up events
    ```bash
    ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic lottery-topic
    ```

The original implementation of Kafka listeners/ports specified by the [override suggestions for default broker configuration](https://hub.docker.com/r/apache/kafka/#overriding-the-default-broker-configuration) was in turn overridden based on [multiple nodes](https://hub.docker.com/r/apache/kafka/#multiple-nodes) and [Connect to Apache Kafka Running in Docker](https://www.baeldung.com/kafka-docker-connection) such that
* `ports` changes to `"19092:19092"`
* `environment.KAFKA_LISTENERS` added `PLAINTEXT_HOST://0.0.0.0:19092`
* `environment.KAFKA_ADVERTISED_LISTENERS` added `PLAINTEXT_HOST://localhost:19092`
* `environment.KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` added `PLAINTEXT_HOST:PLAINTEXT`

The above change ensure that within the Docker containers in the network, the Kafka broker is accessible via port `9092` and within the host, the Kafka broker is accessible via port `19092`.

### Flink Connector - Kafka

Based on [Apache Kafka Connector](https://nightlies.apache.org/flink/flink-docs-lts/docs/connectors/datastream/kafka/), ensure that you've downloaded the following jars (with `kafka-clients` version chosen based on [flink-connector-kafka/pom.xml](https://github.com/apache/flink-connector-kafka/blob/v3.3/pom.xml#L54))
* [org.apache.flink:flink-connector-kafka:3.3.0-1.20](https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka/3.3.0-1.20)
* [org.apache.kafka:kafka-clients:3.4.0](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/3.4.0)

Use the following command to download the jars in question
```bash
curl -o lib/flink-connector-kafka-3.3.0-1.20.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.20/flink-connector-kafka-3.3.0-1.20.jar
curl -o lib/kafka-clients-3.4.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar
```

The `--jarfile` argument appears to only support one uber jar that is assembled via [build.gradle](build.gradle) as `build/libs/pyflink-1.0.0-uber.jar`
```bash
gradle shadowJar --stacktrace
```

## MinIO

MinIO can be interacted with via the MinIO Client (i.e., `mc`)  along with the MinIO Console accessible via http://localhost:9000.

To use the MinIO client,
1. Set up interactive session on the MinIO Client container
    ```bash
    docker exec -it mc bash
    ```
2. Since the alias, `minio` was setup via `docker-compose.yml`, you can run other commands documented [here](https://docs.min.io/community/minio-object-store/reference/minio-mc.html)
    ```bash
    mc ls minio/raw
    ```

Alternatively, you can also use AWS CLI to interact with the buckets created as documented [here](https://docs.min.io/community/minio-object-store/integrations/aws-cli-with-minio.html)
1. Configure the default profile to point to the MinIO server
    ```bash
    aws configure
    ```
    ```
    AWS Access Key ID [None]: admin
    AWS Secret Access Key [None]: password
    Default region name [None]: us-east-1
    Default output format [None]:
    ```
2. Optional configuration can also be setup but they are not necessary
    1. The first configuration is mentioned in the MinIO documentation but it seems to be relevant only for the online version at https://play.min.io:9000.
        ```bash
        aws configure set default.s3.signature_version s3v4
        ```
    2. The second configuration is to avoid passing `--endpoint-url http://localhost:9000` each time to override the AWS end point.
It's recommended in this scenario to be explicit as overriding the `default.endpoint_url` via configuration can impact actual usage of AWS services.
        ```bash
        aws configure set default.endpoint_url http://localhost:9000
        ```
3. Example commands to interact with the MinIO server
    1. Check what buckets are available
        ```bash
        aws --endpoint-url http://localhost:9000 s3 ls
        ```
    2. Check what's in the `s3://raw` bucket created
        ```bash
        aws --endpoint-url http://localhost:9000 s3 ls s3://raw
        ```
    3. Download some [data source](#data-sources) to test streaming (ex: https://catalog.data.gov/dataset/lottery-powerball-winning-numbers-beginning-2010 which is assumed to be in your `Downloads` folder). The command below will copy the file to the `s3://raw` bucket. **Note** that the `$USERPROFILE` is used in the command because AWS CLI can't handle what Git Bash (therefore MinGW) does with the absolute path.
        ```bash
        aws --endpoint-url http://localhost:9000 s3 cp "$USERPROFILE/Downloads/Lottery_Powerball_Winning_Numbers__Beginning_2010.csv" s3://raw/
        ```

### Flink Plugin - S3 File System

To utilize MinIO within a Flink job, you will be leveraging the [FileSystem connector](https://nightlies.apache.org/flink/flink-docs-lts/docs/connectors/datastream/filesystem/) but support for S3 comes through [Hadoop/Presto S3 File Systems plugins](https://nightlies.apache.org/flink/flink-docs-lts/docs/deployment/filesystems/s3/#hadooppresto-s3-file-systems-plugins) which have to be in the Flink job/task manager to work.

See [here](https://nightlies.apache.org/flink/flink-docs-lts/docs/deployment/filesystems/plugins/) for how plugins work in general but for reading files, the recommendation seems to be the Hadoop variant, [org.apache.flink:flink-s3-fs-hadoop:1.20.2](https://mvnrepository.com/artifact/org.apache.flink/flink-s3-fs-hadoop/1.20.2).
The documentation isn't explicitly clear about the exact change but based on trial and error, two additional changes to [docker-compose.yml](docker-compose.yml) was sufficient to enable Flink to read from S3:
* For `services.flink-jobmanager.environment` and `services.flink-taskmanager.environment`, add the following which seems to load the built-in plugin within the image:
    ```yaml
        environment:
          ...
          - ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.20.2.jar
    ```
* For `services.flink-jobmanager.environment` and `services.flink-taskmanager.environment`, add the following to the existing `FLINK_PROPERTIES` variable:
    ```yaml
        environment:
          - |
            FLINK_PROPERTIES=
            ...
            s3.access-key: admin
            s3.secret-key: password
            s3.endpoint: http://minio:9000
            s3.path.style.access: true 
    ```

## Iceberg

Additional research is needed to confirm how to access Iceberg but it appears that the REST API accessible via http://localhost:8181 is the [REST Catalog Spec](https://iceberg.apache.org/rest-catalog-spec/) that adheres to the [OpenAPI specification here](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml).

Iceberg should also be accessible via `jdbc:hive2://localhost:10000` with the user being `beeline` and password being blank. Some database clients may run into an error connecting to Hive due to default namespace, database and/or schema not setup yet. More work is needed to confirm how to set up Hive (which is one way to implement Iceberg) properly.
```bash
docker exec -it spark-iceberg beeline -u jdbc:hive2://localhost:10000/
```

It seems that if you create the table first, then Hive is accessible
1. Connect via Spark SQL interface. `quit;` to exit the interface
    ```bash
    docker exec -it spark-iceberg spark-sql 
    ```
2. Create the table specified in [here](https://iceberg.apache.org/spark-quickstart/#creating-a-table)
    ```sql
    CREATE TABLE demo.nyc.taxis
    (
      vendor_id bigint,
      trip_id bigint,
      trip_distance float,
      fare_amount double,
      store_and_fwd_flag string
    )
    PARTITIONED BY (vendor_id);
    ```
3. Connect via Beeline with the database, `demo` being defined. `!q` to exit the interface.
    ```bash
    docker exec -it spark-iceberg beeline -u jdbc:hive2://localhost:10000/demo
    ```
4. Check the table was created successfully from before.
    ```sql
    SELECT * FROM demo.nyc.taxis;
    ```

### Iceberg Flink Runtime

There appears to be additional work to integrate with Iceberg as a sink. See the resources below:
* https://iceberg.apache.org/docs/latest/flink/
* https://github.com/databricks/docker-spark-iceberg/blob/main/flink-example/src/main/java/io/tabular/flink/lor/example/LORSink.java
* https://github.com/gordonmurray/apache_flink_and_iceberg
* https://stackoverflow.com/questions/78545821/how-to-write-with-pyflink-flink-into-apache-iceberg-on-amazon-s3-with-table-api

The  [org.apache.iceberg:iceberg-flink-runtime-1.20:1.10.0](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-flink-runtime-1.20/1.10.0) library appears to be necessary to run Flink integration with Iceberg. Use the following command to download the jar in question
```bash
curl -o lib/iceberg-flink-runtime-1.20-1.10.0.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.0/iceberg-flink-runtime-1.20-1.10.0.jar
```

The `--jarfile` argument appears to only support one uber jar that is assembled via [build.gradle](build.gradle) as `build/libs/pyflink-1.0.0-uber.jar` which was extended to include this runtime.
```bash
gradle shadowJar --stacktrace
```
The `gradle.properties` was recommended to be added with the following property because "JVM garbage collector is thrashing" when `implementation('software.amazon.awssdk:bundle:2.35.9')` is added to [build.gradle](build.gradle) (which ran into another issue with archive having too many entries).
```properties
org.gradle.jvmargs=-Xmx2g -XX:MaxMetaspaceSize=512m
```

## Data Stream Format 

This section is for additional references and notes regarding handling data stream formats such as CSV and possibly mixed record-type fixed-width data files.

### Data Stream Format - CSV

See [CSV format](https://nightlies.apache.org/flink/flink-docs-lts/docs/connectors/datastream/formats/csv/) for more information. Additional work is needed to get the CSV that's read by [process_csv.py](process_csv.py) to be interpreted correctly (i.e., header isn't propagated and data is broken down into columns).

### Data Sources

Some public and free data sources for testing streaming from [24 Free Public Datasets Sites Every Data Analyst Must Know](https://datahypothesis.com/free-public-datasets-data-analyst-must-know/)
* https://archive-beta.ics.uci.edu/
* https://data.gov/
* https://www.bls.gov/
* https://cloud.google.com/bigquery/public-data
* https://www.kaggle.com/
* https://developer.ibm.com/exchanges/data/
* https://learn.microsoft.com/en-us/azure/open-datasets/dataset-catalog
* https://www.who.int/data/

There are example formats for fixed-width files but publicly accessible datasets are rare due to the shift towards newer formats like CSV/JSON. There are two sets of mixed record-type fixed-width data files that can be used for testing purposes to prove data pipeline can handle these types of files. You may want to leverage a plugin such as [Fixed Width Data Visualizer plugin for Notepad++](https://github.com/shriprem/FWDataViz) to read these files.
* [NOAA ISD](https://registry.opendata.aws/noaa-isd/)
  * [AWS S3 Explorer > noaa-isd-pds > data](https://noaa-isd-pds.s3.amazonaws.com/index.html#data/)
    * You may need to run a command to un-gzip the text for readability
      ```bash
      gunzip -c 010010-99999-2024.gz > 010010-99999-2024
      ```
  * [Technical Documentation](https://www.ncei.noaa.gov/data/global-hourly/doc/isd-format-document.pdf)
* [Public Use Microdata Sample (PUMS) Microdata Dataset](https://www.census.gov/data/datasets/2000/dec/microdata.html)
  * [File Server > census_2000 > datasets > PUMS > OnePercent](https://www2.census.gov/census_2000/datasets/PUMS/OnePercent/)
  * [Technical Documentation](https://www2.census.gov/programs-surveys/decennial/2000/technical-documentation/complete-tech-docs/pums.pdf)

Here are some additional fixed-width file formats that has documentation on how to interpret but no publicly available dataset
* [NACHA ACH](https://achdevguide.nacha.org/ach-file-details) - There is common file examples that are described by [ACH PRO](https://www.ach-pro.com/post/common-nacha-file-examples) if you need to see how it looks like.
* [NSCC Positions & Valuations (POV)](https://www.dtcc.com/-/media/Files/Downloads/Investment-Product-Services/Insurance-and-Retirement-Services/Participant-Support-Services/Record-Layouts/Positions-and-Valuations/POV_Layouts_web_version.xls)

## PyFlink Job Submission

### PyFlink Process CSV

Instructions below is based on [Submitting PyFlink Jobs](https://nightlies.apache.org/flink/flink-docs-lts/docs/deployment/cli/#submitting-pyflink-jobs) to submit PyFlink job, specifically [process_csv.py](process_csv.py)
1. Disable Windows path resolution
    ```bash
    export MSYS_NO_PATHCONV=1
    ```
2. Run Flink command to run the script
    ```bash
    docker run -it --rm \
       -v $(pwd):/opt/flink/app \
       -v $(pwd)/build/libs/pyflink-1.0.0-uber.jar:/opt/flink/lib/pyflink-1.0.0-uber.jar \
       pyflink:1.20.2 \
       /opt/flink/bin/flink run \
       --jobmanager http://host.docker.internal:8081 \
       --python /opt/flink/app/process_csv.py \
       --jarfile /opt/flink/lib/pyflink-1.0.0-uber.jar
    ```

You can use [lottery_avro_schema.json](lottery_avro_schema.json) as the Avro schema to read from the Kafka topic, `lottery-topic` to confirm that the events are written successfully.

### PyFlink Process Kafka

Instructions below is based on [Flink's Python API](https://iceberg.apache.org/docs/latest/flink/#flinks-python-api) to submit PyFlink job, specifically [process_kafka.py](process_kafka.py)
1. Disable Windows path resolution
    ```bash
    export MSYS_NO_PATHCONV=1
    ```
2. Run Flink command to run the script. The `--network` is necessary because the standalone container doesn't know what `iceberg-rest` is.
    ```bash
    docker run -it --rm \
       --network pyflink_iceberg_net \
       -v $(pwd):/opt/flink/app \
       -v $(pwd)/build/libs/pyflink-1.0.0-uber.jar:/opt/flink/lib/pyflink-1.0.0-uber.jar \
       pyflink:1.20.2 \
       /opt/flink/bin/flink run \
       --jobmanager http://host.docker.internal:8081 \
       --python /opt/flink/app/process_kafka.py \
       --jarfile /opt/flink/lib/pyflink-1.0.0-uber.jar
    ```

Connecting to [Iceberg via Hive interface](#iceberg), you should be able to run these commands to check on what's written out.
```sql
SHOW DATABASES;
SHOW TABLES;
SELECT COUNT(*) FROM default.lottery;
SELECT * FROM default.lottery;
```

If you run into the following error, then it means the metadata in [MinIO](#minio) may have been deleted. You would have to interact with Iceberg REST interface to clean up the table as described below:
```
NotFoundException: Location does not exist: s3://warehouse/default/lottery/metadata/00000-....metadata.json
```
```bash
curl -X DELETE \
  http://localhost:8181/v1/namespaces/default/tables/lottery \
  -H "Content-Type: application/json"
```
