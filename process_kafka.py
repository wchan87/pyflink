import json
import logging
import sys

from pyflink.common import WatermarkStrategy, Row
from pyflink.common.typeinfo import RowTypeInfo, Types
from pyflink.datastream import StreamExecutionEnvironment, DataStream, MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.avro import AvroRowDeserializationSchema
from pyflink.table import StreamTableEnvironment, TableResult, Table


def create_avro_schema() -> str:
    avro_schema = {
        'type': 'record',
        'name': 'Lottery',
        'fields': [
            {'name': 'draw_date', 'type': 'string'},
            {'name': 'winning_numbers', 'type': {'type': 'array', 'items': 'int'}},
            {'name': 'multiplier', 'type': ['int', 'null']},
            {'name': 'source_line_number', 'type': 'int'},
        ]
    }
    return json.dumps(avro_schema)

class FlatRecordMapper(MapFunction):
    logger: logging.Logger

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def map(self, record: Row) -> Row:
        self.logger.info("Mapping row %s", record)
        row: Row = Row(
            record[0],
            record[1][0],
            record[1][1],
            record[1][2],
            record[1][3],
            record[1][4],
            record[1][5],
            record[2],
            record[3]
        )
        return row

def create_iceberg_type_info() -> RowTypeInfo:
    return RowTypeInfo(
        [Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT()],
        ['draw_date', 'winning_number_1', 'winning_number_2', 'winning_number_3', 'winning_number_4', 'winning_number_5', 'winning_number_6', 'multiplier', 'source_line_number']
    )

def process():
    env: StreamExecutionEnvironment = StreamExecutionEnvironment.get_execution_environment()
    table_env: StreamTableEnvironment = StreamTableEnvironment.create(env)
    logger: logging.Logger = logging.getLogger(__name__)

    # TODO figure out which of the following is responsible for the Iceberg writes
    # Copilot indicates that "Iceberg requires streaming inserts to be explicitly enabled." This needed to be before the INSERT
    table_env.get_config().set('table.dynamic-table-options.enabled', 'true')
    # Copilot indicates that "Iceberg streaming writes require Flink checkpointing to commit data."
    env.enable_checkpointing(10_000)
    # Copilot suggests the following advanced options to the above
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    # https://iceberg.apache.org/docs/latest/flink-configuration/
    # https://github.com/databricks/docker-spark-iceberg/blob/cc754f6c2d9c6c2c4d2b72273b03d70f3e8628b3/flink-example/src/main/java/io/tabular/flink/lor/example/LORSink.java#L49-L58
    # TODO confirm what Copilot says about execute_sql for CREATE CATALOG/TABLE is DDL (only setup on startup/compile) so "they are executed immediately and synchronously" and "handled by the Flink SQL parser and catalog manager at compile time, not runtime"
    table_env.execute_sql("""
    CREATE CATALOG my_catalog WITH (
        'type'='iceberg',
        'uri'='http://iceberg-rest:8181',
        'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
        'warehouse'='s3://warehouse/wh/',
        's3.endpoint'='http://minio:9000',
        'catalog-impl'='org.apache.iceberg.rest.RESTCatalog'
    )
    """)
    # https://nightlies.apache.org/flink/flink-docs-lts/docs/dev/python/table/intro_to_table_api/
    table_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS `my_catalog`.`default`.`lottery` (
        draw_date STRING,
        winning_number_1 SMALLINT,
        winning_number_2 SMALLINT,
        winning_number_3 SMALLINT,
        winning_number_4 SMALLINT,
        winning_number_5 SMALLINT,
        winning_number_6 SMALLINT,
        multiplier SMALLINT,
        source_line_number INT
    )
    """)

    source: KafkaSource = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_group_id('pyflink-avro-reader-group') \
        .set_topics('lottery-topic') \
        .set_value_only_deserializer(AvroRowDeserializationSchema(avro_schema_string=create_avro_schema())) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_client_id_prefix('pyflink-avro-reader') \
        .build()

    ds: DataStream = env.from_source(source, WatermarkStrategy.no_watermarks(), 'kafka-source')
    flattened_ds: DataStream = ds.map(FlatRecordMapper(logger), output_type=create_iceberg_type_info())

    # https://nightlies.apache.org/flink/flink-docs-lts/docs/dev/python/table/intro_to_table_api/
    # Approach #1 was the original one which wasn't clear about whether it worked or not (could have been related to the checkpointing config further up)
    # table_env.create_temporary_view("lottery_stream", flattened_ds)
    # result: TableResult = table_env.execute_sql("""
    # INSERT INTO `my_catalog`.`default`.`lottery`
    # SELECT * FROM lottery_stream
    # """)
    # Approach #2 worked as execute_insert is the streaming equivalent of INSERT INTO
    table: Table = table_env.from_data_stream(flattened_ds)
    table.execute_insert('`my_catalog`.`default`.`lottery`')

    # This method call was noted to be redundant since Table API is being used and the execute_insert is functionally the job submission
    # env.execute()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')
    process()
