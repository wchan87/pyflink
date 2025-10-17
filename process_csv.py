from datetime import datetime
import json
import logging
import sys

from pyflink.common import Row, Types
# from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, DataStream, MapFunction
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.formats.avro import AvroRowSerializationSchema
from pyflink.datastream.formats.csv import CsvSchema
from pyflink.table import DataTypes, StreamTableEnvironment, EnvironmentSettings, Table

def create_csv_schema() -> CsvSchema:
    csv_schema = CsvSchema.builder() \
        .add_string_column('Draw Date') \
        .add_array_column('Winning Numbers', ' ', element_type=DataTypes.SMALLINT()) \
        .add_number_column('Multiplier', number_type=DataTypes.SMALLINT()) \
        .set_column_separator(',') \
        .set_use_header() \
        .build()
    return csv_schema

class CsvRecordMapper(MapFunction):
    source_line_number: int
    logger: logging.Logger

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.source_line_number = 1 # skips the header

    def parse_winning_numbers(self, winning_numbers_str: str) -> list[int]:
        return [int(x) for x in winning_numbers_str.strip().split(' ') if x.isdigit()]

    def map(self, record: Row) -> Row:
        # Row is accessed by index
        draw_date: str = datetime.strftime(datetime.strptime(record[0], '%m/%d/%Y'), '%Y-%m-%d')
        winning_numbers: list[int] = self.parse_winning_numbers(record[1])
        self.source_line_number += 1 # increment by 1
        self.logger.info(f'Reading data row: {record} from source line number: {self.source_line_number}')
        row: Row = Row(
            draw_date,
            winning_numbers,
            record[2],
            self.source_line_number
        )
        return row

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

def create_avro_type_info() -> RowTypeInfo:
    # Types.OBJECT_ARRAY(Types.INT()) is the right type instead of Types.PRIMITIVE_ARRAY(Types.INT()) would lead to "java.lang.RuntimeException: Failed to serialize row."
    return RowTypeInfo(
        [Types.STRING(), Types.OBJECT_ARRAY(Types.INT()), Types.INT(), Types.INT()],
        ['draw_date', 'winning_numbers', 'multiplier', 'source_line_number']
    )

def process():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    logger: logging.Logger = logging.getLogger(__name__)

    # https://nightlies.apache.org/flink/flink-docs-lts/api/python/reference/pyflink.datastream/api/pyflink.datastream.formats.csv.CsvReaderFormat.html
    # TODO this results in java.lang.IllegalAccessError: class org.apache.flink.formats.csv.PythonCsvUtils tried to access method 'void org.apache.flink.formats.csv.CsvReaderFormat.<init>(org.apache.flink.util.function.SerializableSupplier, org.apache.flink.util.function.SerializableFunction, java.lang.Class, org.apache.flink.formats.common.Converter, org.apache.flink.api.common.typeinfo.TypeInformation, boolean)' (org.apache.flink.formats.csv.PythonCsvUtils is in unnamed module of loader org.apache.flink.util.ChildFirstClassLoader @6f7923a5; org.apache.flink.formats.csv.CsvReaderFormat is in unnamed module of loader 'app')
    # ds: DataStream = env.from_source(
    #     source=FileSource.for_record_stream_format(CsvReaderFormat.for_schema(create_csv_schema()), input_path).build(),
    #     watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    #     source_name='csv-source'
    # )

    # winning_numbers was originally ARRAY<SMALLINT> but Table API runs into Caused by: java.lang.NumberFormatException: For input string: "Winning Numbers"
    # csv.ignore-parse-errors is needed to skip the header as there are no other way
    # https://nightlies.apache.org/flink/flink-docs-lts/docs/connectors/table/formats/csv/
    t_env: StreamTableEnvironment = StreamTableEnvironment.create(env, EnvironmentSettings.new_instance().in_streaming_mode().build())
    t_env.execute_sql('''
        CREATE TABLE lottery (
            draw_date STRING,
            winning_numbers STRING,
            multiplier SMALLINT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 's3://raw/Lottery_Powerball_Winning_Numbers__Beginning_2010.csv',
            'format' = 'csv',
            'csv.field-delimiter' = ',',
            'csv.ignore-parse-errors' = 'true'
        )
    ''')
    table: Table = t_env.sql_query('''
        SELECT * FROM lottery WHERE draw_date <> 'Draw Date'
    ''')
    ds: DataStream = t_env.to_data_stream(table)

    # define the sink
    # transaction.timeout.ms is needed because EXACTLY_ONCE require the broker timeout to be aligned with the producer (i.e., sink here)
    kafka_sink: KafkaSink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic('lottery-topic')
                # .set_key_serialization_schema(SimpleStringSchema())
                .set_value_serialization_schema(AvroRowSerializationSchema(avro_schema_string=create_avro_schema()))
                .build()
            ) \
        .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE) \
        .set_property('transaction.timeout.ms', '600000')  \
        .build()
    # TODO figure out how to pass the key to Kafka
    ds.map(CsvRecordMapper(logger), output_type=create_avro_type_info()).sink_to(kafka_sink)

    # submit for execution
    env.execute()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')
    process()
