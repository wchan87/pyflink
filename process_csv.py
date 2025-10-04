# derived from https://github.com/apache/flink/blob/release-1.20/flink-python/pyflink/examples/datastream/word_count.py
import logging
import sys

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, DataStream
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors import DeliveryGuarantee

def word_count():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)

    input_path: str = 's3://raw/Lottery_Powerball_Winning_Numbers__Beginning_2010.csv'

    ds: DataStream = env.from_source(source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),input_path)
        .process_static_file_set().build(), watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(), source_name="file_source")

    def split(line):
        yield from line.split()

    # compute word count
    processed_ds: DataStream = ds.flat_map(split) \
           .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
           .key_by(lambda i: i[0]) \
           .reduce(lambda i, j: (i[0], i[1] + j[1]))

    print('Printing result to stdout.')
    processed_ds.print()

    # define the sink
    kafka_sink: KafkaSink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic('test-topic')
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()
    ds.map(lambda x: x, output_type=Types.STRING()).sink_to(kafka_sink)

    # submit for execution
    env.execute()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    word_count()
