import os
import json

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import StreamingFileSink, RollingPolicy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common import Configuration, Row
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.common.serialization import Encoder
from pyflink.datastream.functions import MapFunction

# Define a MapFunction to split the transaction_time into day, month, and year
class SplitTransactionDate(MapFunction):
    def map(self, value):
        fromAccountId, toAccountId, amount, transaction_time = value
        day, month, year = transaction_time.split('-')
        return Row(fromAccountId=fromAccountId, toAccountId=toAccountId, amount=amount, transaction_time_day=int(day), transaction_time_month=int(month), transaction_time_year=int(year), transaction_time=transaction_time)


# Define a MapFunction to convert the Row to a string
class RowToString(MapFunction):
    def map(self, value):
        return f"{value.fromAccountId},{value.toAccountId},{value.amount},{value.transaction_time},{value.transaction_time_day},{value.transaction_time_month},{value.transaction_time_year}"


# TODO: Enrich the transaction data with account information (e.g., account holder name)


def main():
    # Set up the execution environment
    config = Configuration()
    cwd = os.getcwd()
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.add_jars(f"hdfs://hdfs-master:9000/flink_jars/flink-sql-connector-kafka-3.2.0-1.19.jar")
    env.add_jars(f"hdfs://hdfs-master:9000/flink_jars/flink-connector-base-1.19.1.jar")
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1) # the writing happens only on one machine

    # Define the Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka-1:19092') \
        .set_topics('transactions') \
        .set_group_id('my_flink_transaction_stream_processing_0') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(JsonRowDeserializationSchema.builder().type_info(Types.ROW_NAMED(
            ["fromAccountId", "toAccountId", "amount", "transaction_time"],
            [Types.STRING(), Types.STRING(), Types.INT(), Types.STRING()]
        )).build()) \
        .build()

    # Add the source to the environment
    kafka_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name='Kafka Source'
    )

    # Define the processing logic
    processed_stream = kafka_stream.map(
        SplitTransactionDate(),
        output_type=Types.ROW_NAMED(
            ["fromAccountId", "toAccountId", "amount", "transaction_time", "transaction_time_day", "transaction_time_month", "transaction_time_year"],
            [Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.INT()]
    ))

    # Convert Row to String
    string_stream = processed_stream.map(
        RowToString(),
        output_type=Types.STRING()
    )

    # Define the HDFs file sink
    output_path = 'hdfs://hdfs-master:9000/flink_stream/transactions/'
    hdfs_sink = StreamingFileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('.txt').build()) \
        .with_rolling_policy(RollingPolicy.default_rolling_policy()) \
        .build()

    # Add the sink to the processed stream
    string_stream.add_sink(hdfs_sink)

    # Print the results to the console (for demonstration purposes)
    processed_stream.print()

    # Execute the environment
    env.execute('Kafka Streaming Job')

if __name__ == '__main__':
    main()