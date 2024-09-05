import os

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common import Configuration


def main():
    # Set up the execution environment
    config = Configuration()
    cwd = os.getcwd()
    # config.set_string("pipeline.jars", \
    #                     f"file:///{cwd}/connector_jars/flink-connector-kafka-3.2.0-1.19.jar, \
    #                     file:///{cwd}/connector_jars/kafka-clients-3.2.0.jar")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    # env.add_jars(f"file:///{cwd}/connector_jars/flink-connector-kafka-3.2.0-1.19.jar")
    # env.add_jars(f"file:///{cwd}/connector_jars/kafka-clients-3.2.0.jar")
    env.add_jars(f"file:///{cwd}/connector_jars/flink-sql-connector-kafka-3.2.0-1.19.jar")
    env.add_jars(f"file:///{cwd}/connector_jars/flink-connector-base-1.19.1.jar")
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    # # Define the Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:29092') \
        .set_topics('transactions') \
        .set_group_id('my_flink_transaction_stream_processing_0') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(JsonRowDeserializationSchema.builder().type_info(Types.BYTE([Types.STRING(), Types.STRING(), Types.INT(), Types.STRING()])).build()) \
        .build()

    # # Add the source to the environment
    kafka_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name='Kafka Source'
    )

    # deserialization_schema = JsonRowDeserializationSchema.builder() \
    #     .type_info(Types.ROW([Types.STRING(), Types.STRING(), Types.INT(), Types.STRING()])) \
    #     .build()
    # kafka_consumer = FlinkKafkaConsumer(
    #     topics='transactions',
    #     deserialization_schema=deserialization_schema,
    #     properties={'bootstrap.servers': 'localhost:29092', 'group.id': 'my_flink_transaction_stream_processing_0'}
    # )
    # kafka_consumer.set_start_from_earliest()
    # env.add_source(kafka_consumer).print()
    # env.execute('oi')

    # Define the processing logic
    processed_stream = kafka_stream.map(lambda x: x, output_type=Types.ROW([Types.STRING(), Types.STRING(), Types.INT(), Types.STRING()]))

    # Print the results to the console (for demonstration purposes)
    processed_stream.print()

    # Execute the environment
    env.execute('Kafka Streaming Job')

if __name__ == '__main__':
    main()