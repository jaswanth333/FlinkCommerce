from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Row, WatermarkStrategy, Configuration
from pyflink.datastream.functions import MapFunction
from pathlib import Path
from datetime import datetime, date
import json

# Constants
JDBC_URL = "jdbc:postgresql://localhost:5433/postgres"
USERNAME = "postgres"
PASSWORD = "postgres"

# Add connector jars via config
config = Configuration()
jar_urls = [
    "file:///path/to/libs/flink-connector-jdbc-3.1.1-1.17.jar",
    "file:///path/to/libs/flink-connector-kafka-3.0.1-1.18.jar",
    "file:///path/to/libs/postgresql-42.6.0.jar",
]
config.set_string("pipeline.jars", ";".join(jar_urls))

env = StreamExecutionEnvironment.get_execution_environment(config)
env.set_parallelism(1)

topic = "financial_transactions"

# Kafka source
kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers("localhost:9092")
    .set_topics(topic)
    .set_group_id("flink-group")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

# Transaction type
transaction_type = Types.ROW([
    Types.STRING(),       # transaction_id
    Types.STRING(),       # product_id
    Types.STRING(),       # product_name
    Types.STRING(),       # product_category
    Types.DOUBLE(),       # product_price
    Types.INT(),          # product_quantity
    Types.STRING(),       # product_brand
    Types.DOUBLE(),       # total_amount
    Types.STRING(),       # currency
    Types.STRING(),       # customer_id
    Types.SQL_TIMESTAMP(),# transaction_date
    Types.STRING()        # payment_method
])

class JsonToTransaction(MapFunction):
    def map(self, value):
        data = json.loads(value)
        return Row(
            data['transaction_id'], data['product_id'], data['product_name'],
            data['product_category'], float(data['product_price']),
            int(data['product_quantity']), data['product_brand'],
            float(data['total_amount']), data['currency'],
            data['customer_id'], datetime.fromisoformat(data['transaction_date']),
            data['payment_method']
        )

transaction_stream = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Kafka source"
).map(JsonToTransaction(), output_type=transaction_type)

transaction_stream.print()

# JDBC options
exec_options = (
    JdbcExecutionOptions.builder()
    .with_batch_size(1000)
    .with_batch_interval_ms(200)
    .with_max_retries(5)
    .build()
)

conn_options = JdbcConnectionOptions.builder() \
    .with_url(JDBC_URL) \
    .with_driver_name("org.postgresql.Driver") \
    .with_user_name(USERNAME) \
    .with_password(PASSWORD) \
    .build()

# Insert / upsert into transactions table
transactions_insert_sql = """
INSERT INTO transactions (
    transaction_id, product_id, product_name, product_category,
    product_price, product_quantity, product_brand, total_amount,
    currency, customer_id, transaction_date, payment_method
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (transaction_id)
DO UPDATE SET
    product_id = EXCLUDED.product_id,
    product_name = EXCLUDED.product_name,
    product_category = EXCLUDED.product_category,
    product_price = EXCLUDED.product_price,
    product_quantity = EXCLUDED.product_quantity,
    product_brand = EXCLUDED.product_brand,
    total_amount = EXCLUDED.total_amount,
    currency = EXCLUDED.currency,
    customer_id = EXCLUDED.customer_id,
    transaction_date = EXCLUDED.transaction_date,
    payment_method = EXCLUDED.payment_method
"""

transaction_stream.add_sink(
    JdbcSink.sink(
        transactions_insert_sql,
        transaction_type,
        lambda ps, r: [
            ps.set_string(1, r[0]),
            ps.set_string(2, r[1]),
            ps.set_string(3, r[2]),
            ps.set_string(4, r[3]),
            ps.set_double(5, r[4]),
            ps.set_int(6, r[5]),
            ps.set_string(7, r[6]),
            ps.set_double(8, r[7]),
            ps.set_string(9, r[8]),
            ps.set_string(10, r[9]),
            ps.set_timestamp(11, java.sql.Timestamp.from(r[10])),
            ps.set_string(12, r[11]),
        ],
        exec_options,
        conn_options
    )
).name("Upsert transactions")

# Aggregation: sales_per_category (assuming table already exists)
sales_per_category_type = Types.ROW([
    Types.SQL_DATE(),
    Types.STRING(),
    Types.DOUBLE()
])

sales_per_category_stream = transaction_stream.map(
    lambda t: Row(date.today(), t[3], t[7]),
    output_type=sales_per_category_type
).key_by(lambda x: x[1]).reduce(
    lambda a, b: Row(a[0], a[1], a[2] + b[2])
)

sales_per_category_insert_sql = """
INSERT INTO sales_per_category (transaction_date, category, total_sales)
VALUES (?, ?, ?)
ON CONFLICT (transaction_date, category)
DO UPDATE SET total_sales = EXCLUDED.total_sales
"""

sales_per_category_stream.add_sink(
    JdbcSink.sink(
        sales_per_category_insert_sql,
        sales_per_category_type,
        lambda ps, r: [
            ps.set_date(1, java.sql.Date.valueOf(r[0].isoformat())),
            ps.set_string(2, r[1]),
            ps.set_double(3, r[2]),
        ],
        exec_options,
        conn_options
    )
).name("Upsert sales_per_category")

env.execute("Flink Ecommerce Realtime Streaming")
