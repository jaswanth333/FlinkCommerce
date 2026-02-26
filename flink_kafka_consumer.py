"""Flink Kafka consumer for real-time e-commerce transaction processing."""

import json
import os
from datetime import datetime
from pathlib import Path

import psycopg2
from pyflink.common import Row, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction


# -----------------------------------------------------------------------------
# 1) Runtime setup
# -----------------------------------------------------------------------------
# Optional: Set your JDK path if Java is not already configured in your system.
os.environ["JAVA_HOME"] = r"C:\Users\SKUNK11\.jdk\jdk-17.0.16"

# Kafka settings
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "financial_transactions"
GROUP_ID = "flink-consumer-group"

# PostgreSQL settings
JDBC_URL = "jdbc:postgresql://localhost:5432/postgres"
DB_USERNAME = "postgres"
DB_PASSWORD = "postgres"


# -----------------------------------------------------------------------------
# 2) SQL: table creation + aggregate upserts
# -----------------------------------------------------------------------------
CREATE_TABLES_SQL = [
    "CREATE TABLE IF NOT EXISTS transactions ("
    "transaction_id VARCHAR(255) PRIMARY KEY, "
    "product_id VARCHAR(255), "
    "product_name VARCHAR(255), "
    "product_category VARCHAR(255), "
    "product_price DOUBLE PRECISION, "
    "product_quantity INTEGER, "
    "product_brand VARCHAR(255), "
    "total_amount DOUBLE PRECISION, "
    "currency VARCHAR(255), "
    "customer_id VARCHAR(255), "
    "transaction_date TIMESTAMP, "
    "payment_method VARCHAR(255))",
    "CREATE TABLE IF NOT EXISTS sales_per_category ("
    "transaction_date DATE, "
    "category VARCHAR(255), "
    "total_sales DOUBLE PRECISION, "
    "PRIMARY KEY (transaction_date, category))",
    "CREATE TABLE IF NOT EXISTS sales_per_day ("
    "transaction_date DATE PRIMARY KEY, "
    "total_sales DOUBLE PRECISION)",
    "CREATE TABLE IF NOT EXISTS sales_per_month ("
    "year INTEGER, "
    "month INTEGER, "
    "total_sales DOUBLE PRECISION, "
    "PRIMARY KEY (year, month))",
]

UPSERT_STATEMENTS = {
    "sales_per_category": (
        "INSERT INTO sales_per_category(transaction_date, category, total_sales) "
        "VALUES (%s, %s, %s) "
        "ON CONFLICT (transaction_date, category) DO UPDATE SET "
        "total_sales = sales_per_category.total_sales + EXCLUDED.total_sales"
    ),
    "sales_per_day": (
        "INSERT INTO sales_per_day(transaction_date, total_sales) "
        "VALUES (%s, %s) "
        "ON CONFLICT (transaction_date) DO UPDATE SET "
        "total_sales = sales_per_day.total_sales + EXCLUDED.total_sales"
    ),
    "sales_per_month": (
        "INSERT INTO sales_per_month(year, month, total_sales) "
        "VALUES (%s, %s, %s) "
        "ON CONFLICT (year, month) DO UPDATE SET "
        "total_sales = sales_per_month.total_sales + EXCLUDED.total_sales"
    ),
}


# -----------------------------------------------------------------------------
# 3) Helper functions
# -----------------------------------------------------------------------------
def parse_jdbc_url(jdbc_url: str):
    """
    Parse JDBC URL like:
    jdbc:postgresql://localhost:5432/postgres

    Returns:
        (host, port, database)
    """
    # Remove jdbc prefix
    host_port_db = jdbc_url.replace("jdbc:postgresql://", "", 1)

    # Split host:port and db name
    host_port, database = host_port_db.split("/", 1)
    host, port = host_port.split(":", 1)

    return host, int(port), database


def ensure_tables_exist(jdbc_url: str, username: str, password: str) -> None:
    """Create required tables if they do not exist."""
    host, port, database = parse_jdbc_url(jdbc_url)

    with psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=username,
        password=password,
    ) as conn:
        with conn.cursor() as cursor:
            for sql in CREATE_TABLES_SQL:
                cursor.execute(sql)
            conn.commit()


# -----------------------------------------------------------------------------
# 4) Flink map functions
# -----------------------------------------------------------------------------
class JsonToTransaction(MapFunction):
    """
    Convert incoming Kafka JSON string into a Flink Row
    with the exact schema expected by transaction_type.
    """

    def map(self, value: str):
        data = json.loads(value)

        # Build row in fixed column order
        return Row(
            data["transactionId"],                  # 0
            data["productId"],                      # 1
            data["productName"],                    # 2
            data["productCategory"],                # 3
            float(data["productPrice"]),            # 4
            int(data["productQuantity"]),           # 5
            data["productBrand"],                   # 6
            float(data.get("totalAmount", 0.0)),    # 7
            data["currency"],                       # 8
            data["customerId"],                     # 9
            datetime.fromisoformat(data["transactionDate"]),  # 10
            data["paymentMethod"],                  # 11
        )


class PostgresUpsertMap(MapFunction):
    """
    For each transaction:
      1) If the transaction already exists, subtract old amount from aggregates.
      2) Upsert the current transaction row.
      3) Add current amount to aggregates.
    """

    def __init__(self, jdbc_url: str, username: str, password: str):
        self.jdbc_url = jdbc_url
        self.username = username
        self.password = password
        self.conn = None
        self.cursor = None

    def open(self, runtime_context):
        """Open PostgreSQL connection once per task."""
        host, port, database = parse_jdbc_url(self.jdbc_url)
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=self.username,
            password=self.password,
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def _apply_sales_upserts(self, tx_date, category: str, amount: float):
        """Execute aggregate upserts (category/day/month)."""
        if self.cursor is None:
            raise RuntimeError("Postgres cursor is not initialized")

        self.cursor.execute(
            UPSERT_STATEMENTS["sales_per_category"],
            (tx_date, category, amount),
        )
        self.cursor.execute(
            UPSERT_STATEMENTS["sales_per_day"],
            (tx_date, amount),
        )
        self.cursor.execute(
            UPSERT_STATEMENTS["sales_per_month"],
            (tx_date.year, tx_date.month, amount),
        )

    def map(self, row: Row):
        """Process one transaction row."""
        if self.cursor is None:
            raise RuntimeError("Postgres cursor is not initialized")

        transaction_id = row[0]
        new_category = row[3]
        new_amount = float(row[7] or 0.0)
        tx_timestamp = row[10]
        tx_date = tx_timestamp.date() if isinstance(tx_timestamp, datetime) else tx_timestamp

        # 1) Reverse old aggregate values if transaction already exists
        self.cursor.execute(
            "SELECT product_category, transaction_date::date, total_amount "
            "FROM transactions WHERE transaction_id = %s",
            (transaction_id,),
        )
        existing = self.cursor.fetchone()

        if existing:
            old_category, old_date, old_amount = existing
            self._apply_sales_upserts(
                old_date,
                old_category,
                -float(old_amount or 0.0),  # negative to reverse old contribution
            )

        # 2) Upsert the base transaction table
        self.cursor.execute(
            "INSERT INTO transactions("
            "transaction_id, product_id, product_name, product_category, "
            "product_price, product_quantity, product_brand, total_amount, "
            "currency, customer_id, transaction_date, payment_method) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (transaction_id) DO UPDATE SET "
            "product_id = EXCLUDED.product_id, "
            "product_name = EXCLUDED.product_name, "
            "product_category = EXCLUDED.product_category, "
            "product_price = EXCLUDED.product_price, "
            "product_quantity = EXCLUDED.product_quantity, "
            "product_brand = EXCLUDED.product_brand, "
            "total_amount = EXCLUDED.total_amount, "
            "currency = EXCLUDED.currency, "
            "customer_id = EXCLUDED.customer_id, "
            "transaction_date = EXCLUDED.transaction_date, "
            "payment_method = EXCLUDED.payment_method",
            (
                row[0], row[1], row[2], row[3], row[4], row[5],
                row[6], row[7], row[8], row[9], row[10], row[11],
            ),
        )

        # 3) Apply current aggregate values
        self._apply_sales_upserts(tx_date, new_category, new_amount)

        return row

    def close(self):
        """Close DB resources."""
        if self.cursor is not None:
            self.cursor.close()
        if self.conn is not None:
            self.conn.close()


# -----------------------------------------------------------------------------
# 5) Main Flink job
# -----------------------------------------------------------------------------
def main():
    """Start Flink streaming job."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Add required jars for Kafka source
    libs_dir = Path(__file__).resolve().parent / "libs"
    kafka_connector_jar = (libs_dir / "flink-connector-kafka-4.0.1-2.0.jar").as_uri()
    kafka_clients_jar = (libs_dir / "kafka-clients-3.7.1.jar").as_uri()
    env.add_jars(kafka_connector_jar, kafka_clients_jar)

    # Build Kafka source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(TOPIC)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Define Row schema (must match JsonToTransaction)
    transaction_type = Types.ROW(
        [
            Types.STRING(),        # transaction_id
            Types.STRING(),        # product_id
            Types.STRING(),        # product_name
            Types.STRING(),        # product_category
            Types.DOUBLE(),        # product_price
            Types.INT(),           # product_quantity
            Types.STRING(),        # product_brand
            Types.DOUBLE(),        # total_amount
            Types.STRING(),        # currency
            Types.STRING(),        # customer_id
            Types.SQL_TIMESTAMP(), # transaction_date
            Types.STRING(),        # payment_method
        ]
    )

    # Create stream from Kafka JSON -> Row
    data_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka source",
    ).map(JsonToTransaction(), output_type=transaction_type)

    # Ensure DB tables exist before processing
    ensure_tables_exist(JDBC_URL, DB_USERNAME, DB_PASSWORD)

    print("Starting Flink job: Kafka -> PostgreSQL transactions + aggregates")

    # Write transactions + aggregate upserts
    data_stream.map(
        PostgresUpsertMap(JDBC_URL, DB_USERNAME, DB_PASSWORD),
        output_type=transaction_type,
    ).name("Upsert transactions and sales aggregates").print()

    env.execute("Flink Ecommerce Realtime Streaming")


if __name__ == "__main__":
    main()
