import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-memory 2g "
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
    "pyspark-shell"
)

import bootstrap_spark
from pyspark.sql.functions import col, struct, to_json

from streaming.configs import INPUT_TOPIC, ensure_topic, jdbc_options, spark_kafka_options
from layout import JAR

bootstrap_spark.prepare()


def main():
    # Етап 3. Запис таблиці athlete_event_results у Kafka-топік athlete_event_results.
    ensure_topic(INPUT_TOPIC)
    spark = bootstrap_spark.session("KushnerukEventsToKafka", JAR)
    events = spark.read.format("jdbc").options(**jdbc_options("athlete_event_results")).load()
    print(f"Прочитано з MySQL рядків: {events.count()}", flush=True)

    payload = events.select(
        col("athlete_id").cast("string").alias("key"),
        to_json(struct(*[col(name) for name in events.columns])).alias("value"),
    )
    (
        payload.write.format("kafka")
        .options(**spark_kafka_options())
        .option("topic", INPUT_TOPIC)
        .save()
    )
    print(f"Записано в топік {INPUT_TOPIC}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
