import os
import shutil
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
from pyspark.sql.functions import avg, col, current_timestamp, from_json, round as spark_round
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from layout import JAR
from streaming.configs import (
    INPUT_TOPIC,
    OUTPUT_TOPIC,
    SINK_TABLE,
    ensure_topic,
    jdbc_options,
    mysql_connect,
    spark_kafka_options,
)

bootstrap_spark.prepare()

CHECKPOINT = ROOT / "checkpoints" / "athlete_avg"

EVENT_SCHEMA = StructType(
    [
        StructField("edition", StringType(), True),
        StructField("edition_id", IntegerType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", LongType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", IntegerType(), True),
        StructField("pos", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("isTeamSport", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)


def prepare_sink():
    schema, table = SINK_TABLE.split(".", 1)
    conn = mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {schema}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    sport VARCHAR(255) NULL,
                    medal VARCHAR(32) NULL,
                    sex VARCHAR(16) NULL,
                    country_noc VARCHAR(16) NULL,
                    avg_height DOUBLE NULL,
                    avg_weight DOUBLE NULL,
                    `timestamp` DATETIME NULL
                )
                """
            )
            cur.execute(f"DELETE FROM {schema}.{table}")
    finally:
        conn.close()
    print(f"Таблиця {SINK_TABLE} готова, попередні рядки прибрані", flush=True)


def publish(batch_df, batch_id):
    rows = batch_df.cache()
    count = rows.count()
    print(f"Мікробатч {batch_id}: груп {count}", flush=True)
    if count == 0:
        rows.unpersist()
        return

    rows.orderBy("sport", "medal", "sex", "country_noc").show(10, truncate=False)

    # Етап 6.а): запис у вихідний Kafka-топік.
    kafka_payload = rows.select(
        col("sport").cast("string").alias("key"),
        col("sport"),
        col("medal"),
        col("sex"),
        col("country_noc"),
        col("avg_height"),
        col("avg_weight"),
        col("timestamp").cast("string").alias("timestamp"),
    )
    from pyspark.sql.functions import struct, to_json

    (
        kafka_payload.select(
            col("key"),
            to_json(struct(
                "sport", "medal", "sex", "country_noc", "avg_height", "avg_weight", "timestamp"
            )).alias("value"),
        )
        .write.format("kafka")
        .options(**spark_kafka_options())
        .option("topic", OUTPUT_TOPIC)
        .save()
    )
    print(f"Етап 6.а) записано в топік {OUTPUT_TOPIC}", flush=True)

    # Етап 6.б): запис у базу даних.
    schema, table = SINK_TABLE.split(".", 1)
    conn = mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {schema}.{table}")
    finally:
        conn.close()
    (
        rows.select(
            "sport", "medal", "sex", "country_noc", "avg_height", "avg_weight", "timestamp"
        )
        .write.format("jdbc")
        .options(**{key: value for key, value in jdbc_options(SINK_TABLE).items() if key != "fetchsize"})
        .mode("append")
        .save()
    )
    print(f"Етап 6.б) записано в {SINK_TABLE}", flush=True)
    rows.unpersist()


def main():
    if CHECKPOINT.exists():
        shutil.rmtree(CHECKPOINT)
    ensure_topic(INPUT_TOPIC)
    ensure_topic(OUTPUT_TOPIC)
    prepare_sink()
    spark = bootstrap_spark.session("KushnerukAthleteStream", JAR)
    kafka_options = spark_kafka_options()

    # Етап 1. Зчитування фізичних показників з MySQL таблиці olympic_dataset.athlete_bio.
    bio_raw = spark.read.format("jdbc").options(**jdbc_options("athlete_bio")).load()

    # Етап 2. Відсіювання порожнього або нечислового зросту і ваги.
    # Порожній рядок, nan і діапазон на кшталт 62,67 не стають double.
    bio = (
        bio_raw.withColumn("height", col("height").cast("double"))
        .withColumn("weight", col("weight").cast("double"))
        .filter(
            col("athlete_id").isNotNull()
            & col("height").isNotNull()
            & col("weight").isNotNull()
        )
        .dropDuplicates(["athlete_id"])
        .select("athlete_id", "sex", "height", "weight")
    )
    print(f"Біографій після фільтра зросту і ваги: {bio.count()}", flush=True)

    # Етап 3. Читання топіка athlete_event_results і розбір JSON у колонки.
    raw = (
        spark.readStream.format("kafka")
        .options(**kafka_options)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )
    parsed = (
        raw.filter(col("value").isNotNull())
        .select(
            from_json(
                col("value").cast("string"),
                EVENT_SCHEMA,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
            ).alias("data")
        )
        .select("data.*")
        .filter(col("_corrupt_record").isNull() & col("athlete_id").isNotNull())
    )

    # Етап 4. Об'єднання результатів з біографією за athlete_id.
    # country_noc береться з результату змагань: це НОК, за який атлет виступав.
    events = parsed.select(
        "athlete_id",
        "sport",
        "medal",
        col("country_noc").alias("event_country_noc"),
    )
    joined = events.join(bio, "athlete_id", "inner").select(
        col("sport"),
        col("medal"),
        col("sex"),
        col("event_country_noc").alias("country_noc"),
        col("height"),
        col("weight"),
    )

    # Етап 5. Середні зріст і вага за видом спорту, медаллю, статтю і країною.
    # Значення nan у medal лишається окремою групою: це виступ без медалі.
    averages = (
        joined.groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            spark_round(avg("height"), 2).alias("avg_height"),
            spark_round(avg("weight"), 2).alias("avg_weight"),
        )
        .withColumn("timestamp", current_timestamp())
    )

    writer = (
        averages.writeStream.outputMode("complete")
        .option("checkpointLocation", str(CHECKPOINT))
        .foreachBatch(publish)
    )
    if "--follow" in sys.argv:
        query = writer.trigger(processingTime="15 seconds").start()
        print("Потік працює. Зупинка: Ctrl+C.", flush=True)
    else:
        query = writer.trigger(availableNow=True).start()
        print("Обробляю все, що вже є в топіку, і завершуюсь.", flush=True)
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
