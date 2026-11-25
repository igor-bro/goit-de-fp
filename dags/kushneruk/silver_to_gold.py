import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def main():
    import os

    os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 2g pyspark-shell"
    import bootstrap_spark

    bootstrap_spark.prepare()
    from pyspark.sql.functions import avg, col, current_timestamp, round as spark_round, when

    from layout import GOLD, SILVER

    spark = bootstrap_spark.session("KushnerukSilverToGold")
    try:
        # Етап 3. Читання silver/athlete_bio і silver/athlete_event_results.
        bio = spark.read.parquet(str(SILVER / "athlete_bio"))
        events = spark.read.parquet(str(SILVER / "athlete_event_results"))

        # Зріст і вага мають бути числовими до розрахунку середнього.
        bio = (
            bio.withColumn("height", col("height").cast("double"))
            .withColumn("weight", col("weight").cast("double"))
            .filter(
                col("athlete_id").isNotNull()
                & col("height").isNotNull()
                & col("weight").isNotNull()
            )
            .dropDuplicates(["athlete_id"])
            .select(
                col("athlete_id").cast("int").alias("athlete_id"),
                "sex",
                "height",
                "weight",
            )
        )
        events = events.select(
            col("athlete_id").cast("int").alias("athlete_id"),
            "sport",
            when(col("medal").isNull() | (col("medal") == ""), "nan")
            .otherwise(col("medal"))
            .alias("medal"),
            "country_noc",
        )

        joined = events.join(bio, "athlete_id", "inner")
        df = (
            joined.groupBy("sport", "medal", "sex", "country_noc")
            .agg(
                spark_round(avg("height"), 2).alias("avg_height"),
                spark_round(avg("weight"), 2).alias("avg_weight"),
            )
            .withColumn("timestamp", current_timestamp())
        )
        # Етап 3. Запис агрегату в gold/avg_stats.
        df.write.mode("overwrite").parquet(str(GOLD / "avg_stats"))
        print("gold/avg_stats", flush=True)
        df.show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
