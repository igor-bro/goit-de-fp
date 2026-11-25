import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def clean_text(text):
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', "", str(text))


def main():
    import os

    os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 2g pyspark-shell"
    import bootstrap_spark

    bootstrap_spark.prepare()
    from pyspark.sql.functions import col, udf
    from pyspark.sql.types import StringType

    from layout import BRONZE, SILVER, TABLES

    # Етап 2. Чистка текстових колонок і дедублікація, запис у silver/{table}.
    clean_text_udf = udf(clean_text, StringType())
    spark = bootstrap_spark.session("KushnerukBronzeToSilver")
    try:
        for table in TABLES:
            df = spark.read.parquet(str(BRONZE / table))
            for field in df.schema.fields:
                if isinstance(field.dataType, StringType):
                    df = df.withColumn(field.name, clean_text_udf(col(field.name)))
            df = df.distinct()
            df.write.mode("overwrite").parquet(str(SILVER / table))
            print(f"silver/{table}", flush=True)
            df.show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
