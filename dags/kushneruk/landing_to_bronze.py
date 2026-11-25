import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def download_data(local_file_path):
    # Етап 1. Завантаження CSV з FTP в оригінальному форматі.
    import requests

    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}", flush=True)
    response = requests.get(downloading_url, timeout=180)
    if response.status_code == 200:
        with open(local_file_path + ".csv", "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}", flush=True)
    else:
        raise SystemExit(f"Failed to download the file. Status code: {response.status_code}")


def _strip_bom(df):
    for name in df.columns:
        if name.startswith("\ufeff"):
            df = df.withColumnRenamed(name, name.replace("\ufeff", ""))
    return df


def main():
    import os

    os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 2g pyspark-shell"
    import bootstrap_spark

    bootstrap_spark.prepare()
    from layout import BRONZE, LANDING, TABLES

    LANDING.mkdir(parents=True, exist_ok=True)
    saved = os.getcwd()
    os.chdir(LANDING)
    try:
        for table in TABLES:
            download_data(table)
    finally:
        os.chdir(saved)

    spark = bootstrap_spark.session("KushnerukLandingToBronze")
    try:
        for table in TABLES:
            df = _strip_bom(
                spark.read.option("header", True).option("escape", '"').csv(str(LANDING / f"{table}.csv"))
            )
            # Етап 1. Збереження прочитаного CSV у parquet, тека bronze/{table}.
            target = BRONZE / table
            df.write.mode("overwrite").parquet(str(target))
            print(f"bronze/{table}", flush=True)
            df.show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
