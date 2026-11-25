# Фінальний проєкт. Стримінг і пакетне озеро

Дві незалежні частини. Пароль MySQL лежить лише в `.env` і в репозиторій не потрапляє.

## Частина 1. Kafka і Spark Streaming

Локальний брокер цієї роботи слухає `localhost:19092`. Порт 9092 на машині зайнятий іншим проєктом, тому його не чіпаємо.

```bash
docker compose up -d
py -3.11 streaming/01_events_to_kafka.py
py -3.11 streaming/02_streaming_pipeline.py
py -3.11 streaming/03_print_topic.py
```

`01` читає `olympic_dataset.athlete_event_results` і пише JSON у топік `athlete_event_results`. `02` читає `athlete_bio`, відкидає порожній і нечисловий зріст або вагу, читає топік, з'єднує за `athlete_id` і рахує середні зріст і вагу за `sport`, `medal`, `sex`, `country_noc`. `foreachBatch` пише агрегат у топік `athlete_avg_stats` і в таблицю `kushneruk.athlete_avg_stats`. `03` друкує перші повідомлення вихідного топіка.

Значення `nan` у колонці медалі лишається окремою групою: це виступ без медалі. `country_noc` береться з результату змагань.

## Частина 2. Landing, bronze, silver, gold

CSV качаються з `https://ftp.goit.study/neoversity/`. Spark-завдання спочатку запускаються локально:

```bash
py -3.11 dags/kushneruk/landing_to_bronze.py
py -3.11 dags/kushneruk/bronze_to_silver.py
py -3.11 dags/kushneruk/silver_to_gold.py
```

`project_solution.py` це DAG Airflow. Три `SparkSubmitOperator` ідуть по черзі і дивляться на з'єднання `spark-default`. Шляхи до скриптів відносно кореня репозиторію: `dags/kushneruk/`.

Потрібні Python 3.11, Java 17 і `jars/mysql-connector-j-8.0.32.jar`.
