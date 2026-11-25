import os
from pathlib import Path

from layout import ROOT


def load_env():
    path = ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_env()

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_SECURITY = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL = os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_USER = os.environ.get("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "")

INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "athlete_event_results")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "athlete_avg_stats")

MYSQL_HOST = os.environ.get("MYSQL_HOST", "217.61.57.46")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "neo_data_admin")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "olympic_dataset")
SINK_TABLE = os.environ.get("SINK_TABLE", "kushneruk.athlete_avg_stats")


def require_password():
    if not MYSQL_PASSWORD:
        raise SystemExit("У .env немає MYSQL_PASSWORD. Скопіюйте .env.example і впишіть пароль з кабінету.")


def kafka_admin_params():
    params = {
        "bootstrap_servers": KAFKA_BOOTSTRAP,
        "security_protocol": KAFKA_SECURITY,
    }
    if KAFKA_SECURITY.startswith("SASL"):
        params["sasl_mechanism"] = KAFKA_SASL
        params["sasl_plain_username"] = KAFKA_USER
        params["sasl_plain_password"] = KAFKA_PASSWORD
    return params


def spark_kafka_options():
    options = {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
        "kafka.security.protocol": KAFKA_SECURITY,
    }
    if KAFKA_SECURITY.startswith("SASL"):
        options["kafka.sasl.mechanism"] = KAFKA_SASL
        options["kafka.sasl.jaas.config"] = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'
        )
    return options


def jdbc_options(table):
    require_password()
    return {
        "url": f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
        "driver": "com.mysql.cj.jdbc.Driver",
        "dbtable": table,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "fetchsize": "10000",
    }


def ensure_topic(name):
    from kafka.admin import KafkaAdminClient, NewTopic

    admin = KafkaAdminClient(**kafka_admin_params(), client_id="kushneruk_fp")
    try:
        admin.create_topics([NewTopic(name=name, num_partitions=3, replication_factor=1)])
        print(f"Створено топік {name}", flush=True)
    except Exception as exc:
        text = str(exc).lower()
        if "already exists" in text or "topicexists" in text:
            print(f"Топік {name} уже є", flush=True)
        else:
            raise
    finally:
        admin.close()


def mysql_connect(database=None):
    import pymysql

    require_password()
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=database or MYSQL_DATABASE,
        connect_timeout=20,
        autocommit=True,
    )
