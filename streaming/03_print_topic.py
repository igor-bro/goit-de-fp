import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from kafka import KafkaConsumer

from streaming.configs import OUTPUT_TOPIC, kafka_admin_params


def main():
    params = kafka_admin_params()
    consumer = KafkaConsumer(
        OUTPUT_TOPIC,
        **params,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=15000,
        group_id="kushneruk_fp_print",
    )
    printed = 0
    for message in consumer:
        key = message.key.decode("utf-8") if message.key else ""
        value = message.value.decode("utf-8") if message.value else ""
        print(f"partition={message.partition} key={key} value={value}", flush=True)
        printed += 1
        if printed >= 15:
            break
    consumer.close()
    if printed == 0:
        raise SystemExit(f"У топіку {OUTPUT_TOPIC} немає повідомлень.")
    print(f"Показано {printed} повідомлень з {OUTPUT_TOPIC}", flush=True)


if __name__ == "__main__":
    main()
