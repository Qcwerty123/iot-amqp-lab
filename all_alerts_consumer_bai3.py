"""Bai 3 - MO RONG: consumer nhan TAT CA canh bao qua topic exchange va ghi log ra file."""
from datetime import datetime
import pika

# ---- Cau hinh ----
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"

TOPIC_EXCHANGE_NAME = "iot_alert_topic_exchange"
QUEUE_NAME = "all_alerts_queue"
BINDING_KEY = "alert.#"      # '#' khop moi thu phia sau: alert.info, alert.warning, alert.critical
LOG_FILE = "alerts_log.txt"


def callback(ch, method, properties, body):
    noi_dung = body.decode("utf-8")
    muc = method.routing_key.split(".")[-1]       # "alert.warning" -> "warning"
    thoi_gian = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dong = f"{thoi_gian} [{muc}] {noi_dung}"

    print(f"[{QUEUE_NAME}] Da nhan: {dong}")
    with open(LOG_FILE, "a", encoding="utf-8") as f:   # ghi log ra file
        f.write(dong + "\n")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT,
                                       credentials=credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.exchange_declare(exchange=TOPIC_EXCHANGE_NAME, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(queue=QUEUE_NAME, exchange=TOPIC_EXCHANGE_NAME, routing_key=BINDING_KEY)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    print(f"[{QUEUE_NAME}] Dang nhan tat ca canh bao ('{BINDING_KEY}'), ghi log vao {LOG_FILE}. Ctrl+C de dung.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung consumer.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()