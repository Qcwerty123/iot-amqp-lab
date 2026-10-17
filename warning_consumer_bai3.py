"""Bai 3 - Warning Consumer: chi nhan canh bao muc warning."""
import pika

# ---- Cau hinh ----
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"

EXCHANGE_NAME = "iot_alert_exchange"
QUEUE_NAME = "warning_queue"
ROUTING_KEY = "warning"


def callback(ch, method, properties, body):
    print(f"[{QUEUE_NAME}] Da nhan: {body.decode('utf-8')}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    # 1. Ket noi broker
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT,
                                       credentials=credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # 2. Khai bao exchange (giong producer) va queue
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # 3. Bind queue voi exchange bang routing key "warning"
    channel.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY)

    # 4. Nhan message
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    print(f"[{QUEUE_NAME}] Dang cho canh bao muc '{ROUTING_KEY}'... Nhan Ctrl+C de dung.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung consumer.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()