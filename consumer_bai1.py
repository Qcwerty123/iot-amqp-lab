"""Bai 1 - Consumer: lang nghe queue iot_lab_queue va in message nhan duoc."""
from datetime import datetime
import pika

# ---- Cau hinh (giong voi producer) ----
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"
QUEUE_NAME = "iot_lab_queue"


def callback(ch, method, properties, body):
    """Ham duoc goi moi khi co message den."""
    message = body.decode("utf-8")
    thoi_gian = datetime.now().strftime("%H:%M:%S")
    print(f"Da nhan message: {message}")
    print(f"Thoi gian nhan: {thoi_gian}")
    print("-" * 50)
    ch.basic_ack(delivery_tag=method.delivery_tag)  # xac nhan da xu ly


def main():
    # 1. Ket noi toi cung broker
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT,
                                       credentials=credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # 2. Khai bao queue (phai giong tham so voi producer)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # 3. Dang ky ham callback de nhan message
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    print(f"Dang lang nghe queue '{QUEUE_NAME}'... Nhan Ctrl+C de dung.")
    try:
        channel.start_consuming()   # chay lien tuc
    except KeyboardInterrupt:
        print("\nDa dung consumer.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()