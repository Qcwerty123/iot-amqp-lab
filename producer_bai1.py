"""Bai 1 - Producer: gui message vao queue iot_lab_queue."""
import sys
import pika

# ---- Cau hinh (sua cho dung thong tin cua ban) ----
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"
QUEUE_NAME = "iot_lab_queue"

HO_TEN = "CAO CONG HOE"
MA_SV = "B23DCCN3349"


def main():
    # So message muon gui (mac dinh 1). Vi du: python producer_bai1.py 5
    so_message = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    # 1. Ket noi toi RabbitMQ broker
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT,
                                       credentials=credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # 2. Tao queue (neu da co thi dung lai)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # 3. Gui message
    for i in range(1, so_message + 1):
        message = f"Xin chao tu ung dung Python AMQP - {MA_SV} - {HO_TEN}"
        if so_message > 1:
            message += f" (#{i})"
        channel.basic_publish(
            exchange="",              # default exchange
            routing_key=QUEUE_NAME,   # routing key = ten queue
            body=message.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2),  # message ben vung
        )
        print(f"Da gui: {message}")

    # 4. Dong ket noi
    connection.close()


if __name__ == "__main__":
    main()