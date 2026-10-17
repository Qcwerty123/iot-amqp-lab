"""Bai 2 - Monitoring Consumer: nhan du lieu cam bien, phan tich va canh bao."""
import json
import pika

# ---- Cau hinh ----
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"
QUEUE_NAME = "sensor_data_queue"

NGUONG_NHIET_DO = 35   # > 35 do C -> canh bao
NGUONG_DO_AM = 40      # < 40 %    -> canh bao


def callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode("utf-8"))  # chuoi JSON -> dict
    except json.JSONDecodeError:
        print(f"Payload khong dung JSON, bo qua: {body!r}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    device_id = data.get("device_id")
    temperature = data.get("temperature")
    humidity = data.get("humidity")

    print(f"Device: {device_id}")
    if "timestamp" in data:
        print(f"Time: {data['timestamp']}")
    print(f"Temperature: {temperature}")
    print(f"Humidity: {humidity}")

    # Phan tich nguong
    if temperature is not None and temperature > NGUONG_NHIET_DO:
        print("CANH BAO: Nhiet do cao")
    if humidity is not None and humidity < NGUONG_DO_AM:
        print("CANH BAO: Do am thap")
    print("-" * 40)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT,
                                       credentials=credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    print(f"Dang giam sat queue '{QUEUE_NAME}'... Nhan Ctrl+C de dung.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung consumer.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()