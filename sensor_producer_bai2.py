"""Bai 2 - Sensor Producer: mo phong cam bien IoT gui du lieu moi 3 giay."""
import json
import random
import time
from datetime import datetime
import pika

# ---- Cau hinh ----
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"
QUEUE_NAME = "sensor_data_queue"

CHU_KY_GUI = 3  # giay
DANH_SACH_CAM_BIEN = ["sensor01", "sensor02", "sensor03"]  # mo rong: nhieu cam bien


def doc_cam_bien(device_id):
    """Sinh du lieu ngau nhien cho 1 cam bien."""
    return {
        "device_id": device_id,
        "temperature": round(random.uniform(25.0, 40.0), 1),  # do C
        "humidity": round(random.uniform(30.0, 80.0), 1),     # %
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # mo rong: thoi gian gui
    }


def main():
    # 1. Ket noi broker
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT,
                                       credentials=credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # 2. Khai bao queue
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    print(f"Bat dau gui du lieu vao '{QUEUE_NAME}' moi {CHU_KY_GUI} giay. Nhan Ctrl+C de dung.")
    try:
        # 3. Gui tuan hoan
        while True:
            for device_id in DANH_SACH_CAM_BIEN:
                data = doc_cam_bien(device_id)
                payload = json.dumps(data)  # dict -> chuoi JSON
                channel.basic_publish(
                    exchange="",
                    routing_key=QUEUE_NAME,
                    body=payload.encode("utf-8"),
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=2,
                    ),
                )
                print(f"Da gui: {payload}")
            time.sleep(CHU_KY_GUI)
    except KeyboardInterrupt:
        print("\nDa dung producer.")
    finally:
        # 4. Dong ket noi
        connection.close()


if __name__ == "__main__":
    main()