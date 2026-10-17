"""Bai 3 - Alert Producer: gui canh bao vao direct exchange voi routing key theo muc do.

Cach chay:
    python alert_producer_bai3.py                       -> gui 3 canh bao mau (info, warning, critical)
    python alert_producer_bai3.py warning "Noi dung"    -> gui 1 canh bao tu nhap
"""
import sys
import pika

# ---- Cau hinh ----
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"

EXCHANGE_NAME = "iot_alert_exchange"          # direct exchange (theo de bai)
TOPIC_EXCHANGE_NAME = "iot_alert_topic_exchange"  # mo rong: topic exchange

MUC_HOP_LE = ["info", "warning", "critical"]

CANH_BAO_MAU = [
    ("info", "Cam bien phong hoc A1 hoat dong binh thuong info"),
    ("warning", "Nhiet do phong may vuot nguong warning"),
    ("critical", "Cam bien kho lanh mat ket noi critical"),
]


def main():
    # Xac dinh danh sach canh bao can gui
    if len(sys.argv) >= 3:
        muc = sys.argv[1].lower()
        if muc not in MUC_HOP_LE:
            print(f"Muc '{muc}' khong hop le. Chon mot trong: {MUC_HOP_LE}")
            return
        danh_sach = [(muc, " ".join(sys.argv[2:]))]
    else:
        danh_sach = CANH_BAO_MAU

    # 1. Ket noi broker
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT,
                                       credentials=credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # 2. Tao direct exchange
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)
    # Mo rong: tao them topic exchange cho consumer nhan tat ca canh bao
    channel.exchange_declare(exchange=TOPIC_EXCHANGE_NAME, exchange_type="topic", durable=True)

    # 3. Gui canh bao: routing key = muc do canh bao
    for muc, noi_dung in danh_sach:
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=muc,                 # info / warning / critical
            body=noi_dung.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        # Mo rong: gui ban sao sang topic exchange voi routing key dang alert.<muc>
        channel.basic_publish(
            exchange=TOPIC_EXCHANGE_NAME,
            routing_key=f"alert.{muc}",
            body=noi_dung.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"[{muc}] Da gui: {noi_dung}")

    # 4. Dong ket noi
    connection.close()


if __name__ == "__main__":
    main()