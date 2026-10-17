# Thực hành: Lập trình Python với giao thức AMQP

## 1. Broker sử dụng

| Thành phần | Giá trị |
|---|---|
| Broker | RabbitMQ 4.3.6, cài trên máy cá nhân |
| Giao thức | AMQP 0-9-1 |
| Host / Port | `localhost` / `5672` |
| Tài khoản | `guest` / `guest` (mặc định) |
| Thư viện Python | `pika` 1.4.4 |

Muốn đổi sang broker dùng chung của lớp thì sửa các hằng số `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASS` ở đầu mỗi file `.py`.

Các queue và exchange đều được khai báo `durable=True`, còn message được gửi với `delivery_mode=2`, nên dữ liệu vẫn còn khi broker khởi động lại. Consumer xác nhận (`basic_ack`) sau khi xử lý xong từng message.

### Cài đặt

```bash
pip install pika
```

Trước khi chạy, cần kiểm tra RabbitMQ đang hoạt động. Trang quản trị nằm ở http://localhost:15672 nếu đã bật plugin `rabbitmq_management`.

## 2. Danh sách file

| File | Vai trò |
|---|---|
| `producer_bai1.py` | Bài 1: gửi lời chào vào `iot_lab_queue` |
| `consumer_bai1.py` | Bài 1: nhận message từ `iot_lab_queue` |
| `sensor_producer_bai2.py` | Bài 2: mô phỏng 3 cảm biến gửi JSON mỗi 3 giây |
| `monitor_consumer_bai2.py` | Bài 2: phân tích dữ liệu và phát cảnh báo |
| `alert_producer_bai3.py` | Bài 3: gửi cảnh báo vào direct exchange `iot_alert_exchange` |
| `warning_consumer_bai3.py` | Bài 3: chỉ nhận cảnh báo mức `warning` |
| `critical_consumer_bai3.py` | Bài 3: chỉ nhận cảnh báo mức `critical` |
| `all_alerts_consumer_bai3.py` | Bài 3 (mở rộng): nhận tất cả cảnh báo qua topic exchange và ghi log ra file |

## 3. Cách chạy từng chương trình

Với mỗi bài, nên **chạy consumer trước** trong một terminal, sau đó chạy producer ở terminal khác. Consumer chạy liên tục cho tới khi nhấn `Ctrl+C`.

### Bài 1: Gửi và nhận message cơ bản qua queue

Mô hình: `producer → iot_lab_queue → consumer` (dùng default exchange, routing key là tên queue).

```bash
# Terminal 1
python consumer_bai1.py

# Terminal 2
python producer_bai1.py        # gửi 1 message
python producer_bai1.py 5      # mở rộng: gửi 5 message liên tiếp
```

### Bài 2: Mô phỏng cảm biến IoT gửi dữ liệu môi trường

Mô hình: `sensor producer → sensor_data_queue → monitoring consumer`.

```bash
# Terminal 1
python monitor_consumer_bai2.py

# Terminal 2
python sensor_producer_bai2.py   # gửi liên tục mỗi 3 giây, Ctrl+C để dừng
```

- Nhiệt độ được sinh ngẫu nhiên trong khoảng 25–40 °C, độ ẩm trong khoảng 30–80 %.
- Ngưỡng cảnh báo: nhiệt độ **> 35** và độ ẩm **< 40**.
- Payload JSON có dạng:

```json
{"device_id": "sensor01", "temperature": 29.5, "humidity": 62.1, "timestamp": "2026-10-17 14:42:17"}
```

### Bài 3: Hệ thống điều phối cảnh báo IoT với exchange

Mô hình:

```
                                   ┌─ routing key "warning"  → warning_queue  → warning consumer
alert producer → iot_alert_exchange┤
   (direct)                        └─ routing key "critical" → critical_queue → critical consumer
                                     (routing key "info" không được queue nào bind, nên message bị bỏ)

alert producer → iot_alert_topic_exchange ── binding "alert.#" → all_alerts_queue → all alerts consumer
   (topic, mở rộng)                                                                   └→ alerts_log.txt
```

```bash
# Terminal 1
python warning_consumer_bai3.py

# Terminal 2
python critical_consumer_bai3.py

# Terminal 3 (mở rộng, không bắt buộc)
python all_alerts_consumer_bai3.py

# Terminal 4
python alert_producer_bai3.py                                  # gửi 3 cảnh báo mẫu: info, warning, critical
python alert_producer_bai3.py warning "Do am kho hang thap"    # gửi 1 cảnh báo tự nhập
```

Mức hợp lệ là `info`, `warning` hoặc `critical`. Mỗi cảnh báo được gửi vào cả hai exchange: direct exchange với routing key `<muc>` và topic exchange với routing key `alert.<muc>`.

> **Lưu ý:** consumer phải chạy ít nhất một lần trước producer để queue được tạo và bind vào exchange. Nếu chưa có queue nào bind, message gửi vào exchange sẽ bị bỏ.

## 4. Kết quả đạt được

Toàn bộ kết quả dưới đây được chạy trên broker RabbitMQ local.

### Bài 1

```
Da gui: Xin chao tu ung dung Python AMQP - B23DCCN3349 - CAO CONG HOE (#1)
Da gui: Xin chao tu ung dung Python AMQP - B23DCCN3349 - CAO CONG HOE (#2)
```

```
Dang lang nghe queue 'iot_lab_queue'... Nhan Ctrl+C de dung.
Da nhan message: Xin chao tu ung dung Python AMQP - B23DCCN3349 - CAO CONG HOE (#1)
Thoi gian nhan: 14:42:14
--------------------------------------------------
Da nhan message: Xin chao tu ung dung Python AMQP - B23DCCN3349 - CAO CONG HOE (#2)
Thoi gian nhan: 14:42:14
--------------------------------------------------
```

- [x] Kết nối được broker
- [x] Gửi được message vào queue (hỗ trợ gửi nhiều message liên tiếp)
- [x] Consumer nhận và hiển thị đúng nội dung cùng thời điểm nhận, chạy liên tục tới khi nhấn Ctrl+C

### Bài 2

Producer:

```
Bat dau gui du lieu vao 'sensor_data_queue' moi 3 giay. Nhan Ctrl+C de dung.
Da gui: {"device_id": "sensor01", "temperature": 27.6, "humidity": 44.5, "timestamp": "2026-10-17 14:42:17"}
Da gui: {"device_id": "sensor02", "temperature": 31.8, "humidity": 65.8, "timestamp": "2026-10-17 14:42:17"}
Da gui: {"device_id": "sensor03", "temperature": 39.2, "humidity": 72.0, "timestamp": "2026-10-17 14:42:17"}
Da gui: {"device_id": "sensor01", "temperature": 31.7, "humidity": 30.6, "timestamp": "2026-10-17 14:42:20"}
...
```

Consumer:

```
Device: sensor03
Time: 2026-10-17 14:42:17
Temperature: 39.2
Humidity: 72.0
CANH BAO: Nhiet do cao
----------------------------------------
Device: sensor01
Time: 2026-10-17 14:42:20
Temperature: 31.7
Humidity: 30.6
CANH BAO: Do am thap
----------------------------------------
Device: sensor02
Time: 2026-10-17 14:42:20
Temperature: 33.4
Humidity: 61.8
----------------------------------------
```

- [x] Payload đúng định dạng JSON (`content_type=application/json`); consumer bỏ qua payload không phải JSON
- [x] Gửi dữ liệu tuần hoàn mỗi 3 giây
- [x] Consumer xử lý đúng và phát hiện cảnh báo nhiệt độ cao và độ ẩm thấp
- [x] Mở rộng: có trường thời gian gửi `timestamp` và mô phỏng 3 cảm biến `sensor01`, `sensor02`, `sensor03`

### Bài 3

Producer gửi 3 cảnh báo:

```
[info] Da gui: Cam bien phong hoc A1 hoat dong binh thuong info
[warning] Da gui: Nhiet do phong may vuot nguong warning
[critical] Da gui: Cam bien kho lanh mat ket noi critical
```

Warning consumer chỉ nhận mức `warning`:

```
[warning_queue] Da nhan: Nhiet do phong may vuot nguong warning
```

Critical consumer chỉ nhận mức `critical`:

```
[critical_queue] Da nhan: Cam bien kho lanh mat ket noi critical
```

Consumer mở rộng (topic exchange) nhận cả 3 mức:

```
[all_alerts_queue] Da nhan: 2026-10-17 14:42:24 [info] Cam bien phong hoc A1 hoat dong binh thuong info
[all_alerts_queue] Da nhan: 2026-10-17 14:42:24 [warning] Nhiet do phong may vuot nguong warning
[all_alerts_queue] Da nhan: 2026-10-17 14:42:24 [critical] Cam bien kho lanh mat ket noi critical
```

Message `info` không được `warning_queue` hay `critical_queue` nhận, đúng như yêu cầu.

- [x] Tạo được direct exchange `iot_alert_exchange`
- [x] Bind `warning_queue` với routing key `warning` và `critical_queue` với routing key `critical`
- [x] Message được phân phối đúng nơi nhận
- [x] Mở rộng: topic exchange `iot_alert_topic_exchange` có consumer nhận tất cả cảnh báo (binding `alert.#`)
- [x] Mở rộng: ghi log cảnh báo ra file `alerts_log.txt`
