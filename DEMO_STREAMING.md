# Kịch bản demo streaming

## Mục tiêu trình bày

Chứng minh một interaction mới không cần chờ chạy lại toàn bộ batch pipeline:

```text
event JSONL mới
  -> file landing trên HDFS
  -> Spark Structured Streaming micro-batch
  -> deduplicate + watermark
  -> Parquet partition event_date/event_type
  -> batch ALS/analytics đọc ở lần chạy kế tiếp
```

Demo dùng file source thay vì Kafka để chạy được trên cluster 2 worker, 1 core,
production được mô tả là hướng mở rộng, không giả vờ là thành phần đang chạy.

## Chuẩn bị

Mở PowerShell tại `BigData/code`, kiểm tra Docker Desktop đang chạy và chạy:

```powershell
python -m py_compile jobs/stream_interactions.py tools/stream_demo_producer.py
```

Để có màn hình trực quan trong lúc demo, mở terminal riêng:

```powershell
python tools/stream_dashboard_server.py
```

Sau đó mở `http://localhost:8090`. Dashboard hiển thị query state, worker,
memory, sơ đồ luồng và link tới Spark UI/HDFS UI.

## Chạy một lệnh

```powershell
.\tools\run_stream_demo.ps1
```

Mặc định producer tạo các batch 5 event, cách nhau 5 giây. Có thể điều chỉnh:

```powershell
.\tools\run_stream_demo.ps1 -BatchSize 3 -IntervalSeconds 10
```

Script tự động:

1. Khởi động NameNode, DataNode và Spark.
2. Tạo HDFS path riêng theo `runId`, tránh đụng checkpoint lần chạy trước.
3. Khởi động `stream_interactions.py` với 512 MB driver/executor.
4. Đọc `data/silver/interactions_clean.jsonl` và chia thành file JSONL nhỏ.
5. Đẩy từng file đã đóng vào HDFS landing.
6. In curated Parquet và checkpoint để dùng khi thuyết trình.

## Kịch bản nói trong 5 phút

### 0:00–0:45 — Bài toán

“Mỗi order/review mới tạo một interaction. Nếu chỉ chạy batch, interaction này
chờ tới lần train kế tiếp. Ta cần một landing flow gần thời gian thực để dữ liệu
được chuẩn hóa và sẵn sàng cho ALS/analytics.”

### 0:45–1:30 — Nguồn vào

Mở file nguồn:

```text
data/silver/interactions_clean.jsonl
```

Chỉ rõ mỗi dòng có `interaction_id`, `user_id`, `product_id`, `event_type`,
`rating`, `timestamp`. Producer không sửa event, chỉ chia file để mô phỏng các
lần event đến theo thời gian.

### 1:30–2:30 — Landing HDFS

Khi terminal in `Landing events_00000.jsonl`, giải thích file đã đóng mới được
copy vào:

```text
/mua/stream/demo/<runId>/incoming/
```

Không ghi trực tiếp file đang mở. Đây là quy tắc tránh Spark đọc file chưa hoàn
tất.

### 2:30–3:45 — Spark xử lý

Chỉ vào `jobs/stream_interactions.py`:

- `readStream.schema(schema).json(...)`: file source + schema cố định.
- `maxFilesPerTrigger=1`: giới hạn tải cho laptop.
- `withWatermark("event_time", "10 minutes")`: xử lý event đến muộn có giới hạn.
- `dropDuplicates(["interaction_id"])`: uploader retry không tạo interaction lặp.
- `writeStream.format("parquet")`: curated layer có schema, không còn landing JSONL.
- checkpoint HDFS: giữ tiến độ khi query restart.

### 3:45–4:30 — Output

Chỉ vào output:

```text
/mua/stream/demo/<runId>/curated/
  event_date=2026-09-09/event_type=purchase/*.parquet
```

Giải thích partition giúp batch query lọc theo ngày hoặc loại event mà không đọc
toàn bộ dữ liệu. `_spark_metadata` và checkpoint là bằng chứng query đã commit
micro-batch.

### 4:30–5:00 — Liên hệ recommender

“Streaming không retrain ALS sau từng dòng vì chi phí và nhiễu quá lớn. Nó làm
landing và curated ingestion. Batch ALS đọc curated Parquet theo lịch, đánh giá
trên validation/test, sau đó mới publish recommendations.”

## Lệnh kiểm tra khi demo

Xem curated output:

```powershell
docker exec namenode hdfs dfs -ls -R /mua/stream/demo
```

Xem checkpoint:

```powershell
docker exec namenode hdfs dfs -ls -R /mua/stream/demo/*/checkpoint
```

Xem Spark UI:

```text
http://localhost:8080
```

Trong UI, chỉ vào app `mua-interactions-stream`, source
`FileStreamSource[...]`, trigger interval và số input rows.

## Nếu demo bị lỗi

- `Name node is in safe mode`: chạy `docker exec namenode hdfs dfsadmin -safemode leave`.
- HDFS không thấy file: kiểm tra file đã đóng ở host trước khi chạy `hdfs dfs -put`.
- Stream không có input: xem log bằng `docker exec spark-master tail -n 50 /tmp/stream-demo-<runId>.log`.
- Checkpoint conflict: chạy lại script vì mỗi lần dùng một `runId` mới.
- Docker thiếu memory: giữ `BatchSize` nhỏ, không chạy full Amazon ALS đồng thời.

## Sản phẩm demo cần ghi vào báo cáo

- Hình kiến trúc file landing → HDFS → Spark Streaming → Parquet.
- Ảnh terminal producer tạo `events_*.jsonl`.
- Ảnh HDFS output partition và `_spark_metadata`.
- Ảnh Spark UI có app streaming.
- Bảng cấu hình: trigger 10 giây, max 1 file/trigger, watermark 10 phút,
  checkpoint HDFS, output Parquet.
