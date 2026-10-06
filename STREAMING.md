# Streaming nhẹ cho interactions

## Mục tiêu và phạm vi

Phần streaming này phục vụ feedback mới từ storefront mà không làm cluster học
tập quá nặng. Nó không retrain ALS cho từng sự kiện. ALS vẫn chạy theo batch theo
lịch; Structured Streaming chỉ làm ingestion và lưu dữ liệu đã chuẩn hóa để batch
job đọc ở lần train kế tiếp.

```text
data/silver/interactions_clean.jsonl
        |
        | producer demo chia thành events_*.jsonl
        v
HDFS /mua/stream/incoming/interactions
        |
        | Spark Structured Streaming, file source
        | maxFilesPerTrigger=1, trigger=10 seconds
        | schema cố định, watermark 10 minutes
        | dropDuplicates(interaction_id)
        v
HDFS /mua/stream/curated/interactions
        |
        | Parquet partitionBy event_date, event_type
        v
batch ALS / aggregation / dashboard
```

## Công nghệ

- **HDFS**: durable landing zone, source và output không phụ thuộc filesystem của
  container Spark.
- **Spark Structured Streaming 3.3**: micro-batch engine; mỗi trigger tối đa đọc
  một file mới để máy yếu không bị dồn tải.
- **JSONL**: format landing đơn giản, append-only, dễ kiểm tra bằng mắt.
- **Parquet**: format curated có schema và partition, phù hợp Spark batch.
- **Checkpoint HDFS**: lưu offset/file đã xử lý; restart query không đọc lại file
  đã commit.
- **Không dùng Kafka**: với cluster hiện tại chỉ 2 worker, 1 core và bộ nhớ thấp,
  Kafka thêm broker, volume và chi phí vận hành nhưng không tăng giá trị demo.

## Chuẩn bị cluster

Từ `BigData/code`:

```powershell
docker compose up -d
docker exec namenode hdfs dfsadmin -safemode leave
docker exec namenode hdfs dfs -mkdir -p /mua/stream/incoming/interactions
docker exec namenode hdfs dfs -mkdir -p /mua/stream/curated/interactions
docker exec namenode hdfs dfs -mkdir -p /mua/stream/checkpoints/interactions
```

## Chạy stream

Job được mount tự động vào `/jobs` trong `spark-master`. Cấu hình memory thấp
giống cluster hiện tại:

```powershell
docker exec spark-master /spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-memory 512m --driver-memory 512m `
  /jobs/stream_interactions.py
```

Job sẽ chạy liên tục. Mỗi 10 giây Spark kiểm tra source, tối đa nhận một file
chưa xử lý, parse theo schema tường minh và ghi Parquet append. Dừng bằng
`Ctrl+C` ở terminal chạy `spark-submit`; checkpoint vẫn được giữ.

## Nạp dữ liệu demo

NameNode đang mount thư mục host `code/data` thành `/data_host`. Có thể tạo file
batch nhỏ tại host bằng producer:

```powershell
python tools/stream_demo_producer.py `
  data/silver/interactions_clean.jsonl `
  data/stream_demo/incoming `
  --batch-size 5 --interval 10
```

Ở terminal khác, copy từng file mới vào landing zone HDFS:

```powershell
docker exec namenode hdfs dfs -put -f `
  /data_host/stream_demo/incoming/events_00000.jsonl `
  /mua/stream/incoming/interactions/
```

Lặp lại với `events_00001.jsonl`, `events_00002.jsonl` hoặc dùng file watcher
của ứng dụng thật để đẩy file. Không copy trực tiếp một file đang được ghi; file
chỉ nên xuất hiện trong HDFS sau khi producer đã đóng file.

Kiểm tra output:

```powershell
docker exec namenode hdfs dfs -find /mua/stream/curated/interactions -name '*.parquet'
docker exec namenode hdfs dfs -ls -R /mua/stream/curated/interactions
```

## Luồng production dự kiến

Trong ứng dụng thật, API order/review ghi event vào outbox hoặc object storage,
sau đó một uploader tạo JSONL immutable trong landing directory. Spark đọc landing
directory, deduplicate theo `interaction_id`, và ghi curated Parquet. Batch ALS
đọc cả `data/public/...` đã đóng băng và output streaming đã chốt tới watermark.

`watermark=10 minutes` chỉ giới hạn trạng thái deduplication cho event đến muộn;
nó không làm mất event mới hợp lệ. Event quá muộn cần được đưa vào quarantine nếu
nghiệp vụ yêu cầu đối soát. Với đồ án hiện tại, chưa nối streaming output vào
MongoDB `suggestions`, vì recommendation phải được train/evaluate offline trước.

## Kiểm chứng

- Schema giữ các trường `interaction_id`, `user_id`, `product_id`, `event_type`,
  `rating`, `quantity`, `weight`, `timestamp` và metadata nguồn.
- `dropDuplicates(interaction_id)` chống ghi lặp khi uploader retry.
- Parquet partition theo `event_date/event_type` giúp batch lọc theo ngày và loại
  sự kiện.
- Checkpoint phải là thư mục riêng, không xóa trong khi query đang chạy.
- Đây là micro-batch streaming gần thời gian thực, không phải hard real-time.

Smoke test đã chạy trên cluster hiện tại với `data/silver/interactions_clean.jsonl`:
file được đưa vào HDFS landing và tạo Parquet thành công dưới
`/mua/stream/curated/interactions`, kèm `_spark_metadata` và checkpoint. Kết quả
được partition theo các ngày sự kiện `2026-09-09` đến `2026-09-18`; không có dòng
bị drop bởi watermark hoặc duplicate trong lần chạy thử.
