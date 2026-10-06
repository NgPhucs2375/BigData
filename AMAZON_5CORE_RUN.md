# Chạy benchmark Amazon 5-core

Full benchmark đã có trên host:

```text
data/public/Clothing_Shoes_and_Jewelry/5core_last_out_full/
```

Docker Compose của cluster mount `code/data` thành `/data_host` trong NameNode.
Ưu tiên upload các file nguồn nén để tránh truyền khoảng 15 GB JSONL:
Chạy từ `BigData/code`:

```powershell
docker compose up -d
docker exec namenode hdfs dfs -mkdir -p /mua/public/Clothing_Shoes_and_Jewelry/5core_source
docker exec namenode hdfs dfs -D dfs.replication=1 -put -f /data_host/public/Clothing_Shoes_and_Jewelry/5core_source/Clothing_Shoes_and_Jewelry.train.csv.gz /mua/public/Clothing_Shoes_and_Jewelry/5core_source/
docker exec namenode hdfs dfs -D dfs.replication=1 -put -f /data_host/public/Clothing_Shoes_and_Jewelry/5core_source/Clothing_Shoes_and_Jewelry.valid.csv.gz /mua/public/Clothing_Shoes_and_Jewelry/5core_source/
docker exec namenode hdfs dfs -D dfs.replication=1 -put -f /data_host/public/Clothing_Shoes_and_Jewelry/5core_source/Clothing_Shoes_and_Jewelry.test.csv.gz /mua/public/Clothing_Shoes_and_Jewelry/5core_source/
docker cp jobs/amazon_benchmark_csv_to_parquet.py spark-master:/jobs/
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 512m --driver-memory 512m /jobs/amazon_benchmark_csv_to_parquet.py
```

Job đầu tiên:

- Đọc train/valid/test bằng schema tường minh.
- Fit mapping user/item một lần trên cả ba split.
- Ghi Parquet riêng cho từng split.
- Ghi `user_mapping` và `item_mapping`.

Train và đánh giá bằng validation độc lập:

```powershell
docker cp jobs/train_als_amazon_5core.py spark-master:/jobs/
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 512m --driver-memory 512m /jobs/train_als_amazon_5core.py
```

Job chọn `(rank, regParam)` bằng **validation**, sau đó chỉ báo cáo RMSE/MAE và
Precision@10/Recall@10 trên **test**. Test không được dùng để chọn model.

Đây là benchmark recommendation độc lập. Không import các Amazon ID vào MongoDB
MÙA, không đưa kết quả Amazon vào collection `suggestions`, và không gộp Amazon
user/product với ID của MÙA. Catalog Amazon metadata là pipeline riêng; ALS
collaborative filtering vẫn có thể chạy trên ratings thuần trước.

Nếu chạy full trên cluster 4 core/4 GB, nên theo dõi Spark UI và có thể bắt đầu
với bản smoke test 10K/split trong `5core_last_out`. Full benchmark khoảng 23M
rows; thời gian, shuffle và disk là số liệu Big Data cần ghi lại, không so sánh
trực tiếp với 748K dữ liệu mô phỏng nếu cấu hình cluster khác.
