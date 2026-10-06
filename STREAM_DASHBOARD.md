# Dashboard trực quan streaming

Dashboard nhẹ chạy bằng Python standard library, không cần npm, React hoặc database. Nó đọc Spark Master REST API và hiển thị query state, worker, memory, app streaming, sơ đồ landing → Spark → Parquet → batch ALS và link tới Spark UI/HDFS UI.

## Chạy

```powershell
cd BigData/code
docker compose up -d
python tools/stream_dashboard_server.py
```

Mở `http://localhost:8090`. Dashboard tự refresh mỗi 5 giây. Các port khác vẫn giữ nguyên: Spark UI `8080`, HDFS UI `9870`, Spark RPC `7077`.

## Demo kết hợp

Terminal 1:

```powershell
python tools/stream_dashboard_server.py
```

Terminal 2:

```powershell
.\tools\run_stream_demo.ps1 -BatchSize 5 -IntervalSeconds 5
```

Trong lúc script in `Landing events_*.jsonl`, mở dashboard để chỉ rõ trạng thái cluster. Bấm link Spark Master UI để cho thấy worker và application; dashboard là lớp trình bày luồng dữ liệu, còn Spark UI là bằng chứng runtime.

Dashboard không giả lập số liệu khi Spark tắt: trạng thái chuyển `OFFLINE`. Nó không thay thế Spark UI, không điều khiển query và không thêm service nặng vào cluster học tập.
