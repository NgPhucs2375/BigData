# Chuẩn hóa interactions sau catalog

Tại `BigData/code`, sử dụng cùng snapshot orders/reviews và products_clean:

```powershell
python normalize_interactions.py
python -m unittest test_normalize_interactions -v
```

Đầu vào mặc định: `data/orders.jsonl`, `data/reviews.jsonl`,
`data/silver/products_clean.jsonl`. Có thể đổi bằng `--input-dir`, `--catalog`,
`--output-dir`. Raw và MongoDB không bị thay đổi.

## Identity và điều kiện chấp nhận

- Email được NFKC, trim và casefold, kiểm tra dạng email rồi SHA-256 với namespace
  email. Không bỏ dấu chấm/plus tag, không nối theo tên hoặc phone.
- user_id là định danh giả danh theo contact email, không phải xác minh sở hữu
  tài khoản. Hash không biến thông tin thành ẩn danh tuyệt đối.
- Chỉ nhận order Paid với fulfillment Unfulfilled/Processing/Shipped/Delivered,
  hoặc Pending + Cod + Delivered. Không nhận Cancelled/Refunded/Failed.
- Timestamp order là createdAt UTC: **proxy cho thời điểm mua**, không phải
  thời điểm thanh toán. Naive datetime từ export MongoDB được hiểu là UTC.
- Một order + một product = một purchase; cộng quantity của các variant cùng
  product. Các order khác nhau vẫn giữ riêng. Không nhân weight theo quantity.
- Unknown product, quantity không phải số nguyên dương, thiếu email/timestamp
  được ghi issues. Order còn các dòng hợp lệ vẫn xuất các dòng đó.
- Review phải Published, rating nguyên 1–5, có timestamp hợp lệ, orderNumber
  nối duy nhất tới order đủ điều kiện, và product nằm trong order đó. Review
  trước createdAt của order bị từ chối. Không map AuthorName sang user.
- Order/review thiếu ID hoặc ID/number trùng gây mơ hồ đều bị từ chối; không
  chọn tùy tiện một bản ghi. Nhiều review khác ID cho cùng user/product vẫn
  là các event khác nhau; khi train explicit cần chính sách chọn rating theo thời gian.
- Catalog chứa cả sản phẩm không active vẫn dùng kiểm tra lịch sử giao dịch;
  lọc availability khi tạo danh sách gợi ý, không xóa lịch sử bằng status hiện tại.
- Source mặc định `seed` vì snapshot hiện có là dữ liệu demo. Chỉ dùng
  `--source-kind real` nếu nguồn thực sự là giao dịch khách hàng thật.
- Không tạo view/cart/wishlist giả từ orders. Redis leaderboard tổng lượt xem
  không cung cấp user-level view log. Cần bổ sung event tracking riêng cho các nguồn đó.

## Output trong data/silver

| File | Nội dung |
|---|---|
| interactions_clean.jsonl | Tất cả event hợp lệ, sắp xếp theo thời gian |
| implicit_interactions.jsonl | Purchase, weight=5, rating=null |
| explicit_ratings.jsonl | Review, rating=1..5, weight=null |
| user_id_mapping.jsonl | user_id, user_source và source_order_ids; không chứa email/phone |
| interaction_issues.jsonl | Source ID và lý do từ chối, không đưa contact vào báo cáo |
| interaction_quality_report.json | Count, user/product coverage, unique pairs, sparsity |

Sparsity = 1 - unique(user, product) / (resolved users × all catalog products).
Nếu không có user/product thì sparsity null. Nhiều event của cùng một cặp chỉ
đếm một lần. Dataset nhỏ dù có sparsity vẫn không chứng minh đủ dữ liệu train.

Hai file implicit và explicit giữ timestamp và provenance để tách train/validation/
test theo thời gian **trước khi** aggregate, tránh để event tương lai lọt vào train.
Weight=5 là trọng số nghiệp vụ ban đầu cho purchase, không phải rating 5 sao;
implicit ALS còn cần chọn cách aggregate, alpha, regParam và kiểm chứng validation.

## Parquet và HDFS

Trên host nếu đã có pyarrow:

```powershell
python normalize_interactions.py --parquet
```

Định dạng Parquet dùng schema tường minh, kể cả khi không có rating/không có event.
Hoặc dùng Spark job đi kèm (không cần pyarrow trên host):

```powershell
docker exec namenode hdfs dfs -mkdir -p /mua/silver
docker exec namenode hdfs dfs -put -f /data_host/silver/interactions_clean.jsonl /mua/silver/
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/interactions_to_parquet.py
```

Output HDFS: `/mua/silver/interactions` (Parquet), `/mua/silver/implicit_interactions`,
`/mua/silver/explicit_ratings`. Không tự thay đổi model ALS cũ hay suggestions.

Job chuẩn hóa hiện giữ index orders và events trong RAM để kiểm tra identity/dedup
cho snapshot ứng dụng. Không dùng chương trình host này làm benchmark 10M;
quy mô lớn cần chuyển các join/dedup/aggregate tương ứng sang Spark.
