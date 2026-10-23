# Ghi chú demo Recommendation MÙA

Ngày cập nhật: 22/10/2026

## 1. Mục tiêu

Xây dựng demo hệ thống gợi ý sản phẩm cho MÙA bằng:

```text
JSONL/CSV
  -> HDFS
  -> Spark DataFrame và Parquet
  -> ALS Collaborative Filtering
  -> Top-K recommendations
  -> MongoDB
  -> API
  -> Frontend MÙA
```

Hệ thống có hai hướng gợi ý:

1. **ALS batch**: Spark huấn luyện offline trên dữ liệu rating mô phỏng, sau đó ghi Top-10 recommendation vào MongoDB.
2. **Personalized serving**: với tài khoản khách thật, backend đọc wishlist, đơn hàng và giỏ hàng để tính affinity trên catalog.

## 2. Trạng thái dữ liệu

### Dataset ALS

Dataset mô phỏng hiện đã được mở rộng:

| Thành phần | Số lượng |
|---|---:|
| User | 10.000 |
| Product/item | 500 |
| Ratings | 1.873.220 |
| Train | 70% |
| Validation | 10% |
| Test | 20% |

File dữ liệu:

- `data/ratings.jsonl`
- `data/items.jsonl`
- `data/evaluation_users.json`

Manifest tại `data/evaluation_users.json` ghi lại seed, kích thước dữ liệu và tỷ lệ chia tập.

### MongoDB storefront

Trạng thái đã kiểm tra:

- 311 sản phẩm Active.
- 300 sản phẩm thuộc catalog ALS.
- 10 tài khoản Customer.
- 4 tài khoản demo có metadata về gu.
- 5.000 user ALS có document recommendation Top-10 trong collection `suggestions`.

## 3. Tài khoản demo

### Tài khoản khách thật

| Nhóm | Email | Mật khẩu |
|---|---|---|
| Profile tổng hợp | `user@mua.vn` | `User@123` |
| Gu chính minimal | `minimal@mua.vn` | `Minimal@123` |
| Gu chính streetwear | `streetwear@mua.vn` | `Street@123` |
| Gu chính classic | `classic@mua.vn` | `Classic@123` |
| Gu chính oversize | `oversize@mua.vn` | `Oversize@123` |

### Phân bổ wishlist

Các tài khoản demo không bị giới hạn vào một style duy nhất:

| Tài khoản | Gu chính | Style phụ | Style khám phá | Style bổ sung |
|---|---|---|---|---|
| `minimal@mua.vn` | 6 minimal | 3 classic | 2 streetwear | 1 oversize |
| `streetwear@mua.vn` | 6 streetwear | 3 oversize | 2 minimal | 1 classic |
| `classic@mua.vn` | 6 classic | 3 minimal | 2 oversize | 1 streetwear |
| `oversize@mua.vn` | 6 oversize | 3 streetwear | 2 classic | 1 minimal |

Điều này mô phỏng tình huống thực tế: người dùng có gu chính nhưng vẫn mua hoặc quan tâm các nhóm sản phẩm khác.

## 4. Logic recommendation đã sửa

Recommendation không còn hiểu theo kiểu:

```text
Thích minimal -> chỉ trả sản phẩm minimal
```

Thay vào đó:

1. Tính điểm affinity từ wishlist, đơn hàng và giỏ hàng.
2. Cộng thêm tín hiệu rating của sản phẩm.
3. Sắp xếp theo điểm tổng hợp.
4. Giới hạn tối đa 4 sản phẩm cùng một style trong Top-10.
5. Bổ sung style phụ và style khám phá.
6. Nếu catalog quá ít style, nới giới hạn để vẫn trả đủ Top-10.

Ví dụ kết quả hợp lý:

```text
4 minimal
3 classic
2 streetwear
1 oversize
```

Các sản phẩm khác style không bị xem là vô nghĩa. Chúng vẫn có thể được gợi ý vì:

- Thuộc style phụ của người dùng.
- Có liên quan đến sản phẩm đã thích hoặc đã mua.
- Có rating tốt.
- Giúp khám phá sản phẩm mới.

Code chính:

`NoSQL/ThuongMaiDienTu-Normal/src/EC.Application/Storefront/Services/RecommendationService.cs`

Script seed:

`BigData/code/tools/seed_demo_storefront.py`

## 5. Các user ALS dùng để so sánh

Frontend đang có các user demo:

```text
u00001
u02500
u05000
u07500
u09999
```

Các user này đại diện cho những gu khác nhau trong dataset mô phỏng:

| User | Gu đại diện |
|---|---|
| `u00001` | minimal |
| `u02500` | oversize |
| `u05000` | classic |
| `u07500` | streetwear |
| `u09999` | holdout/mixed |

Trang demo:

```text
http://localhost:3000/recommendations
```

Có thể chọn chế độ:

- **Cho bạn**: recommendation cho tài khoản đang đăng nhập.
- **Demo ALS**: chọn user ALS để xem Top-10 thay đổi theo user.

## 6. Các lỗi đã sửa

### Route `/me` trả 404

Route động `{userId}` trước đây có thể bắt nhầm URL `/me`.

Đã ưu tiên route tĩnh:

```text
GET /api/v1/recommendations/me
```

Sau khi sửa, API đã trả HTTP 200 cho `user@mua.vn`.

### Recommendation chỉ có ít sản phẩm

Nguyên nhân:

- Tài khoản thật chỉ có ít tín hiệu.
- Catalog trước đó có ít rating demo.
- Cache recommendation giữ kết quả cũ.

Đã xử lý bằng:

- Bổ sung wishlist.
- Bổ sung rating/count cho catalog ALS.
- Xóa cache recommendation khi seed.
- Thêm nhiều tài khoản demo.
- Thêm đa dạng hóa Top-10.

## 7. Các lệnh seed

### Seed tài khoản và catalog storefront

```powershell
python D:\Univer\Nam_4\HKI\BigData\code\tools\seed_demo_storefront.py
```

Script có tính idempotent, có thể chạy lại mà không xóa orders, carts hoặc reviews.

### Sinh dataset ALS

```powershell
python D:\Univer\Nam_4\HKI\BigData\code\buoi03_generate_ratings.py
```

Lệnh mặc định tạo:

```text
10.000 users
500 items
Khoảng 1,87 triệu ratings
```

## 8. Đưa dữ liệu lên HDFS

```powershell
docker exec namenode sh -c "hdfs dfs -mkdir -p /mua; hdfs dfs -put -f /data_host/items.jsonl /mua/items.jsonl; hdfs dfs -put -f /data_host/ratings.jsonl /mua/ratings.jsonl"
```

Chuyển ratings sang Parquet:

```powershell
docker exec spark-master `
  /spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-memory 512m `
  --driver-memory 512m `
  /jobs/buoi03_ratings_eda.py
```

Lưu ý: worker Spark hiện có 768 MB, vì vậy nên dùng `--executor-memory 512m`. Cấu hình mặc định 1 GB có thể khiến job bị treo do worker không đủ tài nguyên.

## 9. Huấn luyện ALS và đánh giá

Huấn luyện:

```powershell
docker exec spark-master `
  /spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-memory 512m `
  --driver-memory 512m `
  /jobs/buoi05_train_als_mua.py
```

Job này:

- Chia dữ liệu theo tỷ lệ 70/10/20.
- Tính baseline item mean.
- Thử nhiều cấu hình `rank`.
- Chọn cấu hình tốt nhất trên validation.
- Báo cáo RMSE và MAE trên test.
- Xuất model ALS.
- Xuất recommendation Top-10 cho toàn bộ user.

Đánh giá ranking:

```powershell
docker exec spark-master `
  /spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-memory 512m `
  --driver-memory 512m `
  /jobs/buoi07_metrics_ranking.py
```

Các metric cần ghi vào báo cáo:

```text
Validation RMSE
Test RMSE
Test MAE
Precision@10
Recall@10
```

Không tự điền số liệu nếu job chưa chạy xong.

## 10. Trạng thái xác nhận cuối cùng

### Đã xác nhận

- Backend build thành công, 0 error.
- Dataset mới đã được tạo.
- Dataset có 10.000 user, 500 item và 1.873.220 rating.
- Dữ liệu đã được upload lên HDFS.
- Parquet ratings mới đã được tạo trên HDFS.
- MongoDB có 4 tài khoản demo mới.
- Hash mật khẩu của 4 tài khoản demo hợp lệ.
- Các wishlist có phân bổ nhiều style.
- MongoDB có recommendation cho 5.000 user ALS.

### Chưa chốt hoàn toàn

Tại thời điểm ghi chú, Docker Desktop trả lỗi nội bộ:

```text
500 Internal Server Error
dockerDesktopLinuxEngine
```

Vì vậy chưa xác nhận end-to-end lần cuối sau khi deploy code đa dạng hóa vào container API.

Đặc biệt cần chạy lại để có số liệu thực nghiệm cuối:

- ALS trên dataset 1,87 triệu ratings.
- Validation RMSE.
- Test RMSE.
- Test MAE.
- Precision@10.
- Recall@10.
- Login và xem recommendation của cả 4 tài khoản demo qua browser.

## 11. Checklist trước khi báo cáo

- [ ] Docker Desktop đang chạy ổn định.
- [ ] API và frontend container ở trạng thái `Up`.
- [ ] Đăng nhập được cả 4 tài khoản demo.
- [ ] Mỗi tài khoản trả về Top-10 recommendation.
- [ ] Top-10 có nhiều style, không chỉ một style.
- [ ] ALS chạy thành công trên dataset mới.
- [ ] Có log RMSE/MAE validation và test.
- [ ] Có log Precision@10 và Recall@10.
- [ ] Chụp màn hình trang `/recommendations`.
- [ ] Chụp màn hình so sánh ít nhất 2 tài khoản.
- [ ] Ghi rõ đâu là dữ liệu mô phỏng, đâu là dữ liệu MÙA thật.

## 12. Kết luận dùng trong báo cáo

Hệ thống đã hoàn thiện phần kiến trúc, dữ liệu mẫu, tài khoản demo và logic phục vụ recommendation. Dataset mô phỏng được mở rộng để có đủ user, item và interaction cho việc chia train, validation và test. Recommendation cho tài khoản thật sử dụng kết hợp tín hiệu hành vi và cơ chế đa dạng hóa, vì vậy không khóa người dùng vào một phong cách duy nhất.

Phần cần hoàn tất trước khi chốt chương thực nghiệm là chạy lại Spark/ALS sau khi Docker hoạt động ổn định và ghi nhận các metric cuối cùng. Không nên tự suy đoán hoặc điền số liệu RMSE, MAE, Precision@10 và Recall@10 nếu chưa có log chạy thực tế.
