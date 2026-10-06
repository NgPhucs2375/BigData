# Dữ liệu đánh giá thật — Amazon Reviews 2023

Nguồn chính thức: https://amazon-reviews-2023.github.io/
Nhóm cung cấp: McAuley Lab. Trích dẫn: Hou et al. (2024), *Bridging Language and
Items for Retrieval and Recommendation*, https://arxiv.org/abs/2403.03952.

Theo thống kê nguồn (số làm tròn):

| Category | Ratings | Users | Items |
|---|---:|---:|---:|
| Amazon_Fashion | 2.5M | 2.0M | 825.9K |
| Clothing_Shoes_and_Jewelry | 66.0M | 22.6M | 7.2M |

Amazon_Fashion phù hợp thử ingestion nhưng rất thưa. Clothing_Shoes_and_Jewelry
là ứng viên chính cho thí nghiệm recommendation quy mô lớn; phải kiểm tra số
user có đủ lịch sử và số user/item còn lại sau lọc. Không cần sinh ratings giả
để đạt 10M nếu dữ liệu này tải được và phù hợp tài nguyên.

## Chạy trên host, không cần Docker hoặc thư viện Python ngoài standard library

```powershell
python import_amazon_reviews.py --download --category Amazon_Fashion --limit 1000
python -m unittest test_import_amazon_reviews -v
```

Chương trình đọc gzip qua HTTP theo luồng rồi dừng ở giới hạn đầu vào; không
tải toàn bộ file trước. Muốn dùng file đã tải:

```powershell
python import_amazon_reviews.py --input data/Amazon_Fashion.jsonl.gz --limit 100000
```

Đọc toàn bộ file cần chủ động `--limit 0`. Có thể chọn `--category
Clothing_Shoes_and_Jewelry` và `--output-dir` khác. Default limit 1000 là smoke
test, không phải bộ đánh giá model; lấy prefix có thể thiên lệch theo thứ tự nguồn.

## Schema và nguồn

- user_id nguồn được hash và namespace `amazon2023_u_`.
- product_id dùng **parent_asin**, namespace `amazon2023_p_`, không dùng asin variant.
- Rating thật 1–5, timestamp epoch **milliseconds**, chuyển UTC giữ millisecond.
  Hỗ trợ timestamp hoặc sort_timestamp, không chấp nhận hai field mâu thuẫn.
- source `amazon_reviews_2023`, source_kind `real`, giữ source_category.
- verified_purchase chỉ là thuộc tính review. Không tạo purchase event từ cờ này.
- Không chuyển Amazon reviewer thành email user của MÙA; không gán Amazon parent
  ASIN sang Mongo product_id hay đưa trực tiếp vào suggestions của website.
- Tách hoàn toàn output khỏi `data/silver` và ratings mô phỏng cũ.

Output `data/public/<category>/`:

| File | Nội dung |
|---|---|
| reviews_raw.jsonl | Các dòng nguồn đã đọc, giữ để kiểm tra |
| explicit_ratings.jsonl | Review hợp lệ theo schema chung |
| observed_product_ids.jsonl | Chỉ ID quan sát được, metadata_loaded=false |
| import_issues.jsonl | Dòng bị từ chối và nguyên nhân |
| dataset_quality_report.json | Độ thưa, users/items, unique pairs, user history |
| source_manifest.json | URL reviews/metadata, nguồn và citation |

Importer dùng SQLite tạm trên disk để dedup và thống kê, không giữ tất cả event
trong RAM. Exact duplicate record bị loại; tương tác lặp khác timestamp/rating
được giữ. Output được dựng trong staging, chỉ công bố khi đọc nguồn thành công.
SQLite giúp ingestion host, không thay thế benchmark Spark phân tán.

## Chưa thể coi đây là catalog thời trang sạch

Đã có importer metadata, nối đúng observed IDs của ratings đã nhập:

```powershell
python import_amazon_metadata.py --download --category Amazon_Fashion --scan-limit 10000
python -m unittest test_import_amazon_metadata -v
```

Metadata và reviews có thứ tự khác nhau. `--scan-limit` giới hạn số dòng metadata
được quét, không phải số sản phẩm sẽ khớp. `--scan-limit 0` quét tới EOF hoặc tới
khi tìm đủ observed IDs; nguồn lớn có thể tốn nhiều thời gian. Có thể dùng
`--input meta_Amazon_Fashion.jsonl.gz`, `--dataset-dir` để đọc snapshot đã tải.

Output riêng trong `data/public/<category>/metadata/`:

- `products_clean.jsonl`: catalog các sản phẩm khớp metadata.
- `metadata_raw.jsonl`: metadata gốc của các sản phẩm khớp.
- `metadata_issues.jsonl`: thiếu field, category không map được, giá trị không rõ.
- `metadata_quality_report.json`: coverage join, số review khớp, độ đầy đủ thuộc tính.
- `ratings_with_metadata.jsonl`: subset reviews có catalog, không thay thế reviews gốc.
- `unmatched_product_ids.jsonl`: chưa tìm thấy trong phần metadata đã quét.
- `metadata_category_dictionary.json`: các mapping nhãn source có thể kiểm tra.

Chỉ map nhãn categories/Department/details theo quy tắc tường minh. Nếu details
thiếu material, đọc các feature là câu thành phần rõ ràng như `100% Polyester`
hoặc `85% Recycled Polyester, 15% Spandex`, ghi provenance theo câu gốc. Không
lấy từ một câu quảng cáo chỉ nhắc đến tên vật liệu; không tự tạo tỷ lệ blend.
Không infer
category hay gender từ title, không tự gán style/season. Taxonomy hiện chưa có
dresses/underwear nên các ngành hàng đó có thể không phân loại được. Size giày
chưa có EU/US/UK thì giữ size_system null và báo unknown, không coi size 10 là EU.
is_active null vì metadata crawl không chứng minh trạng thái bán hiện tại.
price_amount giữ USD, price_vnd null; store được giữ source_store, không thay Brand.

`observed_product_ids` không có tên, attributes hoặc giá giả. Metadata đã join
vẫn có thể thiếu categories/details, cần kiểm tra quality report trước khi dùng
cho content-based/hybrid. Giá metadata là **USD**, không gán thẳng vào price_vnd. Store không nhất
thiết là Brand; category có thể rỗng, phải ghi report thay vì suy đoán.

Đánh giá ALS tiếp theo cần split theo thời gian, báo tỷ lệ user/item chưa thấy,
chọn cấu hình bằng validation, giữ test độc lập. Việc lấy latest rating và lọc
hoạt động phải tuân thủ thời điểm split; không lấy tương tác tương lai vào train.

## So sánh hai snapshot mẫu

Sau khi nhập cả hai category, tạo báo cáo bằng:

```powershell
python compare_public_datasets.py
```

Output: `data/public/dataset_comparison.json`, gồm lịch sử user, mức overlap
giữa các user trên cùng item, số cặp còn lại trong diagnostic 5-core, coverage
metadata và mâu thuẫn gender giữa Department/categories. Đây là phân tích mẫu
nhỏ trong RAM, không dùng làm job xử lý toàn bộ 66M reviews.

Với 1000 reviews và 10000 dòng metadata được quét mỗi nguồn:

| Chỉ số | Amazon_Fashion | Clothing_Shoes_and_Jewelry |
|---|---:|---:|
| Users | 268 | 102 |
| Users có ít nhất 5 sản phẩm | 25 | 57 |
| Sản phẩm khớp metadata | 25 | 33 |
| Review khớp metadata | 26 | 37 |
| Sản phẩm có category_l2 | 0 | 16 |
| Sản phẩm có material | 11 | 11 |

Khuyến nghị tạm thời: dùng Clothing_Shoes_and_Jewelry làm ứng viên nhập thêm để
đánh giá ALS, vì mẫu có lịch sử user dày hơn và source categories rõ hơn. Chưa
kết luận chất lượng mô hình hoặc dataset đầy đủ từ prefix này. Cần kiểm tra
overlap ở phía item, không chỉ số reviews của mỗi user. Khi nguồn Department
mâu thuẫn với categories, normalization hiện ưu tiên Department; report so sánh
ghi nhận số mâu thuẫn để kiểm duyệt trước khi dùng gender làm feature.

## Official 5-core benchmark

Nguồn cung cấp sẵn bộ `5core/last_out_w_his` với split theo thời gian. Với
`Clothing_Shoes_and_Jewelry`, thống kê nguồn là khoảng 18.1M train, 2.5M valid,
2.5M test; người dùng và item đã qua điều kiện 5-core trên toàn bộ benchmark.
Importer dùng CSV chính thức, không tự tái tạo split:

```powershell
python import_amazon_benchmark.py --download --split train valid test --limit 10000
python -m unittest test_import_amazon_benchmark -v
```

`--limit 10000` chỉ là smoke test theo prefix của từng split, không giữ được bảo
đảm 5-core ở output bị cắt. Full split cần tải các file `.csv.gz` về đĩa bằng
downloader có retry/resume, sau đó import offline:

```powershell
python import_amazon_benchmark.py --input-dir data/public/Clothing_Shoes_and_Jewelry/5core_source --split train valid test --limit 0 --output-dir data/public/Clothing_Shoes_and_Jewelry/5core_last_out_full
```

Output `data/public/Clothing_Shoes_and_Jewelry/5core_last_out/` có
`train.jsonl`, `valid.jsonl`, `test.jsonl` và report. Các split chính thức dùng
`parent_asin`, timestamp milliseconds và rating; `history` không được dùng làm
feature vì đã được encode từ tương lai/quá trình split.

Full benchmark đã được nhập tại:

```text
data/public/Clothing_Shoes_and_Jewelry/5core_last_out_full/
```

Kết quả thực tế:

| Split | Records | Invalid | Duplicates |
|---|---:|---:|---:|
| train | 18,052,575 | 0 | 0 |
| valid | 2,524,981 | 0 | 0 |
| test | 2,524,981 | 0 | 0 |

File nguồn gzip được giữ tại `5core_source/`. JSONL chuẩn hóa dùng namespace
riêng `amazon2023_*`; chưa đưa vào MongoDB storefront và chưa ghép giả với
catalog MÙA.
