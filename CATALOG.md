# Chuẩn hóa catalog MÙA — bước trước recommendation

## Chạy

Tại `BigData/code`, export dữ liệu mới nếu MongoDB đang chạy:

```powershell
python buoi02_export_mongo.py
python normalize_catalog.py --default-brand mua
python -m unittest test_normalize_catalog -v
```

`--default-brand mua` là khai báo nguồn dữ liệu thuộc thương hiệu MÙA, không phải
một giá trị tự suy đoán. Không truyền tham số này thì brand thiếu vẫn là null.

Để xuất thêm Parquet (schema tường minh, giữ array và variants):

```powershell
pip install pyarrow
python normalize_catalog.py --default-brand mua --parquet
docker exec namenode hdfs dfs -mkdir -p /mua/silver/products
docker exec namenode hdfs dfs -put -f /data_host/silver/products_clean.parquet /mua/silver/products/
```

Spark đọc bằng `spark.read.parquet("hdfs://namenode:8020/mua/silver/products")`.
Có thể đọc JSONL trực tiếp bằng Spark rồi ghi Parquet nếu không cài pyarrow trên host.

## Đầu vào và đầu ra

Đầu vào mặc định: `data/products.jsonl`, `data/categories.jsonl` nếu có.
Chọn nguồn khác bằng `--input-dir`; chọn nơi xuất bằng `--output-dir`.
Chương trình không cập nhật MongoDB. Dữ liệu raw được giữ nguyên.

Đầu ra mặc định `data/silver/`:

| File | Nội dung |
|---|---|
| products_clean.jsonl | Catalog chuẩn hóa, một dòng mỗi product |
| products_clean.parquet | Tùy chọn `--parquet` |
| categories_clean.jsonl | Toàn bộ cây taxonomy, code/parent_code/path |
| product_attribute_dictionary.json | Vocabulary, alias, taxonomy và quy tắc seed có phiên bản |
| collections_dictionary.json | Mã merchandising, tách khỏi categories |
| product_id_mapping.jsonl | ID nguồn ổn định và provenance của quy tắc suy ra |
| catalog_issues.jsonl | Giá trị lạ, thiếu field, tham chiếu chưa giải quyết, trùng ID |
| catalog_quality_report.json | Tổng số đầu vào/đầu ra, lỗi theo field và phân bố category |

## Quy ước

- Không có field `fashion`. `category_l1/l2/l3` theo cây người dùng chốt.
- `category_l3` được phép null; không ép phân loại cấp 3 khi không có chứng cứ.
  Coach jacket giữ ở `outerwear/jacket`, không bị gán thành bomber.
- Collections là merchandising độc lập, không phải loại sản phẩm.
- `gender`: male/female/unisex/kids. Không tự gán unisex cho mọi sản phẩm.
- `style`, `material`, `colors`, `sizes`, `season` là array không trùng.
- `fit` là code riêng. Oversized có thể là style hoặc fit nếu nguồn khai báo tương ứng.
- Cotton 240gsm -> material cotton, weight_gsm 240. Không đổi 13oz thành GSM
  vì không biết oz đang chỉ đơn vị nào. Không tự suy ra tỷ lệ vật liệu trong blend.
- Màu chỉ ánh xạ qua dictionary tường minh; `midnight-shadow-black` bị báo lỗi.
  Các màu seed như Ink/Bone/Sand được quy về black/cream/beige theo alias có thể sửa.
- Size chữ được giữ uppercase; OS -> ONE_SIZE. Size số hai chữ số (có thể .5)
  được chấp nhận chỉ với footwear. Dữ liệu số size hiện được hiểu theo nguồn;
  chưa chuyển giữa EU/US/UK, cần thêm size_system nếu ghép các nguồn quốc tế.
- ONE_SIZE là size, không phải fit; fit phụ kiện/giày có thể null.
- Season có rainy-season. Thiếu season không tự gán all-season.
- Brand mã lowercase, dấu câu chuẩn hóa; MUA Official -> mua.
- `price_vnd` là giá nhỏ nhất trong variants hợp lệ, fallback priceFrom.
  Nguồn phải là VND; pipeline này chưa quy đổi ngoại tệ.
- `variants` giữ từng cặp SKU–color–size–price–stock. Colors/sizes trên product
  chỉ là tổng hợp để phân tích, không đại diện cho mọi tổ hợp có thể mua.
- `is_active` phản ánh status Active, không phải kiểm tra tồn kho hoặc lịch đăng.
- ID MongoDB được giữ. Nếu có source_item_id/itemId thì dùng lại; nếu không,
  tạo mua-<hash product_id>, ổn định khi thứ tự dòng thay đổi. Không gộp sản phẩm
  chỉ vì tên giống nhau; chỉ từ chối ID product/source trùng.
- Giá trị chưa biết trả null/array rỗng và ghi báo cáo, không tạo thông tin giả.
  Đây là bản clean để review, không bảo đảm mọi dòng đủ điều kiện train.

## Bổ sung thông tin được kiểm duyệt

### Hồ sơ biên tập cho catalog demo

`catalog_seed_profiles.json` bổ sung gender/style/season/category/brand cho 11 mẫu
seed hiện có. Chỉ áp dụng khi chủ động truyền tham số:

```powershell
python buoi02_export_mongo.py --collections categories collections
python normalize_catalog.py --default-brand mua --seed-profiles catalog_seed_profiles.json
```

Đây là thiết kế merchandising cho collection demo unisex, không phải thông tin
được nhà sản xuất xác minh. Nhãn bổ sung có provenance `editorial_demo_profile`
trong mapping và báo cáo. Thông tin khai báo trực tiếp trong nguồn được ưu tiên;
style/season trong tags được giữ và hợp với profile nếu nguồn chưa có field riêng.
Các mẫu lặp có suffix số dùng cùng profile theo slug gốc; không phụ thuộc ID DB.
Rust của Wool Beanie quy về brown theo quy ước biên tập riêng, không áp dụng
mapping này cho toàn dataset. GSM và tỷ lệ thành phần không có nguồn vẫn là null.

Không export lại products nếu chỉ muốn bổ sung categories cho snapshot đang có.
Categories của DB hiện tại có thể không còn cùng ID với snapshot cũ; trường hợp
này vẫn phải ghi unresolved_reference, không sửa ID trong raw để che lỗi.

`--overrides reviewed_attributes.json` nhận JSON theo product_id, ví dụ:

```json
{
  "6aae2af7366e055356ea7601": {
    "category": "basic-t-shirt",
    "gender": "unisex",
    "style": ["minimal", "basic"],
    "fit": "boxy",
    "season": ["all-season"],
    "brand": "mua"
  }
}
```

Các override là thông tin người quản trị xác nhận; không tự infer gender/style/
season từ tên. Product chỉ có danh mục cha có thể dùng mapping slug cho các mẫu
seed đã biết trong dictionary. Nếu nguồn khai báo các category mâu thuẫn thì báo
lỗi, không chọn tùy tiện. Khi thiếu categories export, tham chiếu không giải
quyết được vẫn được ghi nhận ngay cả khi mapping seed xác định được category.

JSONL được xử lý từng product, không nạp toàn bộ ratings vào RAM. Tùy chọn Parquet
host gom catalog vào RAM; với catalog lớn dùng Spark để chuyển JSONL sang Parquet.
Chuẩn hóa catalog chưa biến dữ liệu seed thành dữ liệu hành vi thật; ALS hiện tại
chưa dùng catalog này cho tới khi bước chuẩn hóa interactions được triển khai.
