# NYC Taxi Big Data Project – Proof-of-Concept Phân Tích Giao Thông Thời Gian Thực

- **Môn học:** Dữ Liệu Lớn (Big Data)
- **Đề tài:** Xây dựng hệ thống thử nghiệm (Proof-of-Concept) phân tích dữ liệu giao thông thời gian thực dựa trên kiến trúc Big Data
- **Thành viên thực hiện:** Lê Phi Hưng
- **Nguồn dữ liệu:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (`yellow_tripdata_2026_07.csv` & `taxi_zone_lookup.csv`)

## 1. Cấu trúc thư mục dự án (Checkpoint 3)

```text
bigdata_project/
├── config/
│   └── config.json                 # Cấu hình đường dẫn raw, lookup, output và ngưỡng lọc nghiệp vụ
├── data/
│   ├── source/
│   │   ├── yellow_tripdata_2026_07.csv
│   │   └── taxi_zone_lookup.csv    # Tự động tải từ NYC TLC (265 khu vực)
│   └── raw/
│       └── nyc_taxi/
│           └── ingest_date=2026-10-24/
│               └── run_20261024_084906/
├── logs/
│   ├── env_check.txt               # Kết quả kiểm tra môi trường (Checkpoint 2)
│   ├── ingest_manifest.json        # Manifest truy vết dữ liệu raw (3,530,109 dòng)
│   ├── quality_report.json         # Bằng chứng kiểm định chất lượng dữ liệu (Checkpoint 3)
│   ├── pipeline_baseline.json      # Số liệu baseline runtime 43.99s, row counts, partitions
│   └── explain_plan.txt            # Kế hoạch thực thi Spark explain("formatted")
├── metadata/
│   └── data_dictionary.md          # Từ điển dữ liệu Fact, Dimension và Output KPI
├── output/
│   └── traffic_zone_hourly_kpi/    # Kết quả tổng hợp 10,778 dòng KPI giao thông theo Vùng & Giờ
├── reports/
│   └── checkpoint3.md              # Báo cáo nghiệm thu chi tiết Checkpoint 3
├── src/
│   ├── env_check.py                # Smoke test kiểm tra môi trường Spark
│   ├── ingest_nyc_taxi.py          # Job Ingest dữ liệu từ source vào raw storage (Checkpoint 2)
│   ├── pipeline.py                 # End-to-End Spark Processing Pipeline (Checkpoint 3)
│   └── quality_report.py           # Kiểm định chất lượng dữ liệu Nulls/Duplicates/Invalid/Unmatched
├── Dockerfile
├── .dockerignore
└── README.md
```

## 2. Yêu cầu hệ thống & Cài đặt môi trường

- **Python:** `3.13.7` (Local Windows) / `3.10.12` (Docker)
- **PySpark:** `3.5.0`
- **Java:** OpenJDK 11+

Kiểm tra môi trường (Smoke Test):

```bash
python src/env_check.py
```

## 3. Hướng dẫn chạy toàn bộ quy trình (Reproducibility)

### Bước 1: Ingest dữ liệu vào Raw Storage (Checkpoint 2)
```bash
python src/ingest_nyc_taxi.py
```

### Bước 2: Chạy kiểm định chất lượng dữ liệu (Checkpoint 3 – D5)
```bash
python src/quality_report.py
```
- **Đầu ra:** `logs/quality_report.json` (Runtime thực tế: **28.49 giây** trên **3,530,109 dòng**).

### Bước 3: Chạy End-to-End Spark Processing Pipeline (Checkpoint 3 – D1, D2, D3, D4, D6)
```bash
python src/pipeline.py
```
- **Đầu ra chính:**
  - Dữ liệu KPI tổng hợp: `output/traffic_zone_hourly_kpi/` (**10,778 dòng** KPI theo khu vực và khung giờ)
  - Kế hoạch thực thi vật lý/logic: `logs/explain_plan.txt`
  - Số liệu Baseline & Row Counts: `logs/pipeline_baseline.json` (Runtime baseline: **43.99 giây**)

## 4. Tóm tắt Kết quả Thực nghiệm (Checkpoint 3 Baseline)

| Giai đoạn Pipeline | Số lượng bản ghi / Thông số | Ghi chú nghiệm thu |
| :--- | :--- | :--- |
| **Input Raw (`yellow_tripdata_2026_07.csv`)** | **3,530,109** dòng (**439.03 MB**) | Chia làm **16 partitions** ban đầu |
| **Sau Transform 1 (Data Cleaning)** | **3,317,606** dòng | Loại bỏ **212,503** dòng lỗi thời gian/quãng đường/cước phí (**6.02%**) |
| **Sau Transform 2 (Speed & Duration Filter)** | **3,312,154** dòng | Loại bỏ **5,452** dòng ngoại lai thời lượng/vận tốc (**0.16%**) |
| **Sau Transform 3 (Feature Derivation)** | **3,312,154** dòng | Giữ lại **93.83%** dữ liệu sạch; gắn nhãn giờ cao điểm & ùn tắc |
| **Sau Left Join (`taxi_zone_lookup.csv`)** | **3,312,154** dòng | Khớp vùng hợp lệ: **3,308,365** dòng (**99.89%**); mã 264/265: **3,789** dòng |
| **Output Aggregation (`traffic_zone_hourly_kpi`)** | **10,778** dòng (**1 partition**) | Chế độ ghi `overwrite` đảm bảo tính lũy đẳng (idempotent) |
| **Baseline Execution Time** | **43.99 giây** | Đã lưu Physical Plan tại `logs/explain_plan.txt` |

Xem báo cáo đầy đủ tại [`reports/checkpoint3.md`](reports/checkpoint3.md) và từ điển dữ liệu tại [`metadata/data_dictionary.md`](metadata/data_dictionary.md).
