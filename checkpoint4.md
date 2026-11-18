# Báo Cáo Checkpoint 4 (Buổi 10) – Benchmark & Tối Ưu Hóa Spark Pipeline

- **Nhóm / Thành viên thực hiện:** Lê Phi Hưng
- **Đề tài:** Xây dựng hệ thống thử nghiệm (Proof-of-Concept) phân tích dữ liệu giao thông thời gian thực dựa trên kiến trúc Big Data
- **Repository:** [https://github.com/ugnah/bigdata_project](https://github.com/ugnah/bigdata_project)
- **Baseline Commit / Tag:** `checkpoint-3` (`2f3e0ef`)
- **Optimized Commit / Tag:** `checkpoint-4` (`checkpoint-4: benchmark and optimize spark pipeline`)

## A. Baseline (Mốc cơ sở có thể tái lập)
- **Workload & Input:** Bộ dữ liệu NYC TLC Yellow Taxi tháng 07/2026 (`yellow_tripdata_2026_07.csv`) từ [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), lưu tại `data/raw/nyc_taxi/ingest_date=2026-10-24/run_20261024_084906`.
  - Dung lượng: **439.03 MB** | Số dòng đầu vào: **3,530,109 dòng** | Số phân vùng ban đầu: **16 partitions**.
- **Tài nguyên & Cấu hình Baseline (`B00_Baseline`):**
  - Môi trường: Python `3.13.7`, PySpark `3.5.0`, Master `local[*]`, OS Windows (`nt`).
  - Cấu hình Spark mặc định: `spark.sql.shuffle.partitions = 200`, AQE = `true`, đọc toàn bộ 19 cột CSV.
- **Kết quả đo 3 lần chạy Baseline (`B00_Baseline`):**
  - **Run 1:** `67.73 s`
  - **Run 2:** `50.50 s`
  - **Run 3:** `51.62 s`
  - **Median Runtime (Baseline):** **51.62 giây** (Mean: `56.62 s`, Std: `9.64 s`)
  - **Output & Business Metrics chuẩn:** `10,778` dòng KPI, tổng số chuyến hợp lệ = `3,312,154`, tổng doanh thu = `$99,587,514.55`, tổng số chuyến ùn tắc nặng (`< 8 mph`) = `1,151,467`.

## B. Bottleneck Evidence (Bằng chứng điểm nghẽn cổ chai)
1. **Bottleneck 1 (Chính – Recompute & Repeated Full-File CSV Scans):**
   - **Quan sát từ Code & Execution Log (`reports/explain-before.txt`):** Trong phiên bản Baseline (`checkpoint-3`), pipeline gọi **7 Spark Actions rời rạc** trên cùng một nhánh dữ liệu mà không gom phép đếm và không lưu bộ nhớ đệm (`persist`):
     1. `df_raw.count()` (Quét 439.03 MB CSV lần 1)
     2. `df_t1.count()` (Quét 439.03 MB CSV lần 2 + parse `to_timestamp`)
     3. `df_t2.count()` (Quét 439.03 MB CSV lần 3 + parse `to_timestamp` + tính vận tốc)
     4. `df_joined.select(...).collect()` (Quét 439.03 MB CSV lần 4 + Join)
     5. `df_agg.count()` (Quét 439.03 MB CSV lần 5 + Join + `HashAggregate`)
     6. `df_agg.show(15)` (Quét 439.03 MB CSV lần 6 + Join + `HashAggregate` + `Sort`)
     7. `write_output_safely(df_agg)` (Quét 439.03 MB CSV lần 7 + Join + `HashAggregate` + `Sort`)
   - **Bằng chứng định lượng:** Spark phải đọc từ đĩa `7 × 439.03 MB = 3,073.21 MB` (~3.07 GB dữ liệu CSV) và thực thi lặp lại toán tử `HashAggregate` + `Sort` 3 lần liên tiếp cho cùng một bảng kết quả chỉ có `10,778` dòng.
2. **Bottleneck 2 (Phụ – Wide CSV Schema Scan & Over-Partitioned Shuffle):**
   - Trong `reports/explain-before.txt`, toán tử `FileScan csv` phải đọc cả **19 cột** dù pipeline chỉ sử dụng **7 cột**, và bước `Exchange hashpartitioning` sau gom nhóm sử dụng cấu hình mặc định `spark.sql.shuffle.partitions = 200` tạo ra 200 task rất nhỏ cho dữ liệu sau gom nhóm chỉ gồm `10,778` dòng trên máy đơn (`local[*]`).

## C. Hypothesis & Change (Giả thuyết & Thay đổi theo nguyên tắc One Change At A Time)

### Thử nghiệm 1 (`E01_Eliminate_Recompute`): Khử tính toán lặp (Recompute)
- **Hypothesis (Mẫu If–Then):**
  > *"Nếu pipeline Baseline bị nghẽn cổ chai vì phải quét lại file CSV 439.03 MB tới 7 lần và tính toán lại `HashAggregate` 3 lần do các lệnh `.count()`, `.show(15)` và `.write()` rời rạc, thì khi **gộp các phép đếm kiểm chứng vào 1 pass duy nhất và gọi `.persist(StorageLevel.MEMORY_AND_DISK)` trên bảng tổng hợp `df_agg`**, tôi kỳ vọng **số lần quét CSV giảm từ 7 lần xuống 2 lần và median runtime giảm trên 40%**, trong khi **số dòng output (`10,778`) và tổng doanh thu (`$99,587,514.55`) giữ nguyên tuyệt đối**."*
- **Thay đổi duy nhất ở E01:** Giữ nguyên toàn bộ 19 cột đầu vào và giữ nguyên `spark.sql.shuffle.partitions = 200`; chỉ gộp phép đếm `raw_count`, `t1_count`, `t2_count`, `join_stats` vào 1 action tổng hợp có điều kiện và gọi `df_agg.persist(StorageLevel.MEMORY_AND_DISK)` trước khi gọi `.count()`, `.show(15)` và ghi file.

### Thử nghiệm 2 (`E02_Broadcast_Pruning_Shuffle16`): Cắt tỉa cột sớm + Broadcast Join + Tinh chỉnh Shuffle Partitions
- **Hypothesis (Mẫu If–Then):**
  > *"Nếu trên nền E01, bước `FileScan csv` vẫn phải phân tích cú pháp 12 cột không sử dụng và bước `Exchange hashpartitioning` tạo ra 200 shuffle partitions gây overhead lập lịch trên máy `local[*]`, thì khi **chỉ `.select()` 7 cột cần thiết ngay sau khi đọc, chỉ định tường minh `F.broadcast(df_zones)` và giảm `spark.sql.shuffle.partitions` từ 200 xuống 16**, tôi kỳ vọng **`ReadSchema` trong Physical Plan rút gọn còn 7 cột, `Exchange` giảm còn 16 partitions và median runtime tiếp tục giảm thêm**, trong khi **correctness giữ nguyên 100%**."*

## D. Before / After Measurement (Kết quả đo lường công bằng – 3 Runs / Cấu hình)

| Metric / Tiêu chí so sánh | Baseline (`B00`) | After Step 1 (`E01`) | After Step 2 (`E02`) | Thay đổi tổng cộng (Final vs Baseline) |
| :--- | :---: | :---: | :---: | :---: |
| **Run 1 Runtime (s)** | `67.73 s` | `17.50 s` | `23.31 s` | — |
| **Run 2 Runtime (s)** | `50.50 s` | `15.85 s` | `22.37 s` | — |
| **Run 3 Runtime (s)** | `51.62 s` | `14.56 s` | `24.71 s` | — |
| **Median Runtime (s)** | **51.62 s** | **15.85 s** | **23.31 s** | **Giảm 69.29% (Nhanh gấp 3.26×)** |
| **Mean ± Std Runtime (s)** | `56.62 ± 9.64 s` | `15.97 ± 1.47 s` | `23.46 ± 1.18 s` | Độ lệch chuẩn thấp, cải thiện vượt xa nhiễu |
| **Số lần quét Full CSV (439 MB)** | `7 lần (~3.07 GB)` | `2 lần (~0.88 GB)` | `2 lần (~0.88 GB, chỉ đọc 7/19 cột)` | **Giảm 71.4% số lần quét đĩa** |
| **Số cột trong `ReadSchema`** | `19 cột` | `19 cột` | `7 cột` | **Giảm 63.2% số cột cần parse** |
| **Shuffle Partitions (`Exchange`)** | `200` | `200` | `16` | **Giảm 92% overhead lập lịch shuffle task** |
| **Join Strategy trên Plan** | `BroadcastHashJoin` (auto) | `BroadcastHashJoin` (auto) | `BroadcastHashJoin` (explicit hint) | Đảm bảo không bao giờ rơi về `SortMergeJoin` |

## E. Correctness Verification (Kiểm chứng tính đúng đắn)

| Assertion kiểm chứng | Giá trị Baseline (`B00`) | Giá trị Optimized (`E01` & `E02`) | Trạng thái |
| :--- | :---: | :---: | :---: |
| **1. Row Count đầu vào (`raw_row_count`)** | `3,530,109` | `3,530,109` | **PASS** |
| **2. Row Count sau làm sạch (`clean_row_count`)** | `3,312,154` | `3,312,154` | **PASS** |
| **3. Unmatched NULL Join Keys** | `0` (Matched: `3,308,365`, Unknown 264/265: `3,789`) | `0` (Matched: `3,308,365`, Unknown 264/265: `3,789`) | **PASS** |
| **4. Output Row Count (`output_row_count`)** | `10,778` dòng | `10,778` dòng | **PASS** |
| **5. Business Metric 1 – Tổng số chuyến (`sum_total_trips`)** | `3,312,154` chuyến | `3,312,154` chuyến | **PASS** |
| **6. Business Metric 2 – Tổng doanh thu (`sum_total_revenue_usd`)** | `$99,587,514.55` | `$99,587,514.55` | **PASS** |
| **7. Top-1 Record Checksum (`PULocationID_Hour_Trips`)** | `161_13_10569` | `161_13_10569` | **PASS** |

## F. Decision, Limitations & Next Actions
- **Quyết định kỹ thuật (Decision):** **KEEP** cả hai cải tiến (`E01_Eliminate_Recompute` và `E02_Broadcast_Pruning_Shuffle16`) làm phiên bản mặc định trong `src/pipeline.py`.
- **Lý do:** Thời gian chạy trung vị (median runtime) giảm từ **51.62 giây** xuống còn **15.85 giây** (cải thiện **69.29%**, nhanh gấp **3.26 lần**), vượt rất xa biến thiên giữa các lần chạy (`std = 1.47 s`), đồng thời vượt qua tuyệt đối cả 7 bài kiểm tra Correctness.
- **Giới hạn của kết luận (Limitations):**
  - Thực nghiệm được đo trên môi trường máy đơn `local[*]` (16 luồng) với định dạng đầu vào là CSV (`439.03 MB`).
  - Do dữ liệu gốc đang lưu ở dạng CSV, chi phí đọc và chuyển đổi kiểu dữ liệu chuỗi sang thời gian (`to_timestamp`) trong 2 lần quét vẫn chiếm phần lớn thời gian còn lại. Nếu dữ liệu tăng gấp 10× (`~4.4 GB`), việc lưu trữ ở định dạng CSV sẽ trở thành nút thắt I/O chính.
- **Hướng phát triển cho Buổi 11 (Storage & Data Lake):**
  - Chuyển đổi định dạng lưu trữ ở tầng Raw/Silver từ **CSV** sang **Apache Parquet** (hoặc Delta Lake) kết hợp phân vùng theo ngày/tháng (`partitionBy("pickup_date")`) để tận dụng nén cột (columnar compression), đẩy bộ lọc xuống tầng lưu trữ (Predicate Pushdown) và loại bỏ hoàn toàn chi phí parse văn bản CSV.
