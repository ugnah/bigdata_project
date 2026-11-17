# Báo Cáo Checkpoint 3 – Spark Processing Pipeline

## A. Thông tin nhóm
- **Tên đề tài:** Xây dựng hệ thống thử nghiệm (Proof-of-Concept) phân tích dữ liệu giao thông thời gian thực dựa trên kiến trúc Big Data
- **Thành viên:** Lê Phi Hưng
- **Repository:** [https://github.com/ugnah/bigdata_project](https://github.com/ugnah/bigdata_project)
- **Commit/Tag:** `checkpoint-3` (`checkpoint-3: spark processing pipeline`)
- **Môi trường thực thi:** Python `3.13.7`, PySpark `3.5.0`, Master `local[*]`, OS Windows (`nt`)

## B. Input Contract
| Thuộc tính | Chi tiết |
| :--- | :--- |
| **Dataset 1 (Fact Table)** | NYC TLC Yellow Taxi Trip Records tháng 07/2026 (`yellow_tripdata_2026_07.csv`) từ [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| **Snapshot / Raw Path** | `data/raw/nyc_taxi/ingest_date=2026-10-24/run_20261024_084906` (theo `logs/ingest_manifest.json`) |
| **Format, Size & Row Count** | CSV (`header=true`), **439.03 MB**, **3,530,109 dòng**, **16 partitions** ban đầu |
| **Grain (Độ mịn dữ liệu)** | Mỗi dòng đại diện cho **01 chuyến xe taxi vàng (Yellow Taxi trip)** hoàn thành tại thành phố New York |
| **Key(s) chính** | Khóa ngoại không gian: `PULocationID` (điểm đón), `DOLocationID` (điểm trả); trường thời gian: `tpep_pickup_datetime`, `tpep_dropoff_datetime` |
| **Dataset 2 (Dimension Table)** | Bảng tra cứu vùng Taxi Zone Lookup (`data/source/taxi_zone_lookup.csv`, **265 dòng**) từ [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), khóa chính `LocationID` (`1–265`) |

## C. Chuỗi Transformations & Biến động Số dòng (Row Counts)
1. **Transformation 1 – Làm sạch dữ liệu & Lọc vi phạm miền giá trị cơ bản (`Data Cleaning & Domain Validation`):**
   - **Thực hiện:** Chuẩn hóa kiểu dữ liệu thời gian (`pickup_ts`, `dropoff_ts`), ép kiểu số nguyên cho `PULocationID`, `DOLocationID` và số thực cho `trip_distance`, `fare_amount`, `total_amount`. Lọc bỏ các bản ghi có timestamp `NULL`, `dropoff_ts <= pickup_ts`, `trip_distance < 0.1` dặm hoặc `> 200` dặm, `fare_amount < 0.01` USD hoặc `total_amount < 0.01` USD. Không lọc theo `passenger_count IS NULL` vì có tới `969,727` dòng (`27.47%`) là chuyến xe E-Hail/Flex Fare hợp lệ không nhập số hành khách.
   - **Kết quả Row Count:** Từ **3,530,109** dòng giảm còn **3,317,606** dòng (đã loại bỏ **212,503** dòng lỗi, chiếm **6.02%**).
2. **Transformation 2 – Tính toán chỉ số động học & Lọc ngoại lai vận tốc (`Speed & Duration Outlier Filter`):**
   - **Thực hiện:** Tính thời lượng di chuyển `trip_duration_min = (dropoff_ts - pickup_ts) / 60` (phút) và vận tốc trung bình `speed_mph = trip_distance / (trip_duration_min / 60)`. Lọc bỏ các chuyến có `trip_duration_min < 1.0` phút hoặc `> 300.0` phút (5 giờ), và vận tốc `speed_mph < 0.5` mph hoặc `> 80.0` mph.
   - **Kết quả Row Count:** Từ **3,317,606** dòng giảm còn **3,312,154** dòng (đã loại bỏ thêm **5,452** dòng ngoại lai vận tốc/thời lượng, chiếm **0.16%**).
3. **Transformation 3 – Trích xuất chiều thời gian & Phân tầng mức độ ùn tắc (`Temporal & Congestion Categorization`):**
   - **Thực hiện:** Trích xuất `pickup_date`, `pickup_hour` (`0–23`), `day_of_week`, cờ `is_weekend`, cờ giờ cao điểm `is_rush_hour` (`07:00–09:00` và `16:00–19:00` ngày thường), và phân loại trạng thái giao thông `traffic_status`:
     - `Heavy Congestion` (Ùn tắc nghiêm trọng): `speed_mph < 8.0` mph (`is_congested_trip = 1`).
     - `Moderate Traffic` (Đông đúc): `8.0 <= speed_mph < 15.0` mph.
     - `Free Flow` (Thông thoáng): `speed_mph >= 15.0` mph.
   - **Kết quả Row Count:** Giữ nguyên **3,312,154** dòng hợp lệ (**93.83%** tổng dữ liệu gốc).

## D. Kiểm chứng Phép kết nối (Join Validation)
- **Join Key & Join Type:** `LEFT JOIN` giữa bảng chuyến đi đã làm sạch (`PULocationID`) và bảng chiều vùng địa lý `taxi_zone_lookup.csv` (`LocationID`).
- **Kiểm tra Uniqueness:** Bảng `taxi_zone_lookup.csv` có **265** dòng và **265** khóa `LocationID` duy nhất (`lookup_total_rows = 265`, `lookup_distinct_keys = 265`). Pipeline gọi thêm `.dropDuplicates(["LocationID"])` để đảm bảo khóa phía bảng chiều là duy nhất tuyệt đối.
- **Đối chiếu Row Counts trước và sau Join:**
  - Số dòng trước Join (`rows_before_join`): **3,312,154** dòng
  - Số dòng sau Join (`rows_after_join`): **3,312,154** dòng (bảo toàn 100%, không xảy ra nhân bản dữ liệu / data explosion)
- **Thống kê Matched / Unmatched & Chính sách xử lý:**
  - Số dòng khớp vùng địa lý xác định tại NYC (`matched_valid_zone_rows`): **3,308,365** dòng (**99.89%**)
  - Số dòng rơi vào mã vùng `264` (`NV`) hoặc `265` (`NA` – Outside NYC) (`unknown_or_outside_nyc_rows`): **3,789** dòng (**0.11%**)
  - Số dòng không tồn tại khóa trong bảng tra cứu (`unmatched_null_key_rows`): **0** dòng
  - **Chính sách xử lý:** Giữ lại toàn bộ bản ghi qua `LEFT JOIN` và chuẩn hóa bằng `coalesce(pickup_borough, "Unknown")`, `coalesce(pickup_zone, "Unmapped Zone")` để bảo toàn tổng lưu lượng và doanh thu toàn hệ thống.

## E. Aggregation & Output
- **Output Grain:** Mỗi dòng kết quả đại diện cho **01 Khu vực đón (`PULocationID`, `pickup_borough`, `pickup_zone`) trong 01 Khung giờ (`pickup_hour`) phân theo loại ngày (`is_weekend`, `is_rush_hour`)**.
- **KPI / Metrics chính:** `total_trips`, `avg_speed_mph`, `avg_duration_min`, `avg_distance_miles`, `congested_trips`, `congestion_rate_pct`, `total_revenue_usd`, `avg_fare_usd`.
- **Đường dẫn, Định dạng & Kết quả:**
  - Đường dẫn: `output/traffic_zone_hourly_kpi`
  - Định dạng & Chế độ ghi: `csv` (`header=true`), `write_mode = overwrite` (đảm bảo tính lũy đẳng idempotency khi chạy lại)
  - Tổng số dòng Output (`output_row_count`): **10,778 dòng** (gộp về **1 partition**)

## F. Quality Evidence & Baseline Thực Nghiệm

### 1. Bảng tổng hợp Baseline Pipeline (`logs/pipeline_baseline.json`)
| Chỉ số kiểm định & Baseline | Giá trị thực nghiệm | Nguồn log đối chiếu |
| :--- | :--- | :--- |
| **Input Raw Rows** | **3,530,109** dòng | `input_contract.raw_row_count` |
| **Input Size (MB)** | **439.03 MB** | `input_contract.input_size_mb` |
| **Rows sau Transform 1 (Cleaning)** | **3,317,606** dòng (loại **212,503** dòng) | `transformations.after_t1_cleaning_count` |
| **Rows sau Transform 2 (Outlier Filter)** | **3,312,154** dòng (loại **5,452** dòng) | `transformations.after_t2_outlier_filter_count` |
| **Tỷ lệ dữ liệu sạch giữ lại** | **93.83%** (tổng loại bỏ **217,955** dòng) | `transformations.valid_row_ratio_pct` |
| **Matched Valid Zone Rows** | **3,308,365** dòng (**99.89%**) | `join_validation.matched_valid_zone_rows` |
| **Unknown / Outside NYC Rows (ID 264/265)** | **3,789** dòng (**0.11%**), Unmatched NULL: **0** | `join_validation.unknown_or_outside_nyc_rows` |
| **Output Aggregated Rows** | **10,778** dòng | `aggregation_output.output_row_count` |
| **Partitions (Input → Output)** | **16** partitions → **1** partition | `initial_partitions` → `output_partitions` |
| **Baseline Runtime (`pipeline.py`)** | **43.99 giây** | `baseline_performance.total_runtime_seconds` |
| **Execution Plan (`explain("formatted")`)** | **Đã lưu (`Yes`)** | `logs/explain_plan.txt` |

### 2. Bảng phát hiện Chất lượng Dữ liệu trên tập Raw (`logs/quality_report.json`, Runtime: `28.49s`)
| Hạng mục kiểm định | Số lượng bản ghi vi phạm | Tỷ lệ (%) trên 3,530,109 dòng | Giải thích & Xử lý trong Pipeline |
| :--- | :--- | :--- | :--- |
| **Null `passenger_count` / `RatecodeID` / `store_and_fwd_flag`** | **969,727** dòng | **27.4702%** | Các chuyến E-Hail / Flex Fare không ghi nhận số khách; giữ lại trong pipeline để tránh mất 27.47% lưu lượng thực tế |
| **Null trên các cột khóa & tọa độ/thời gian/giá cước** | **0** dòng | **0.0000%** | `VendorID`, `pickup/dropoff`, `PULocationID`, `DOLocationID`, `trip_distance`, `fare_amount` đầy đủ 100% |
| **Duplicate rows (trùng khóa sự kiện kết hợp)** | **13,545** dòng | **0.3837%** | Tỷ lệ trùng lặp rất nhỏ (`3,516,564` dòng duy nhất) |
| **Quãng đường lỗi (`< 0.1` hoặc `> 200` miles)** | **158,034** dòng | **4.4767%** | Đã loại bỏ triệt để ở Transformation 1 |
| **Thời gian lỗi (`dropoff <= pickup`)** | **42,316** dòng | **1.1987%** | Đã loại bỏ triệt để ở Transformation 1 |
| **Giá cước âm hoặc bằng 0 (`fare/total <= 0`)** | **16,969** dòng | **0.4807%** | Chuyến hủy/hoàn tiền hoặc lỗi đồng hồ; đã loại bỏ ở Transformation 1 |
| **Thời lượng ngoại lai (`< 1` phút hoặc `> 300` phút)** | **82,527** dòng | **2.3378%** | Đã loại bỏ ở Transformation 1 & 2 |
| **Vận tốc ngoại lai (`< 0.5` hoặc `> 80` mph)** | **189,953** dòng | **5.3809%** | Lỗi nhảy GPS hoặc đứng im bấm giờ; đã loại bỏ ở Transformation 1 & 2 |
| **Khóa vùng `PULocationID` thuộc mã 264/265 (Raw)** | **6,200** dòng | **0.1756%** | Sau bước lọc T1+T2 còn **3,789** dòng; được gán nhãn `"Unknown"` khi Join |

## G. Trả lời 6 Câu hỏi Phản biện Checkpoint 3 của Giảng viên
1. **Mỗi dòng input đại diện cho đối tượng/sự kiện gì?**
   - Mỗi dòng trong `yellow_tripdata_2026_07.csv` đại diện cho một sự kiện chuyến đi taxi vàng (Yellow Taxi trip) hoàn chỉnh tại NYC với đầy đủ thời điểm/khu vực đón trả, quãng đường và chi tiết cước phí.
2. **Join key có unique ở phía nào? Row count trước và sau join thay đổi thế nào?**
   - Join key `LocationID` là duy nhất (unique) ở phía bảng chiều `taxi_zone_lookup.csv` (`265` dòng ứng với `265` mã duy nhất). Do đó khi thực hiện `LEFT JOIN`, số dòng trước và sau khi join giữ nguyên tuyệt đối ở mức **3,312,154 dòng**, không phát sinh nhân bản dữ liệu.
3. **Unmatched records được xử lý ra sao?**
   - Có **0** dòng bị `NULL` khóa ngoại (`unmatched_null_key_rows = 0`) và **3,789** dòng rơi vào mã vùng `264/265` (`Unknown/Outside NYC`). Pipeline giữ nguyên các dòng này bằng `LEFT JOIN` và chuẩn hóa tên quận về `"Unknown"` để bảo toàn tổng lưu lượng và doanh thu.
4. **Aggregation này trả lời câu hỏi nào trong proposal?**
   - Trả lời câu hỏi cốt lõi: *"Khu vực (Zone/Borough) nào tại NYC có mật độ giao thông cao nhất và chịu mức độ ùn tắc nghiêm trọng nhất (vận tốc trung bình thấp nhất, tỷ lệ chuyến dưới 8 mph cao nhất) theo từng khung giờ trong ngày và giờ cao điểm?"*
5. **Pipeline chạy lại lần thứ hai có tạo duplicate hoặc output không nhất quán không?**
   - Không. Pipeline có tính lũy đẳng (**idempotent**): dữ liệu Raw chỉ đọc, các bước lọc tất định và bước ghi sử dụng `mode("overwrite")`, luôn tạo ra đúng **10,778 dòng** kết quả nhất quán.
6. **Baseline hôm nay là gì để Buổi 10 có thể so sánh trước/sau tối ưu?**
   - Baseline hiện tại đạt **43.99 giây** trên tập dữ liệu **439.03 MB** (`3,530,109` dòng, `16` partitions đầu vào) với định dạng CSV và kế hoạch thực thi mặc định lưu tại `logs/explain_plan.txt`. Ở Buổi 9–10, nhóm sẽ tối ưu bằng cách chuyển dữ liệu sang **Parquet**, sử dụng **Broadcast Join** cho bảng `taxi_zone_lookup.csv`, `cache()` DataFrame sau bước làm sạch để tránh quét lại file khi đếm số dòng, và tinh chỉnh số lượng shuffle partitions.

## H. Blocker & Next Actions (Hướng tới Buổi 9–10)
- **Blocker đã xử lý:** Đã khắc phục giới hạn thiếu `winutils.exe` trên Windows khi ghi output CSV và lỗi trích xuất `explain("formatted")`.
- **Next Action 1:** Tối ưu hóa thời gian chạy từ mốc baseline **43.99 giây** bằng kỹ thuật `cache()`, Broadcast Join và định dạng cột Parquet (Owner: Lê Phi Hưng – Deadline: Buổi 9).
- **Next Action 2:** Phân tích sâu hiện tượng lệch dữ liệu (Data Skew) tại các vùng trọng điểm khu vực Manhattan trong giờ cao điểm (Owner: Lê Phi Hưng – Deadline: Buổi 10).
