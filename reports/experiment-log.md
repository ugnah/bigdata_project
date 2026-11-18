# Experiment Log – Đồ Án 4 (Benchmark & Tối Ưu Pipeline)

- **Dự án:** Phân tích dữ liệu giao thông thời gian thực dựa trên kiến trúc Big Data ([ugnah/bigdata_project](https://github.com/ugnah/bigdata_project))
- **Dataset:** `yellow_tripdata_2026_07.csv` (**3,530,109 dòng**, **439.03 MB**, **16 partitions** đầu vào)
- **Môi trường cố định:** Python `3.13.7`, PySpark `3.5.0`, Master `local[*]`, OS `nt`

## 1. Bảng Tổng Hợp Các Thử Nghiệm (Experiment Log Table)

| EXP ID | Commit | Thay đổi chính (One Change At A Time) | Giả thuyết (Hypothesis) | Run 1 (s) | Run 2 (s) | Run 3 (s) | Median (s) | Δ vs Baseline | Plan / Execution Signal | Correctness | Decision |
| :--- | :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :--- | :---: | :---: |
| **B00_Baseline** | `2f3e0ef` | Baseline nguyên bản từ Checkpoint 3 (gọi 7 actions rời rạc, đọc 19 cột, `shuffle.partitions=200`) | Mốc tham chiếu cơ sở chưa tối ưu | 67.73 | 50.50 | 51.62 | **51.62 s** | 0.00% | 7 Spark Jobs quét lặp lại `FileScan csv` | PASS | BASELINE |
| **E01_Eliminate_Recompute** | `2f3e0ef` | Gom phép đếm kiểm chứng vào 1 pass + `persist(MEMORY_AND_DISK)` trên bảng tổng hợp `df_agg` | Khử việc đọc lại file CSV 439.03 MB 7 lần và tránh tính lại `HashAggregate` khi vừa `.count()`, `.show()`, vừa `.write()` | 17.50 | 15.85 | 14.56 | **15.85 s** | **-69.29%** | Giảm từ 7 lần `FileScan csv` xuống 2 lần; `InMemoryTableScan` trên `df_agg` | PASS | **KEEP** |
| **E02_Broadcast_Pruning_Shuffle16** | `2f3e0ef` | Trên nền E01: Cắt tỉa cột sớm (`select` 7/19 cột) + `F.broadcast(df_zones)` + giảm `spark.sql.shuffle.partitions` từ 200 xuống 16 | Giảm khối lượng parse CSV đầu vào, đảm bảo `BroadcastHashJoin` và giảm overhead lập lịch 200 task nhỏ xuống 16 task khớp số luồng CPU | 23.31 | 22.37 | 24.71 | **23.31 s** | **-54.84%** (vs E01: --47.07%) | `ReadSchema` rút gọn 7 cột; `Exchange hashpartitioning(..., 16)` | PASS | **KEEP (Conditional)** |

## 2. Kiểm Chứng Correctness Across All Runs

Tất cả 9 lần chạy (3 runs × 3 cấu hình) đều vượt qua 100% cả 7 assertions đối chiếu:
- `raw_row_count` = **3,530,109** dòng
- `clean_row_count` (sau T1 + T2 + T3) = **3,312,154** dòng
- `unmatched_null_key_rows` = **0** dòng (khớp vùng hợp lệ: **3,308,365** dòng, mã 264/265: **3,789** dòng)
- `output_row_count` = **10,778** dòng
- `sum_total_trips` = **3,312,154** chuyến
- `sum_total_revenue_usd` = **$99,587,514.55**
- `top1_record_signature` (`PULocationID_Hour_Trips`) = `161_13_10569`
