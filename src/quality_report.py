import os
import sys
import json
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

from pipeline import load_config, resolve_raw_input_path, ensure_zone_lookup

# Thiết lập HADOOP_HOME cho môi trường Windows
if os.name == "nt":
    hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
    if os.path.exists(hadoop_home):
        os.environ["HADOOP_HOME"] = hadoop_home
        hadoop_bin = os.path.join(hadoop_home, "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + hadoop_bin


def main():
    start_time = time.time()
    cfg = load_config()
    thresholds = cfg["filter_thresholds"]

    print("=" * 80)
    print("NYC TAXI BIG DATA PROJECT - CHECKPOINT 3: DATA QUALITY REPORT")
    print("=" * 80)

    spark = (
        SparkSession.builder.appName("NYC_Taxi_Checkpoint3_QualityReport")
        .master("local[*]")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_path = resolve_raw_input_path(cfg)
        print(f"\n[1] Đang đọc dữ liệu Raw cần kiểm định tại: {raw_path}")
        df_raw = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(raw_path)
        )

        total_rows = df_raw.count()
        print(f" -> Tổng số bản ghi (Total Rows): {total_rows:,}")

        # ---------------------------------------------------------------------
        # 1. KIỂM TRA GIÁ TRỊ NULL TRÊN CÁC CỘT CHÍNH
        # ---------------------------------------------------------------------
        print("\n[2] Kiểm tra tỷ lệ Null trên các trường quan trọng...")
        key_columns = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "RatecodeID",
            "store_and_fwd_flag",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "total_amount",
        ]
        existing_cols = [c for c in key_columns if c in df_raw.columns]
        null_exprs = [
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in existing_cols
        ]
        null_row = df_raw.select(null_exprs).collect()[0].asDict()

        null_summary = {}
        for col_name, null_cnt in null_row.items():
            null_cnt = int(null_cnt or 0)
            pct = round((null_cnt / max(total_rows, 1)) * 100.0, 4)
            null_summary[col_name] = {"null_count": null_cnt, "null_pct": pct}
            print(f"   - {col_name:<24}: {null_cnt:>10,} nulls ({pct:.2f}%)")

        # ---------------------------------------------------------------------
        # 2. KIỂM TRA BẢN GHI TRÙNG LẶP (DUPLICATE CHECK)
        # ---------------------------------------------------------------------
        print("\n[3] Kiểm tra bản ghi trùng lặp (Duplicates)...")
        dup_Composite_Keys = [
            c
            for c in [
                "VendorID",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "trip_distance",
                "total_amount",
            ]
            if c in df_raw.columns
        ]
        distinct_rows = df_raw.dropDuplicates(dup_Composite_Keys).count()
        duplicate_rows = total_rows - distinct_rows
        duplicate_pct = round((duplicate_rows / max(total_rows, 1)) * 100.0, 4)
        print(
            f"   - Số bản ghi duy nhất theo khóa sự kiện: {distinct_rows:,} "
            f"| Bản ghi trùng lặp: {duplicate_rows:,} ({duplicate_pct:.2f}%)"
        )

        # ---------------------------------------------------------------------
        # 3. KIỂM TRA DÒNG DỮ LIỆU KHÔNG HỢP LỆ / DỊ BIỆT (INVALID ROWS)
        # ---------------------------------------------------------------------
        print("\n[4] Kiểm tra các quy tắc nghiệp vụ & vật lý (Invalid Rows)...")
        df_typed = (
            df_raw.withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
            .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))
            .withColumn("PULocationID", F.col("PULocationID").cast(IntegerType()))
            .withColumn("DOLocationID", F.col("DOLocationID").cast(IntegerType()))
            .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
            .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
            .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
            .withColumn("passenger_count", F.col("passenger_count").cast(DoubleType()))
            .withColumn(
                "duration_min",
                (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0,
            )
            .withColumn(
                "speed_mph",
                F.when(
                    F.col("duration_min") > 0,
                    F.col("trip_distance") / (F.col("duration_min") / 60.0),
                ).otherwise(F.lit(-1.0)),
            )
        )

        invalid_metrics = df_typed.select(
            F.sum(
                F.when(
                    F.col("dropoff_ts").isNull()
                    | F.col("pickup_ts").isNull()
                    | (F.col("dropoff_ts") <= F.col("pickup_ts")),
                    1,
                ).otherwise(0)
            ).alias("invalid_timestamps_or_non_positive_duration"),
            F.sum(
                F.when(
                    F.col("trip_distance").isNull()
                    | (F.col("trip_distance") < thresholds["min_trip_distance_miles"])
                    | (F.col("trip_distance") > thresholds["max_trip_distance_miles"]),
                    1,
                ).otherwise(0)
            ).alias("invalid_trip_distance"),
            F.sum(
                F.when(
                    F.col("fare_amount").isNull()
                    | (F.col("fare_amount") < thresholds["min_fare_amount"])
                    | F.col("total_amount").isNull()
                    | (F.col("total_amount") < thresholds["min_total_amount"]),
                    1,
                ).otherwise(0)
            ).alias("invalid_negative_or_zero_fare"),
            F.sum(
                F.when(
                    (F.col("duration_min") < thresholds["min_duration_minutes"])
                    | (F.col("duration_min") > thresholds["max_duration_minutes"]),
                    1,
                ).otherwise(0)
            ).alias("outlier_duration_lt1m_or_gt300m"),
            F.sum(
                F.when(
                    (F.col("speed_mph") < thresholds["min_speed_mph"])
                    | (F.col("speed_mph") > thresholds["max_speed_mph"]),
                    1,
                ).otherwise(0)
            ).alias("outlier_speed_lt0_5_or_gt80mph"),
            F.sum(
                F.when(
                    F.col("passenger_count").isNotNull() & (F.col("passenger_count") <= 0),
                    1,
                ).otherwise(0)
            ).alias("zero_or_negative_passengers"),
        ).collect()[0].asDict()

        for rule_name, cnt in invalid_metrics.items():
            cnt = int(cnt or 0)
            pct = round((cnt / max(total_rows, 1)) * 100.0, 4)
            invalid_metrics[rule_name] = {"count": cnt, "pct": pct}
            print(f"   - {rule_name:<45}: {cnt:>10,} rows ({pct:.2f}%)")

        # ---------------------------------------------------------------------
        # 4. KIỂM TRA KHỚP KHÓA VỚI BẢNG TAXI ZONE LOOKUP (UNMATCHED KEYS)
        # ---------------------------------------------------------------------
        print("\n[5] Kiểm tra tính toàn vẹn khóa ngoại (LocationID Join Integrity)...")
        lookup_path = ensure_zone_lookup(cfg["paths"]["zone_lookup_path"])
        df_zones = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(lookup_path)
            .withColumn("LocationID", F.col("LocationID").cast(IntegerType()))
            .dropDuplicates(["LocationID"])
        )

        unmatched_pu = (
            df_typed.join(
                df_zones,
                df_typed["PULocationID"] == df_zones["LocationID"],
                how="left_anti",
            ).count()
        )
        unmatched_do = (
            df_typed.join(
                df_zones,
                df_typed["DOLocationID"] == df_zones["LocationID"],
                how="left_anti",
            ).count()
        )
        unknown_pu_264_265 = df_typed.filter(F.col("PULocationID").isin(264, 265)).count()
        unknown_do_264_265 = df_typed.filter(F.col("DOLocationID").isin(264, 265)).count()

        print(f"   - PULocationID không tồn tại trong bảng Lookup: {unmatched_pu:,}")
        print(f"   - DOLocationID không tồn tại trong bảng Lookup: {unmatched_do:,}")
        print(f"   - PULocationID thuộc mã 264/265 (Unknown/Outside NYC): {unknown_pu_264_265:,}")
        print(f"   - DOLocationID thuộc mã 264/265 (Unknown/Outside NYC): {unknown_do_264_265:,}")

        report = {
            "timestamp": datetime.now().isoformat(),
            "raw_input_path": raw_path,
            "total_rows": total_rows,
            "null_summary": null_summary,
            "duplicate_check": {
                "composite_keys": dup_Composite_Keys,
                "distinct_rows": distinct_rows,
                "duplicate_rows": duplicate_rows,
                "duplicate_pct": duplicate_pct,
            },
            "invalid_row_rules": invalid_metrics,
            "join_key_integrity": {
                "lookup_path": lookup_path,
                "unmatched_PULocationID_rows": unmatched_pu,
                "unmatched_DOLocationID_rows": unmatched_do,
                "unknown_code_264_265_PULocationID_rows": unknown_pu_264_265,
                "unknown_code_264_265_DOLocationID_rows": unknown_do_264_265,
            },
            "runtime_seconds": round(time.time() - start_time, 2),
        }

        os.makedirs("logs", exist_ok=True)
        report_path = cfg["paths"]["quality_report_path"]
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4, ensure_ascii=False)

        print(f"\n[SUCCESS] Đã xuất báo cáo chất lượng dữ liệu tại: {report_path}")

    except Exception as e:
        print(f"\n[ERROR] Kiểm định chất lượng thất bại: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
