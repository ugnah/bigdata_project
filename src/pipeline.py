import os
import sys
import io
import csv
import json
import time
import glob
import urllib.request
from contextlib import redirect_stdout
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# Thiết lập HADOOP_HOME cho môi trường Windows (nếu có C:\hadoop)
if os.name == "nt":
    hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
    if os.path.exists(hadoop_home):
        os.environ["HADOOP_HOME"] = hadoop_home
        hadoop_bin = os.path.join(hadoop_home, "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + hadoop_bin


def load_config(config_path="config/config.json"):
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "project_name": "NYC_Taxi_Traffic_Analysis_PoC",
        "checkpoint": "checkpoint-3",
        "paths": {
            "raw_input_dir": "data/raw/nyc_taxi/ingest_date=2026-10-24/run_20261024_084906",
            "raw_base_dir": "data/raw/nyc_taxi",
            "zone_lookup_path": "data/source/taxi_zone_lookup.csv",
            "output_dir": "output/traffic_zone_hourly_kpi",
            "quality_report_path": "logs/quality_report.json",
            "baseline_report_path": "logs/pipeline_baseline.json",
            "explain_plan_path": "logs/explain_plan.txt",
        },
        "io_options": {
            "input_format": "csv",
            "output_format": "csv",
            "write_mode": "overwrite",
            "coalesce_output": 1,
        },
        "filter_thresholds": {
            "min_trip_distance_miles": 0.1,
            "max_trip_distance_miles": 200.0,
            "min_duration_minutes": 1.0,
            "max_duration_minutes": 300.0,
            "min_speed_mph": 0.5,
            "max_speed_mph": 80.0,
            "min_fare_amount": 0.01,
            "min_total_amount": 0.01,
            "heavy_congestion_mph": 8.0,
            "moderate_congestion_mph": 15.0,
        },
    }


def resolve_raw_input_path(cfg):
    """Tìm đường dẫn dữ liệu raw từ config hoặc tự động dò bản run mới nhất."""
    configured_path = cfg["paths"]["raw_input_dir"]
    if os.path.exists(configured_path):
        return configured_path

    manifest_path = "logs/ingest_manifest.json"
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
                dest = manifest.get("destination")
                if dest and os.path.exists(dest):
                    return dest
        except Exception:
            pass

    base_dir = cfg["paths"].get("raw_base_dir", "data/raw/nyc_taxi")
    if os.path.exists(base_dir):
        runs = sorted(glob.glob(os.path.join(base_dir, "ingest_date=*", "run_*")))
        if runs:
            return runs[-1]

    source_fallback = "data/source/yellow_tripdata_2026_07.csv"
    if os.path.exists(source_fallback):
        return source_fallback

    raise FileNotFoundError(
        f"Không tìm thấy dữ liệu raw tại '{configured_path}' hoặc '{base_dir}'. "
        "Hãy kiểm tra lại đường dẫn trong config/config.json."
    )


def ensure_zone_lookup(lookup_path):
    """Đảm bảo bảng tra cứu Taxi Zone Lookup tồn tại (tải từ NYC TLC hoặc tạo bảng chuẩn nếu offline)."""
    if os.path.exists(lookup_path) and os.path.getsize(lookup_path) > 0:
        return lookup_path

    os.makedirs(os.path.dirname(lookup_path), exist_ok=True)
    tlc_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    print(f"[INFO] Đang tải bảng tra cứu vùng từ NYC TLC: {tlc_url} ...")
    try:
        urllib.request.urlretrieve(tlc_url, lookup_path)
        print(f"[INFO] Đã lưu bảng tra cứu tại: {lookup_path}")
        return lookup_path
    except Exception as e:
        print(f"[WARN] Không thể tải trực tiếp ({e}). Khởi tạo bảng tra cứu chuẩn cục bộ...")
        fallback_rows = [
            "LocationID,Borough,Zone,service_zone",
            "1,EWR,Newark Airport,EWR",
            "4,Manhattan,Alphabet City,Yellow Zone",
            "12,Manhattan,Battery Park,Yellow Zone",
            "13,Manhattan,Battery Park City,Yellow Zone",
            "24,Manhattan,Bloomingdale,Yellow Zone",
            "41,Manhattan,Central Harlem,Boro Zone",
            "42,Manhattan,Central Harlem North,Boro Zone",
            "43,Manhattan,Central Park,Yellow Zone",
            "45,Manhattan,Chinatown,Yellow Zone",
            "48,Manhattan,Clinton East,Yellow Zone",
            "50,Manhattan,Clinton West,Yellow Zone",
            "68,Manhattan,East Chelsea,Yellow Zone",
            "70,Queens,East Elmhurst,Boro Zone",
            "74,Manhattan,East Harlem North,Boro Zone",
            "75,Manhattan,East Harlem South,Boro Zone",
            "79,Manhattan,East Village,Yellow Zone",
            "87,Manhattan,Financial District North,Yellow Zone",
            "88,Manhattan,Financial District South,Yellow Zone",
            "90,Manhattan,Flatiron,Yellow Zone",
            "100,Manhattan,Garment District,Yellow Zone",
            "107,Manhattan,Gramercy,Yellow Zone",
            "113,Manhattan,Greenwich Village North,Yellow Zone",
            "114,Manhattan,Greenwich Village South,Yellow Zone",
            "125,Manhattan,Hudson Sq,Yellow Zone",
            "132,Queens,JFK Airport,Airports",
            "137,Manhattan,Kips Bay,Yellow Zone",
            "138,Queens,LaGuardia Airport,Airports",
            "140,Manhattan,Lenox Hill East,Yellow Zone",
            "141,Manhattan,Lenox Hill West,Yellow Zone",
            "142,Manhattan,Lincoln Square East,Yellow Zone",
            "143,Manhattan,Lincoln Square West,Yellow Zone",
            "144,Manhattan,Little Italy/NoLiTa,Yellow Zone",
            "148,Manhattan,Lower East Side,Yellow Zone",
            "151,Manhattan,Manhattan Valley,Yellow Zone",
            "158,Manhattan,Meatpacking/West Village West,Yellow Zone",
            "161,Manhattan,Midtown Center,Yellow Zone",
            "162,Manhattan,Midtown East,Yellow Zone",
            "163,Manhattan,Midtown North,Yellow Zone",
            "164,Manhattan,Midtown South,Yellow Zone",
            "170,Manhattan,Murray Hill,Yellow Zone",
            "186,Manhattan,Penn Station/Madison Sq West,Yellow Zone",
            "209,Manhattan,Seaport,Yellow Zone",
            "211,Manhattan,SoHo,Yellow Zone",
            "224,Manhattan,Stuy Town/Peter Cooper Village,Yellow Zone",
            "229,Manhattan,Sutton Place/Turtle Bay North,Yellow Zone",
            "230,Manhattan,Times Sq/Theatre District,Yellow Zone",
            "231,Manhattan,TriBeCa/Civic Center,Yellow Zone",
            "232,Manhattan,Two Bridges/Seward Park,Yellow Zone",
            "233,Manhattan,UN/Turtle Bay South,Yellow Zone",
            "234,Manhattan,Union Sq,Yellow Zone",
            "236,Manhattan,Upper East Side North,Yellow Zone",
            "237,Manhattan,Upper East Side South,Yellow Zone",
            "238,Manhattan,Upper West Side North,Yellow Zone",
            "239,Manhattan,Upper West Side South,Yellow Zone",
            "246,Manhattan,West Chelsea/Hudson Yards,Yellow Zone",
            "249,Manhattan,West Village,Yellow Zone",
            "261,Manhattan,World Trade Center,Yellow Zone",
            "262,Manhattan,Yorkville East,Yellow Zone",
            "263,Manhattan,Yorkville West,Yellow Zone",
            "264,Unknown,NV,N/A",
            "265,Unknown,NA,N/A",
        ]
        with open(lookup_path, "w", encoding="utf-8") as f:
            f.write("\n".join(fallback_rows) + "\n")
        return lookup_path


def get_dir_size_mb(path):
    if os.path.isfile(path):
        return round(os.path.getsize(path) / (1024 * 1024), 2)
    total = 0
    for root, _, files in os.walk(path):
        for file in files:
            if not file.endswith(".crc"):
                total += os.path.getsize(os.path.join(root, file))
    return round(total / (1024 * 1024), 2)


def capture_explain_plan(df):
    """Lấy chuỗi Execution Plan formatted tương thích với mọi phiên bản PySpark 3.x."""
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            df.explain(mode="formatted")
        plan_text = buf.getvalue()
        if plan_text.strip():
            return plan_text
    except Exception:
        pass
    try:
        return df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")
    except Exception:
        return df._jdf.queryExecution().toString()


def write_output_safely(df_agg, output_dir, out_fmt="csv", write_mode="overwrite", coalesce_n=1):
    """Ghi dữ liệu Output bằng Spark writer; tự động fallback sang Python CSV nếu thiếu winutils.exe trên Windows."""
    writer_df = df_agg.coalesce(coalesce_n) if coalesce_n else df_agg
    try:
        if out_fmt == "parquet":
            writer_df.write.mode(write_mode).parquet(output_dir)
        else:
            writer_df.write.mode(write_mode).option("header", "true").csv(output_dir)
        print(f" -> Đã ghi Output ({out_fmt.upper()}, mode={write_mode}) vào: {output_dir}")
    except Exception as spark_write_err:
        print(
            f" -> [WARN] Spark native writer gặp giới hạn Windows IO ({spark_write_err.__class__.__name__}). "
            "Chuyển sang ghi trực tiếp file CSV chuẩn vào thư mục output..."
        )
        os.makedirs(output_dir, exist_ok=True)
        out_csv_file = os.path.join(output_dir, "part-00000-traffic_zone_hourly_kpi.csv")
        cols = df_agg.columns
        rows = df_agg.collect()
        with open(out_csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            for r in rows:
                writer.writerow([r[c] for c in cols])
        print(f" -> Đã ghi thành công {len(rows):,} dòng KPI vào: {out_csv_file}")


def main():
    start_time = time.time()
    cfg = load_config()
    thresholds = cfg["filter_thresholds"]

    print("=" * 80)
    print("NYC TAXI BIG DATA PROJECT - CHECKPOINT 3: SPARK PROCESSING PIPELINE")
    print("=" * 80)

    spark = (
        SparkSession.builder.appName("NYC_Taxi_Checkpoint3_Pipeline")
        .master("local[*]")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # =====================================================================
        # BƯỚC 1: ĐỌC DỮ LIỆU RAW & KIỂM TRA INPUT CONTRACT
        # =====================================================================
        raw_path = resolve_raw_input_path(cfg)
        input_size_mb = get_dir_size_mb(raw_path)
        print(f"\n[STEP 1] Đọc dữ liệu Raw từ: {raw_path} (Dung lượng: {input_size_mb} MB)")

        df_raw = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(raw_path)
        )

        initial_partitions = df_raw.rdd.getNumPartitions()
        print(f" -> Số lượng Partitions ban đầu: {initial_partitions}")
        print(" -> Schema dữ liệu đầu vào:")
        df_raw.printSchema()

        # =====================================================================
        # BƯỚC 2: TRANSFORMATIONS (LÀM SẠCH, TÍNH TOÁN CHỈ SỐ & PHÂN LOẠI)
        # =====================================================================
        print("\n[STEP 2] Thực hiện chuỗi Transformations...")

        df_typed = (
            df_raw.withColumn("pickup_ts", F.to_timestamp(F.col("tpep_pickup_datetime")))
            .withColumn("dropoff_ts", F.to_timestamp(F.col("tpep_dropoff_datetime")))
            .withColumn("PULocationID", F.col("PULocationID").cast(IntegerType()))
            .withColumn("DOLocationID", F.col("DOLocationID").cast(IntegerType()))
            .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
            .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
            .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
            .withColumn("passenger_count", F.col("passenger_count").cast(DoubleType()))
        )

        cond_t1 = (
            F.col("pickup_ts").isNotNull()
            & F.col("dropoff_ts").isNotNull()
            & F.col("PULocationID").isNotNull()
            & F.col("DOLocationID").isNotNull()
            & (F.col("dropoff_ts") > F.col("pickup_ts"))
            & (F.col("trip_distance") >= thresholds["min_trip_distance_miles"])
            & (F.col("trip_distance") <= thresholds["max_trip_distance_miles"])
            & (F.col("fare_amount") >= thresholds["min_fare_amount"])
            & (F.col("total_amount") >= thresholds["min_total_amount"])
        )

        # --- Transformation 1: Lọc bản ghi lỗi cơ bản ---
        df_t1 = df_typed.filter(cond_t1)

        # --- Transformation 2: Tính toán thời lượng chuyến đi (phút), vận tốc (mph) & lọc ngoại lai vật lý ---
        df_with_kinetics = df_t1.withColumn(
            "trip_duration_min",
            F.round((F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0, 2),
        ).withColumn(
            "speed_mph",
            F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60.0), 2),
        )

        cond_t2 = (
            (F.col("trip_duration_min") >= thresholds["min_duration_minutes"])
            & (F.col("trip_duration_min") <= thresholds["max_duration_minutes"])
            & (F.col("speed_mph") >= thresholds["min_speed_mph"])
            & (F.col("speed_mph") <= thresholds["max_speed_mph"])
        )
        df_t2 = df_with_kinetics.filter(cond_t2)

        # --- Transformation 3: Trích xuất đặc trưng thời gian & Phân loại trạng thái giao thông ---
        df_t3 = (
            df_t2.withColumn("pickup_date", F.to_date("pickup_ts"))
            .withColumn("pickup_hour", F.hour("pickup_ts"))
            .withColumn("day_of_week", F.dayofweek("pickup_ts"))
            .withColumn(
                "is_weekend",
                F.when(F.col("day_of_week").isin(1, 7), F.lit(True)).otherwise(F.lit(False)),
            )
            .withColumn(
                "is_rush_hour",
                F.when(
                    (~F.col("is_weekend"))
                    & (
                        F.col("pickup_hour").between(7, 9)
                        | F.col("pickup_hour").between(16, 19)
                    ),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )
            .withColumn(
                "traffic_status",
                F.when(
                    F.col("speed_mph") < thresholds["heavy_congestion_mph"],
                    F.lit("Heavy Congestion"),
                )
                .when(
                    F.col("speed_mph") < thresholds["moderate_congestion_mph"],
                    F.lit("Moderate Traffic"),
                )
                .otherwise(F.lit("Free Flow")),
            )
            .withColumn(
                "is_congested_trip",
                F.when(F.col("speed_mph") < thresholds["heavy_congestion_mph"], F.lit(1)).otherwise(
                    F.lit(0)
                ),
            )
        )

        # Đo đếm số dòng qua từng bước Transformation
        raw_count = df_raw.count()
        t1_count = df_t1.count()
        t2_count = df_t2.count()
        t3_count = t2_count

        t1_dropped = raw_count - t1_count
        t2_dropped = t1_count - t2_count

        print(f" -> Tổng số dòng ban đầu (Raw Row Count): {raw_count:,}")
        print(
            f" -> [Transform 1 - Data Cleaning] Trước: {raw_count:,} | Sau: {t1_count:,} "
            f"| Đã loại bỏ: {t1_dropped:,} dòng lỗi ({(t1_dropped / max(raw_count, 1)) * 100:.2f}%)"
        )
        print(
            f" -> [Transform 2 - Speed & Duration Outlier Filter] Trước: {t1_count:,} | Sau: {t2_count:,} "
            f"| Đã loại bỏ: {t2_dropped:,} dòng ngoại lai ({(t2_dropped / max(t1_count, 1)) * 100:.2f}%)"
        )
        print(
            f" -> [Transform 3 - Temporal & Traffic Feature Derivation] Giữ nguyên số dòng hợp lệ: {t3_count:,}"
        )

        # =====================================================================
        # BƯỚC 3: JOIN VỚI BẢNG TRA CỨU TAXI ZONE & KIỂM CHỨNG JOIN CORRECTNESS
        # =====================================================================
        print("\n[STEP 3] Kiểm tra & Thực hiện Join với bảng Taxi Zone Lookup...")
        lookup_path = ensure_zone_lookup(cfg["paths"]["zone_lookup_path"])
        df_zones_raw = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(lookup_path)
            .withColumn("LocationID", F.col("LocationID").cast(IntegerType()))
        )

        zone_total_rows = df_zones_raw.count()
        zone_distinct_keys = df_zones_raw.select("LocationID").distinct().count()
        print(
            f" -> Kiểm tra Uniqueness bảng Zone Lookup: Tổng số dòng = {zone_total_rows}, "
            f"Số LocationID duy nhất = {zone_distinct_keys}"
        )

        df_zones = df_zones_raw.dropDuplicates(["LocationID"]).select(
            F.col("LocationID").alias("pu_loc_id"),
            F.col("Borough").alias("pickup_borough"),
            F.col("Zone").alias("pickup_zone"),
            F.col("service_zone").alias("pickup_service_zone"),
        )

        df_joined = df_t3.join(
            df_zones,
            df_t3["PULocationID"] == df_zones["pu_loc_id"],
            how="left",
        )

        join_stats = (
            df_joined.select(
                F.count("*").alias("rows_after_join"),
                F.sum(F.when(F.col("pu_loc_id").isNull(), 1).otherwise(0)).alias("unmatched_null"),
                F.sum(
                    F.when(
                        F.col("PULocationID").isin(264, 265)
                        | F.col("pickup_borough").isin("Unknown", "N/A"),
                        1,
                    ).otherwise(0)
                ).alias("unknown_zone"),
            )
            .collect()[0]
            .asDict()
        )

        rows_after_join = int(join_stats["rows_after_join"] or 0)
        unmatched_null_count = int(join_stats["unmatched_null"] or 0)
        unknown_zone_count = int(join_stats["unknown_zone"] or 0)
        matched_valid_zone_count = rows_after_join - unmatched_null_count - unknown_zone_count

        df_joined_clean = (
            df_joined.withColumn(
                "pickup_borough",
                F.coalesce(F.col("pickup_borough"), F.lit("Unknown")),
            )
            .withColumn(
                "pickup_zone",
                F.coalesce(F.col("pickup_zone"), F.lit("Unmapped Zone")),
            )
            .withColumn(
                "pickup_service_zone",
                F.coalesce(F.col("pickup_service_zone"), F.lit("N/A")),
            )
            .drop("pu_loc_id")
        )

        print(f" -> Row count trước Join: {t3_count:,} | Row count sau Join: {rows_after_join:,}")
        print(f" -> Số bản ghi khớp khu vực xác định (Matched Valid Zones): {matched_valid_zone_count:,}")
        print(f" -> Số bản ghi thuộc mã Unknown/Outside NYC (ID 264, 265): {unknown_zone_count:,}")
        print(f" -> Số bản ghi không khớp khóa (Unmatched NULL Key): {unmatched_null_count:,}")

        # =====================================================================
        # BƯỚC 4: AGGREGATION (TỔNG HỢP KPI GIAO THÔNG THEO VÙNG & KHUNG GIỜ)
        # =====================================================================
        print("\n[STEP 4] Tổng hợp KPI Giao thông (Aggregation)...")
        df_agg = (
            df_joined_clean.groupBy(
                "PULocationID",
                "pickup_borough",
                "pickup_zone",
                "pickup_hour",
                "is_weekend",
                "is_rush_hour",
            )
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
                F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
                F.sum("is_congested_trip").alias("congested_trips"),
                F.round(
                    (F.sum("is_congested_trip") / F.count("*")) * 100.0, 2
                ).alias("congestion_rate_pct"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue_usd"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
            )
            .orderBy(F.desc("total_trips"), F.asc("avg_speed_mph"))
        )

        agg_row_count = df_agg.count()
        agg_partitions = df_agg.rdd.getNumPartitions()
        print(f" -> Tổng số dòng Output sau Aggregation: {agg_row_count:,}")
        print(f" -> Số Partitions của Output DataFrame: {agg_partitions}")
        print("\n -> Top 15 Khu vực & Khung giờ có lưu lượng giao thông cao nhất:")
        df_agg.show(15, truncate=False)

        # =====================================================================
        # BƯỚC 5: LƯU EXECUTION PLAN, OUTPUT & BASELINE METRICS
        # =====================================================================
        print("\n[STEP 5] Lưu Execution Plan, Output và Baseline Metrics...")
        os.makedirs("logs", exist_ok=True)
        output_dir = cfg["paths"]["output_dir"]
        os.makedirs(os.path.dirname(output_dir), exist_ok=True)

        # 5.1 Lưu Execution Plan (explain formatted)
        explain_path = cfg["paths"]["explain_plan_path"]
        formatted_plan = capture_explain_plan(df_agg)
        with open(explain_path, "w", encoding="utf-8") as f:
            f.write(formatted_plan)
        print(f" -> Đã lưu Execution Plan tại: {explain_path}")

        # 5.2 Ghi dữ liệu Output (tự động xử lý nếu thiếu winutils.exe trên Windows)
        write_mode = cfg["io_options"].get("write_mode", "overwrite")
        out_fmt = cfg["io_options"].get("output_format", "csv").lower()
        coalesce_n = cfg["io_options"].get("coalesce_output", 1)
        write_output_safely(df_agg, output_dir, out_fmt, write_mode, coalesce_n)

        # 5.3 Lưu Baseline Metrics
        runtime_sec = round(time.time() - start_time, 2)
        baseline_info = {
            "timestamp": datetime.now().isoformat(),
            "checkpoint": "checkpoint-3",
            "environment": {
                "python_version": sys.version.split()[0],
                "spark_version": spark.version,
                "master": spark.sparkContext.master,
                "os": os.name,
            },
            "input_contract": {
                "raw_input_path": raw_path,
                "input_size_mb": input_size_mb,
                "raw_row_count": raw_count,
                "initial_partitions": initial_partitions,
            },
            "transformations": {
                "after_t1_cleaning_count": t1_count,
                "t1_dropped_rows": t1_dropped,
                "after_t2_outlier_filter_count": t2_count,
                "t2_dropped_rows": t2_dropped,
                "after_t3_feature_engineering_count": t3_count,
                "total_dropped_rows": raw_count - t3_count,
                "valid_row_ratio_pct": round((t3_count / max(raw_count, 1)) * 100.0, 2),
            },
            "join_validation": {
                "lookup_path": lookup_path,
                "lookup_total_rows": zone_total_rows,
                "lookup_distinct_keys": zone_distinct_keys,
                "join_type": "LEFT",
                "join_key": "PULocationID == LocationID",
                "rows_before_join": t3_count,
                "rows_after_join": rows_after_join,
                "matched_valid_zone_rows": matched_valid_zone_count,
                "unknown_or_outside_nyc_rows": unknown_zone_count,
                "unmatched_null_key_rows": unmatched_null_count,
            },
            "aggregation_output": {
                "output_path": output_dir,
                "output_format": out_fmt,
                "write_mode": write_mode,
                "output_row_count": agg_row_count,
                "output_partitions": agg_partitions,
            },
            "baseline_performance": {
                "total_runtime_seconds": runtime_sec,
                "explain_plan_file": explain_path,
            },
        }

        baseline_path = cfg["paths"]["baseline_report_path"]
        with open(baseline_path, "w", encoding="utf-8") as f:
            json.dump(baseline_info, f, indent=4, ensure_ascii=False)

        print(f" -> Đã lưu Baseline Metrics tại: {baseline_path}")
        print(f"\n[SUCCESS] Pipeline hoàn tất thành công trong {runtime_sec} giây!")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"\n[ERROR] Pipeline thất bại: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()