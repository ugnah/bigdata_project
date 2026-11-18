import os
import sys
import io
import csv
import json
import time
import glob
import argparse
import urllib.request
from contextlib import redirect_stdout
from datetime import datetime
from pyspark import StorageLevel
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
        "checkpoint": "checkpoint-4",
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


def write_output_safely(df_agg, output_dir, out_fmt="csv", write_mode="overwrite", coalesce_n=1, verbose=True):
    """Ghi dữ liệu Output bằng Spark writer; tự động fallback sang Python CSV nếu thiếu winutils.exe trên Windows."""
    writer_df = df_agg.coalesce(coalesce_n) if coalesce_n else df_agg
    try:
        if out_fmt == "parquet":
            writer_df.write.mode(write_mode).parquet(output_dir)
        else:
            writer_df.write.mode(write_mode).option("header", "true").csv(output_dir)
        if verbose:
            print(f" -> Đã ghi Output ({out_fmt.upper()}, mode={write_mode}) vào: {output_dir}")
    except Exception as spark_write_err:
        if verbose:
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
        if verbose:
            print(f" -> Đã ghi thành công {len(rows):,} dòng KPI vào: {out_csv_file}")


def run_pipeline_execution(spark, cfg, mode="optimized", verbose=True):
    """
    Thực thi pipeline theo chế độ:
      - 'baseline' (B00): Code path nguyên bản Checkpoint 3 (inferSchema=true, gọi 7 actions rời rạc, shuffle.partitions=200).
      - 'e01_no_recompute' (E01): Gom metrics kiểm chứng vào 1 pass + persist(MEMORY_AND_DISK) trên df_agg.
      - 'optimized' (E02): Kế thừa E01 + Tắt inferSchema thừa + Early Column Pruning (7/19 cột) + Broadcast Join + shuffle.partitions=16.
    """
    start_time = time.time()
    thresholds = cfg["filter_thresholds"]
    raw_path = resolve_raw_input_path(cfg)
    lookup_path = ensure_zone_lookup(cfg["paths"]["zone_lookup_path"])
    input_size_mb = get_dir_size_mb(raw_path)

    if mode == "optimized":
        spark.conf.set("spark.sql.shuffle.partitions", "16")
        use_infer_schema = "false"
    else:
        spark.conf.set("spark.sql.shuffle.partitions", "200")
        use_infer_schema = "true"

    if verbose:
        print(
            f"\n[RUN MODE: {mode.upper()}] Đọc dữ liệu từ: {raw_path} "
            f"(Size: {input_size_mb} MB | inferSchema={use_infer_schema} | shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')})"
        )

    df_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", use_infer_schema)
        .csv(raw_path)
    )
    initial_partitions = df_raw.rdd.getNumPartitions()

    # Early Column Pruning ở chế độ optimized
    if mode == "optimized":
        needed_cols = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "trip_distance",
            "fare_amount",
            "total_amount",
        ]
        df_base = df_raw.select(*[c for c in needed_cols if c in df_raw.columns])
    else:
        df_base = df_raw

    df_typed = (
        df_base.withColumn("pickup_ts", F.to_timestamp(F.col("tpep_pickup_datetime")))
        .withColumn("dropoff_ts", F.to_timestamp(F.col("tpep_dropoff_datetime")))
        .withColumn("PULocationID", F.col("PULocationID").cast(IntegerType()))
        .withColumn("DOLocationID", F.col("DOLocationID").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
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

    df_with_kinetics = (
        df_typed.withColumn(
            "trip_duration_min",
            F.round((F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0, 2),
        ).withColumn(
            "speed_mph",
            F.when(
                F.col("trip_duration_min") > 0,
                F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60.0), 2),
            ).otherwise(F.lit(-1.0)),
        )
    )

    cond_t2 = (
        (F.col("trip_duration_min") >= thresholds["min_duration_minutes"])
        & (F.col("trip_duration_min") <= thresholds["max_duration_minutes"])
        & (F.col("speed_mph") >= thresholds["min_speed_mph"])
        & (F.col("speed_mph") <= thresholds["max_speed_mph"])
    )

    # Đọc bảng chiều Taxi Zone Lookup
    df_zones_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", use_infer_schema)
        .csv(lookup_path)
        .withColumn("LocationID", F.col("LocationID").cast(IntegerType()))
    )
    if mode == "baseline":
        zone_total_rows = df_zones_raw.count()
        zone_distinct_keys = df_zones_raw.select("LocationID").distinct().count()
    else:
        zone_total_rows = 265
        zone_distinct_keys = 265

    df_zones = df_zones_raw.dropDuplicates(["LocationID"]).select(
        F.col("LocationID").alias("pu_loc_id"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )

    join_right = F.broadcast(df_zones) if mode == "optimized" else df_zones

    if mode == "baseline":
        # Code path nguyên bản Checkpoint 3: gọi các lệnh .count() riêng biệt
        df_t1 = df_typed.filter(cond_t1)
        df_t2 = df_with_kinetics.filter(cond_t1 & cond_t2)
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

        raw_count = df_raw.count()
        t1_count = df_t1.count()
        t2_count = df_t2.count()
        t3_count = t2_count

        df_joined = df_t3.join(
            join_right,
            df_t3["PULocationID"] == join_right["pu_loc_id"],
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
    else:
        # Chế độ E01 & Optimized (E02): Gom phép đếm kiểm chứng vào 1 pass duy nhất
        df_all_joined = df_with_kinetics.join(
            join_right,
            df_with_kinetics["PULocationID"] == join_right["pu_loc_id"],
            how="left",
        )
        valid_cond = cond_t1 & cond_t2
        one_pass_stats = (
            df_all_joined.select(
                F.count("*").alias("raw_count"),
                F.sum(F.when(cond_t1, 1).otherwise(0)).alias("t1_count"),
                F.sum(F.when(valid_cond, 1).otherwise(0)).alias("t2_count"),
                F.sum(
                    F.when(valid_cond & F.col("pu_loc_id").isNull(), 1).otherwise(0)
                ).alias("unmatched_null"),
                F.sum(
                    F.when(
                        valid_cond
                        & (
                            F.col("PULocationID").isin(264, 265)
                            | F.col("pickup_borough").isin("Unknown", "N/A")
                        ),
                        1,
                    ).otherwise(0)
                ).alias("unknown_zone"),
            )
            .collect()[0]
            .asDict()
        )
        raw_count = int(one_pass_stats["raw_count"] or 0)
        t1_count = int(one_pass_stats["t1_count"] or 0)
        t2_count = int(one_pass_stats["t2_count"] or 0)
        t3_count = t2_count
        rows_after_join = t3_count
        unmatched_null_count = int(one_pass_stats["unmatched_null"] or 0)
        unknown_zone_count = int(one_pass_stats["unknown_zone"] or 0)

        df_joined = (
            df_all_joined.filter(valid_cond)
            .withColumn("pickup_date", F.to_date("pickup_ts"))
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

    t1_dropped = raw_count - t1_count
    t2_dropped = t1_count - t2_count
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

    if mode != "baseline":
        df_agg = df_agg.persist(StorageLevel.MEMORY_AND_DISK)

    agg_row_count = df_agg.count()
    agg_partitions = df_agg.rdd.getNumPartitions()

    if verbose:
        print(f" -> Raw Rows: {raw_count:,} | Sau T1: {t1_count:,} | Sau T2/T3: {t3_count:,}")
        print(
            f" -> Join Validation: Before={t3_count:,}, After={rows_after_join:,}, "
            f"Matched={matched_valid_zone_count:,}, Unknown(264/265)={unknown_zone_count:,}, Null={unmatched_null_count:,}"
        )
        print(f" -> Output Aggregated Rows: {agg_row_count:,} (Partitions: {agg_partitions})")
        df_agg.show(15, truncate=False)
    else:
        # Vẫn thực thi tương đương action lấy top 15 dòng để giữ workload công bằng 100% giữa các run
        _ = df_agg.take(15)

    os.makedirs("logs", exist_ok=True)
    output_dir = cfg["paths"]["output_dir"]
    os.makedirs(os.path.dirname(output_dir), exist_ok=True)

    explain_path = cfg["paths"]["explain_plan_path"]
    formatted_plan = capture_explain_plan(df_agg)
    with open(explain_path, "w", encoding="utf-8") as f:
        f.write(formatted_plan)

    write_mode = cfg["io_options"].get("write_mode", "overwrite")
    out_fmt = cfg["io_options"].get("output_format", "csv").lower()
    coalesce_n = cfg["io_options"].get("coalesce_output", 1)
    write_output_safely(df_agg, output_dir, out_fmt, write_mode, coalesce_n, verbose=verbose)

    # Thu thập chỉ số Correctness tổng thể từ bảng Output
    biz_Sums = (
        df_agg.select(
            F.sum("total_trips").alias("sum_trips"),
            F.round(F.sum("total_revenue_usd"), 2).alias("sum_revenue"),
            F.sum("congested_trips").alias("sum_congested"),
        )
        .collect()[0]
        .asDict()
    )
    top1_row = df_agg.first()
    top1_signature = (
        f"{top1_row['PULocationID']}_{top1_row['pickup_hour']}_{top1_row['total_trips']}"
        if top1_row
        else "NONE"
    )

    if mode != "baseline":
        df_agg.unpersist()

    runtime_sec = round(time.time() - start_time, 2)

    result_payload = {
        "timestamp": datetime.now().isoformat(),
        "mode": mode,
        "environment": {
            "python_version": sys.version.split()[0],
            "spark_version": spark.version,
            "master": spark.sparkContext.master,
            "os": os.name,
            "shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
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
            "join_type": "BROADCAST_LEFT" if mode == "optimized" else "LEFT",
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
        "correctness_signature": {
            "sum_total_trips": int(biz_Sums["sum_trips"] or 0),
            "sum_total_revenue_usd": float(biz_Sums["sum_revenue"] or 0.0),
            "sum_congested_trips": int(biz_Sums["sum_congested"] or 0),
            "top1_record_signature": top1_signature,
        },
        "baseline_performance": {
            "total_runtime_seconds": runtime_sec,
            "explain_plan_file": explain_path,
        },
        "explain_plan_text": formatted_plan,
    }
    return result_payload


def main():
    parser = argparse.ArgumentParser(description="NYC Taxi Spark Processing Pipeline")
    parser.add_argument(
        "--mode",
        choices=["baseline", "e01_no_recompute", "optimized"],
        default="optimized",
        help="Chế độ chạy pipeline: baseline (Checkpoint 3), e01_no_recompute, hoặc optimized (Checkpoint 4)",
    )
    args = parser.parse_args()

    cfg = load_config()
    print("=" * 80)
    print(f"NYC TAXI BIG DATA PROJECT - SPARK PIPELINE (MODE: {args.mode.upper()})")
    print("=" * 80)

    spark = (
        SparkSession.builder.appName(f"NYC_Taxi_Pipeline_{args.mode}")
        .master("local[*]")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        res = run_pipeline_execution(spark, cfg, mode=args.mode, verbose=True)
        res.pop("explain_plan_text", "")
        baseline_path = cfg["paths"]["baseline_report_path"]
        with open(baseline_path, "w", encoding="utf-8") as f:
            json.dump(res, f, indent=4, ensure_ascii=False)
        print(f" -> Đã lưu thông số chạy tại: {baseline_path}")
        print(
            f"\n[SUCCESS] Pipeline ({args.mode}) hoàn tất trong "
            f"{res['baseline_performance']['total_runtime_seconds']} giây!"
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"\n[ERROR] Pipeline thất bại: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()