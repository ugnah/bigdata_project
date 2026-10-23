import os
import sys
import json
from datetime import datetime

# --- FIX WINDOWS HADOOP_HOME ---
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';C:\\hadoop\\bin'
# --------------------------------

from pyspark.sql import SparkSession

def main():
    # 1. Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Ingestion_Job") \
        .master("local[*]") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
        .getOrCreate()

    print(f"[INFO] Spark Version: {spark.version}")

    # 2. Định nghĩa đường dẫn
    SOURCE_PATH = "data/source/yellow_tripdata_2026_07.csv"
    INGEST_DATE = datetime.now().strftime("%Y-%m-%d")
    RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
    RAW_DEST_PATH = f"data/raw/nyc_taxi/{INGEST_DATE}/{RUN_ID}"

    print(f"[INFO] Bat dau Ingestion tu: {SOURCE_PATH}")

    # 3. Doc CSV - Dem row_count TRUOC khi ghi (de dam bao manifest luon co du lieu)
    try:
        df = spark.read.csv(SOURCE_PATH, header=True, inferSchema=True)
        row_count = df.count()
        print(f"[INFO] Da doc {row_count} dong tu source")

        # 4. Ghi ra CSV (fallback tu Parquet de tranh loi Windows native IO)
        os.makedirs(RAW_DEST_PATH, exist_ok=True)
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(RAW_DEST_PATH)
        print(f"[SUCCESS] Ingestion hoan tat! Du lieu luu tai: {RAW_DEST_PATH}")
        
        status = "SUCCESS"
        
    except Exception as e:
        print(f"[ERROR] Ingestion that bai: {e}")
        row_count = 0
        status = f"FAILED: {str(e)}"

    # 5. Tao Manifest/Log (LUON CHAY du co loi hay khong - dung finally)
    manifest = {
        "timestamp": datetime.now().isoformat(),
        "source": SOURCE_PATH,
        "destination": RAW_DEST_PATH,
        "row_count": row_count,
        "status": status,
        "run_id": RUN_ID,
        "spark_version": spark.version,
        "python_version": sys.version.split()[0]
    }

    os.makedirs("logs", exist_ok=True)
    manifest_path = "logs/ingest_manifest.json"
    
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4, ensure_ascii=False)
        f.flush()  # Buoc flush de dam bao du lieu duoc ghi ngay
    
    print(f"[LOG] Manifest da duoc ghi vao {manifest_path}")
    
    spark.stop()
    print("[INFO] Spark session da dong")

if __name__ == "__main__":
    main()