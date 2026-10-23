import sys
import pyspark
from pyspark.sql import SparkSession

print("="*50)
print("[TEST] ENVIRONMENT SMOKE TEST")
print("="*50)
print(f"[OK] Python Version: {sys.version.split()[0]}")
print(f"[OK] PySpark Version: {pyspark.__version__}")

try:
    spark = SparkSession.builder \
        .appName("EnvCheck_SmokeTest") \
        .master("local[*]") \
        .getOrCreate()
    
    print(f"[OK] Spark Version: {spark.version}")
    
    # Phép tính Spark đơn giản để chứng minh engine hoạt động
    df = spark.range(10000)
    count = df.count()
    print(f"[PASS] Spark Execution PASS: Successfully counted {count} rows.")
    
    spark.stop()
    print("[SUCCESS] Smoke test hoan tat thanh cong!")
    
except Exception as e:
    print(f"[FAIL] Spark Smoke Test FAILED: {e}")
    sys.exit(1)