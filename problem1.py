#!/usr/bin/env python3
"""
Problem 1 - Optimized Version (English)
This script reads Spark application logs from S3, extracts log levels,
aggregates counts, samples random entries, and writes outputs.
"""

import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand, count as f_count


# === Utility: ensure the output directory exists ===
def ensure_out_dir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


# === Build Spark session with optimized configurations ===
def build_spark_optimized(master_url: str | None):
    try:
        SparkSession.builder.getOrCreate().stop()
    except Exception:
        pass

    builder = SparkSession.builder.appName("Problem1-Optimized")
    if master_url:
        builder = builder.master(master_url)

    builder = (
        builder
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skew.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.default.parallelism", "200")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    return spark


# === Main entry point ===
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("master", nargs="?", default=None)
    parser.add_argument("--out_dir", type=str, default="data/output")
    parser.add_argument("--net-id", type=str, required=True)
    args = parser.parse_args()

    out_dir = ensure_out_dir(args.out_dir)

    print("Starting optimized Problem 1 ...")
    spark = build_spark_optimized(args.master)

    try:
        # 1. Read data from S3
        s3_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/*"
        print(f"Reading logs from: {s3_path}")

        # Load text files directly from S3 with controlled partitioning
        raw_rdd = spark.sparkContext.textFile(s3_path, minPartitions=100)
        df = spark.createDataFrame(raw_rdd.map(lambda x: (x,)), ["log_line"])

        total_lines = df.count()
        print(f"Total lines read: {total_lines}")

        # 2. Extract log levels using regex
        print("=== Parsing and aggregating logs ===")
        parsed = (
            df.select(
                regexp_extract(col("log_line"),
                               r'(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias("timestamp"),
                regexp_extract(col("log_line"),
                               r'(INFO|WARN|ERROR|DEBUG)', 1).alias("log_level"),
                col("log_line").alias("log_entry")
            )
            .filter(col("log_level") != "")
        )

        parsed.cache()

        # 3. Aggregate by log level
        print("=== Counting log levels ===")
        counts = (
            parsed.groupBy("log_level")
            .agg(f_count("*").alias("count"))
            .orderBy(col("count").desc())
        )

        # 4. Randomly sample 10 entries
        print("=== Sampling 10 random log entries ===")
        sample10 = (
            parsed.orderBy(rand())
            .limit(10)
            .select("log_entry", "log_level")
        )

        # 5. Collect results
        print("=== Collecting results ===")
        total_with_level = parsed.count()
        unique_levels = counts.count()

        print(f"Processing summary:")
        print(f"  Total lines: {total_lines}")
        print(f"  Lines with log levels: {total_with_level}")
        print(f"  Unique log levels: {unique_levels}")

        counts_data = counts.collect()
        sample_data = sample10.collect()

        parsed.unpersist()

        # 6. Write outputs
        out_counts = out_dir / "problem1_counts.csv"
        out_sample = out_dir / "problem1_sample.csv"
        out_summary = out_dir / "problem1_summary.txt"

        with open(out_counts, "w", encoding="utf-8") as f:
            f.write("log_level,count\n")
            for row in counts_data:
                f.write(f"{row['log_level']},{row['count']}\n")

        with open(out_sample, "w", encoding="utf-8") as f:
            f.write("log_entry,log_level\n")
            for row in sample_data:
                log_entry = row['log_entry'].replace('"', '""')
                f.write(f'"{log_entry}","{row["log_level"]}"\n')

        with open(out_summary, "w", encoding="utf-8") as f:
            f.write(f"Total log lines processed: {total_lines}\n")
            f.write(f"Total lines with log levels: {total_with_level}\n")
            f.write(f"Unique log levels found: {unique_levels}\n\n")
            f.write("Log level distribution:\n")
            for row in counts_data:
                lvl = row["log_level"]
                cnt = int(row["count"])
                pct = (cnt / total_with_level * 100) if total_with_level > 0 else 0
                f.write(f"  {lvl:5s}: {cnt:10d} ({pct:6.2f}%)\n")

        print("[SUCCESS] Optimized version completed successfully!")

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
