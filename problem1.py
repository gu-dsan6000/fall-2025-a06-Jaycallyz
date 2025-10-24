#!/usr/bin/env python3
"""
Problem 1 - Optimized version to reduce stages
"""

import os
import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, trim, rand, count as f_count
)

def ensure_out_dir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def build_spark_optimized(master_url: str | None):
    try:
        SparkSession.builder.getOrCreate().stop()
    except:
        pass
    
    builder = SparkSession.builder.appName("Problem1-Optimized")
    if master_url:
        builder = builder.master(master_url)
    
    # 优化配置
    builder = (
        builder
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skew.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.default.parallelism", "200")
    )
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    return spark

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("master", nargs="?", default=None)
    parser.add_argument("--out_dir", type=str, default="data/output")
    parser.add_argument("--net-id", type=str, required=True)
    args = parser.parse_args()

    out_dir = ensure_out_dir(args.out_dir)
    
    print("Starting OPTIMIZED Problem1...")
    spark = build_spark_optimized(args.master)

    try:
        # 1. 读取数据
        s3_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/*"
        print(f"Reading from S3: {s3_path}")
        
        # 使用textFile直接读取，控制分区
        raw_rdd = spark.sparkContext.textFile(s3_path, minPartitions=100)
        df = spark.createDataFrame(raw_rdd.map(lambda x: (x,)), ["log_line"])
        
        total_lines = df.count()
        print(f"Total lines read: {total_lines}")

        # 2. 解析日志
        print("=== 开始解析和聚合 ===")
        parsed = df.select(
            regexp_extract(col("log_line"), r'(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias("ts_raw"),
            regexp_extract(col("log_line"), r'(INFO|WARN|ERROR|DEBUG)', 1).alias("log_level"),
            col("log_line").alias("log_entry")
        ).filter(col("log_level") != "")

        # 缓存中间结果
        parsed.cache()
        
        # 3. 聚合操作
        print("=== 开始计数操作 ===")
        counts = (
            parsed
            .groupBy("log_level")
            .agg(f_count("*").alias("count"))
            .orderBy(col("count").desc())
        )

        print("=== 开始采样操作 ===")
        sample10 = (
            parsed
            .orderBy(rand())
            .limit(10)
            .select("log_entry", "log_level")
        )

        # 4. 收集结果
        print("=== 收集结果 ===")
        total_with_level = parsed.count()
        unique_levels = counts.count()

        print(f"处理完成:")
        print(f"  总行数: {total_lines}")
        print(f"  带级别的行数: {total_with_level}")
        print(f"  唯一级别数: {unique_levels}")

        # 写入结果
        counts_data = counts.collect()
        sample_data = sample10.collect()

        # 清理缓存
        parsed.unpersist()

        # 写入文件
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

        print(f"[SUCCESS] 优化版本完成!")

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
