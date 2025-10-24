#!/usr/bin/env python3
"""
Problem 2 - Cluster Usage Analysis
Generates per-application timelines and cluster summaries from Spark logs.
"""

import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import input_file_name, regexp_extract, to_timestamp, col


def ensure_out_dir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def build_spark(master_url: str | None):
    try:
        SparkSession.builder.getOrCreate().stop()
    except Exception:
        pass

    builder = SparkSession.builder.appName("Problem2-ClusterUsage")
    if master_url:
        builder = builder.master(master_url)

    builder = (
        builder
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    return spark


def plot_results(timeline_csv: Path, cluster_csv: Path, out_dir: Path):
    """Generate bar and density plots from CSVs."""
    timeline = pd.read_csv(timeline_csv, parse_dates=["app_start", "app_end"])
    clusters = pd.read_csv(cluster_csv, parse_dates=["cluster_first_app", "cluster_last_app"])

    # 1) Bar chart
    plt.figure(figsize=(9, 5))
    sns.barplot(data=clusters.sort_values("num_applications", ascending=False),
                x="cluster_id", y="num_applications")
    plt.title("Applications per Cluster")
    plt.xlabel("Cluster ID")
    plt.ylabel("Applications")
    plt.tight_layout()
    plt.savefig(out_dir / "problem2_bar_chart.png", dpi=150)
    plt.close()

    # 2) Density/Histogram
    top_cluster = clusters.sort_values("num_applications", ascending=False)["cluster_id"].iloc[0]
    tl_top = timeline[timeline["cluster_id"] == top_cluster].copy()
    tl_top["duration_sec"] = (tl_top["app_end"] - tl_top["app_start"]).dt.total_seconds()

    plt.figure(figsize=(9, 5))
    sns.histplot(tl_top, x="duration_sec", bins=50, kde=True)
    plt.xscale("log")
    plt.xlabel("Duration (seconds, log scale)")
    plt.title(f"Duration Distribution (Cluster {top_cluster})  n={len(tl_top)}")
    plt.tight_layout()
    plt.savefig(out_dir / "problem2_density_plot.png", dpi=150)
    plt.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("master", nargs="?", default=None)
    parser.add_argument("--net-id", type=str, help="Your NetID")
    parser.add_argument("--out_dir", type=str, default="data/output")
    parser.add_argument("--sample", action="store_true", help="Use local sample data instead of S3")
    args = parser.parse_args()

    out_dir = ensure_out_dir(args.out_dir)
    timeline_path = out_dir / "problem2_timeline.csv"
    cluster_path = out_dir / "problem2_cluster_summary.csv"
    stats_path = out_dir / "problem2_stats.txt"

    spark = build_spark(args.master)

    try:
        # 1) Choose data source
        if args.sample:
            data_path = "data/sample/application_*/*"
        else:
            data_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/*"

        print(f"Reading logs from: {data_path}")

        logs = (
            spark.read.text(data_path)
            .withColumn("file_path", input_file_name())
            .withColumn("ts_str",
                        regexp_extract(col("value"),
                                       r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1))
            .withColumn("timestamp", to_timestamp(col("ts_str"), "yy/MM/dd HH:mm:ss"))
            .withColumn("application_id",
                        regexp_extract(col("file_path"), r'(application_\d+_\d+)', 1))
            .withColumn("cluster_id",
                        regexp_extract(col("application_id"), r'application_(\d+)_\d+', 1))
        ).filter(col("application_id") != "").filter(col("timestamp").isNotNull())

        logs.cache()

        # 2) Per-application timeline
        timeline_sdf = (
            logs.groupBy("application_id", "cluster_id")
            .agg(F.min("timestamp").alias("app_start"),
                 F.max("timestamp").alias("app_end"))
            .orderBy("app_start")
        )

        timeline_pdf = timeline_sdf.toPandas()
        timeline_pdf = timeline_pdf[["application_id", "cluster_id", "app_start", "app_end"]]
        timeline_pdf.to_csv(timeline_path, index=False)

        # 3) Cluster summary
        cluster_sdf = (
            timeline_sdf.groupBy("cluster_id")
            .agg(F.countDistinct("application_id").alias("num_applications"),
                 F.min("app_start").alias("cluster_first_app"),
                 F.max("app_end").alias("cluster_last_app"))
            .orderBy(F.col("num_applications").desc())
        )

        cluster_pdf = cluster_sdf.toPandas()
        cluster_pdf = cluster_pdf[
            ["cluster_id", "num_applications", "cluster_first_app", "cluster_last_app"]
        ]
        cluster_pdf.to_csv(cluster_path, index=False)

        # 4) Stats
        total_clusters = cluster_pdf.shape[0]
        total_apps = timeline_pdf["application_id"].nunique()
        avg_apps = total_apps / total_clusters if total_clusters > 0 else 0
        with open(stats_path, "w", encoding="utf-8") as f:
            f.write(f"Total unique clusters: {total_clusters}\n")
            f.write(f"Total applications: {total_apps}\n")
            f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
            f.write("Most heavily used clusters:\n")
            for _, r in cluster_pdf.head(10).iterrows():
                f.write(f"  Cluster {r['cluster_id']}: {int(r['num_applications'])} applications\n")

        # 5) Visualizations
        plot_results(timeline_path, cluster_path, out_dir)

        print("[SUCCESS] Problem 2 completed successfully!")

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
