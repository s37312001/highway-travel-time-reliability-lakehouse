import argparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


SUMMARY_COLUMNS = [
    "summary_level",
    "pair_id",
    "day_type",
    "time_period",
    "travel_time_sec",
    "volume",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Calculate volume-weighted P50, P95 "
            "and Buffer Index."
        )
    )

    parser.add_argument(
        "--warehouse",
        required=True,
    )

    parser.add_argument(
        "--catalog",
        default="hadoop_prod",
    )

    parser.add_argument(
        "--namespace",
        default="tdcs",
    )

    parser.add_argument(
        "--source-table",
        default="m04a_histogram",
    )

    parser.add_argument(
        "--target-table",
        default="m04a_reliability_summary",
    )

    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=32,
    )

    parser.add_argument(
        "--output-partitions",
        type=int,
        default=4,
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.shuffle_partitions <= 0:
        raise ValueError(
            "--shuffle-partitions 必須大於 0"
        )

    if args.output_partitions <= 0:
        raise ValueError(
            "--output-partitions 必須大於 0"
        )

    source_table = (
        f"{args.catalog}."
        f"{args.namespace}."
        f"{args.source_table}"
    )

    target_table = (
        f"{args.catalog}."
        f"{args.namespace}."
        f"{args.target_table}"
    )

    spark = (
        SparkSession.builder
        .appName(
            "tdcs_m04a_reliability_summary"
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{args.catalog}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            f"spark.sql.catalog.{args.catalog}.type",
            "hadoop",
        )
        .config(
            f"spark.sql.catalog.{args.catalog}.warehouse",
            args.warehouse,
        )
        .config(
            "spark.sql.defaultCatalog",
            args.catalog,
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )
        .config(
            "spark.sql.shuffle.partitions",
            str(args.shuffle_partitions),
        )
        .getOrCreate()
    )

    source = spark.table(source_table)

    required_columns = {
        "pair_id",
        "day_type",
        "time_period",
        "vehicle_type_group",
        "travel_time_sec",
        "total_volume",
    }

    missing_columns = (
        required_columns - set(source.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Histogram table 缺少必要欄位："
            f"{sorted(missing_columns)}；"
            f"目前欄位：{source.columns}"
        )

    base = (
        source
        .select(
            F.trim(
                F.col("pair_id")
            ).alias("pair_id"),

            F.trim(
                F.col("day_type")
            ).alias("day_type"),

            F.trim(
                F.col("time_period")
            ).alias("time_period"),

            F.trim(
                F.col("vehicle_type_group")
            ).alias("vehicle_type_group"),

            F.col("travel_time_sec")
            .cast("int")
            .alias("travel_time_sec"),

            F.col("total_volume")
            .cast("long")
            .alias("volume"),
        )
        .filter(
            F.col("pair_id").isNotNull()
        )
        .filter(
            F.col("day_type").isNotNull()
        )
        .filter(
            F.col("time_period").isNotNull()
        )
        .filter(
            F.col("vehicle_type_group") == "ALL"
        )
        .filter(
            F.col("travel_time_sec") > 0
        )
        .filter(
            F.col("volume") > 0
        )
        .persist(
            StorageLevel.MEMORY_AND_DISK
        )
    )

    # L1：只看平假日，不分時段。
    l1_day_type = (
        base
        .groupBy(
            "pair_id",
            "day_type",
            "travel_time_sec",
        )
        .agg(
            F.sum("volume").alias("volume")
        )
        .withColumn(
            "summary_level",
            F.lit("L1_DAY_TYPE"),
        )
        .withColumn(
            "time_period",
            F.lit("ALL"),
        )
        .select(SUMMARY_COLUMNS)
    )

    # L2：只看時段，不分平假日。
    l2_time_period = (
        base
        .groupBy(
            "pair_id",
            "time_period",
            "travel_time_sec",
        )
        .agg(
            F.sum("volume").alias("volume")
        )
        .withColumn(
            "summary_level",
            F.lit("L2_TIME_PERIOD"),
        )
        .withColumn(
            "day_type",
            F.lit("ALL"),
        )
        .select(SUMMARY_COLUMNS)
    )

    # L3：平假日與時段一起分析。
    l3_day_type_time_period = (
        base
        .groupBy(
            "pair_id",
            "day_type",
            "time_period",
            "travel_time_sec",
        )
        .agg(
            F.sum("volume").alias("volume")
        )
        .withColumn(
            "summary_level",
            F.lit(
                "L3_DAY_TYPE_TIME_PERIOD"
            ),
        )
        .select(SUMMARY_COLUMNS)
    )

    # L4：整體，不分平假日與時段。
    l4_overall = (
        base
        .groupBy(
            "pair_id",
            "travel_time_sec",
        )
        .agg(
            F.sum("volume").alias("volume")
        )
        .withColumn(
            "summary_level",
            F.lit("L4_OVERALL"),
        )
        .withColumn(
            "day_type",
            F.lit("ALL"),
        )
        .withColumn(
            "time_period",
            F.lit("ALL"),
        )
        .select(SUMMARY_COLUMNS)
    )

    summary_histogram = (
        l1_day_type
        .unionByName(l2_time_period)
        .unionByName(
            l3_day_type_time_period
        )
        .unionByName(l4_overall)
        .repartition(
            "summary_level",
            "pair_id",
        )
    )

    group_columns = [
        "summary_level",
        "pair_id",
        "day_type",
        "time_period",
    ]

    group_window = (
        Window.partitionBy(*group_columns)
    )

    cumulative_window = (
        Window
        .partitionBy(*group_columns)
        .orderBy(
            F.col(
                "travel_time_sec"
            ).asc()
        )
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow,
        )
    )

    ranked = (
        summary_histogram
        .withColumn(
            "cumulative_volume",
            F.sum("volume").over(
                cumulative_window
            ),
        )
        .withColumn(
            "total_volume",
            F.sum("volume").over(
                group_window
            ),
        )
    )

    # 累積車流量第一次達到 50% 與 95% 時，
    # 對應的 travel_time_sec 即為 P50、P95。
    percentile_summary = (
        ranked
        .groupBy(*group_columns)
        .agg(
            F.min(
                F.when(
                    F.col("cumulative_volume")
                    >= (
                        F.col("total_volume")
                        * F.lit(0.50)
                    ),
                    F.col("travel_time_sec"),
                )
            ).alias(
                "p50_travel_time_sec"
            ),

            F.min(
                F.when(
                    F.col("cumulative_volume")
                    >= (
                        F.col("total_volume")
                        * F.lit(0.95)
                    ),
                    F.col("travel_time_sec"),
                )
            ).alias(
                "p95_travel_time_sec"
            ),
        )
    )

    final_summary = (
        percentile_summary
        .withColumn(
            "buffer_time_sec",
            F.col("p95_travel_time_sec")
            - F.col("p50_travel_time_sec"),
        )
        .withColumn(
            "buffer_index",
            F.when(
                F.col("p50_travel_time_sec") > 0,
                F.col("buffer_time_sec")
                / F.col("p50_travel_time_sec"),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "reliability_level",
            F.when(
                F.col("buffer_index").isNull(),
                F.lit(None).cast("string"),
            )
            .when(
                F.col("buffer_index") < 0.25,
                "穩定",
            )
            .when(
                F.col("buffer_index") < 0.50,
                "普通",
            )
            .when(
                F.col("buffer_index") < 1.00,
                "不穩定",
            )
            .otherwise("高風險"),
        )
        .select(
            "summary_level",
            "pair_id",
            "day_type",
            "time_period",
            "p50_travel_time_sec",
            "p95_travel_time_sec",
            "buffer_time_sec",
            "buffer_index",
            "reliability_level",
        )
    )

    final_to_write = (
        final_summary
        .repartition(
            args.output_partitions,
            "summary_level",
        )
        .sortWithinPartitions(
            "summary_level",
            "pair_id",
            "day_type",
            "time_period",
        )
    )

    spark.sql(
        f"""
        CREATE NAMESPACE IF NOT EXISTS
        {args.catalog}.{args.namespace}
        """
    )

    if args.overwrite:
        spark.sql(
            f"DROP TABLE IF EXISTS {target_table}"
        )

    (
        final_to_write
        .writeTo(target_table)
        .using("iceberg")
        .tableProperty(
            "format-version",
            "2",
        )
        .partitionedBy(
            "summary_level"
        )
        .create()
    )

    print("Summary level row counts:")

    (
        spark.table(target_table)
        .groupBy("summary_level")
        .count()
        .orderBy("summary_level")
        .show(truncate=False)
    )

    invalid_count = (
        spark.table(target_table)
        .filter(
            F.col("p95_travel_time_sec")
            < F.col("p50_travel_time_sec")
        )
        .count()
    )

    print(
        f"Invalid P95 < P50 count: "
        f"{invalid_count}"
    )

    print(
        f"Source table: {source_table}"
    )
    print(
        f"Target table: {target_table}"
    )

    base.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()