import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Create a volume-weighted M04A "
            "histogram Iceberg table."
        )
    )

    parser.add_argument(
        "--warehouse",
        required=True,
        help="Iceberg warehouse 路徑。",
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
        default="m04a_clean",
    )

    parser.add_argument(
        "--target-table",
        default="m04a_histogram",
    )

    parser.add_argument(
        "--exclude-date",
        action="append",
        default=None,
        help=(
            "需要排除的日期，可重複指定；"
            "預設排除 2026-07-01。"
        ),
    )

    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=60,
        help="Spark shuffle partitions，預設 60。",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="允許刪除並重建既有目標表。",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.shuffle_partitions <= 0:
        raise ValueError(
            "--shuffle-partitions 必須大於 0"
        )

    exclude_dates = (
        args.exclude_date or ["2026-07-01"]
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
        .appName("tdcs_m04a_histogram")
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
            "spark.sql.adaptive."
            "coalescePartitions.enabled",
            "true",
        )
        .config(
            "spark.sql.shuffle.partitions",
            str(args.shuffle_partitions),
        )
        .getOrCreate()
    )

    clean = spark.table(source_table)

    required_columns = {
        "pair_id",
        "record_date",
        "day_type",
        "time_period",
        "travel_time_sec",
        "volume",
    }

    missing_columns = (
        required_columns - set(clean.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Clean table 缺少必要欄位："
            f"{sorted(missing_columns)}；"
            f"目前欄位：{clean.columns}"
        )

    base = (
        clean
        .select(
            "pair_id",
            "record_date",
            "day_type",
            "time_period",
            "travel_time_sec",
            "volume",
        )
        .filter(
            ~F.col("record_date")
            .cast("string")
            .isin(exclude_dates)
        )
        .filter(
            F.col("pair_id").isNotNull()
        )
        .filter(
            F.col("day_type").isin(
                "Weekday",
                "Weekend",
            )
        )
        .filter(
            F.col("time_period").isNotNull()
        )
        .filter(
            F.col("travel_time_sec") > 0
        )
        .filter(
            F.col("volume") > 0
        )
    )

    # 將不同 vehicle_type 整合成 ALL。
    histogram = (
        base
        .withColumn(
            "vehicle_type_group",
            F.lit("ALL"),
        )
        .groupBy(
            "pair_id",
            "day_type",
            "time_period",
            "vehicle_type_group",
            "travel_time_sec",
        )
        .agg(
            F.sum("volume").alias(
                "total_volume"
            )
        )
        .select(
            "pair_id",
            "day_type",
            "time_period",
            "vehicle_type_group",
            "travel_time_sec",
            "total_volume",
        )
    )

    # 讓相同 Iceberg partition 的資料集中。
    histogram_to_write = (
        histogram
        .repartition(
            "day_type",
            "time_period",
        )
        .sortWithinPartitions(
            "day_type",
            "time_period",
            "pair_id",
            "travel_time_sec",
        )
    )

    spark.sql(
        f"""
        CREATE NAMESPACE IF NOT EXISTS
        {args.catalog}.{args.namespace}
        """
    )

    # 沒有 --overwrite 時，不會主動刪除原本的表。
    if args.overwrite:
        spark.sql(
            f"DROP TABLE IF EXISTS {target_table}"
        )

    (
        histogram_to_write
        .writeTo(target_table)
        .using("iceberg")
        .tableProperty(
            "format-version",
            "2",
        )
        .partitionedBy(
            "day_type",
            "time_period",
        )
        .create()
    )

    print(
        "Histogram Iceberg table "
        "created successfully."
    )
    print(
        f"Source table: {source_table}"
    )
    print(
        f"Target table: {target_table}"
    )
    print(
        f"Excluded dates: {exclude_dates}"
    )

    spark.stop()


if __name__ == "__main__":
    main()