import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Export the M04A reliability summary "
            "to a single Tableau-ready CSV file."
        )
    )

    parser.add_argument(
        "--warehouse",
        required=True,
    )

    parser.add_argument(
        "--output-path",
        required=True,
        help=(
            "例如 hdfs:///user/<user>/dataset/"
            "tdcs_tableau/"
            "m04a_reliability_summary.csv"
        ),
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
        default="m04a_reliability_summary",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
    )

    return parser.parse_args()


def write_single_csv(
    spark,
    dataframe,
    output_path,
    overwrite=False,
):
    sc = spark.sparkContext

    hadoop_conf = (
        sc._jsc.hadoopConfiguration()
    )

    path_class = (
        spark._jvm.org.apache.hadoop.fs.Path
    )

    output = path_class(output_path)

    temporary = path_class(
        f"{output_path}.__temporary__"
    )

    file_system = output.getFileSystem(
        hadoop_conf
    )

    parent = output.getParent()

    if (
        parent is not None
        and not file_system.exists(parent)
    ):
        file_system.mkdirs(parent)

    if file_system.exists(output):
        if not overwrite:
            raise FileExistsError(
                f"輸出檔案已存在：{output_path}；"
                "如需覆蓋，請使用 --overwrite。"
            )

        file_system.delete(
            output,
            True,
        )

    if file_system.exists(temporary):
        if not overwrite:
            raise FileExistsError(
                f"暫存輸出已存在：{temporary}；"
                "如需清除，請使用 --overwrite。"
            )

        file_system.delete(
            temporary,
            True,
        )

    (
        dataframe
        .coalesce(1)
        .write
        .mode("errorifexists")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(str(temporary))
    )

    part_file = None

    for status in file_system.listStatus(
        temporary
    ):
        name = (
            status.getPath().getName()
        )

        if (
            name.startswith("part-")
            and name.endswith(".csv")
        ):
            part_file = status.getPath()
            break

    if part_file is None:
        raise RuntimeError(
            "找不到 Spark 產生的 CSV part 檔案。"
        )

    rename_success = file_system.rename(
        part_file,
        output,
    )

    if not rename_success:
        raise RuntimeError(
            f"無法將 {part_file} "
            f"重新命名為 {output_path}"
        )

    file_system.delete(
        temporary,
        True,
    )


def main():
    args = parse_args()

    source_table = (
        f"{args.catalog}."
        f"{args.namespace}."
        f"{args.source_table}"
    )

    spark = (
        SparkSession.builder
        .appName(
            "tdcs_tableau_csv_export"
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
        .getOrCreate()
    )

    summary = spark.table(source_table)

    required_columns = {
        "summary_level",
        "pair_id",
        "day_type",
        "time_period",
        "p50_travel_time_sec",
        "p95_travel_time_sec",
        "buffer_time_sec",
        "buffer_index",
        "reliability_level",
    }

    missing_columns = (
        required_columns - set(summary.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Summary table 缺少必要欄位："
            f"{sorted(missing_columns)}；"
            f"目前欄位：{summary.columns}"
        )

    tableau = (
        summary
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
        .withColumn(
            "p50_travel_time_min",
            F.round(
                F.col(
                    "p50_travel_time_sec"
                )
                / F.lit(60.0),
                2,
            ),
        )
        .withColumn(
            "p95_travel_time_min",
            F.round(
                F.col(
                    "p95_travel_time_sec"
                )
                / F.lit(60.0),
                2,
            ),
        )
        .withColumn(
            "buffer_time_min",
            F.round(
                F.col("buffer_time_sec")
                / F.lit(60.0),
                2,
            ),
        )
        .orderBy(
            "summary_level",
            "pair_id",
            "day_type",
            "time_period",
        )
    )

    row_count = tableau.count()

    write_single_csv(
        spark=spark,
        dataframe=tableau,
        output_path=args.output_path,
        overwrite=args.overwrite,
    )

    print(
        f"Source table: {source_table}"
    )

    print(
        f"CSV output: {args.output_path}"
    )

    print(
        f"Row count: {row_count}"
    )

    spark.stop()


if __name__ == "__main__":
    main()