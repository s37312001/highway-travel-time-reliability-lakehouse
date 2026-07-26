import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


EXCLUDED_SERVICE_PAIR_IDS = [
    "01F0532S_01F0532N",
    "01F0557N_01F0557S",
    "03F0698S_03F0698N",
    "03F0746S_03F0746N",
    "03F0783N_03F0783S",
    "03F1710S_03F1710N",
    "03F1739N_03F1739S",
    "03F2306S_03F2306N",
    "03F2336N_03F2336S",
    "03F2747S_03F2747N",
    "03F2777N_03F2777S",
    "03F3187S_03F3187N",
    "03F3211N_03F3211S",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Clean M04A Parquet data and create an Iceberg table."
    )

    parser.add_argument(
        "--input-path",
        required=True,
        help="M04A Parquet 根目錄，底下每個資料夾名稱為 pair_id。",
    )

    parser.add_argument(
        "--calendar-path",
        required=True,
        help="calendar.csv 或 calendar.parquet 的路徑。",
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
        "--table-name",
        default="m04a_clean",
    )

    parser.add_argument(
        "--write-partitions",
        type=int,
        default=240,
        help="寫入 Iceberg 前的重新分區數，預設為 240。",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="允許刪除並重建既有目標表。",
    )

    return parser.parse_args()


def read_calendar(spark, calendar_path):
    if calendar_path.lower().endswith(".csv"):
        calendar_raw = (
            spark.read
            .option("header", "true")
            .option("encoding", "UTF-8")
            .csv(calendar_path)
        )
    else:
        calendar_raw = spark.read.parquet(calendar_path)

    required_columns = {
        "record_date",
        "day_type",
    }

    missing_columns = required_columns - set(calendar_raw.columns)

    if missing_columns:
        raise ValueError(
            f"Calendar 缺少必要欄位：{sorted(missing_columns)}；"
            f"目前欄位：{calendar_raw.columns}"
        )

    calendar_date = F.coalesce(
        F.to_date(
            F.col("record_date").cast("string"),
            "yyyy/M/d",
        ),
        F.to_date(
            F.col("record_date").cast("string"),
            "yyyy/MM/dd",
        ),
        F.to_date(
            F.col("record_date").cast("string"),
            "yyyy-MM-dd",
        ),
        F.to_date(
            F.col("record_date").cast("string"),
            "yyyyMMdd",
        ),
        F.to_date(F.col("record_date")),
    )

    return (
        calendar_raw
        .select(
            calendar_date.alias("cal_date"),
            F.trim(F.col("day_type")).alias("day_type"),
        )
        .filter(F.col("cal_date").isNotNull())
        .filter(
            F.col("day_type").isin(
                "Weekday",
                "Weekend",
            )
        )
        .dropDuplicates(["cal_date"])
    )


def main():
    args = parse_args()

    if args.write_partitions <= 0:
        raise ValueError(
            "--write-partitions 必須大於 0"
        )

    target_table = (
        f"{args.catalog}."
        f"{args.namespace}."
        f"{args.table_name}"
    )

    spark = (
        SparkSession.builder
        .appName("tdcs_m04a_clean")
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
        .getOrCreate()
    )

    spark.conf.set(
        "spark.sql.legacy.timeParserPolicy",
        "LEGACY",
    )

    # 將 RecordDate 統一轉成 Spark Date。
    record_date = F.coalesce(
        F.to_date(
            F.col("RecordDate").cast("string"),
            "yyyy/M/d",
        ),
        F.to_date(
            F.col("RecordDate").cast("string"),
            "yyyy/MM/dd",
        ),
        F.to_date(
            F.col("RecordDate").cast("string"),
            "yyyy-MM-dd",
        ),
        F.to_date(
            F.col("RecordDate").cast("string"),
            "yyyyMMdd",
        ),
        F.to_date(F.col("RecordDate")),
    )

    # 移除 RecordTime 中不是數字的內容。
    time_digits = F.regexp_replace(
        F.col("RecordTime").cast("string"),
        "[^0-9]",
        "",
    )

    time_4digit = F.lpad(
        time_digits,
        4,
        "0",
    )

    hour = (
        F.when(
            F.length(time_digits) <= 2,
            time_digits.cast("int"),
        )
        .otherwise(
            F.substring(
                time_4digit,
                1,
                2,
            ).cast("int")
        )
    )

    minute = (
        F.when(
            F.length(time_digits) <= 2,
            F.lit(0),
        )
        .otherwise(
            F.substring(
                time_4digit,
                3,
                2,
            ).cast("int")
        )
    )

    record_time_minute = F.concat_ws(
        ":",
        F.lpad(
            hour.cast("string"),
            2,
            "0",
        ),
        F.lpad(
            minute.cast("string"),
            2,
            "0",
        ),
    )

    time_period = (
        F.when(
            hour.between(0, 5),
            "凌晨",
        )
        .when(
            hour.between(6, 8),
            "早尖峰",
        )
        .when(
            hour.between(9, 15),
            "日間",
        )
        .when(
            hour.between(16, 18),
            "晚尖峰",
        )
        .when(
            hour.between(19, 23),
            "夜間",
        )
        .otherwise("未分類")
    )

    # 讀取所有 pair_id 資料夾中的 Parquet。
    raw = (
        spark.read
        .option(
            "recursiveFileLookup",
            "true",
        )
        .parquet(args.input_path)
        .withColumn(
            "pair_id",
            F.regexp_extract(
                F.input_file_name(),
                r"/([^/]+)/[^/]+\.parquet$",
                1,
            ),
        )
    )

    required_raw_columns = {
        "RecordDate",
        "RecordTime",
        "VehicleType",
        "TravelTime",
        "Volume",
    }

    missing_raw_columns = (
        required_raw_columns - set(raw.columns)
    )

    if missing_raw_columns:
        raise ValueError(
            f"M04A Parquet 缺少必要欄位："
            f"{sorted(missing_raw_columns)}；"
            f"目前欄位：{raw.columns}"
        )

    base = (
        raw
        .select(
            F.trim(
                F.col("pair_id")
            ).alias("pair_id"),

            record_date.alias(
                "record_date"
            ),

            record_time_minute.alias(
                "record_time_minute"
            ),

            time_period.alias(
                "time_period"
            ),

            F.col("VehicleType")
            .cast("string")
            .alias("vehicle_type"),

            F.col("TravelTime")
            .cast("int")
            .alias("travel_time_sec"),

            F.col("Volume")
            .cast("long")
            .alias("volume"),
        )
        .withColumn(
            "record_year",
            F.year("record_date"),
        )
        .withColumn(
            "record_month",
            F.date_format(
                "record_date",
                "yyyy-MM",
            ),
        )
        .filter(
            F.col("pair_id").isNotNull()
        )
        .filter(
            F.col("pair_id") != ""
        )
        .filter(
            ~F.col("pair_id").isin(
                *EXCLUDED_SERVICE_PAIR_IDS
            )
        )
        .filter(
            F.col("record_date").isNotNull()
        )
        .filter(
            F.col("record_time_minute").isNotNull()
        )
        .filter(
            F.col("vehicle_type").isNotNull()
        )
        .filter(
            F.col("vehicle_type") != ""
        )
        .filter(
            F.col("travel_time_sec").isNotNull()
        )
        .filter(
            F.col("volume").isNotNull()
        )
        .filter(
            F.col("travel_time_sec") > 0
        )
        .filter(
            F.col("volume") > 0
        )
        .filter(
            F.col("time_period") != "未分類"
        )
    )

    calendar = read_calendar(
        spark,
        args.calendar_path,
    )

    # calendar 只有約兩千筆，使用 broadcast join。
    clean = (
        base
        .join(
            F.broadcast(calendar),
            base.record_date == calendar.cal_date,
            "inner",
        )
        .select(
            "pair_id",
            "record_date",
            "record_year",
            "record_month",
            "record_time_minute",
            "day_type",
            "time_period",
            "vehicle_type",
            "travel_time_sec",
            "volume",
        )
    )

    clean_to_write = (
        clean
        .repartition(
            args.write_partitions,
            "record_month",
        )
        .sortWithinPartitions(
            "record_month",
            "record_date",
            "record_time_minute",
            "pair_id",
            "vehicle_type",
            "day_type",
            "time_period",
        )
    )

    clean_to_write.createOrReplaceTempView(
        "m04a_clean_source"
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

    spark.sql(
        f"""
        CREATE TABLE {target_table}
        USING iceberg
        PARTITIONED BY (months(record_date))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.spark.fanout.enabled' = 'true'
        )
        AS
        SELECT
            pair_id,
            record_date,
            record_year,
            record_month,
            record_time_minute,
            day_type,
            time_period,
            vehicle_type,
            travel_time_sec,
            volume
        FROM m04a_clean_source
        """
    )

    print(
        "Clean Iceberg table created successfully."
    )
    print(
        f"Target table: {target_table}"
    )

    spark.stop()


if __name__ == "__main__":
    main()