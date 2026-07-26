import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Join reliability results with road "
            "coordinates and export GeoJSON."
        )
    )

    parser.add_argument(
        "--warehouse",
        required=True,
    )

    parser.add_argument(
        "--location-path",
        required=True,
        help="location.csv 或 location.parquet 路徑。",
    )

    parser.add_argument(
        "--output-path",
        required=True,
        help=(
            "例如 hdfs:///dataset/tdcs_geojson/"
            "reliability_map.geojson"
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


def read_location(spark, location_path):
    if location_path.lower().endswith(".csv"):
        return (
            spark.read
            .option("header", "true")
            .option("encoding", "UTF-8")
            .csv(location_path)
        )

    return spark.read.parquet(location_path)


def write_single_text_file(
    spark,
    output_path,
    text,
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

    # 將完整 GeoJSON 寫成一個 part 檔。
    sc.parallelize(
        [text],
        1,
    ).saveAsTextFile(
        str(temporary)
    )

    part_file = None

    for status in file_system.listStatus(
        temporary
    ):
        name = (
            status.getPath().getName()
        )

        if name.startswith("part-"):
            part_file = status.getPath()
            break

    if part_file is None:
        raise RuntimeError(
            "找不到 Spark 產生的 part 檔案。"
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
            "tdcs_reliability_geojson"
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
            "spark.sql.shuffle.partitions",
            "16",
        )
        .getOrCreate()
    )

    summary = spark.table(source_table)

    location_raw = read_location(
        spark,
        args.location_path,
    )

    required_summary_columns = {
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

    missing_summary_columns = (
        required_summary_columns
        - set(summary.columns)
    )

    if missing_summary_columns:
        raise ValueError(
            f"Summary table 缺少必要欄位："
            f"{sorted(missing_summary_columns)}"
        )

    required_location_columns = {
        "pair_id",
        "pair_roadname",
        "start_gantry_id",
        "start_roadname",
        "start_link_id",
        "start_longitude",
        "start_latitude",
        "end_gantry_id",
        "end_roadname",
        "end_link_id",
        "end_longitude",
        "end_latitude",
        "distance_km",
        "direction",
    }

    missing_location_columns = (
        required_location_columns
        - set(location_raw.columns)
    )

    if missing_location_columns:
        raise ValueError(
            f"Location 缺少必要欄位："
            f"{sorted(missing_location_columns)}"
        )

    location = (
        location_raw
        .select(
            F.trim(
                F.col("pair_id")
            ).alias("pair_id"),

            F.trim(
                F.col("pair_roadname")
            ).alias("pair_roadname"),

            F.trim(
                F.col("start_gantry_id")
            ).alias("start_gantry_id"),

            F.trim(
                F.col("start_roadname")
            ).alias("start_roadname"),

            F.trim(
                F.col("start_link_id")
            ).alias("start_link_id"),

            F.col("start_longitude")
            .cast("double")
            .alias("start_longitude"),

            F.col("start_latitude")
            .cast("double")
            .alias("start_latitude"),

            F.trim(
                F.col("end_gantry_id")
            ).alias("end_gantry_id"),

            F.trim(
                F.col("end_roadname")
            ).alias("end_roadname"),

            F.trim(
                F.col("end_link_id")
            ).alias("end_link_id"),

            F.col("end_longitude")
            .cast("double")
            .alias("end_longitude"),

            F.col("end_latitude")
            .cast("double")
            .alias("end_latitude"),

            F.col("distance_km")
            .cast("double")
            .alias("distance_km"),

            F.trim(
                F.col("direction")
            ).alias("direction"),
        )
    )

    # 避免重複 pair_id 造成 Join 後資料倍增。
    duplicate_location_count = (
        location
        .groupBy("pair_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_location_count:
        raise ValueError(
            "Location 存在重複 pair_id；"
            f"重複數量："
            f"{duplicate_location_count}"
        )

    # 檢查每一個 Summary 路段是否都有位置資料。
    missing_pair_count = (
        summary
        .select("pair_id")
        .distinct()
        .join(
            location.select("pair_id"),
            on="pair_id",
            how="left_anti",
        )
        .count()
    )

    if missing_pair_count:
        raise ValueError(
            "部分 Summary 路段找不到 Location；"
            f"缺少路段數量："
            f"{missing_pair_count}"
        )

    joined = (
        summary
        .join(
            F.broadcast(location),
            on="pair_id",
            how="inner",
        )
        .filter(
            F.col(
                "start_longitude"
            ).isNotNull()
        )
        .filter(
            F.col(
                "start_latitude"
            ).isNotNull()
        )
        .filter(
            F.col(
                "end_longitude"
            ).isNotNull()
        )
        .filter(
            F.col(
                "end_latitude"
            ).isNotNull()
        )
    )

    features = []

    # Summary 約 8,064 筆，可安全傳回 Driver。
    for row in joined.toLocalIterator():
        feature = {
            "type": "Feature",

            "geometry": {
                "type": "LineString",

                "coordinates": [
                    [
                        row["start_longitude"],
                        row["start_latitude"],
                    ],
                    [
                        row["end_longitude"],
                        row["end_latitude"],
                    ],
                ],
            },

            "properties": {
                "pair_id": (
                    row["pair_id"]
                ),

                "pair_roadname": (
                    row["pair_roadname"]
                ),

                "summary_level": (
                    row["summary_level"]
                ),

                "day_type": (
                    row["day_type"]
                ),

                "time_period": (
                    row["time_period"]
                ),

                "direction": (
                    row["direction"]
                ),

                "start_gantry_id": (
                    row["start_gantry_id"]
                ),

                "start_roadname": (
                    row["start_roadname"]
                ),

                "start_link_id": (
                    row["start_link_id"]
                ),

                "end_gantry_id": (
                    row["end_gantry_id"]
                ),

                "end_roadname": (
                    row["end_roadname"]
                ),

                "end_link_id": (
                    row["end_link_id"]
                ),

                "distance_km": (
                    row["distance_km"]
                ),

                "p50_travel_time_sec": (
                    row[
                        "p50_travel_time_sec"
                    ]
                ),

                "p95_travel_time_sec": (
                    row[
                        "p95_travel_time_sec"
                    ]
                ),

                "buffer_time_sec": (
                    row["buffer_time_sec"]
                ),

                "buffer_index": (
                    row["buffer_index"]
                ),

                "reliability_level": (
                    row["reliability_level"]
                ),
            },
        }

        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "name": "tdcs_reliability_map",
        "features": features,
    }

    geojson_text = json.dumps(
        geojson,
        ensure_ascii=False,
        allow_nan=False,
    )

    write_single_text_file(
        spark=spark,
        output_path=args.output_path,
        text=geojson_text,
        overwrite=args.overwrite,
    )

    print(
        f"GeoJSON output: "
        f"{args.output_path}"
    )

    print(
        f"Feature count: {len(features)}"
    )

    spark.stop()


if __name__ == "__main__":
    main()