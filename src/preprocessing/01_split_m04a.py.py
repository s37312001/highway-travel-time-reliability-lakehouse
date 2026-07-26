from pathlib import Path
import argparse
import shutil
import time
from datetime import datetime
import pandas as pd


COL_NAMES = [
    "datetime",
    "start_gantry",
    "end_gantry",
    "vehicle_type",
    "travel_time_sec",
    "volume",
]


def expand_user_path(path_text: str | Path) -> Path:
    """支援 Linux 的 ~，例如 ~/raw/M04A。"""
    return Path(path_text).expanduser().resolve()


def format_date_no_zero(dt_series: pd.Series) -> pd.Series:
    """把日期格式轉成 2026/6/14，不補 0。"""
    return (
        dt_series.dt.year.astype(str)
        + "/"
        + dt_series.dt.month.astype(str)
        + "/"
        + dt_series.dt.day.astype(str)
    )


def parse_filter_date(date_text: str | None) -> pd.Timestamp | None:
    """支援 YYYYMMDD 或 YYYY-MM-DD，回傳日期 Timestamp。"""
    if not date_text:
        return None

    cleaned = date_text.strip().replace("-", "")
    if len(cleaned) != 8 or not cleaned.isdigit():
        raise ValueError(f"日期格式錯誤：{date_text}，請使用 YYYYMMDD，例如 20260113")

    return pd.to_datetime(cleaned, format="%Y%m%d")


def is_date_folder_name(name: str) -> bool:
    """判斷資料夾名稱是否像 20260504。"""
    return len(name) == 8 and name.isdigit()


def is_hour_folder_name(name: str) -> bool:
    """判斷資料夾名稱是否像 00~23。"""
    return len(name) == 2 and name.isdigit() and 0 <= int(name) <= 23


def folder_date_passes_filters(
    date_folder_name: str,
    year_filter: str | None = None,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
) -> bool:
    """
    先用日期資料夾名稱做篩選。
    例如 --year 2026 時，只允許 2026xxxx 資料夾通過，2025xxxx 直接跳過。
    """
    if not is_date_folder_name(date_folder_name):
        return False

    if year_filter is not None and not date_folder_name.startswith(year_filter):
        return False

    folder_ts = pd.to_datetime(date_folder_name, format="%Y%m%d", errors="coerce")
    if pd.isna(folder_ts):
        return False

    if start_ts is not None and folder_ts < start_ts:
        return False

    if end_ts is not None and folder_ts > end_ts:
        return False

    return True


def get_date_folder_name_from_path(path: Path) -> str | None:
    """從路徑中找出第一個 YYYYMMDD 格式的資料夾名稱。"""
    for part in path.parts:
        if is_date_folder_name(part):
            return part
    return None


def get_standard_hour_dirs(date_dir: Path) -> list[Path]:
    """
    只取得日期資料夾底下第一層的 00~23 小時資料夾。

    例如會讀：
      ~/raw/M04A/20250101/00
      ~/raw/M04A/20250101/01
      ...
      ~/raw/M04A/20250101/23

    不會讀：
      ~/raw/M04A/20250101/20250101
      ~/raw/M04A/20250101/20250101/00
    """
    return sorted(
        [p for p in date_dir.iterdir() if p.is_dir() and is_hour_folder_name(p.name)],
        key=lambda x: x.name,
    )


def collect_scan_roots(
    input_dir: Path,
    recursive: bool = True,
    year_filter: str | None = None,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
) -> tuple[list[Path], int]:
    """
    決定真正要掃描哪些資料夾。

    新版重點：只掃標準小時資料夾 00~23。
    因此如果解壓縮後多出這種巢狀資料夾：
      ~/raw/M04A/20250101/20250101
    程式會跳過，不會再往下讀它裡面的 CSV。

    支援 input 層級：
      ~/raw/M04A
      ~/raw/M04A/20260113
      ~/raw/M04A/20260113/00
    """
    skipped_date_dirs = 0

    if not recursive:
        return [input_dir], skipped_date_dirs

    # 情境 1：input 直接是小時資料夾，例如 ~/raw/M04A/20260113/00
    if is_hour_folder_name(input_dir.name) and is_date_folder_name(input_dir.parent.name):
        if folder_date_passes_filters(input_dir.parent.name, year_filter, start_ts, end_ts):
            return [input_dir], skipped_date_dirs
        return [], 1

    # 情境 2：input 直接是日期資料夾，例如 ~/raw/M04A/20260113
    # 只回傳它底下第一層 00~23，不用 rglob 往下掃，避免讀到 20260113/20260113。
    if is_date_folder_name(input_dir.name):
        if folder_date_passes_filters(input_dir.name, year_filter, start_ts, end_ts):
            hour_dirs = get_standard_hour_dirs(input_dir)
            if hour_dirs:
                return hour_dirs, skipped_date_dirs
            # 若該日期資料夾沒有 00~23，才退回掃日期資料夾本層的 CSV。
            return [input_dir], skipped_date_dirs
        return [], 1

    # 情境 3：input 是 M04A 根資料夾，例如 ~/raw/M04A
    # 只收第一層 YYYYMMDD 日期資料夾；每個日期資料夾只進入第一層 00~23。
    date_dirs = [p for p in input_dir.iterdir() if p.is_dir() and is_date_folder_name(p.name)]
    if date_dirs:
        scan_roots: list[Path] = []
        for date_dir in sorted(date_dirs, key=lambda x: x.name):
            if folder_date_passes_filters(date_dir.name, year_filter, start_ts, end_ts):
                hour_dirs = get_standard_hour_dirs(date_dir)
                if hour_dirs:
                    scan_roots.extend(hour_dirs)
                else:
                    # 若日期資料夾沒有 00~23，才退回掃日期資料夾本層的 CSV。
                    scan_roots.append(date_dir)
            else:
                skipped_date_dirs += 1
        return scan_roots, skipped_date_dirs

    # 情境 4：非標準結構。退回掃 input 本層，但不做無限制 rglob。
    return [input_dir], skipped_date_dirs


def find_m04a_files(
    input_dir: Path,
    recursive: bool = True,
    year_filter: str | None = None,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
) -> tuple[list[Path], int, int]:
    """
    找出 TDCS_M04A_*.csv / .CSV。

    如果 input_dir 是 ~/raw/M04A，會先用日期資料夾名稱篩選年份。
    例如 --year 2026 時，只進入 2026xxxx 資料夾，不進入 2025xxxx。

    回傳：
      files, scan_root_count, skipped_date_dir_count
    """
    scan_roots, skipped_date_dirs = collect_scan_roots(
        input_dir=input_dir,
        recursive=recursive,
        year_filter=year_filter,
        start_ts=start_ts,
        end_ts=end_ts,
    )

    files: list[Path] = []
    seen: set[str] = set()

    for root in scan_roots:
        # collect_scan_roots() 已經把根目錄限制在標準 00~23 小時資料夾，
        # 這裡只掃該資料夾本層，避免讀到 YYYYMMDD/YYYYMMDD 這種巢狀資料夾。
        iterator = root.iterdir()

        for p in iterator:
            if not p.is_file():
                continue

            name_lower = p.name.lower()
            if not (name_lower.startswith("tdcs_m04a_") and name_lower.endswith(".csv")):
                continue

            # 保險機制：如果檔案路徑中有日期資料夾，也先用資料夾年份過濾。
            date_folder_name = get_date_folder_name_from_path(p)
            if date_folder_name is not None:
                if not folder_date_passes_filters(date_folder_name, year_filter, start_ts, end_ts):
                    continue

            # resolve 後去重，避免符號連結或大小寫問題造成重複。
            key = str(p.resolve())
            if key in seen:
                continue

            seen.add(key)
            files.append(p)

    return sorted(files, key=lambda x: str(x).lower()), len(scan_roots), skipped_date_dirs


def delete_existing_year_outputs(output_dir: Path, year_filter: str | None) -> int:
    """
    只刪除指定年份的輸出 CSV，保留其他年份與既有 pair 資料夾。

    例如 year_filter=2025 時，只刪：
      ~/dataset/M04A/01F0017N_01F0005N/2025_01F0017N_01F0005N.csv

    不會刪除：
      ~/dataset/M04A/01F0017N_01F0005N/2026_01F0017N_01F0005N.csv
    """
    if year_filter is None or not output_dir.exists():
        return 0

    deleted_count = 0
    pattern = f"{year_filter}_*.csv"
    for csv_path in output_dir.glob(f"*/{pattern}"):
        if csv_path.is_file():
            csv_path.unlink()
            deleted_count += 1
    return deleted_count


def split_m04a_by_pair_year_folder(
    input_dir: Path,
    output_dir: Path,
    append: bool = False,
    reset_output: bool = False,
    recursive: bool = True,
    year_filter: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    only_existing_pairs: bool = False,
    verbose: bool = False,
) -> None:
    """
    遞迴讀取 input_dir 裡所有 TDCS_M04A_*.csv / .CSV，
    依照 B 欄 + C 欄建立 pair_id 資料夾，
    再依照 A 欄年份建立 年份_pair_id.csv。

    預設行為：
      1. 不會清空整個 output_dir，所以已完成的 2026 檔案會保留。
      2. 若有指定 --year 且沒有加 --append，會先刪除該年份舊輸出 CSV，避免重跑同一年時重複 append。
      3. pair_id 資料夾若已存在，直接使用；若不存在，預設會建立。
      4. 若加 --only-existing-pairs，pair_id 資料夾不存在時會跳過，不會新增任何新的 pair 資料夾。

    可讀取的 input 層級：
      ~/raw/M04A
      ~/raw/M04A/20260113
      ~/raw/M04A/20260113/00

    輸出資料層級：
      ~/dataset/M04A/01F0017N_01F0005N/2026_01F0017N_01F0005N.csv
    """
    input_dir = expand_user_path(input_dir)
    output_dir = expand_user_path(output_dir)

    if not input_dir.exists():
        raise FileNotFoundError(f"找不到輸入資料夾：{input_dir}")

    if not input_dir.is_dir():
        raise NotADirectoryError(f"輸入路徑不是資料夾：{input_dir}")

    if year_filter is not None:
        year_filter = str(year_filter).strip()
        if len(year_filter) != 4 or not year_filter.isdigit():
            raise ValueError(f"年份格式錯誤：{year_filter}，請使用 2026 這種格式")

    start_ts = parse_filter_date(start_date)
    end_ts = parse_filter_date(end_date)
    if start_ts is not None and end_ts is not None and start_ts > end_ts:
        raise ValueError("start-date 不可以晚於 end-date")

    # 只有明確加 --reset-output 才會刪除整個 M04A 輸出資料夾。
    # 這樣處理 2025 時，不會把已完成的 2026 結果刪掉。
    if reset_output and output_dir.exists():
        shutil.rmtree(output_dir)

    # 自動建立 M04A 這層資料夾；如果已存在，不會重複新增。
    output_dir.mkdir(parents=True, exist_ok=True)

    # 預設不 append：只清掉本次年份的舊 CSV，避免同一年重跑造成資料列重複。
    # 不會刪除其他年份，例如處理 2025 時會保留 2026_*.csv。
    deleted_year_outputs = 0
    if (not append) and (not reset_output) and year_filter is not None:
        deleted_year_outputs = delete_existing_year_outputs(output_dir, year_filter)

    files, scan_root_count, skipped_date_dirs = find_m04a_files(
        input_dir,
        recursive=recursive,
        year_filter=year_filter,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    if not files:
        scan_mode = "遞迴" if recursive else "單層"
        filter_text = f"，年份篩選={year_filter}" if year_filter else ""
        raise FileNotFoundError(
            f"在 {input_dir} 以{scan_mode}方式找不到符合條件的 TDCS_M04A_*.csv 或 TDCS_M04A_*.CSV{filter_text}"
        )

    print(f"輸入根資料夾：{input_dir}")
    print(f"輸出根資料夾：{output_dir}")
    print(f"掃描模式：{'只讀標準 00~23 小時資料夾，不掃巢狀日期資料夾' if recursive else '只讀取 input 這一層'}")
    print(f"實際進入掃描的日期/小時資料夾數：{scan_root_count}")
    print(f"因年份/日期篩選跳過的日期資料夾數：{skipped_date_dirs}")
    print(f"找到原始檔數量：{len(files)}")
    print(f"是否清空整個輸出資料夾：{'是' if reset_output else '否'}")
    print(f"是否直接 append 到既有 CSV：{'是' if append else '否'}")
    if year_filter is not None and not append and not reset_output:
        print(f"本次已先刪除既有 {year_filter}_*.csv 數量：{deleted_year_outputs}")
    print(f"是否只寫入既有 pair 資料夾：{'是' if only_existing_pairs else '否'}")

    if year_filter:
        print(f"年份篩選：先用資料夾名稱篩選 {year_filter}xxxx，再用 A 欄年份保險確認 = {year_filter}")
    if start_ts is not None:
        print(f"開始日期篩選：{start_ts.strftime('%Y-%m-%d')}")
    if end_ts is not None:
        print(f"結束日期篩選：{end_ts.strftime('%Y-%m-%d')}")

    if verbose:
        for p in files:
            print(f"  - {p}")
    else:
        preview_count = min(5, len(files))
        print("前幾個檔案範例：")
        for p in files[:preview_count]:
            print(f"  - {p}")
        if len(files) > preview_count:
            print(f"  ... 其餘 {len(files) - preview_count} 個檔案省略，若要全部列出可加 --verbose")

    total_rows_read = 0
    total_rows_written = 0
    skipped_empty_files = 0
    skipped_missing_pair_rows = 0
    skipped_missing_pair_ids: set[str] = set()
    created_pair_dirs: set[Path] = set()
    reused_pair_dirs: set[Path] = set()
    output_files: set[Path] = set()
    output_pairs: set[str] = set()

    for i, file_path in enumerate(files, start=1):
        if verbose:
            print(f"處理中 [{i}/{len(files)}]：{file_path}")
        elif i == 1 or i % 100 == 0 or i == len(files):
            print(f"處理進度：{i}/{len(files)}")

        df = pd.read_csv(
            file_path,
            header=None,
            names=COL_NAMES,
            dtype={
                "datetime": "string",
                "start_gantry": "string",
                "end_gantry": "string",
                "vehicle_type": "string",
                "travel_time_sec": "string",
                "volume": "string",
            },
            encoding="utf-8-sig",
        )
        total_rows_read += len(df)

        # 轉換 A 欄日期時間，例如：2026/06/14 23:55
        dt = pd.to_datetime(df["datetime"], format="%Y/%m/%d %H:%M", errors="coerce")
        valid_mask = dt.notna()

        if not valid_mask.all() and verbose:
            bad_rows = len(df) - int(valid_mask.sum())
            print(f"  注意：{file_path.name} 有 {bad_rows} 列日期時間無法解析，已略過。")

        df = df[valid_mask].copy()
        dt = dt[valid_mask]

        if df.empty:
            skipped_empty_files += 1
            continue

        # 保險機制：雖然前面已用資料夾年份篩掉其他年份，這裡仍用 A 欄確認。
        if year_filter is not None:
            mask = dt.dt.year.astype(str) == year_filter
            df = df[mask].copy()
            dt = dt[mask]

        if start_ts is not None:
            mask = dt.dt.normalize() >= start_ts
            df = df[mask].copy()
            dt = dt[mask]

        if end_ts is not None:
            mask = dt.dt.normalize() <= end_ts
            df = df[mask].copy()
            dt = dt[mask]

        if df.empty:
            skipped_empty_files += 1
            continue

        df["year"] = dt.dt.year.astype(str)
        df["date"] = format_date_no_zero(dt)
        df["time"] = dt.dt.strftime("%H:%M")
        df["pair_id"] = df["start_gantry"] + "_" + df["end_gantry"]

        # 同一路段、同一年，寫到同一個年份_pair_id.csv
        # 例如：~/dataset/M04A/01F0017N_01F0005N/2025_01F0017N_01F0005N.csv
        for (pair_id, year), g in df.groupby(["pair_id", "year"], sort=False):
            pair_dir = output_dir / pair_id

            if pair_dir.exists():
                reused_pair_dirs.add(pair_dir)
            else:
                if only_existing_pairs:
                    skipped_missing_pair_rows += len(g)
                    skipped_missing_pair_ids.add(pair_id)
                    continue
                pair_dir.mkdir(parents=True, exist_ok=True)
                created_pair_dirs.add(pair_dir)

            out_path = pair_dir / f"{year}_{pair_id}.csv"
            out_df = g[["date", "time", "vehicle_type", "travel_time_sec", "volume"]]

            # mode='a'：處理多個日期/小時資料夾時，同一 pair_id + year 會接在同一個 CSV 後面。
            # 若沒有加 --append，程式開頭已先刪掉同年份舊 CSV，因此不會重複同一年資料。
            out_df.to_csv(out_path, mode="a", header=False, index=False, encoding="utf-8-sig")

            output_pairs.add(pair_id)
            output_files.add(out_path)
            total_rows_written += len(out_df)

    print("完成！")
    print(f"讀取原始檔數量：{len(files)}")
    print(f"讀取原始資料列數：{total_rows_read}")
    print(f"略過空檔或篩選後無資料的檔案數：{skipped_empty_files}")
    print(f"沿用既有 pair 資料夾數量：{len(reused_pair_dirs)}")
    print(f"本次新建立 pair 資料夾數量：{len(created_pair_dirs)}")
    print(f"因 pair 資料夾不存在而跳過的 pair 數量：{len(skipped_missing_pair_ids)}")
    print(f"因 pair 資料夾不存在而跳過的資料列數：{skipped_missing_pair_rows}")
    print(f"輸出路段資料夾數量：{len(output_pairs)}")
    print(f"輸出 CSV 檔案數量：{len(output_files)}")
    print(f"輸出資料列數：{total_rows_written}")
    print(f"輸出根資料夾：{output_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "讀取 M04A 日期/小時資料夾，且只掃標準 00~23 小時資料夾，依 B+C 欄建立路段資料夾，"
            "並依 A 欄年份輸出 年份_pair_id.csv。"
        )
    )
    parser.add_argument(
        "--input",
        required=True,
        help=(
            "原始資料根資料夾。可填 ~/raw/M04A、~/raw/M04A/20260113，"
            "或 ~/raw/M04A/20260113/00。預設只讀日期資料夾底下第一層 00~23，不讀 20260113/20260113 這種巢狀資料夾。"
        ),
    )
    parser.add_argument(
        "--output",
        required=True,
        help="依 pair_id 和年份整理後的 CSV 輸出根目錄。",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="不要刪除既有同年份 CSV，直接接續 append。一般重跑同一年不建議使用，避免資料重複。",
    )
    parser.add_argument(
        "--reset-output",
        action="store_true",
        help="先清空整個輸出根資料夾。注意：這會刪掉已完成的其他年份資料，例如 2026。",
    )
    parser.add_argument(
        "--only-existing-pairs",
        action="store_true",
        help="只把資料寫進已存在的 pair 資料夾；如果 pair 資料夾不存在就跳過，不新增資料夾。",
    )
    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="只讀取 --input 這一層，不讀子資料夾。一般不需要加；若 input 已經是單一 00 資料夾，加不加都可以。",
    )
    parser.add_argument(
        "--year",
        default=None,
        help="先用日期資料夾名稱篩選指定年份，例如 --year 2026 只讀取 2026xxxx 日期資料夾，再用 A 欄年份保險確認。",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="只輸出指定起始日期之後的資料，格式 YYYYMMDD，例如 20260112。也會先用日期資料夾名稱篩選。",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="只輸出指定結束日期之前的資料，格式 YYYYMMDD，例如 20260113。也會先用日期資料夾名稱篩選。",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="列出所有讀到的檔案與較詳細處理訊息。",
    )

    args = parser.parse_args()

    start_wall_time = datetime.now()
    start_perf = time.perf_counter()
    print(f"程式開始時間：{start_wall_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        split_m04a_by_pair_year_folder(
            input_dir=Path(args.input),
            output_dir=Path(args.output),
            append=args.append,
            reset_output=args.reset_output,
            recursive=not args.no_recursive,
            year_filter=args.year,
            start_date=args.start_date,
            end_date=args.end_date,
            only_existing_pairs=args.only_existing_pairs,
            verbose=args.verbose,
        )
    finally:
        end_wall_time = datetime.now()
        elapsed_seconds = time.perf_counter() - start_perf
        elapsed_minutes = int(elapsed_seconds // 60)
        remaining_seconds = elapsed_seconds % 60

        print("=" * 50)
        print(f"程式結束時間：{end_wall_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"總耗時秒數：{elapsed_seconds:.2f} 秒")
        print(f"總耗時：{elapsed_minutes} 分 {remaining_seconds:.2f} 秒")
        print("=" * 50)


if __name__ == "__main__":
    main()
