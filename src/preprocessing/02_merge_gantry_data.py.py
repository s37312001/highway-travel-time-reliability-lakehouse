import argparse
import shutil
from pathlib import Path

import pandas as pd

def merge_and_clean_gantry_data(
    base_data_dir,
    mapping_csv,
    delete_source=False,
):
    """
    固定讀取 check_data.csv，將舊門架 CSV 檔案改名。
    若目標無同名檔案則單純複製；若有同名檔案則進行縱向合併並重新按時間排序。
    最後將舊門架資料夾刪除。
    
    :param base_data_dir: 存放所有門架資料夾的根目錄路徑
    """
    # 1. 讀取對照表 CSV
    csv_path = Path(mapping_csv).expanduser().resolve()
    base_path = Path(base_data_dir).expanduser().resolve()

    if not csv_path.exists():
        raise FileNotFoundError(f"找不到門架對照表：{csv_path}")

    if not base_path.exists():
        raise FileNotFoundError(f"找不到門架資料目錄：{base_path}")

    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    required_columns = {"route", "note", "target_route"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"門架對照表缺少欄位：{sorted(missing_columns)}"
        )

    source_col = "route"
    note_col = "note"
    target_col = "target_route"
    
    # 篩選 N 欄包含「合併」字眼的資料
    merge_tasks = df[df[note_col].astype(str).str.contains("合併", na=False)]
    
    print(f"找到 {len(merge_tasks)} 筆需要合併的門架紀錄，開始處理...\n")
    
    # 2. 遍歷每一筆合併任務
    for _, row in merge_tasks.iterrows():
        source_gantry = str(row[source_col]).strip()
        target_gantry = str(row[target_col]).strip()
        
        # 將來源與目標相同的資料列跳過
        if source_gantry == target_gantry:
            continue
        
        source_dir = base_path / source_gantry
        target_dir = base_path / target_gantry
        
        # 檢查來源資料夾是否存在
        if not source_dir.exists():
            print(f"找不到來源資料夾: 【{source_gantry}】，跳過。")
            continue
            
        # 確保目標資料夾存在
        target_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"正在處理 【{source_gantry}】 -> 【{target_gantry}】...")
        
        # 3. 處理舊門架資料夾底下的所有 CSV 檔
        for src_file in source_dir.glob("*.csv"):
            new_filename = src_file.name.replace(source_gantry, target_gantry)
            dest_file = target_dir / new_filename
            
            # 【核心修改】狀況 A：目標資料夾不存在改名後的同名檔案 -> 單純複製整個檔案（不進行排序）
            if not dest_file.exists():
                shutil.copy2(src_file, dest_file)
                print(f"  └─ 檔案改名並複製成功: {src_file.name} -> {new_filename}")
            
            # 【核心修改】狀況 B：目標資料夾已存在同名檔案 -> 進行 CSV 內文縱向合併，此時才重新排序
            else:
                try:
                    # 無標頭檔的 CSV 指定 header=None
                    df_src = pd.read_csv(src_file, header=None)
                    df_dst = pd.read_csv(dest_file, header=None)
                    
                    # 縱向合併並去除完全重複的資料列
                    df_merged = pd.concat([df_dst, df_src], ignore_index=True).drop_duplicates()
                    print(f"  └─ 偵測到同名檔案 {new_filename}，進行內容合併與重新排序...")
                    
                    # 只有在縱向合併過後，才執行時間軸排序（1/1在前、12/31在後）
                    try:
                        df_merged['tmp_datetime'] = pd.to_datetime(
                            df_merged[0].astype(str) + ' ' + df_merged[1].astype(str), 
                            errors='coerce'
                        )
                        df_merged = df_merged.sort_values(by='tmp_datetime').drop(columns=['tmp_datetime'])
                    except Exception as sort_err:
                        print(f"     時間排序失敗，採用預設文字排序: {sort_err}")
                        df_merged = df_merged.sort_values(by=[0, 1])
                    
                    # 儲存合併且排序後的檔案 (不寫入 header)
                    df_merged.to_csv(
                                        dest_file,
                                        index=False,
                                        header=False,
                                        encoding="utf-8",
                                    )
                    
                except Exception as e:
                    print(f"  └─ 檔案內容合併失敗 ({new_filename}): {e}")
                    
        # 該舊門架下所有檔案處理完畢後，刪除舊的來源資料夾
        if delete_source:
            try:
                shutil.rmtree(source_dir)
                print(f"  成功刪除舊資料夾: 【{source_gantry}】")
            except Exception as rm_err:
                print(
                    f"  刪除舊資料夾 【{source_gantry}】失敗: {rm_err}"
                )
        else:
            print(f"  已保留來源資料夾: 【{source_gantry}】")
            
    print("\n所有 CSV 資料處理與舊資料夾清理完成！")


def main():
    parser = argparse.ArgumentParser(
        description="依門架對照表合併新舊 pair_id 資料。"
    )

    parser.add_argument(
        "--data-dir",
        required=True,
        help="split.py 產生的 pair_id CSV 根目錄。",
    )

    parser.add_argument(
        "--mapping-file",
        required=True,
        help="check_data.csv 的路徑。",
    )

    parser.add_argument(
        "--delete-source",
        action="store_true",
        help="合併成功後刪除舊門架資料夾；預設保留。",
    )

    args = parser.parse_args()

    merge_and_clean_gantry_data(
        base_data_dir=args.data_dir,
        mapping_csv=args.mapping_file,
        delete_source=args.delete_source,
    )


if __name__ == "__main__":
    main()