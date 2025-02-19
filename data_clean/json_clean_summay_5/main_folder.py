import os
import glob
from main_process_def import process_text_pipeline
from move_rm import move_rm

def main_folder(input_folder, output_base):
    # 入力フォルダから全てのJSONファイルを取得
    json_files = glob.glob(os.path.join(input_folder, "*.json"))

    if not json_files:
        print(f"警告: {input_folder} にJSONファイルが見つかりませんでした。")
        exit(1)

    print(f"処理対象のファイル数: {len(json_files)}")

    # 各JSONファイルに対して処理を実行
    for json_file in json_files:
        try:
            # ファイル名から拡張子を除いた名前を取得
            file_name = os.path.splitext(os.path.basename(json_file))[0]
            print(f"\n{file_name} の処理を開始します...")

            # 出力ディレクトリの作成
            output_dir = os.path.join(output_base, file_name)
            os.makedirs(output_dir, exist_ok=True)

            # 出力ファイルパスの作成
            output_file = os.path.join(output_dir, file_name)

            # メイン処理実行
            process_text_pipeline(json_file.replace(".json", ""), output_file)
            move_rm(output_dir, output_file)

            print(f"{file_name} の処理が完了しました")

        except Exception as e:
            print(f"エラー: {file_name} の処理中に問題が発生しました: {str(e)}")
            continue

    print("\n全ての処理が完了しました")



if __name__ == "__main__":
    # 処理対象のディレクトリ指定
    input_folder = "./text_dlsite_data_clean_3/text_dlsite_1-1000"
    output_base = "./text_dlsite_data_clean_4/text_dlsite_1-1000"

    main_folder(input_folder, output_base)