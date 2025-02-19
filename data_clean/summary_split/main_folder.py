import os
import glob
from text_id_text_json import convert_json_format

def main_folder(input_folder, output_base):
    # 入力フォルダから全てのJSONファイルを取得
    json_files = glob.glob(os.path.join(input_folder, "*.json"))

    if not json_files:
        print(f"警告: {input_folder} にJSONファイルが見つかりませんでした。")
        exit(1)

    print(f"処理対象のファイル数: {len(json_files)}")

    # 出力ディレクトリの作成（一度だけ）
    os.makedirs(output_base, exist_ok=True)

    # 各JSONファイルに対して処理を実行
    for json_file in json_files:
        try:
            # ファイル名から拡張子を除いた名前を取得
            file_name = os.path.splitext(os.path.basename(json_file))[0]
            print(f"\n{file_name} の処理を開始します...")

            # 出力ファイルパスの作成（サブフォルダなし）
            output_file = os.path.join(output_base, f"{file_name}.json")

            # メイン処理実行
            convert_json_format(json_file, output_file)

            print(f"{file_name} の処理が完了しました")

        except Exception as e:
            print(f"エラー: {file_name} の処理中に問題が発生しました: {str(e)}")
            continue

    print("\n全ての処理が完了しました")

if __name__ == "__main__":
    # 処理対象のディレクトリ指定
    input_folder = "./data/text_dmm_1-1000"
    output_base = "./formatted_data/text_dmm_1-1000-20250214"

    main_folder(input_folder, output_base)