import shutil
import os

def move_rm(output_dir, output_file):
    shutil.copy(f"{output_file}_10.json", f"{output_dir}.json")

    file_indices = [1, 3, 5, 7, 8, 9, 10, 20]     

    for index in file_indices:
        file_path = f"{output_file}_{index}.json"
        try:
            os.remove(file_path)
            print(f"削除しました: {file_path}")
        except Exception as e:
            print(f"エラーが発生しました: {e}")


if __name__ == "__main__":
    # 処理対象のファイル指定
    input_folder = "./test_data"
    output_base = "./output"
    input_file_name = "pro-touch_text"
    #input_file_name = "aix_tex"

    # 入力ファイルパスの作成
    input_file = os.path.join(input_folder, input_file_name)
    
    # 出力ディレクトリの作成
    output_dir = os.path.join(output_base, input_file_name)
    os.makedirs(output_dir, exist_ok=True)
    
    # 出力ファイルパスの作成
    output_file = os.path.join(output_dir, input_file_name)

    move_rm(output_dir, output_file)

    
