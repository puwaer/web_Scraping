import os
from main_process_def import process_text_pipeline
from move_rm import move_rm


if __name__ == "__main__":
    # 処理対象のファイル指定
    input_folder = "./test_data"
    output_base = "./output"
    #input_file_name = "comic_text_split_part_1"
    input_file_name = "maniax_text_split_part_1"

    # 入力ファイルパスの作成
    input_file = os.path.join(input_folder, input_file_name)
    
    # 出力ディレクトリの作成
    output_dir = os.path.join(output_base, input_file_name)
    os.makedirs(output_dir, exist_ok=True)
    
    # 出力ファイルパスの作成
    output_file = os.path.join(output_dir, input_file_name)


    print(input_file)
    print(output_dir)    
    print(output_file)
    
    # メイン処理実行
    process_text_pipeline(input_file, output_file)
    move_rm(output_dir, output_file)