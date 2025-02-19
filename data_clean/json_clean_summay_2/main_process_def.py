import os
from multiprocessing import Pool, cpu_count, Manager
from clean_jp_20_class_speed import TextFilter
from clean_many import clean_many_file
from clean_space_speed import process_space_file
from clean_text_block_speed import process_text_block_file


def process_text_pipeline(input_file, output_file):
    #clean_text_block_speed.py
    input_file_1 = f"{input_file}.json"
    output_file_1 = f"{output_file}_1.json"
    block_num = 10

    process_text_block_file(input_file_1, output_file_1, block_num)


    #clean_space_speed.py
    input_file_2 = f'{output_file}_1.json'
    output_file_2 = f'{output_file}_2.json'
    process_space_file(input_file_2, output_file_2)


    #clean_many.py
    input_file_3 = f'{output_file}_2.json'
    output_file_3 = f'{output_file}_3.json'
    clean_many_file(input_file_3, output_file_3)


    #clean_jp_20_class_speed.py
    input_file_4 = f'{output_file}_3.json'
    output_file_4 = f'{output_file}_4.json'
    text_filter = TextFilter()
    text_filter.filter_json_file(
        input_file_4, 
        output_file_4,
        chunk_size=100,  
        num_processes=int(cpu_count() * 1.5)
    )






if __name__ == "__main__":
    # 処理対象のファイル指定
    input_folder = "./text_dlsite_etc/1-1000/"
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