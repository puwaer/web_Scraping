import os
from multiprocessing import Pool, cpu_count, Manager
from clean_text_1 import process_text_1_file
from clean_text_2 import process_text_2_file
from clean_jp_nihon_block import filter_japanese_texts
from similarity_block import process_similarity_block_file
from similarity_word import remove_duplicates
from clean_jp_20_class_speed import TextFilter


def process_text_pipeline(input_file, output_file):
    #clean_text_1.py
    input_file_1 = f"{input_file}.json"
    output_file_1 = f"{output_file}_1.json"
    process_text_1_file(input_file_1, output_file_1)


    #clean_text_2.py
    input_file_2 = f'{output_file}_1.json'
    output_file_2 = f'{output_file}_2.json'
    process_text_2_file(input_file_2, output_file_2)


    #clean_jp_nihon_block.py
    input_file_3 = f'{output_file}_2.json'
    output_file_3 = f'{output_file}_3.json'
    filter_japanese_texts(input_file_3, output_file_3)

    #similarity_block.py
    input_file_4 = f'{output_file}_3.json'
    output_file_4 = f'{output_file}_4.json'
    process_similarity_block_file(input_file_4, output_file_4)


    #similarity_word.py
    input_file_5 = f'{output_file}_4.json'
    output_file_5 = f'{output_file}_5.json'
    remove_duplicates(input_file_5, output_file_5)


    #clean_jp_20_class_speed.py
    input_file_6 = f'{output_file}_5.json'
    output_file_6 = f'{output_file}_6.json'   
    text_filter = TextFilter()
    text_filter.filter_json_file(
        input_file_6, 
        output_file_6,
        chunk_size=100,  
        num_processes=int(cpu_count() * 1.5)
    )


if __name__ == "__main__":
    # 処理対象のファイル指定
    input_folder = "./test_data"
    output_base = "./output"
    input_file_name = "maniax_text_split_8"

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