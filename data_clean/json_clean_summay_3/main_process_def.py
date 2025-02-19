import os
from multiprocessing import Pool, cpu_count, Manager
from clean_jp_20_class_speed import TextFilter
from similarity_word_class_speed import ParallelTextSimilarityFilter
from clean_many import clean_many_file
from clean_space_speed import process_space_file
from clean_text_block_speed import process_text_block_file
from clean_delete_nn import process_delete_nn_file
from clean_emoji import process_emoji_file
from clean_num import process_num_file
from clean_sign import process_sign_file
from clean_url import process_url_file
from clean_jp_nihon import filter_japanese_texts


def process_text_pipeline(input_file, output_file):
    #clean_text_block_speed.py
    input_file_1 = f"{input_file}.json"
    output_file_1 = f"{output_file}_1.json"
    block_num_1 = 10
    process_text_block_file(input_file_1, output_file_1, block_num_1)


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

    #clean_emoji.py
    input_file_5 = f'{output_file}_4.json'
    output_file_5 = f'{output_file}_5.json'
    process_emoji_file(input_file_5, output_file_5)


    #clean_num.py
    input_file_6 = f'{output_file}_5.json'
    output_file_6 = f'{output_file}_6.json'
    process_num_file(input_file_6, output_file_6)


    #clean_sign.py
    input_file_7 = f'{output_file}_6.json'
    output_file_7 = f'{output_file}_7.json'
    process_sign_file(input_file_7, output_file_7)


    #clean_url.py
    input_file_8 = f'{output_file}_7.json'
    output_file_8 = f'{output_file}_8.json'
    process_url_file(input_file_8, output_file_8)


    #clean_text_block_speed.py
    input_file_9 = f'{output_file}_8.json'
    output_file_9 = f'{output_file}_9.json'
    block_num_2 = 10
    process_text_block_file(input_file_9, output_file_9, block_num_2)


    #clean_jp_20_class_speed.py
    input_file_10 = f'{output_file}_9.json'
    output_file_10 = f'{output_file}_10.json'
    text_filter = TextFilter()
    text_filter.filter_json_file(
        input_file_10, 
        output_file_10,
        chunk_size=100,  
        num_processes=int(cpu_count() * 1.5)
    )


    #clean_many.py
    input_file_11 = f'{output_file}_10.json'
    output_file_11 = f'{output_file}_11.json'
    clean_many_file(input_file_11, output_file_11)


    #clean_delete_nn.py
    input_file_12 = f'{output_file}_11.json'
    output_file_12 = f'{output_file}_12.json'
    process_delete_nn_file(input_file_12, output_file_12)


    #clean_emoji.py
    input_file_13 = f'{output_file}_12.json'
    output_file_13 = f'{output_file}_13.json'
    process_emoji_file(input_file_13, output_file_13)

    #clean_jp_nihon.jp
    input_file_14 = f'{output_file}_13.json'
    output_file_14 = f'{output_file}_14.json'
    filter_japanese_texts(input_file_14, output_file_14)


    #similarity_word_class_speed.py
    input_file_15 = f'{output_file}_14.json'
    output_file_15 = f'{output_file}_15.json'
    text_filter = ParallelTextSimilarityFilter(
        similarity_threshold=0.8,
        chunk_size=500  # チャンクサイズは必要に応じて調整可能
    )
    text_filter.process_file(input_file_15, output_file_15)  



if __name__ == "__main__":
    # 処理対象のファイル指定
    input_folder = "./test_data"
    output_base = "./output"
    input_file_name = "maniax_text_split_1"

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