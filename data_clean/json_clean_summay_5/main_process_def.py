import os
import multiprocessing as mp
from similarity_word import remove_duplicates
from first_text_class_speed import TextAnalyzer
from first_text_nn import process_nn_file
from clean_list_match import process_match_file
from similarity_fuzz_speed import Deduplicator, process_fuzz_file

def process_text_pipeline(input_file, output_file):
    #first_text_class_speed.py
    input_file_analysis_1 = f"{input_file}.json"
    output_file_analysis_1 = f"{output_file}_analysis_results_1.json"
    analyzer = TextAnalyzer(input_file_analysis_1, output_file_analysis_1, chunk_size=1000)
    analyzer.process()

    #first_text_nn.py
    input_file_analysis_2 = f'{output_file}_analysis_results_1.json'
    output_file_analysis_2 = f'{output_file}_analysis_results_2.json'
    process_nn_file(input_file_analysis_2, output_file_analysis_2)

    #clean_list_match.py
    input_file_1 = f'{input_file}.json'
    output_file_1 = f'{output_file}_1.json'
    process_match_file(input_file_1, output_file_1, output_file_analysis_2)

    #similarity_word.py
    input_file_2 = f'{output_file}_1.json'
    output_file_2 = f'{output_file}_2.json'
    remove_duplicates(input_file_2, output_file_2)

    #similarity_fuzz_speed.py
    input_file = f'{output_file}_2.json'
    output_file = f'{output_file}_3.json'
    # プロセスプールの初期化方法を設定
    mp.set_start_method('spawn', force=True)
    deduplicator = Deduplicator(init=True)
    process_fuzz_file(input_file, output_file, deduplicator, threshold=0.7)




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