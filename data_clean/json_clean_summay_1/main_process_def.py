import os
from clean_jp_20_class_speed import TextFilter
from similarity_word_class_speed import ParallelTextSimilarityFilter
from first_text_class_speed import TextAnalyzer
#from clean_list_class import CleaningConfig
#from clean_list_class import TextProcessor
from clean_list_nn_class import CleaningConfig_nn
from clean_list_nn_class import TextProcessor_nn
from clean_delete_nn import process_delete_nn_file
from clean_many import clean_many_file
from multiprocessing import Pool, cpu_count, Manager

def process_text_pipeline(input_file, output_file):
    #clean_jp_20_class_speed.py
    input_file_1 = f"{input_file}.json"
    output_file_1 = f"{output_file}_1.json"

    # TextFilterのインスタンスを作成して処理を実行
    text_filter = TextFilter()
    text_filter.filter_json_file(
        input_file_1, 
        output_file_1,
        chunk_size=100,  
        num_processes=int(cpu_count() * 1.5)
    )

    #clean_delete_nn.py
    input_file_20 = f"{output_file}_1.json"
    output_file_20 = f"{output_file}_20.json"
    process_delete_nn_file(input_file_20, output_file_20)
    

    #similarity_word_class_speed.py
    input_file_2 = f"{output_file}_20.json"
    output_file_2 = f"{output_file}_2.json"  
    # フィルターのインスタンス化と実行
    text_filter = ParallelTextSimilarityFilter(
        similarity_threshold=0.8,
        chunk_size=500  # チャンクサイズは必要に応じて調整可能
    )
    text_filter.process_file(input_file_2, output_file_2) 


    #first_text_class_speed.py
    input_file_analysis_1 = f"{output_file}_2.json"
    output_file_analysis_1 = f"{output_file}_analysis_results_1.json"
    
    analyzer = TextAnalyzer(input_file_analysis_1, output_file_analysis_1, chunk_size=1000)
    analyzer.process()


    #first_text_nn_class.py
    analysis_file_1 = f'{output_file}_analysis_results_1.json'
    input_file_3 = f'{output_file}_2.json'
    output_file_3 = f'{output_file}_3.json'
    
    config_1 = CleaningConfig_nn(
        #frequency_threshold=10,     # 通常の頻度閾値
        frequency_threshold=10,     # 通常の頻度閾値
        min_length=0,               # 最小長
        newline_threshold=10,       # 改行文字の閾値
        sort_by_length=True         # 長さでソート
    )

    processor = TextProcessor_nn(input_file_3, output_file_3, analysis_file_1)
    processor.process(config_1)


    #clean_many.py
    input_file_4 = f'{output_file}_3.json'
    output_file_4 = f'{output_file}_4.json'
    clean_many_file(input_file_4, output_file_4)


    #first_text_class_speed.py
    input_file_analysis_2 = f"{output_file}_4.json"
    output_file_analysis_2 = f"{output_file}_analysis_results_2.json"
    
    analyzer = TextAnalyzer(input_file_analysis_2, output_file_analysis_2, chunk_size=1000)
    analyzer.process()


    #first_text_nn_class.py
    analysis_file_2 = f'{output_file}_analysis_results_2.json'
    input_file_5 = f'{output_file}_4.json'
    output_file_5 = f'{output_file}_5.json'
    
    config_2 = CleaningConfig_nn(
        frequency_threshold=20,     # 通常の頻度閾値
        min_length=0,               # 最小長
        newline_threshold=30,       # 改行文字の閾値
        sort_by_length=True         # 長さでソート
    )

    processor = TextProcessor_nn(input_file_5, output_file_5, analysis_file_2)
    processor.process(config_2)


    #clean_many.py
    input_file_6 = f'{output_file}_5.json'
    output_file_6 = f'{output_file}_6.json'
    clean_many_file(input_file_6, output_file_6)


    #first_text_class_speed.py
    input_file_analysis_3 = f"{output_file}_6.json"
    output_file_analysis_3 = f"{output_file}_analysis_results_3.json"
    
    analyzer = TextAnalyzer(input_file_analysis_3, output_file_analysis_3, chunk_size=1000)
    analyzer.process()


    #first_text_nn_class.py
    analysis_file_3 = f'{output_file}_analysis_results_3.json'
    input_file_7 = f'{output_file}_6.json'
    output_file_7 = f'{output_file}_7.json'
    
    config_3 = CleaningConfig_nn(
        frequency_threshold=20,     # 通常の頻度閾値
        min_length=0,               # 最小長
        newline_threshold=30,       # 改行文字の閾値
        sort_by_length=True         # 長さでソート
    )

    processor = TextProcessor_nn(input_file_7, output_file_7, analysis_file_3)
    processor.process(config_3)


    #clean_many.py
    input_file_8 = f'{output_file}_7.json'
    output_file_8 = f'{output_file}_8.json'
    clean_many_file(input_file_8, output_file_8)


    #clean_jp_20_class_speed.py
    input_file_9 = f"{output_file}_8.json"
    output_file_9 = f"{output_file}_9.json"

    # TextFilterのインスタンスを作成して処理を実行
    text_filter = TextFilter()
    text_filter.filter_json_file(
        input_file_9, 
        output_file_9,
        chunk_size=100,  
        num_processes=int(cpu_count() * 1.5)
    )

    
    #similarity_word_class_speed.py
    input_file_10 = f"{output_file}_9.json"
    output_file_10 = f"{output_file}_10.json"
    text_filter = ParallelTextSimilarityFilter(
        similarity_threshold=0.8,
        chunk_size=500  # チャンクサイズは必要に応じて調整可能
    )
    text_filter.process_file(input_file_10, output_file_10)  
    


if __name__ == "__main__":
    # 処理対象のファイル指定
    input_folder = "./test_data"
    output_base = "./output"
    #input_file_name = "rental_text"
    input_file_name = "pro-touch_text"

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