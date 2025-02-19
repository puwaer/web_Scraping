import json
import re
from multiprocessing import Pool, cpu_count, Manager
from functools import partial
import time
from tqdm import tqdm
from datetime import datetime, timedelta

class TextFilter:
    def __init__(self, min_length=20, hangul_ratio_threshold=0.0001):
        self.min_length = min_length
        self.hangul_ratio_threshold = hangul_ratio_threshold

    @staticmethod
    def is_hangul(char):
        return '\u3130' <= char <= '\u318F' or '\uAC00' <= char <= '\uD7AF'

    def get_hangul_ratio(self, text):
        hangul_count = sum(1 for char in text if self.is_hangul(char))
        return hangul_count / len(text) if len(text) > 0 else 0

    def filter_text(self, text):
        if len(text) <= self.min_length:
            return False
        
        if self.get_hangul_ratio(text) >= self.hangul_ratio_threshold:
            return False
            
        if any("\u3040" <= char <= "\u30ff" or "\u4e00" <= char <= "\u9fff" for char in text):
            if any("\u3040" <= char <= "\u309F" or "\u30A0" <= char <= "\u30FF" for char in text):
                return True
        return False

    def process_chunk(self, chunk_with_index):
        chunk, index = chunk_with_index
        filtered_texts = [text for text in chunk if self.filter_text(text)]
        return index, filtered_texts, len(filtered_texts)

    def format_time(self, seconds):
        return str(timedelta(seconds=int(seconds)))

    def filter_json_file(self, input_file, output_file, chunk_size=100, num_processes=None):
        start_time = time.time()
        
        if num_processes is None:
            num_processes = cpu_count()
        
        print(f"\nStarted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Using {num_processes} processes")
        
        # JSONファイルを読み込む
        with open(input_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        total_items = len(json_data)
        print(f"Total items to process: {total_items:,}")

        # データをチャンクに分割
        chunks = [(json_data[i:i + chunk_size], i//chunk_size) 
                 for i in range(0, len(json_data), chunk_size)]
        
        # 結果を格納する辞書（順序保持のため）
        results_dict = {}
        filtered_count = 0
        
        # プログレスバーの設定
        pbar = tqdm(total=total_items, 
                   desc="Processing", 
                   unit="items",
                   dynamic_ncols=True,
                   bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]")

        # 並列処理を実行
        with Pool(processes=num_processes) as pool:
            for index, filtered_chunk, chunk_filtered_count in pool.imap_unordered(self.process_chunk, chunks):
                results_dict[index] = filtered_chunk
                filtered_count += chunk_filtered_count
                pbar.update(chunk_size)
                
                # プログレスバーの説明を更新
                pbar.set_postfix({
                    'Filtered': f"{filtered_count:,}",
                    'Filter Rate': f"{(filtered_count/pbar.n*100):.1f}%"
                })

        pbar.close()

        # 結果を元の順序で結合
        filtered_data = []
        for i in range(len(chunks)):
            if i in results_dict:
                filtered_data.extend(results_dict[i])

        # 結果を書き出し
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=2)

        end_time = time.time()
        total_time = end_time - start_time
        
        # 最終結果の表示
        print("\nProcessing Summary:")
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total processing time: {self.format_time(total_time)}")
        print(f"Total items processed: {total_items:,}")
        print(f"Filtered items: {len(filtered_data):,} ({(len(filtered_data)/total_items)*100:.1f}%)")
        print(f"Average processing speed: {total_items/total_time:.1f} items/sec")

if __name__ == "__main__":
    file_name = "./text_dlsite_etc/1-1000/maniax_text"
    #file_name = "./test_data/aix_text"
    #file_name = "./test_data/rental_text"
    input_file = f"{file_name}.json"
    output_file = f"{file_name}_1.json"

    # TextFilterのインスタンスを作成して処理を実行
    text_filter = TextFilter()
    text_filter.filter_json_file(
        input_file, 
        output_file,
        chunk_size=100,  
        num_processes=int(cpu_count() * 1.5)
    )