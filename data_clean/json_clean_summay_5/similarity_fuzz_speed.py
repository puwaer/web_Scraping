import joblib
import json
from rapidfuzz import fuzz
from tqdm import tqdm
import multiprocessing as mp
from itertools import combinations
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed

class Deduplicator:
    def __init__(self, cache_path="cache", init=False):
        self.cache_path = cache_path
        if init:
            self.seen = []
        else:
            try:
                self.seen = joblib.load(self.cache_path)
            except FileNotFoundError:
                print("No cache found, initializing new cache.")
                self.seen = []

    def is_duplicated(self, text, threshold=0.9):
        for seen_text in self.seen:
            similarity = fuzz.ratio(seen_text, text) / 100
            if similarity >= threshold:
                return True
        self.seen.append(text)
        return False

    def save_state(self):
        joblib.dump(self.seen, self.cache_path)

def process_chunk(args):
    chunk, seen_texts, threshold = args
    results = []
    local_seen = seen_texts.copy()
    
    for text in chunk:
        is_duplicate = False
        for seen_text in local_seen:
            similarity = fuzz.ratio(seen_text, text) / 100
            if similarity >= threshold:
                is_duplicate = True
                break
        if not is_duplicate:
            results.append(text)
            local_seen.append(text)
    
    return results, local_seen

def process_fuzz_file(input_file, output_file, deduplicator, threshold=0.9):
    try:
        # データの読み込み
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        total_count = len(data)
        print(f"処理開始: 合計 {total_count} 件のテキストを処理します...")

        # CPU数とチャンクサイズの最適化
        num_cores = mp.cpu_count()
        optimal_chunk_count = num_cores * 4  # CPUコア数の4倍のチャンク数
        chunk_size = max(1, len(data) // optimal_chunk_count)
        
        # データを小さいチャンクに分割
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        
        unique_data = []
        seen_texts = deduplicator.seen.copy()
        futures = []
        
        # ProcessPoolExecutorを使用して並列処理
        with ProcessPoolExecutor(max_workers=num_cores) as executor:
            # すべてのチャンクをサブミット
            futures = [
                executor.submit(process_chunk, (chunk, seen_texts, threshold))
                for chunk in chunks
            ]
            
            # 進捗バーの設定
            with tqdm(total=len(chunks), desc="重複チェック中") as pbar:
                # 完了したタスクから順次結果を処理
                for future in as_completed(futures):
                    try:
                        chunk_result, new_seen = future.result()
                        unique_data.extend(chunk_result)
                        seen_texts = new_seen
                        pbar.update()
                    except Exception as e:
                        print(f"チャンク処理中にエラーが発生: {str(e)}")

        # 結果を保存
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(unique_data, f, ensure_ascii=False, indent=4)
        
        # seen_textsを更新
        deduplicator.seen = seen_texts
        
        # 結果の表示
        duplicates_count = total_count - len(unique_data)
        print(f"処理完了:")
        print(f"- 入力データ数: {total_count}")
        print(f"- 重複データ数: {duplicates_count}")
        print(f"- uniqueデータ数: {len(unique_data)}")
        print(f"- 保存先: {output_file}")
        
    except FileNotFoundError:
        print(f"入力ファイル {input_file} が見つかりませんでした。")
    except json.JSONDecodeError:
        print(f"{input_file} のJSONデコードに失敗しました。")


if __name__ == "__main__":
    # ファイル名指定
    file_name = "./input/ai_text"
    input_file = f'{file_name}_2.json'
    output_file = f'{file_name}_4.json'
    
    # プロセスプールの初期化方法を設定
    mp.set_start_method('spawn', force=True)
    
    # Deduplicatorの初期化
    deduplicator = Deduplicator(init=True)
    
    # JSONファイル処理
    process_fuzz_file(input_file, output_file, deduplicator, threshold=0.7)
    
    # キャッシュ状態を保存
    #deduplicator.save_state()