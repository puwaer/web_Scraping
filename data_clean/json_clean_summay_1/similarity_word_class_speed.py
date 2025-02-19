import json
import difflib
from typing import List, Any
import multiprocessing as mp
from itertools import combinations
import math
import time
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

class ParallelTextSimilarityFilter:
    def __init__(self, similarity_threshold: float = 0.8, chunk_size: int = 1000):
        """
        並列処理による類似テキストフィルタリングクラスの初期化
        
        Args:
            similarity_threshold (float): 類似度の閾値（0.0 〜 1.0）
            chunk_size (int): 並列処理時のチャンクサイズ
        """
        self.similarity_threshold = similarity_threshold
        self.chunk_size = chunk_size
        self.num_processes = mp.cpu_count()  # 使用可能なCPUコア数
        
    def _split_into_chunks(self, texts: List[str]) -> List[tuple[int, List[str]]]:
        """
        テキストリストを並列処理用のチャンクに分割
        
        Args:
            texts (List[str]): 入力テキストリスト
            
        Returns:
            List[tuple[int, List[str]]]: インデックス付きチャンクのリスト
        """
        n = len(texts)
        chunk_size = min(self.chunk_size, math.ceil(n / self.num_processes))
        return [(i, texts[i:i + chunk_size]) for i in range(0, n, chunk_size)]

    def _process_chunk(self, chunk_data: tuple[int, List[str]]) -> tuple[int, List[str]]:
        """
        個別のチャンクを処理する関数
        
        Args:
            chunk_data (tuple[int, List[str]]): インデックスと処理対象のテキストチャンク
            
        Returns:
            tuple[int, List[str]]: インデックスとフィルタリング済みのテキストリスト
        """
        chunk_index, chunk = chunk_data
        filtered_texts = []
        
        for text in chunk:
            if not filtered_texts:
                filtered_texts.append(text)
            else:
                last_text = filtered_texts[-1]
                similarity = self._calculate_similarity(last_text, text)
                
                if similarity > self.similarity_threshold:
                    if len(last_text) < len(text):
                        filtered_texts[-1] = text
                else:
                    filtered_texts.append(text)
                    
        return chunk_index, filtered_texts

    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """
        2つのテキスト間の類似度を計算（高速化のため文字列長による事前フィルタリングを追加）
        
        Args:
            text1 (str): 比較対象テキスト1
            text2 (str): 比較対象テキスト2
            
        Returns:
            float: 類似度（0.0 〜 1.0）
        """
        # 文字列長による事前フィルタリング
        len1, len2 = len(text1), len(text2)
        if abs(len1 - len2) / max(len1, len2) > 0.3:  # 長さの差が30%以上ある場合
            return 0.0
        
        return difflib.SequenceMatcher(None, text1, text2).ratio()

    def _merge_results(self, chunk_results: List[tuple[int, List[str]]], total_chunks: int) -> List[str]:
        """
        並列処理の結果をマージして最終的なフィルタリング
        
        Args:
            chunk_results (List[tuple[int, List[str]]]): 各チャンクの処理結果
            total_chunks (int): 全チャンク数
            
        Returns:
            List[str]: 最終的なフィルタリング結果
        """
        # チャンクインデックスでソート
        sorted_results = sorted(chunk_results, key=lambda x: x[0])
        merged = []
        for _, result in sorted_results:
            merged.extend(result)
        
        print("\nMerging results...")
        return self._process_chunk((0, merged))[1]

    def filter_similar_texts(self, texts: List[str]) -> List[str]:
        """
        並列処理で類似テキストをフィルタリング
        
        Args:
            texts (List[str]): フィルタリング対象のテキストリスト
            
        Returns:
            List[str]: フィルタリング後のテキストリスト
        """
        chunks = self._split_into_chunks(texts)
        total_chunks = len(chunks)
        chunk_results = []
        
        print(f"\nProcessing {total_chunks} chunks using {self.num_processes} CPU cores...")
        
        with ProcessPoolExecutor(max_workers=self.num_processes) as executor:
            futures = {executor.submit(self._process_chunk, chunk): i for i, chunk in enumerate(chunks)}
            
            with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
                for future in as_completed(futures):
                    chunk_results.append(future.result())
                    pbar.update(1)
        
        return self._merge_results(chunk_results, total_chunks)

    def process_file(self, input_file: str, output_file: str) -> None:
        """
        JSONファイルを読み込み、並列処理でフィルタリングを行い、結果を保存
        
        Args:
            input_file (str): 入力JSONファイルのパス
            output_file (str): 出力JSONファイルのパス
        """
        try:
            start_time = time.time()
            print(f"Processing started: {input_file}")
            
            # JSONファイルの読み込み
            print("Loading input file...")
            with open(input_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            print(f"Input data size: {len(json_data):,} texts")
            
            # 並列フィルタリング処理
            filtered_data = self.filter_similar_texts(json_data)
            
            print(f"\nFiltered data size: {len(filtered_data):,} texts")
            print(f"Reduction ratio: {(1 - len(filtered_data)/len(json_data))*100:.1f}%")
            
            # 結果の保存
            print(f"\nSaving results to {output_file}...")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(filtered_data, f, ensure_ascii=False, indent=2)
            
            end_time = time.time()
            processing_time = end_time - start_time
            print(f"\nProcessing completed in {processing_time:.2f} seconds")
            print(f"Average processing speed: {len(json_data)/processing_time:.1f} texts/second")
            
        except FileNotFoundError:
            print(f"Error: File {input_file} not found.")
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON format in {input_file}")
        except Exception as e:
            print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    # ファイルパスの設定
    file_name = "./output/ai_text"
    input_file = f"{file_name}_1.json"
    output_file = f"{file_name}_20.json"
    
    # フィルターのインスタンス化と実行
    text_filter = ParallelTextSimilarityFilter(
        similarity_threshold=0.8,
        chunk_size=1000
    )
    text_filter.process_file(input_file, output_file)