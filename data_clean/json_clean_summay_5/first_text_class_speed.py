import json
from collections import defaultdict
import multiprocessing as mp
from functools import partial
import tqdm
import os
import math

class TextAnalyzer:
    def __init__(self, input_file, output_file, chunk_size=1000):
        self.input_file = input_file
        self.output_file = output_file
        self.chunk_size = chunk_size
        self.output_dir = f"{os.path.splitext(input_file)[0]}_splits"

    def process(self):
        """メイン処理を実行"""
        try:
            split_files = self._split_json_file()
            if not split_files:
                return
            
            self._analyze_files(split_files)
            self._cleanup_temp_files()
            
        except Exception as e:
            print(f"処理中にエラーが発生しました: {str(e)}")

    def _split_json_file(self):
        """JSONファイルを分割"""
        os.makedirs(self.output_dir, exist_ok=True)
        self._clear_existing_splits()
        
        try:
            data = self._load_json_file()
            return self._create_splits(data)
        except Exception as e:
            print(f"ファイル分割中にエラーが発生しました: {str(e)}")
            return []

    def _clear_existing_splits(self):
        """既存の分割ファイルを削除"""
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))

    def _load_json_file(self):
        """JSONファイルを読み込み、検証"""
        with open(self.input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if not isinstance(data, list):
            raise ValueError("JSONファイルはリスト形式である必要があります")
        if len(data) == 0:
            raise ValueError("JSONファイルが空です")
            
        return data

    def _create_splits(self, data):
        """データを分割してファイルに保存"""
        total_items = len(data)
        num_chunks = math.ceil(total_items / self.chunk_size)
        split_files = []
        
        print(f"ファイルを{num_chunks}個のチャンクに分割します...")
        
        for i in range(num_chunks):
            start_idx = i * self.chunk_size
            end_idx = min((i + 1) * self.chunk_size, total_items)
            chunk_data = data[start_idx:end_idx]
            
            output_file = os.path.join(self.output_dir, f"chunk_{i+1}.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(chunk_data, f, ensure_ascii=False, indent=2)
            
            split_files.append(output_file)
            print(f"チャンク {i+1}/{num_chunks} を保存しました: {output_file}")
        
        return split_files

    def _cleanup_temp_files(self):
        """一時ファイルを削除"""
        print("\n一時ファイルの削除...")
        if os.path.exists(self.output_dir):
            for f in os.listdir(self.output_dir):
                os.remove(os.path.join(self.output_dir, f))
            os.rmdir(self.output_dir)
            print("一時ファイルを削除しました。")

    def _analyze_files(self, split_files):
        """分割ファイルを分析"""
        analyzer = TextPrefixAnalyzer()
        results = analyzer.analyze_files(split_files)
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\n分析結果を {self.output_file} に保存しました。")


class TextPrefixAnalyzer:
    def analyze_files(self, split_files):
        """複数ファイルの分析を実行"""
        all_results = []
        for file_path in split_files:
            results = self._analyze_single_file(file_path)
            all_results.append(results)
        
        prefix_frequencies = self._merge_results(all_results)
        return self._format_results(prefix_frequencies)

    def _analyze_single_file(self, file_path):
        """単一ファイルの分析"""
        try:
            texts = self._load_texts(file_path)
            texts_set = set(texts)
            
            # チャンクサイズを最適化
            num_cores = mp.cpu_count()
            chunk_size = max(1, len(texts) // (num_cores * 4))  # チャンクサイズを小さくして並列処理を効率化
            chunks = self._prepare_chunks(texts, texts_set, chunk_size)
            
            return self._process_chunks(chunks, file_path)
            
        except Exception as e:
            print(f"チャンク {file_path} の処理中にエラーが発生しました: {str(e)}")
            return []

    def _load_texts(self, file_path):
        """テキストデータの読み込み"""
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _prepare_chunks(self, texts, texts_set, chunk_size):
        """処理チャンクの準備"""
        return [(texts, i, min(i + chunk_size, len(texts)), texts_set) 
                for i in range(0, len(texts), chunk_size)]

    def _process_chunks(self, chunks, file_path):
        """チャンクの並列処理"""
        with mp.Pool(mp.cpu_count()) as pool:
            return list(tqdm.tqdm(
                pool.imap(self._process_text_chunk, chunks),
                total=len(chunks),
                desc=f"Analyzing {os.path.basename(file_path)}"
            ))

    @staticmethod
    def _process_text_chunk(chunk_data):
        """チャンクごとのプレフィックス処理"""
        texts, start_idx, end_idx, all_texts_set = chunk_data
        prefix_counts = {}
        
        for text in texts[start_idx:end_idx]:
            parts = text.split('\n')
            current_prefix = ""
            
            for part in parts:
                current_prefix = (current_prefix + '\n' + part).lstrip('\n')
                count = sum(1 for t in all_texts_set if t.startswith(current_prefix))
                if count > 1:
                    complete_prefix = TextPrefixAnalyzer._find_complete_prefix(current_prefix, all_texts_set)
                    if complete_prefix not in prefix_counts or count > prefix_counts[complete_prefix]:
                        prefix_counts[complete_prefix] = count
        
        return prefix_counts

    @staticmethod
    def _find_complete_prefix(text, texts_set):
        """完全なプレフィックスを検索"""
        for i in range(1, 4):
            test_prefix = text + '\n' * i
            if not any(t.startswith(test_prefix) for t in texts_set):
                return text + '\n' * (i - 1)
        return text + '\n' * 3

    def _merge_results(self, all_results):
        """結果のマージと冗長なプレフィックスの除去を最適化"""
        # プレフィックスを一度のイテレーションでマージ
        prefix_dict = {}
        for results in all_results:
            for result in results:
                for prefix, count in result.items():
                    if prefix not in prefix_dict or count > prefix_dict[prefix]:
                        prefix_dict[prefix] = count

        # プレフィックスを頻度でソート
        sorted_items = sorted(prefix_dict.items(), key=lambda x: (-x[1], -len(x[0])))
        
        # 効率的な冗長性チェック
        result = []
        seen_prefixes = set()
        
        for prefix, count in sorted_items:
            # 既存の高頻度プレフィックスのサブセットでないかチェック
            is_redundant = False
            for existing_prefix in seen_prefixes:
                if existing_prefix.startswith(prefix):
                    is_redundant = True
                    break
            
            if not is_redundant:
                result.append((prefix, count))
                seen_prefixes.add(prefix)

        return result

    def _format_results(self, filtered_prefixes):
        """結果の整形"""
        return [{
            "frequency": count,
            "text": prefix,
            "length": len(prefix)
        } for prefix, count in filtered_prefixes]


if __name__ == "__main__":
    file_name = "./input/aix_text"
    input_file = f"{file_name}.json"
    output_file = f"{file_name}_analysis_results_1.json"
    
    analyzer = TextAnalyzer(input_file, output_file, chunk_size=1000)
    analyzer.process()