import json
import os
from pathlib import Path
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from functools import partial
import logging

class JSONTextExtractor:
    """JSONファイルからテキストフィールドを抽出するクラス"""
    
    def __init__(self, input_dir, output_file, num_processes=None):
        """
        初期化メソッド
        
        Args:
            input_dir (str): 入力JSONファイルのディレクトリパス
            output_file (str): 出力JSONファイルのパス
            num_processes (int, optional): 使用するプロセス数。デフォルトはCPUコア数
        """
        self.input_path = Path(input_dir)
        self.output_path = Path(output_file)
        self.num_processes = num_processes or min(cpu_count(), self._count_json_files())
        self.unique_texts = set()
        
        # ロガーの設定
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def _count_json_files(self):
        """JSONファイルの数を数える"""
        return len(list(self.input_path.glob('*.json')))

    def _process_json_file(self, file_path):
        """
        1つのJSONファイルからテキストフィールドを抽出する
        
        Args:
            file_path (Path): 処理するJSONファイルのパス
        
        Returns:
            set: 抽出されたテキストのセット
        """
        texts = set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                if isinstance(data, dict):
                    self._extract_from_dict(data, texts)
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            self._extract_from_dict(item, texts)
                            
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error in {file_path}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error with {file_path}: {e}")
        
        return texts

    def _extract_from_dict(self, data_dict, texts):
        """
        辞書からテキストフィールドを抽出する
        
        Args:
            data_dict (dict): 処理する辞書
            texts (set): 抽出したテキストを追加するセット
        """
        for key, value in data_dict.items():
            if key.startswith('text') and isinstance(value, str):
                texts.add(value)

    def calculate_memory_usage(self, texts):
        """
        テキストのメモリ使用量を計算する
        
        Args:
            texts (list): テキストのリスト
        
        Returns:
            float: メモリ使用量（MB）
        """
        return sum(len(text.encode('utf-8')) for text in texts) / (1024 * 1024)

    def extract_and_save(self):
        """テキストを抽出して保存する"""
        json_files = list(self.input_path.glob('*.json'))
        total_files = len(json_files)
        
        if total_files == 0:
            self.logger.warning(f"No JSON files found in {self.input_path}")
            return
        
        self.logger.info(f"Processing {total_files} files using {self.num_processes} processes...")
        
        # 並列処理の実行
        with Pool(self.num_processes) as pool:
            results = list(tqdm(
                pool.imap(self._process_json_file, json_files),
                total=total_files,
                desc="Extracting texts"
            ))
            
            # 結果をマージ
            for result in results:
                self.unique_texts.update(result)
        
        # 結果をソートしてリストに変換
        all_texts = sorted(list(self.unique_texts))
        
        # 出力ファイルの保存
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.output_path, 'w', encoding='utf-8') as f:
            json.dump(all_texts, f, ensure_ascii=False, indent=2)
        
        memory_usage = self.calculate_memory_usage(all_texts)
        self.logger.info(f"\nExtracted {len(all_texts)} unique text fields to {self.output_path}")
        self.logger.info(f"Memory usage per text: {memory_usage:.2f} MB")

"""
if __name__ == "__main__":
    # ロギングの基本設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # 使用例
    input_directory = "./example_data/document"
    output_file = "document_text.json"
    
    # オプション: プロセス数を指定（例: CPUコア数の半分を使用）
    num_processes = max(1, cpu_count() // 2)
    
    extractor = JSONTextExtractor(
        input_dir=input_directory,
        output_file=output_file,
        num_processes=num_processes
    )
    extractor.extract_and_save()
"""

if __name__ == "__main__":
    # ロギングの基本設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # メインのディレクトリ
    base_directory = "./text_example"
    
    # オプション: プロセス数を指定（例: CPUコア数の半分を使用）
    num_processes = max(1, cpu_count() // 2)
    
    # base_directory内のすべてのサブフォルダをループ処理
    for folder_name in os.listdir(base_directory):
        folder_path = os.path.join(base_directory, folder_name)
        
        # サブフォルダかどうかを確認
        if os.path.isdir(folder_path):
            # 各フォルダに対して出力ファイル名を設定
            output_file = f"{base_directory}/{folder_name}_text.json"
            
            # JSONTextExtractor のインスタンスを作成して処理を実行
            extractor = JSONTextExtractor(
                input_dir=folder_path,
                output_file=output_file,
                num_processes=num_processes
            )
            extractor.extract_and_save()