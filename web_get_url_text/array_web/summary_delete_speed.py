import json
import os
from pathlib import Path
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from functools import partial

def process_json_file(file_path):
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
                for key, value in data.items():
                    if key.startswith('text') and isinstance(value, str):
                        texts.add(value)
            
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        for key, value in item.items():
                            if key.startswith('text') and isinstance(value, str):
                                texts.add(value)
                                
    except json.JSONDecodeError as e:
        print(f"Error reading {file_path}: {e}")
    except Exception as e:
        print(f"Unexpected error with {file_path}: {e}")
    
    return texts

def extract_text_fields(input_dir, output_file, num_processes=None):
    """
    指定されたディレクトリ内のすべてのJSONファイルからtext0, text1などの
    テキストフィールドを並列で抽出し、重複を削除して1つのJSONファイルにまとめる
    
    Args:
        input_dir (str): 入力JSONファイルのディレクトリパス
        output_file (str): 出力JSONファイルのパス
        num_processes (int, optional): 使用するプロセス数。デフォルトはCPUコア数
    """
    input_path = Path(input_dir)
    
    # JSONファイルのリストを取得
    json_files = list(input_path.glob('*.json'))
    total_files = len(json_files)
    
    if total_files == 0:
        print(f"No JSON files found in {input_dir}")
        return
    
    # プロセス数の設定
    if num_processes is None:
        num_processes = min(cpu_count(), total_files)
    
    print(f"Processing {total_files} files using {num_processes} processes...")
    
    # 並列処理の実行
    unique_texts = set()
    with Pool(num_processes) as pool:
        # tqdmで進捗バーを表示しながら処理
        results = list(tqdm(
            pool.imap(process_json_file, json_files),
            total=total_files,
            desc="Extracting texts"
        ))
        
        # 結果をマージ
        for result in results:
            unique_texts.update(result)
    
    # 結果をソートしてリストに変換
    all_texts = sorted(list(unique_texts))
    
    # 出力ファイルの保存
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_texts, f, ensure_ascii=False, indent=2)
    
    print(f"\nExtracted {len(all_texts)} unique text fields to {output_file}")
    print(f"Memory usage per text: {sum(len(text.encode('utf-8')) for text in all_texts) / (1024*1024):.2f} MB")

if __name__ == "__main__":
    input_directory = "./comic"
    output_file = "comic_text.json"
    
    # オプション: プロセス数を指定（例: CPUコア数の半分を使用）
    num_processes = max(1, cpu_count() // 2)
    extract_text_fields(input_directory, output_file, num_processes)
    
    # デフォルトのプロセス数で実行
    #extract_text_fields(input_directory, output_file)