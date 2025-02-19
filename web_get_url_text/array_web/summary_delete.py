import json
import os
from pathlib import Path

def extract_text_fields(input_dir, output_file):
    """
    指定されたディレクトリ内のすべてのJSONファイルからtext0, text1などの
    テキストフィールドを抽出し、重複を削除して1つのJSONファイルにまとめる
    
    Args:
        input_dir (str): 入力JSONファイルのディレクトリパス
        output_file (str): 出力JSONファイルのパス
    """
    # 結果を格納するセット（重複を自動的に排除）
    unique_texts = set()
    
    # 入力ディレクトリのパスを作成
    input_path = Path(input_dir)
    
    # すべてのJSONファイルを処理
    for json_file in input_path.glob('*.json'):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # データが辞書の場合
                if isinstance(data, dict):
                    # text0, text1などのキーを探す
                    for key in data.keys():
                        if key.startswith('text'):
                            unique_texts.add(data[key])
                
                # データがリストの場合
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            for key in item.keys():
                                if key.startswith('text'):
                                    unique_texts.add(item[key])
                            
        except json.JSONDecodeError as e:
            print(f"Error reading {json_file}: {e}")
        except Exception as e:
            print(f"Unexpected error with {json_file}: {e}")
    
    # セットをリストに変換（ソートして順序を一定に）
    all_texts = sorted(list(unique_texts))
    
    # 結果を出力ファイルに書き込む
    output_path = Path(output_file)
    
    # 出力ディレクトリが存在しない場合は作成
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_texts, f, ensure_ascii=False, indent=2)
    
    print(f"Extracted {len(all_texts)} unique text fields to {output_file}")

# スクリプトを実行
if __name__ == "__main__":
    input_directory = "kosen_data/course/"
    output_file = "kosen_data/text_course.json"
    
    extract_text_fields(input_directory, output_file)