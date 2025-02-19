import json
import re

def normalize_newlines(text):
    """
    連続する改行文字(\n)が10個以上続く場合、6個に置き換える
    
    Args:
        text (str): 処理対象のテキスト
    Returns:
        str: 処理後のテキスト
    """
    # 6個以上連続する改行を4個に置換
    return re.sub(r'\n{6,}', '\n' * 4, text)

def process_delete_nn_file(input_file, output_file):
    """
    JSONファイルを読み込み、テキスト内の改行を正規化して保存する
    
    Args:
        input_file (str): 入力JSONファイルのパス
        output_file (str): 出力JSONファイルのパス
    """
    try:
        # JSONファイルを読み込む
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # 文字列型のデータを再帰的に処理する関数
        def process_data(item):
            if isinstance(item, str):
                return normalize_newlines(item)
            elif isinstance(item, dict):
                return {k: process_data(v) for k, v in item.items()}
            elif isinstance(item, list):
                return [process_data(i) for i in item]
            else:
                return item
        
        # データを処理
        processed_data = process_data(data)
        
        # 処理したデータを保存
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=2)
            
        print(f"処理が完了しました。出力ファイル: {output_file}")
        
    except json.JSONDecodeError as e:
        print(f"JSONの解析に失敗しました: {e}")
    except Exception as e:
        print(f"エラーが発生しました: {e}")

# 使用例
if __name__ == "__main__":
    file_name = "./test_data/comic_text_split_19_2"

    input_file = f"{file_name}.json"  # 入力JSONファイルのパス
    output_file = f"{file_name}_1.json"

    process_delete_nn_file(input_file, output_file)