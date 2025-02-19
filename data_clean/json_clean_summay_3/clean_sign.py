import json
import re

def clean_text(text):
    """テキストから特定のパターンのみを除去する
    
    Args:
        text (str): 処理対象のテキスト
    
    Returns:
        str: クリーニング後のテキスト
    """
    # 削除対象のパターンを定義
    patterns = [
        (r'(?<=[=-])[=-]{1,}(?=[=-])', ''),  # -- や == の連続（前後の文字は保持）
        (r'(?<=_)_{1,}(?=_)', ''),           # __ の連続
        #(r'(?<=[・。*])[・。*]{1,}(?=[・。*])', ''),  # ・。* の連続
        #(r'(?<=[:+:-・])[:+:-・]{1,}(?=[:+:-・])', ''),  # :+:-・ の連続
    ]
    
    # 各パターンに対して置換処理を実行
    cleaned_text = text
    for pattern, replacement in patterns:
        cleaned_text = re.sub(pattern, replacement, cleaned_text)
    
    return cleaned_text

def process_sign_file(input_file, output_file):
    """
    JSONファイルを読み込み、テキストをクリーニングして新しいJSONファイルに保存
    
    Args:
        input_file (str): 入力JSONファイルのパス
        output_file (str): 出力JSONファイルのパス
    """
    try:
        # JSONファイルを読み込み
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # データ構造に応じて再帰的に処理
        def clean_recursive(obj):
            if isinstance(obj, str):
                return clean_text(obj)
            elif isinstance(obj, list):
                return [clean_recursive(item) for item in obj]
            elif isinstance(obj, dict):
                return {key: clean_recursive(value) for key, value in obj.items()}
            else:
                return obj
        
        # データをクリーニング
        cleaned_data = clean_recursive(data)
        
        # 結果を新しいJSONファイルに保存
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, ensure_ascii=False, indent=2)
            
        print(f"処理が完了しました。結果は {output_file} に保存されました。")
            
    except json.JSONDecodeError as e:
        print(f"JSONファイルの読み込みエラー: {e}")
    except Exception as e:
        print(f"エラーが発生しました: {e}")

# 使用例
if __name__ == "__main__":
    file_name = "./test_data/maniax_text_split"
    input_file = f'{file_name}_21.json'
    output_file = f'{file_name}_22.json'
    process_sign_file(input_file, output_file)