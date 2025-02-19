import json
import pandas as pd

def convert_json_to_parquet(input_json_file, output_parquet_file):
    # 入力JSONファイルを読み込む
    with open(input_json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # データ型の検証（データがリストであることを確認）
    if not isinstance(data, list):
        raise ValueError("入力JSONは配列（リスト）形式である必要があります")
    
    # 新しい形式に変換
    new_data = []
    for i, text in enumerate(data, 1):
        # textが文字列であることを確認
        if not isinstance(text, str):
            text = str(text)  # 文字列でない場合は変換
        
        new_data.append({
            "id": str(i),
            "text": text
        })
    
    # DataFrameに変換
    df = pd.DataFrame(new_data)
    
    # Parquetファイルとして保存
    df.to_parquet(output_parquet_file, index=False)
    
    print(f"変換が完了しました: {output_parquet_file}")

# 使用例
input_file = './data/text_dlsite_1-1000/ai_text.json'
output_file = './data/output.parquet'
convert_json_to_parquet(input_file, output_file)