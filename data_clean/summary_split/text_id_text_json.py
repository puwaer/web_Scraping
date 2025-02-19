import json

def convert_json_format(input_file, output_file):
    # 入力JSONファイルを読み込む
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 新しい形式に変換
    new_data = []
    for i, text in enumerate(data, 1):
        new_data.append({
            "id": str(i),
            "text": text
        })
    
    # 出力JSONファイルに書き込む
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(new_data, f, ensure_ascii=False, indent=2)
    
    print(f"変換が完了しました: {output_file}")

if __name__ == "__main__":
    input_file = './data/text_dlsite_1-1000/ai_text.json'
    output_file = './data/output.json'
    convert_json_format(input_file, output_file)