import json
import re

def process_text_2_file(file_path, output_path):
    try:
        # JSONファイルを読み込む
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # JSON内のすべての文字列を処理
        def process_text(text):
            lines = text.splitlines()
            #cleaned_lines = [line for line in lines if '://' not in line]
            #cleaned_lines = [line for line in lines if 'を削除しました' not in line]
            cleaned_lines = [line for line in lines if '追加お気に入りに追加' not in line]
            return '\n'.join(cleaned_lines)

        # データが文字列の場合のみ処理
        if isinstance(data, str):
            data = process_text(data)
        elif isinstance(data, list):
            data = [process_text(item) if isinstance(item, str) else item for item in data]
        elif isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, str):
                    data[key] = process_text(value)

        # 結果を新しいJSONファイルに保存
        with open(output_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

        print(f"Processed JSON saved to {output_path}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    file_name = "./test_data/ai_text"
    input_file = f'{file_name}_10.json'
    output_file = f'{file_name}_11.json'
    process_text_2_file(input_file, output_file)
