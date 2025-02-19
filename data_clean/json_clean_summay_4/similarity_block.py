import json

def process_similarity_block_file(input_file_path, output_file_path):
    """
    JSONファイルから重複する短い文章を削除する関数
    
    Args:
        input_file_path (str): 入力JSONファイルのパス
        output_file_path (str): 出力JSONファイルのパス
    """
    try:
        # JSONファイルを読み込む
        with open(input_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # データ構造をデバッグ表示
        #print("読み込んだデータの構造:", type(data))
        #print("データの内容:", data)
        
        def remove_duplicate_short_text(text):
            lines = text.split('\n')
            seen = set()
            result = []
            
            for line in lines:
                if line in seen:
                    continue
                seen.add(line)
                result.append(line)
            
            return '\n'.join(result)

        # データがリストの場合の処理
        if isinstance(data, list):
            processed_data = []
            for item in data:
                # itemが文字列の場合
                if isinstance(item, str):
                    processed_text = remove_duplicate_short_text(item)
                    processed_data.append(processed_text)
                # itemが辞書の場合
                elif isinstance(item, dict):
                    processed_item = {}
                    for key, value in item.items():
                        processed_key = remove_duplicate_short_text(key)
                        processed_item[processed_key] = value
                    processed_data.append(processed_item)
        
        # データが辞書の場合の処理
        elif isinstance(data, dict):
            processed_data = {}
            for key, value in data.items():
                processed_key = remove_duplicate_short_text(key)
                processed_data[processed_key] = value
        
        # 処理結果を新しいJSONファイルに書き込む
        with open(output_file_path, 'w', encoding='utf-8') as file:
            json.dump(processed_data, file, ensure_ascii=False, indent=2)
            
        print(f"処理が完了しました。結果は {output_file_path} に保存されました。")
            
    except FileNotFoundError:
        print("指定されたファイルが見つかりません。")
    except json.JSONDecodeError:
        print("JSONファイルの形式が正しくありません。")
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        # スタックトレースを表示
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    file_name = "./test_data/ai_text"
    input_file = f'{file_name}_12.json'
    output_file = f'{file_name}_13.json'
    
    process_similarity_block_file(input_file, output_file)