import json
import re

def calculate_japanese_ratio(text):
    """
    テキスト中の日本語文字（ひらがな/カタカナ）の割合を計算する
    """
    # ひらがな/カタカナの正規表現パターン
    japanese_pattern = r'[ぁ-んァ-ン]'
    
    # 日本語文字をカウント
    japanese_chars = len(re.findall(japanese_pattern, text))
    
    # 空白と改行を除いた総文字数
    total_chars = len(''.join(text.split()))
    
    # 文字数が0の場合は0を返す
    if total_chars == 0:
        return 0
        
    return japanese_chars / total_chars

def is_japanese_text(text):
    """
    テキストが日本語として適切かどうかを判定する
    """
    # 日本語文字の割合を計算
    ratio = calculate_japanese_ratio(text)
    
    # 日本語文字が一定割合（例: 10%）以上含まれているかチェック
    return ratio >= 0.05

def filter_japanese_texts(input_file, output_file):
    """
    JSONファイルから適切な日本語テキストのみを抽出し、新しいJSONファイルに保存する
    """
    try:
        # 入力JSONファイルを読み込む
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 結果を格納するリスト
        filtered_data = []
        
        # 各テキストをチェック
        if isinstance(data, list):
            for text in data:
                if isinstance(text, str) and is_japanese_text(text):
                    filtered_data.append(text)
                        
        # 結果を新しいJSONファイルに書き込む
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=2)
            
        print(f"処理が完了しました。結果は {output_file} に保存されました。")
        print(f"抽出されたテキスト数: {len(filtered_data)}")
        
    except FileNotFoundError:
        print(f"エラー: {input_file} が見つかりません。")
    except json.JSONDecodeError:
        print(f"エラー: {input_file} は有効なJSONファイルではありません。")
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
if __name__ == "__main__":
    file_name = "./output/ai_text/ai_text"
    input_file = f'{file_name}_14.json'
    output_file = f'{file_name}_20.json'
    
    # フィルター処理を実行
    filter_japanese_texts(input_file, output_file)