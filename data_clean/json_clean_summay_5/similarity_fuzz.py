import joblib
import json
from rapidfuzz import fuzz
from tqdm import tqdm

class Deduplicator:
    def __init__(self, cache_path="cache", init=False):
        self.cache_path = cache_path
        if init:
            self.seen = []
        else:
            try:
                self.seen = joblib.load(self.cache_path)
            except FileNotFoundError:
                print("No cache found, initializing new cache.")
                self.seen = []

    def is_duplicated(self, text, threshold=0.9):
        for seen_text in self.seen:
            similarity = fuzz.ratio(seen_text, text) / 100
            if similarity >= threshold:
                return True
        self.seen.append(text)
        return False

    def save_state(self):
        joblib.dump(self.seen, self.cache_path)

def process_json(input_file, output_file, deduplicator, threshold=0.9):
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        total_count = len(data)
        unique_data = []
        duplicates_count = 0

        print(f"処理開始: 合計 {total_count} 件のテキストを処理します...")
        
        for text in tqdm(data, desc="重複チェック中"):
            if not deduplicator.is_duplicated(text, threshold):
                unique_data.append(text)
            else:
                duplicates_count += 1

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(unique_data, f, ensure_ascii=False, indent=4)

        print(f"処理完了:")
        print(f"- 入力データ数: {total_count}")
        print(f"- 重複データ数: {duplicates_count}")
        print(f"- uniqueデータ数: {len(unique_data)}")
        print(f"- 保存先: {output_file}")

    except FileNotFoundError:
        print(f"入力ファイル {input_file} が見つかりませんでした。")
    except json.JSONDecodeError:
        print(f"{input_file} のJSONデコードに失敗しました。")



if __name__ == "__main__":
    # ファイル名指定
    file_name = "./input/ai_text"
    input_file = f'{file_name}_2.json'
    output_file = f'{file_name}_3.json'

    # Deduplicatorの初期化
    deduplicator = Deduplicator(init=True)

    # JSONファイル処理
    process_json(input_file, output_file, deduplicator, threshold=0.9)

    # キャッシュ状態を保存
    deduplicator.save_state()
