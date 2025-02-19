import json
import re
import emoji
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

def clean_text(text):
    """テキストから絵文字を削除し、末尾の改行を整理する関数"""
    if not isinstance(text, str):
        return text
    # 絵文字を削除 
    text = emoji.replace_emoji(text, '')
    # 末尾の改行をまとめる
    text = text.rstrip('\n')
    return text

def process_chunk(chunk):
    """データのチャンクを処理する関数"""
    if isinstance(chunk, list):
        return [clean_text(item) for item in chunk]
    elif isinstance(chunk, dict):
        return {k: clean_text(v) for k, v in chunk.items()}
    return clean_text(chunk)

def process_emoji_file(input_file, output_file, chunk_size=1000):
    """JSONファイルを並列処理する関数"""
    try:
        print("JSONファイルを読み込んでいます...")
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # プロセス数を設定
        num_processes = min(cpu_count(), 16)  # 最大16プロセス
        print(f"使用プロセス数: {num_processes}")

        if isinstance(data, list):
            # リストをチャンクに分割
            chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
            total_items = len(data)
        elif isinstance(data, dict):
            # 辞書をチャンクに分割
            items = list(data.items())
            chunks = [dict(items[i:i+chunk_size]) for i in range(0, len(items, chunk_size))]
            total_items = len(data)
        else:
            chunks = [data]
            total_items = 1

        print(f"処理対象アイテム数: {total_items}")

        # 並列処理の実行
        processed_data = []
        with Pool(processes=num_processes) as pool:
            # tqdmで進捗バーを表示
            with tqdm(total=total_items, desc="処理進捗") as pbar:
                for chunk_result in pool.imap(process_chunk, chunks):
                    if isinstance(chunk_result, list):
                        processed_data.extend(chunk_result)
                        pbar.update(len(chunk_result))
                    elif isinstance(chunk_result, dict):
                        processed_data.append(chunk_result)
                        pbar.update(len(chunk_result))
                    else:
                        processed_data = chunk_result
                        pbar.update(1)

        # 結果の保存
        print("\n処理結果を保存中...")
        with open(output_file, 'w', encoding='utf-8') as f:
            if isinstance(data, dict):
                # 辞書の場合は結果をマージ
                merged_data = {}
                for d in processed_data:
                    merged_data.update(d)
                json.dump(merged_data, f, ensure_ascii=False, indent=2)
            else:
                json.dump(processed_data, f, ensure_ascii=False, indent=2)

        print("処理が完了しました")
        return True

    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        return False

if __name__ == "__main__":
    file_name = "./test_data/maniax_text_split"
    #input_file = f'{file_name}_1.json'
    #output_file = f'{file_name}_20.json'

    input_file = f'{file_name}_27.json'
    output_file = f'{file_name}_28.json'  
    
    process_emoji_file(input_file, output_file)