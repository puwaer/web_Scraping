import json
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import os

def remove_spaces(text):
    """
    文字列から半角・全角スペースのみを消去する関数
    改行（\n）は保持する
    """
    lines = text.split('\n')
    processed_lines = [line.replace(' ', '').replace('　', '') for line in lines]
    return '\n'.join(processed_lines)

def process_item(item):
    """
    JSON内の単一のアイテムを処理する関数
    """
    if isinstance(item, str):
        return remove_spaces(item)
    elif isinstance(item, dict):
        return {key: remove_spaces(value) if isinstance(value, str) else value for key, value in item.items()}
    return item

def process_space_file(input_file, output_file):
    """
    JSONファイル全体を並列処理し、結果を保存する関数
    """
    try:
        # JSONファイルを読み込む
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # データがリストまたは辞書かを確認
        if isinstance(data, list):
            iterable = data
        elif isinstance(data, dict):
            iterable = list(data.items())
        else:
            raise ValueError("JSONの形式がサポートされていません（リストまたは辞書が必要です）")

        # 並列処理を設定
        num_workers = min(cpu_count(), 4)  # 最大CPUコア数に制限
        with Pool(processes=num_workers) as pool:
            with tqdm(total=len(iterable), desc="Processing") as pbar:
                if isinstance(data, list):
                    results = []
                    for result in pool.imap_unordered(process_item, iterable):
                        results.append(result)
                        pbar.update(1)
                    processed_data = results
                elif isinstance(data, dict):
                    results = []
                    for result in pool.imap_unordered(process_item, [value for key, value in iterable]):
                        results.append(result)
                        pbar.update(1)
                    processed_data = {key: value for (key, _), value in zip(iterable, results)}

        # 処理結果を新しいJSONファイルに保存
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=2)

        return True
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        return False

# 使用例
if __name__ == "__main__":
    file_name = "./text_dlsite_etc/1-1000/text_test"
    input_file = f'{file_name}_1.json'
    output_file = f'{file_name}_2.json'
    process_space_file(input_file, output_file)
