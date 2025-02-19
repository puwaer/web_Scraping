import json
import re
import emoji
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import os

def calculate_number_ratio(text):
    """テキスト内の数字の比率を計算する"""
    if not isinstance(text, str):
        return 0
    
    numbers = re.findall(r'\d', text)
    total_chars = len(''.join(text.split()))
    
    if total_chars == 0:
        return 0
        
    return len(numbers) / total_chars

def clean_text(text, threshold=0.3):
    """テキストを処理する関数"""
    if not isinstance(text, str):
        return text
        
    # 絵文字を削除
    text = emoji.replace_emoji(text, '')
    
    # 行ごとに処理
    lines = text.strip().split('\n')
    result_lines = []
    current_block = []
    
    for line in lines:
        if line.strip():
            current_block.append(line)
        else:
            if current_block:
                block_text = '\n'.join(current_block)
                if calculate_number_ratio(block_text) < threshold:
                    result_lines.extend(current_block)
                current_block = []
            result_lines.append('')
    
    if current_block:
        block_text = '\n'.join(current_block)
        if calculate_number_ratio(block_text) < threshold:
            result_lines.extend(current_block)
    
    # 末尾の改行を整理
    return '\n'.join(result_lines).rstrip('\n')

class ChunkProcessor:
    def __init__(self, threshold=0.3):
        self.threshold = threshold

    def __call__(self, chunk):
        """データのチャンクを処理する関数"""
        if isinstance(chunk, list):
            return [clean_text(item, self.threshold) for item in chunk]
        elif isinstance(chunk, dict):
            return {k: clean_text(v, self.threshold) for k, v in chunk.items()}
        return clean_text(chunk, self.threshold)

def process_num_file(input_file, output_file, threshold=0.3, chunk_size=1000):
    """JSONファイルを並列処理する関数
    
    Args:
        input_file (str): 入力JSONファイルのパス
        output_file (str): 出力JSONファイルのパス
        threshold (float): 数字の比率の閾値（デフォルト: 0.3）
        chunk_size (int): 処理チャンクのサイズ（デフォルト: 1000）
    """
    try:
        # 入力ファイルの存在確認
        if not os.path.exists(input_file):
            print(f"入力ファイル '{input_file}' が見つかりません。")
            return False

        print("JSONファイルを読み込んでいます...")
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # プロセス数を設定
        num_processes = min(cpu_count(), 16)  # 最大16プロセス
        print(f"使用プロセス数: {num_processes}")
        print(f"数字の比率閾値: {threshold}")

        # データの分割処理
        if isinstance(data, list):
            chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
            total_items = len(data)
        elif isinstance(data, dict):
            items = list(data.items())
            chunks = [dict(items[i:i+chunk_size]) for i in range(0, len(items), chunk_size)]
            total_items = len(data)
        else:
            chunks = [data]
            total_items = 1

        print(f"処理対象アイテム数: {total_items}")

        # 並列処理の実行
        processor = ChunkProcessor(threshold)
        processed_data = []
        with Pool(processes=num_processes) as pool:
            # tqdmで進捗バーを表示
            with tqdm(total=total_items, desc="処理進捗") as pbar:
                for chunk_result in pool.imap(processor, chunks):
                    if isinstance(chunk_result, list):
                        processed_data.extend(chunk_result)
                        pbar.update(len(chunk_result))
                    elif isinstance(chunk_result, dict):
                        processed_data.append(chunk_result)
                        pbar.update(len(chunk_result))
                    else:
                        processed_data = chunk_result
                        pbar.update(1)

        # 出力ディレクトリの確認と作成
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

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

        print(f"処理が完了しました。結果は '{output_file}' に保存されました。")
        return True

    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        return False

if __name__ == "__main__":
    # 使用例
    file_name = "./test_data/maniax_text_split"
    input_file = f'{file_name}_20.json'
    output_file = f'{file_name}_21.json'
    
    process_num_file(input_file, output_file)