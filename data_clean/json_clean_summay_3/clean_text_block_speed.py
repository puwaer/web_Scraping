import json
import multiprocessing as mp
from functools import partial
import os
from tqdm import tqdm  # 進捗表示用

def process_text_block(text, block_num):
    """
    テキストブロックを処理する関数
    - 空白を除いて文字数をカウント
    - 改行「\n」までの各ブロックを確認
    - block_num文字以内のブロックを削除（ただし改行のみの場合は保持）
    """
    result_blocks = []
    blocks = text.split('\n')
    
    i = 0
    while i < len(blocks):
        current_block = blocks[i]
        cleaned_text = ''.join(current_block.split())
        
        if len(cleaned_text) > block_num:
            result_blocks.append(current_block)
            if i < len(blocks) - 1:
                result_blocks.append('')
        elif current_block.strip() == '':
            result_blocks.append(current_block)
        
        i += 1

    return '\n'.join(result_blocks)

def process_chunk(args):
    """
    データのチャンクを処理する関数
    """
    chunk, block_num = args
    processed_chunk = {}
    for key, value in chunk.items():
        if isinstance(value, str):
            processed_chunk[key] = process_text_block(value, block_num)
        else:
            processed_chunk[key] = value
    return processed_chunk

def split_dict_into_chunks(data, chunk_size):
    """
    辞書をチャンクに分割する関数
    """
    items = list(data.items())
    for i in range(0, len(items), chunk_size):
        yield dict(items[i:i + chunk_size])

def process_text_block_file(input_file, output_file, block_num):
    """
    JSONファイルを並列処理する関数
    """
    try:
        # JSONファイルを読み込む
        print("JSONファイルを読み込んでいます...")
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # プロセス数を決定
        num_processes = min(mp.cpu_count(), 16)
        print(f"使用プロセス数: {num_processes}")

        if isinstance(data, dict):
            total_items = len(data)
            print(f"処理対象アイテム数: {total_items}")
            
            # データをチャンクに分割
            chunk_size = max(1, len(data) // (num_processes * 2))
            chunks = list(split_dict_into_chunks(data, chunk_size))
            
            # プログレスバー付きで並列処理
            with mp.Pool(processes=num_processes) as pool:
                process_args = [(chunk, block_num) for chunk in chunks]
                processed_chunks = list(tqdm(
                    pool.imap(process_chunk, process_args),
                    total=len(chunks),
                    desc="処理進捗"
                ))
            
            # 結果を統合
            print("結果を統合しています...")
            result = {}
            for chunk in processed_chunks:
                result.update(chunk)

        elif isinstance(data, list):
            total_items = len(data)
            print(f"処理対象アイテム数: {total_items}")
            
            chunk_size = max(1, len(data) // (num_processes * 2))
            chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
            
            with mp.Pool(processes=num_processes) as pool:
                result = []
                with tqdm(total=len(data), desc="処理進捗") as pbar:
                    for chunk in chunks:
                        processed_chunk = []
                        for item in chunk:
                            if isinstance(item, str):
                                processed_chunk.append(process_text_block(item, block_num))
                            else:
                                processed_chunk.append(item)
                            pbar.update(1)
                        result.extend(processed_chunk)

        # 処理結果を保存
        print("処理結果を保存しています...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print("処理が正常に完了しました。")
        return True
    
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        return False

if __name__ == "__main__":
    file_name = "./test_data/maniax_text_split"
    input_file = f'{file_name}_23.json'
    output_file = f'{file_name}_24.json'

    #input_file = "./text_dlsite_etc/1-1000/maniax_text.json"
    #input_file = "./text_dlsite_etc/1-1000/maniax_text_split_part_1.json"
    block_num = 10

    process_text_block_file(input_file, output_file, block_num)