import json
from multiprocessing import Pool, cpu_count

def process_chunk(chunk):
    """データの一部に対して重複削除を実行"""
    return sorted(set(chunk), key=chunk.index)

def remove_duplicates(input_file, output_file):
    try:
        # JSONファイルを読み込み
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            # CPUコア数に基づいてデータをチャンクに分割
            num_chunks = cpu_count()
            chunk_size = len(data) // num_chunks + 1
            chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
            
            # 並列処理
            with Pool(num_chunks) as pool:
                processed_chunks = pool.map(process_chunk, chunks)
            
            # 結果を結合して重複削除
            unique_data = sorted(set(item for chunk in processed_chunks for item in chunk), key=data.index)
            
            # 結果をJSONファイルに保存
            with open(output_file, 'w', encoding='utf-8') as f_out:
                json.dump(unique_data, f_out, ensure_ascii=False, indent=2)
            
            print(f"重複を削除した結果を '{output_file}' に保存しました。")
        else:
            print("入力ファイルのフォーマットが正しくありません。リスト形式のJSONを指定してください。")
    except Exception as e:
        print(f"エラーが発生しました: {e}")


if __name__ == "__main__":
    file_name = "./input/aix_text"
    input_file = f'{file_name}_1.json'
    output_file = f'{file_name}_2.json'
    
    remove_duplicates(input_file, output_file)