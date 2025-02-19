import json
import os

def get_file_size(file_path):
    return os.path.getsize(file_path)

def split_large_json_by_size(input_file, output_dir, num_parts):
    # 出力ディレクトリが存在しない場合は作成
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 入力ファイルの総サイズを取得
    total_size = get_file_size(input_file)
    target_size_per_part = total_size / num_parts

    print(f"総ファイルサイズ: {total_size:,} bytes")
    print(f"目標分割サイズ: {target_size_per_part:,.0f} bytes")

    current_part = 0
    current_chunk = []
    current_chunk_size = 0

    # `base_file`の名前を抽出
    base_name = os.path.splitext(os.path.basename(input_file))[0]

    with open(input_file, 'r', encoding='utf-8') as f:
        # 最初の '[' をスキップ
        f.readline()
        
        while True:
            line = f.readline()
            if not line or line.strip() == ']':
                break

            # カンマを除去して JSON パース
            item = json.loads(line.strip().rstrip(','))
            
            # アイテムのサイズを計算（UTF-8でのバイト数）
            item_json = json.dumps(item, ensure_ascii=False)
            item_size = len(item_json.encode('utf-8'))

            # 現在のチャンクが目標サイズを超えた場合、ファイルに書き出す
            if current_chunk_size + item_size > target_size_per_part and current_chunk:
                output_file = os.path.join(output_dir, f'{base_name}_split_{current_part + 1}.json')
                with open(output_file, 'w', encoding='utf-8') as out_f:
                    json.dump(current_chunk, out_f, ensure_ascii=False, indent=4)

                chunk_size = get_file_size(output_file)
                print(f"作成: {output_file} (サイズ: {chunk_size:,} bytes)")

                current_part += 1
                current_chunk = []
                current_chunk_size = 0

            current_chunk.append(item)
            current_chunk_size += item_size

    # 残ったアイテムを最後のファイルに出力
    if current_chunk:
        output_file = os.path.join(output_dir, f'{base_name}_split_{current_part + 1}.json')
        with open(output_file, 'w', encoding='utf-8') as out_f:
            json.dump(current_chunk, out_f, ensure_ascii=False, indent=4)

        chunk_size = get_file_size(output_file)
        print(f"作成: {output_file} (サイズ: {chunk_size:,} bytes)")

if __name__ == "__main__":
    base_file = './text_dlsite_data_clean_1/text_dlsite_2001-3000/home_text'
    input_json = f'{base_file}.json'
    output_directory = f'{base_file}'
    num_splits = 5

    split_large_json_by_size(input_json, output_directory, num_splits)
