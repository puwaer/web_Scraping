import json
import os

def split_large_json(input_file, output_dir, num_parts):
    # 出力ディレクトリが存在しない場合は作成
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # ファイル全体を確認せずに行をカウント（リスト形式を前提）
    total_items = 0
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip() in ['[', ']']:
                continue
            total_items += 1

    print(f"総アイテム数: {total_items}")

    # 各分割ファイルのアイテム数を計算
    items_per_part = total_items // num_parts
    remainder = total_items % num_parts  # 余り分を追加するための処理

    print(f"1分割あたりのアイテム数: {items_per_part}, 余り: {remainder}")

    start_index = 0
    current_part = 0
    current_count = 0
    current_chunk = []

    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            stripped_line = line.strip()
            if stripped_line in ['[', ']']:
                continue

            # アイテムを追加
            current_chunk.append(json.loads(stripped_line.rstrip(',')))

            current_count += 1
            if current_count >= items_per_part + (1 if current_part < remainder else 0):
                # 現在のチャンクをファイルに書き出す
                output_file = os.path.join(output_dir, f'split_part_{current_part + 1}.json')
                with open(output_file, 'w', encoding='utf-8') as out_f:
                    json.dump(current_chunk, out_f, ensure_ascii=False, indent=4)

                print(f"作成: {output_file} (アイテム数: {len(current_chunk)})")

                # 初期化
                current_part += 1
                current_count = 0
                current_chunk = []

    # 残ったアイテムを出力（万が一余りが残った場合）
    if current_chunk:
        output_file = os.path.join(output_dir, f'split_part_{current_part + 1}.json')
        with open(output_file, 'w', encoding='utf-8') as out_f:
            json.dump(current_chunk, out_f, ensure_ascii=False, indent=4)

        print(f"作成: {output_file} (アイテム数: {len(current_chunk)})")

if __name__ == "__main__":
    # 使用例
    base_file = './text_dlsite_etc/1-1000/comic_text'
    input_json = f'{base_file}.json'  # 分割したい元のJSONファイル
    output_directory = f'{base_file}'                              # 分割後のファイルを保存するディレクトリ
    num_splits = 10                                          # 分割数

    split_large_json(input_json, output_directory, num_splits)
