import json


# textフィールドの末尾の改行を削除する関数
def remove_trailing_newline(json_data):
    for item in json_data:
        if "text" in item and item["text"].endswith("\n"):
            item["text"] = item["text"].rstrip("\n")
    return json_data

# ファイルの読み込みと書き込み
def process_nn_file(input_path, output_path):
    # 入力ファイルを読み込む
    with open(input_path, 'r', encoding='utf-8') as infile:
        data = json.load(infile)

    # 改行を削除
    updated_data = remove_trailing_newline(data)

    # 結果を出力ファイルに書き込む
    with open(output_path, 'w', encoding='utf-8') as outfile:
        json.dump(updated_data, outfile, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    file_name = "./input/aix_text"
    input_file = f'{file_name}_analysis_results_1.json'
    output_file = f'{file_name}_analysis_results_2.json'

    process_nn_file(input_file, output_file)
