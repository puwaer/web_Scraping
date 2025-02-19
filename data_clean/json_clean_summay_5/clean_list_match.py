import json

# 完全一致で項目を削除する関数
def remove_matching_entries(json_data, filter_list):
    # フィルタリストのテキスト部分を抽出して、\nで分割して比較
    filter_texts = []
    for filter_item in filter_list:
        # テキストを\nで分割
        texts = filter_item['text'].split('\n')
        filter_texts.extend(texts)
    
    # データに含まれるすべてのテキストをチェックし、完全一致するものを削除
    updated_data = []
    for text in json_data:
        # テキストを\nで分割してチェック
        parts = text.split('\n')
        
        # テキスト全体がフィルタに一致する場合、削除
        if all(part in filter_texts for part in parts):  # 部分一致でなく完全一致
            continue  # 完全一致するテキストは削除
        updated_data.append(text)
    
    return updated_data

# ファイルの読み込みと書き込み
def process_match_file(input_path, output_path, filter_path):
    # 入力ファイルを読み込む
    with open(input_path, 'r', encoding='utf-8') as infile:
        data = json.load(infile)

    # フィルタリストを読み込む
    with open(filter_path, 'r', encoding='utf-8') as filterfile:
        filters = json.load(filterfile)

    # 一致する項目を削除
    updated_data = remove_matching_entries(data, filters)

    # 結果を出力ファイルに書き込む
    with open(output_path, 'w', encoding='utf-8') as outfile:
        json.dump(updated_data, outfile, ensure_ascii=False, indent=2)

# メイン処理
if __name__ == "__main__":
    file_name = "./input/aix_text"
    input_file = f'{file_name}.json'
    output_file = f'{file_name}_1.json'
    filter_file = "./input/aix_text_analysis_results_2.json"

    # 処理実行
    process_match_file(input_file, output_file, filter_file)
