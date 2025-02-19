import json
import re

def clean_many_file(input_file, output_file):
    """
    JSONファイル内の文字列データから、最初の空白、改行、
    日付、時刻、期間形式（例: ～11/8 23時59分）、カッコ内の数字や
    不要な先頭記号、特定のパターン（例: 数字＋件＋改行）を削除する。

    Args:
        input_file (str): 入力JSONファイルのパス
        output_file (str): 出力JSONファイルのパス
    """
    # JSONファイルを読み込み
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 最初にある日付、時刻、期間形式、カッコ内の数字、特定パターン（例: 数字＋件＋改行）、
    # 空白、改行を削除する関数
    def remove_leading_unnecessary_text(text):
        # 日付＋時刻＋期間形式や不要な先頭記号を削除
        text = re.sub(
            r"^\s*[-–—~～]?\s*(\d{1,4}[-年/]\d{1,2}[-月/]\d{1,2}([日]?)?|\d{1,2}/\d{1,2})?\s*(\d{1,2}[時:]\d{1,2}(分)?)?\s*[-–—~～]?\s*",
            "",
            text
        )
        # カッコ内の数字（例: (49本)）を削除
        text = re.sub(r"\s*\（[^）]*\）", "", text)
        # 特定のパターン（数字＋件＋改行）を削除
        text = re.sub(r"\d+件[\s\n]+", "", text)
        # 残りの最初の空白や改行を削除
        return re.sub(r"^[\s\n]+", "", text)

    # データの加工処理
    if isinstance(data, dict):  # JSONが辞書の場合
        for key in data:
            if isinstance(data[key], str):  # 値が文字列の場合のみ処理
                data[key] = remove_leading_unnecessary_text(data[key])
    elif isinstance(data, list):  # JSONがリストの場合
        for i in range(len(data)):
            if isinstance(data[i], str):  # 要素が文字列の場合のみ処理
                data[i] = remove_leading_unnecessary_text(data[i])

    # 処理後のデータを新しいJSONファイルに保存
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print(f"処理が完了しました。出力ファイル: {output_file}")

if __name__ == "__main__":
    file_name = "./test_data/maniax_text_split"
    input_file = f'{file_name}_25.json'
    output_file = f'{file_name}_26.json'

    clean_many_file(input_file, output_file)