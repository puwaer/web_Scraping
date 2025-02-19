from r18_array_web_json_error import WebTextCrawlerWithCookies
from summary_delete_speed import extract_text_fields
import json
import glob
import os

def load_json(file_path):
    """JSONファイルを読み込む関数"""
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

def filter_pdf_urls(urls):
    """PDFのURLを除外する関数"""
    return [url for url in urls if not url.lower().endswith('.pdf')]

def process_single_json(json_path, output_base_dir):
    """単一のJSONファイルを処理する関数"""
    # ファイル名から拡張子を除いてサブフォルダ名として使用
    base_name = os.path.splitext(os.path.basename(json_path))[0]
    
    # 出力ディレクトリのパスを作成
    output_dir = os.path.join(output_base_dir, base_name)
    output_file = f"{output_dir}_text.json"
    
    # 出力ディレクトリが存在しない場合は作成
    os.makedirs(output_dir, exist_ok=True)
    
    # JSONからURLを読み込み
    array_web_url = load_json(json_path)
    
    # PDFのURLを除外
    filtered_urls = filter_pdf_urls(array_web_url)
    
    # 除外されたURL数を表示
    excluded_count = len(array_web_url) - len(filtered_urls)
    if excluded_count > 0:
        print(f"PDFファイルとして {excluded_count} 件のURLを除外しました")
    
    # クッキー設定
    cookies = [
        {'name': 'OptanonConsent', 
         'value': 'isGpcEnabled=0&datestamp=Fri+Oct+25+2024+22%3A19%3A15+GMT%2B0900+(%E6%97%A5%E6%9C%AC%E6%A8%99%E6%BA%96%E6%99%82)&version=6.23.0&isIABGlobal=false&hosts=&consentId=88957a0d-9fc8-4ddf-b6be-b107a12edb47&interactionCount=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0003%3A1%2CC0002%3A1%2CC0001%3A1&AwaitingReconsent=false',
         'domain': '.dlsite.com'},
        {'name': 'age_check_done', 'value': '1', 'domain': '.dmm.co.jp'},
    ]
    
    # クローラーの初期化と実行
    crawler = WebTextCrawlerWithCookies(
        urls=filtered_urls,  # フィルタリング済みのURLリストを使用
        cookies=cookies,
        output_dir=output_dir,
        delay=0,
        timeout=30,
        max_workers=8,
        max_retries=8
    )
    
    crawler.crawl()
    
    # テキストフィールドの抽出
    extract_text_fields(output_dir, output_file)
    
    print(f"{json_path} の処理が完了しました")

def process_all_json_files(input_directory, output_base_dir):
    """指定ディレクトリ内のすべてのJSONファイルを処理する関数"""
    # 入力ディレクトリが存在しない場合はエラー
    if not os.path.exists(input_directory):
        raise FileNotFoundError(f"入力ディレクトリが見つかりません: {input_directory}")
    
    # 出力ベースディレクトリが存在しない場合は作成
    os.makedirs(output_base_dir, exist_ok=True)
    
    # ディレクトリ内のすべてのJSONファイルを取得
    json_files = glob.glob(os.path.join(input_directory, "*.json"))
    
    if not json_files:
        print(f"警告: {input_directory} にJSONファイルが見つかりませんでした。")
        return
    
    # 各JSONファイルを処理
    for json_file in json_files:
        print(f"{json_file} を処理中...")
        try:
            process_single_json(json_file, output_base_dir)
        except Exception as e:
            print(f"エラー: {json_file} の処理中に問題が発生しました: {str(e)}")

if __name__ == "__main__":
    # デフォルトのパス設定
    default_input_dir = "./class_kosen_url"
    default_output_dir = "./text_dlsite_data/"
    
    # コマンドライン引数を使用する場合のインポート
    import argparse
    
    # コマンドライン引数の設定
    parser = argparse.ArgumentParser(description='データクローラー')
    parser.add_argument('--input', '-i', default=default_input_dir,
                      help='入力JSONファイルのディレクトリパス')
    parser.add_argument('--output', '-o', default=default_output_dir,
                      help='出力ディレクトリのベースパス')
    parser.add_argument('--single', '-s',
                      help='単一のJSONファイルのみを処理する場合のファイルパス')
    
    args = parser.parse_args()
    
    # 実行モードの選択
    if args.single:
        # 単一ファイルの処理
        print(f"単一ファイルモード: {args.single}")
        process_single_json(args.single, args.output)
    else:
        # ディレクトリ内のすべてのファイルを処理
        print(f"一括処理モード: {args.input}")
        process_all_json_files(args.input, args.output)