from r18_array_web_json_error import WebTextCrawlerWithCookies
from summary_delete_speed import extract_text_fields
import json
import glob
import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import time
import logging
from tqdm import tqdm
import psutil  # CPU使用率の監視用
from typing import List, Dict


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
        max_workers=32,
        max_retries=4
    )
    
    crawler.crawl()
    
    # テキストフィールドの抽出
    extract_text_fields(output_dir, output_file)
    
    print(f"{json_path} の処理が完了しました")

def process_all_json_files(input_directory: str, output_base_dir: str, num_processes: int = None):
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
    # ロギングの設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('crawler.log')
        ]
    )
    
    # CPUコア数を取得して設定
    num_cores = psutil.cpu_count(logical=True)
    logging.info(f"Using {num_cores} logical CPU cores")

    # デフォルトのパス設定
    default_input_dir = "./class_kosen_url"
    default_output_dir = "kosen_data"
    
    import argparse
    
    parser = argparse.ArgumentParser(description='High Performance Data Crawler')
    parser.add_argument('--input', '-i', default=default_input_dir,
                      help='Input JSON files directory path')
    parser.add_argument('--output', '-o', default=default_output_dir,
                      help='Output base directory path')
    parser.add_argument('--single', '-s',
                      help='Process single JSON file path')
    parser.add_argument('--processes', '-p', type=int, 
                      default=num_cores,
                      help='Number of processes to use')
    
    args = parser.parse_args()
    
    try:
        if args.single:
            logging.info(f"Single file mode: {args.single}")
            process_single_json(args.single, args.output, args.processes)
        else:
            logging.info(f"Batch processing mode: {args.input}")
            process_all_json_files(args.input, args.output, args.processes)
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise