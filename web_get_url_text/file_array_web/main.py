from array_web_json import WebTextCrawlerWithCookies
import os
import json
from typing import List, Dict, Set
from pathlib import Path
from tqdm import tqdm
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil
import argparse


def process_json_file(file_path):
    texts = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, dict):
                for key, value in data.items():
                    if key.startswith('text') and isinstance(value, str):
                        texts.add(value)
    except json.JSONDecodeError as e:
        print(f"Error reading {file_path}: {e}")
    return texts

def extract_text_fields(input_dir, output_file, num_processes=None):
    input_path = Path(input_dir)
    json_files = list(input_path.glob('*.json'))
    total_files = len(json_files)
    
    if total_files == 0:
        print(f"No JSON files found in {input_dir}")
        return
    
    unique_texts = set()
    with ThreadPoolExecutor(max_workers=num_processes or psutil.cpu_count(logical=True)) as pool:
        results = list(tqdm(pool.map(process_json_file, json_files), total=total_files, desc="Extracting texts"))
        for result in results:
            unique_texts.update(result)
    
    all_texts = sorted(list(unique_texts))
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_texts, f, ensure_ascii=False, indent=2)
    
    print(f"\nExtracted {len(all_texts)} unique text fields to {output_file}")


def load_json(file_path):
    """JSONファイルを読み込む関数"""
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

def process_single_json(json_path, output_base_dir):
    base_name = os.path.splitext(os.path.basename(json_path))[0]
    output_dir = os.path.join(output_base_dir, base_name)
    output_file = f"{output_dir}_text.json"
    os.makedirs(output_dir, exist_ok=True)
    array_web_url = load_json(json_path)
    filtered_urls = [url for url in array_web_url if not url.lower().endswith('.pdf')]

    cookies = [
        {'name': 'OptanonConsent', 
         'value': 'isGpcEnabled=0&datestamp=Fri+Oct+25+2024+22%3A19%3A15+GMT%2B0900+(%E6%97%A5%E6%9C%AC%E6%A8%99%E6%BA%96%E6%99%82)&version=6.23.0&isIABGlobal=false&hosts=&consentId=88957a0d-9fc8-4ddf-b6be-b107a12edb47&interactionCount=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0003%3A1%2CC0002%3A1%2CC0001%3A1&AwaitingReconsent=false',
         'domain': '.dlsite.com'},
        {'name': 'age_check_done', 'value': '1', 'domain': '.dmm.co.jp'},
    ]
 
    crawler = WebTextCrawlerWithCookies(
        urls=filtered_urls,
        cookies=cookies,
        output_dir=output_dir,
        timeout=30,
        max_workers=psutil.cpu_count(logical=True),
        max_retries=4
    )
    crawler.crawl()
    extract_text_fields(output_dir, output_file)

def process_all_json_files(input_directory: str, output_base_dir: str, num_processes: int = None):
    if not os.path.exists(input_directory):
        raise FileNotFoundError(f"入力ディレクトリが見つかりません: {input_directory}")
    
    os.makedirs(output_base_dir, exist_ok=True)
    json_files = glob.glob(os.path.join(input_directory, "*.json"))
    
    if not json_files:
        print(f"警告: {input_directory} にJSONファイルが見つかりませんでした。")
        return
    
    with ThreadPoolExecutor(max_workers=num_processes or psutil.cpu_count(logical=True)) as executor:
        futures = {executor.submit(process_single_json, json_file, output_base_dir): json_file for json_file in json_files}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files"):
            try:
                future.result()
            except Exception as e:
                print(f"Error: {str(e)}")

if __name__ == "__main__":

    
    parser = argparse.ArgumentParser(description='High Performance Data Crawler')
    parser.add_argument('--input', '-i', default="./class_kosen_url", help='Input JSON files directory path')
    parser.add_argument('--output', '-o', default="kosen_data", help='Output base directory path')
    parser.add_argument('--single', '-s', help='Process single JSON file path')
    parser.add_argument('--processes', '-p', type=int, default=psutil.cpu_count(logical=True), help='Number of processes to use')
    
    args = parser.parse_args()
    
    try:
        if args.single:
            process_single_json(args.single, args.output)
        else:
            process_all_json_files(args.input, args.output, args.processes)
    except Exception as e:
        print(f"Fatal error: {str(e)}")
