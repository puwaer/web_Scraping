from web_get_url_text.array_web.cookies_array_web_json import WebTextCrawlerWithCookies
from summary_delete import extract_text_fields
import json
import glob

# JSONファイルを読み込む関数
def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

# JSONファイルの内容を使って処理を行う関数
def process_json_data(json_data):
    # データの表示 (例)
    print("JSON Data:")
    for url in json_data:
        print(url)

# 任意のJSONファイルからデータを取得して処理する
file_path = "./data/course.json"                #使用するurlが入ったJSONファイルのパスを指定
array_web_url = load_json(file_path)
output_dir = "kosen_data/course"                #出力するファイルのパス

output_file = f"{output_dir}_text.json"         #まとめたデータを出力するパス


# それぞれのサイトに対応するクッキー情報をリストで設定
cookies = [
    {'name': 'example_cookie', 'value': 'example_value', 'domain': '.example.com'},
    
]

if __name__ == "__main__":
    crawler = WebTextCrawlerWithCookies(
        urls=array_web_url,
        cookies=cookies,
        output_dir= output_dir,
        delay=0.01
    )

    crawler.crawl()
    
    extract_text_fields(output_dir, output_file)