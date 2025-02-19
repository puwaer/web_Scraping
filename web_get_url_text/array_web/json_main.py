from r18_array_web_json import WebTextCrawlerWithCookies
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
    {'name': 'OptanonConsent', 'value': 'isGpcEnabled=0&datestamp=Fri+Oct+25+2024+22%3A19%3A15+GMT%2B0900+(%E6%97%A5%E6%9C%AC%E6%A8%99%E6%BA%96%E6%99%82)&version=6.23.0&isIABGlobal=false&hosts=&consentId=88957a0d-9fc8-4ddf-b6be-b107a12edb47&interactionCount=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0003%3A1%2CC0002%3A1%2CC0001%3A1&AwaitingReconsent=false', 'domain': '.dlsite.com'},
    {'name': 'age_check_done', 'value': '1', 'domain': '.dmm.co.jp'},
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