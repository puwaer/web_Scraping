from r18_array_web_json import WebTextCrawlerWithCookies

# URLと対応するクッキー情報
array_web_url = [
    "https://www.dlsite.com/maniax/",
    "https://www.dlsite.com/maniax/work/=/product_id/RJ01252312.html"
    "https://www.dmm.co.jp/top/", 
    "https://www.dmm.co.jp/digital/videoa/-/detail/=/cid=ipvr00285/", 
]

# それぞれのサイトに対応するクッキー情報をリストで設定
cookies = [
    {'name': 'OptanonConsent', 'value': 'isGpcEnabled=0&datestamp=Fri+Oct+25+2024+22%3A19%3A15+GMT%2B0900+(%E6%97%A5%E6%9C%AC%E6%A8%99%E6%BA%96%E6%99%82)&version=6.23.0&isIABGlobal=false&hosts=&consentId=88957a0d-9fc8-4ddf-b6be-b107a12edb47&interactionCount=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0003%3A1%2CC0002%3A1%2CC0001%3A1&AwaitingReconsent=false', 'domain': '.dlsite.com'},
    {'name': 'age_check_done', 'value': '1', 'domain': '.dmm.co.jp'},
]

if __name__ == "__main__":
    crawler = WebTextCrawlerWithCookies(
        urls=array_web_url,
        cookies=cookies,
        output_dir="crawled_data",
        delay=0.5
    )

    crawler.crawl()