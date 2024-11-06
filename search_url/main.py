#from search_url import URLScraper
#from search_all_url import URLScraper
from search_all_url_cheack import URLScraper

def main():
    # スクレイピングの設定
    base_url = "https://www.example.com/"
    file_name = "kosen"
    delay_time = 0.05
    batch_size = 200
    max_pages = None  # None for unlimited, or set a number like 1000
    progress_interval = 10  # 進捗表示の間隔（秒）
    stall_time = 60  # URL増加が止まってから終了するまでの時間（秒）
    
    # スクレイパーの作成と実行
    scraper = URLScraper(
        base_url=base_url,
        file_name=file_name,
        delay_time=delay_time,
        batch_size=batch_size,
        max_pages=max_pages,
        progress_interval=progress_interval,
        stall_time=stall_time
    )
    scraper.run()

if __name__ == "__main__":
    main()