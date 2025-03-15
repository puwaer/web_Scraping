# web_get_url_text

## 概要
本プロジェクトは、指定したURLを起点として再帰的にリンクを収集し、ウェブページの内容をJSONファイルに保存するツール群です。

## ファイル構成

### 1. `./search_url/main.py`
指定したURLで始まるリンクを再帰的に収集するスクリプト。

### 2. `./array_web/file_json_main.py`
収集したURLからウェブページの文章をスクレイピングし、JSONファイルにまとめるスクリプト。

### 3. `./class_url/class_url.py`
URLを読み込み、2番目のパスごとに個別のJSONファイルを作成するスクリプト。

### 4. `./class_url/url_check.py`
指定されたフォルダ内のすべてのJSONファイルを読み込み、URLを1つのリストにまとめ、重複を除去するスクリプト。

### 5. `./class_url/url_split.py`
JSONファイルに保存されたURLリストを分割するスクリプト。

## 使い方
1. `./search_url/main.py` を実行して、対象URLのリンクを収集する。
2. `./class_url/class_url.py` を実行して、収集したデータを整理する。
3. `./class_url/url_check.py` を実行して、重複を除去したURLリストを作成する。
4. `./class_url/url_split.py` を実行して、大量のURLを分割して扱いやすくする。
5. `./array_web/file_json_main.py` を実行して、収集したURLからテキストデータを取得する。

## 環境
- Python 3.x

## 注意点
- スクレイピングを行う際は、対象サイトの `robots.txt` を確認し、適切な利用規約を守ってください。
- 大量のリクエストを短時間で送ると、IPがブロックされる可能性があります。適切な遅延を設定してください。

## ライセンス
本プロジェクトはMITライセンスの下で提供されます。

