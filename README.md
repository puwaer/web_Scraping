# web_Scraping

## 概要
このリポジトリは、特定のWebサイトからデータセットを作成し、特化型LLM（大規模言語モデル）を構築するためのツールを提供します。

## 目的
特定のURLからデータを取得し、特定分野向けのLLM学習用データセットを作成することを目的としています。

## 機能

### `data_clean`
- 指定したURLから再帰的にURLを収集し、それらのページからテキストデータを取得します。

### `web_get_url_text`
- 収集したテキストデータをフィルタリングし、LLM学習用のクリーンなデータセットを作成します。

## 詳細ドキュメント
各機能の詳細は以下をご覧ください。

- `data_clean` → `./data_clean/README.md`
- `web_get_url_text` → `./web_get_url_text/README.md`

## 作成されたデータセット
このプログラムで作成されたデータセットは、以下のHugging Faceで公開されています。
このようなデータセットを作成することが出来ます。
- [dlsite-jp-v1](https://huggingface.co/datasets/puwaer/dlsite-jp-v1)
- [dmm-fanza-jp-v1](https://huggingface.co/datasets/puwaer/dmm-fanza-jp-v1)

## ライセンス
本プロジェクトはMITライセンスの下で提供されます。




