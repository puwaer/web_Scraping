import json
import glob
import os
from typing import List, Dict, Set

def merge_json_files(folder_path: str) -> tuple[List[str], int]:
    """
    指定されたフォルダ内のすべてのJSONファイルを読み込み、
    URLを1つのリストにまとめ、重複を除去します。
    
    Args:
        folder_path: JSONファイルが存在するフォルダのパス
    
    Returns:
        tuple: (重複除去後のURLリスト, 重複していたURL数)
    """
    # すべてのURLを保存するセット
    all_urls: Set[str] = set()
    # 重複カウント用の一時的なリスト
    temp_urls: List[str] = []
    
    # フォルダ内のすべてのJSONファイルを検索
    json_files = glob.glob(os.path.join(folder_path, "*.json"))
    
    # 各JSONファイルを処理
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # URLを取得（JSONの構造に応じて適切に修正が必要）
                if isinstance(data, list):
                    urls = data
                elif isinstance(data, dict) and 'urls' in data:
                    urls = data['urls']
                else:
                    urls = []
                
                # 一時リストに追加（重複カウント用）
                temp_urls.extend(urls)
                # セットに追加（重複除去）
                all_urls.update(urls)
        except Exception as e:
            print(f"ファイル {json_file} の処理中にエラーが発生しました: {str(e)}")
            continue

    # 重複数を計算
    duplicates_count = len(temp_urls) - len(all_urls)
    
    try:
        # 結果を新しいJSONファイルに保存
        output_file = os.path.join(folder_path, "merged_urls.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(list(all_urls), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"結果の保存中にエラーが発生しました: {str(e)}")
        raise
    
    return list(all_urls), duplicates_count

def main():
    folder_path = "./dmm_url" # カレントディレクトリを使用。必要に応じて変更してください。
    
    try:
        merged_urls, duplicate_count = merge_json_files(folder_path)
        print(f"処理が完了しました。")
        print(f"重複していたURL数: {duplicate_count}")
        print(f"ユニークなURL数: {len(merged_urls)}")
        print(f"結果は 'merged_urls.json' に保存されました。")
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")

if __name__ == "__main__":
    main()