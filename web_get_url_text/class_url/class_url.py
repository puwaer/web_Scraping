import json
from urllib.parse import urlparse
from collections import defaultdict
import os

def process_urls_to_separate_files(input_json_path, output_dir):
    """
    JSONファイルからURLを読み込み、2番目のパスごとに個別のJSONファイルを作成する
    
    Args:
        input_json_path (str): 入力JSONファイルのパス
        output_dir (str): 出力ディレクトリのパス
    """
    try:
        # output_urlディレクトリが存在しない場合は作成
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"ディレクトリを作成しました: {output_dir}")

        # 入力JSONファイルを読み込む
        with open(input_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # URLのリストを取得（配列とオブジェクトの両方に対応）
        urls = data if isinstance(data, list) else list(data)
        
        # 空白と引用符を削除
        urls = [url.strip().strip('"') for url in urls if url.strip()]
        
        # パスでグループ化
        path_groups = defaultdict(list)
        
        for url in urls:
            # URLをパース
            parsed = urlparse(url)
            
            # パスを分割して要素を取得
            path_parts = [p for p in parsed.path.split('/') if p]
            
            # 2番目のパスが存在する場合、グループに追加
            if len(path_parts) >= 1:
                second_path = path_parts[0]
                path_groups[second_path].append(url)
        
        # 各グループごとに個別のファイルを作成
        for path, group_urls in path_groups.items():
            # 出力ファイル名を作成
            output_file = os.path.join(output_dir, f"{path}.json")
            
            # カスタム形式でファイルに書き出し
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('[\n')
                # ベースパスを最初に書き出し
                f.write(f'    "{main_url}{path}",\n')
                # 個別のURLを書き出し
                for i, url in enumerate(group_urls):
                    if i < len(group_urls) - 1:
                        f.write(f'    "{url}",\n')
                    else:
                        f.write(f'    "{url}"\n')  # 最後のURLにはカンマをつけない
                f.write(']\n')
            
            print(f"ファイルを作成しました: {output_file}")
        
        print(f"\n処理が完了しました。ファイルは {output_dir} に保存されました。")
        return True
        
    except FileNotFoundError:
        print(f"エラー: 入力ファイル {input_json_path} が見つかりません。")
        return False
    except json.JSONDecodeError:
        print(f"エラー: {input_json_path} の形式が正しくありません。")
        return False
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        return False

# 使用例
main_url = "https://www.numazu-ct.ac.jp/"
input_file = "kosen_url/kosen_merged.json"  # 入力JSONファイルのパス
output_directory = "class_kosen_url"  # 出力ディレクトリ

# ファイルを処理
success = process_urls_to_separate_files(input_file, output_directory)

if success:
    print("\n各ファイルの内容:")
    for filename in os.listdir(output_directory):
        if filename.endswith('.json'):
            print(f"\n{filename}:")
            with open(os.path.join(output_directory, filename), 'r', encoding='utf-8') as f:
                print(f.read())

