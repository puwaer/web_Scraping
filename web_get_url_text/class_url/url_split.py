import json
import math

def split_json_file(input_filename, num_splits):
    try:
        # JSONファイルを読み込む
        with open(input_filename, 'r', encoding='utf-8') as f:
            input_data = json.load(f)
            
        if not isinstance(input_data, list):
            print("エラー: 入力JSONはリスト形式である必要があります")
            return
            
        total_length = len(input_data)
        
        if total_length == 0:
            print("エラー: 入力JSONが空です")
            return
            
        # 各ファイルに入れる要素数を計算
        items_per_file = math.ceil(total_length / num_splits)
        
        # データを分割して保存
        for i in range(num_splits):
            start_idx = i * items_per_file
            end_idx = min((i + 1) * items_per_file, total_length)
            
            # 分割したデータ
            split_data = input_data[start_idx:end_idx]
            
            # 元のファイル名をベースに出力ファイル名を生成
            base_filename = input_filename.rsplit('.', 1)[0]
            output_filename = f'{base_filename}_split_{i + 1}.json'
            
            # ファイルに保存
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(split_data, f, ensure_ascii=False, indent=4)
            
            print(f'作成完了: {output_filename} ({len(split_data)}件のデータ)')
            
        print(f'\n合計: {total_length}件のデータを{num_splits}個のファイルに分割しました')
            
    except FileNotFoundError:
        print(f"エラー: ファイル '{input_filename}' が見つかりません")
    except json.JSONDecodeError:
        print(f"エラー: '{input_filename}' は有効なJSONファイルではありません")
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")

# 使用例：
input_filename = "monthly.json"  # 分割したいJSONファイルのパス
num_splits = 30               # 分割したい数

# 実行
split_json_file(input_filename, num_splits)