import pyarrow.parquet as pq
import pandas as pd
import math
import os

def split_parquet(input_file, output_prefix, num_parts):
    # 入力ファイル名から拡張子を除いたベース名を取得
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    
    # Parquetファイルを読み込む
    table = pq.read_table(input_file)
    
    # DataFrameに変換
    df = table.to_pandas()
    
    # 行数をnum_partsで割ったサイズを求める
    chunk_size = math.ceil(len(df) / num_parts)
    
    for i in range(num_parts):
        # チャンクごとに分割
        chunk_df = df[i * chunk_size: (i + 1) * chunk_size]
        
        # 新しいParquetファイルの出力パスを作成
        output_file = os.path.join(output_prefix, f"{base_name}_{i + 1}.parquet")
        
        # 出力ディレクトリが存在しない場合は作成
        os.makedirs(output_prefix, exist_ok=True)
        
        # 新しいParquetファイルとして保存
        chunk_df.to_parquet(output_file)
        print(f"Created: {output_file}")

if __name__ == "__main__":
    input_file = './test_data/train-00000-of-00034.parquet'  # 分割したいParquetファイル
    output_prefix = 'output'  # 出力ファイルのプレフィックス
    num_parts = 100            # 分割するパート数

    split_parquet(input_file, output_prefix, num_parts)
