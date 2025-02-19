# 通常実行（自動的に前回の状態から再開）
python crawler.py -i input_dir -o output_dir

# 初めから実行（前回の状態を無視）
python crawler.py -i input_dir -o output_dir --no-resume

# プロセス数を指定して実行
python crawler.py -i input_dir -o output_dir -p 2