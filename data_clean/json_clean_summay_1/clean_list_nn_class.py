import json
import sys
from multiprocessing import Pool, cpu_count
from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class CleaningConfig_nn:
    """テキストクリーニングの設定を保持するデータクラス"""
    frequency_threshold: int
    min_length: int
    newline_threshold: int  # 改行文字の閾値を追加
    sort_by_length: bool = True

class FrequentString:
    """頻出文字列を表すクラス"""
    def __init__(self, data: Dict[str, Any]):
        self.text = data["text"]
        self.frequency = data["frequency"]
        self.length = len(self.text)
        self.newline_count = self.text.count('\n')

    def meets_criteria(self, config: CleaningConfig_nn) -> bool:
        """文字列が指定された条件を満たすかチェック"""
        # 通常の条件チェック
        basic_criteria = (self.frequency >= config.frequency_threshold and 
                         self.length >= config.min_length)
        
        # 改行文字が指定された閾値以上の場合は、他の条件に関係なく True を返す
        newline_criteria = self.newline_count >= config.newline_threshold
        
        return basic_criteria or newline_criteria

class FrequentStringLoader:
    """頻出文字列のローディングを担当するクラス"""
    def __init__(self, analysis_file: str):
        self.analysis_file = analysis_file

    def load(self, config: CleaningConfig_nn) -> List[FrequentString]:
        """設定に基づいて頻出文字列をロード"""
        try:
            with open(self.analysis_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                strings = [FrequentString(item) for item in data]
                filtered = [s for s in strings if s.meets_criteria(config)]
                
                if config.sort_by_length:
                    filtered.sort(key=lambda x: x.length, reverse=True)
                
                return filtered
        except Exception as e:
            print(f"頻出文字列の読み込み中にエラーが発生: {e}")
            raise

class TextCleaner:
    """テキストクリーニングを実行するクラス"""
    def __init__(self, config: CleaningConfig_nn):
        self.config = config

    def clean_text(self, text: str, frequent_strings: List[FrequentString]) -> str:
        """単一のテキストをクリーニング"""
        cleaned = text
        for freq_string in frequent_strings:
            cleaned = cleaned.replace(freq_string.text, "")
        return cleaned

    def clean_texts_parallel(self, texts: List[str], frequent_strings: List[FrequentString]) -> List[str]:
        """複数のテキストを並列処理でクリーニング"""
        with Pool(cpu_count()) as pool:
            return pool.starmap(self.clean_text, 
                              [(text, frequent_strings) for text in texts])

class TextProcessor_nn:
    """テキスト処理全体を制御するクラス"""
    def __init__(self, input_file: str, output_file: str, analysis_file: str):
        self.input_file = input_file
        self.output_file = output_file
        self.string_loader = FrequentStringLoader(analysis_file)

    def load_input_texts(self) -> List[str]:
        """入力テキストの読み込み"""
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"入力ファイルの読み込み中にエラーが発生: {e}")
            raise

    def save_output_texts(self, texts: List[str]):
        """処理結果の保存"""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(texts, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"出力ファイルの保存中にエラーが発生: {e}")
            raise

    def process(self, config: CleaningConfig_nn):
        """テキスト処理のメインフロー"""
        try:
            # 頻出文字列のロード
            frequent_strings = self.string_loader.load(config)
            print(f"頻出文字列を {len(frequent_strings)} 件ロードしました")

            # 入力テキストのロード
            input_texts = self.load_input_texts()
            print(f"入力テキスト {len(input_texts)} 件を処理します")

            # テキストクリーニングの実行
            cleaner = TextCleaner(config)
            cleaned_texts = cleaner.clean_texts_parallel(input_texts, frequent_strings)
            print("テキストのクリーニングが完了しました")

            # 結果の保存
            self.save_output_texts(cleaned_texts)
            print(f"処理結果を {self.output_file} に保存しました")

        except Exception as e:
            print(f"処理中にエラーが発生: {e}")
            raise

if __name__ == "__main__":
    file_name = "./test_data/rental_text"
    #file_name = "./test_data/aix_text"
    #file_name = "./test_data/research_text"

    analysis_file = f'{file_name}_analysis_results_1.json'
    input_file = f'{file_name}_2.json'
    output_file = f'{file_name}_10.json'
    
    config = CleaningConfig_nn(
        frequency_threshold=10,    # 通常の頻度閾値
        min_length=0,             # 最小長
        newline_threshold=10,     # 改行文字の閾値
        sort_by_length=True       # 長さでソート
    )

    processor = TextProcessor_nn(input_file, output_file, analysis_file)
    processor.process(config)