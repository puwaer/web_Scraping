# data_clean

## Overview
This project is a collection of tools for cleaning and organizing text within JSON files. It processes JSON files in a specified folder, removes unnecessary text, organizes the data, and allows for data integration and splitting.

## File Structure

### 1. `./main_folder.py`
A script for cleaning text in specified JSON files.

### 2. `./main_file.py`
A script for cleaning text within JSON files in a specified folder.

## Processing Details

### `./json_clean_summay_1/main_file.py`
- Removes short text (less than 20 characters).
- Deletes non-Japanese text.
- Lists occurrences of texts starting with the same sentence.
- Removes text based on duplication and length.
- Normalizes consecutive newline characters to `\n * 4`.
- Removes highly similar text.

### `./json_clean_summay_2/main_file.py`
- Removes short text (less than 20 characters).
- Removes unnecessary information such as spaces, line breaks, dates, times, and numbers inside parentheses.
- Deletes only half-width and full-width spaces from the text.
- Processes text blocks:
  - Counts characters excluding spaces.
  - Checks each block until a `\n` line break.
  - Deletes blocks with fewer than 10 characters.
- Removes highly similar text.

### `./json_clean_summay_3/main_file.py`
- Removes short text (less than 20 characters).
- Normalizes consecutive newline characters to `\n * 4`.
- Deletes emojis and uncommon characters.
- Removes text with a high percentage of numbers.
- Removes highly similar text.
- Deletes only half-width and full-width spaces from the text.
- Processes text blocks:
  - Counts characters excluding spaces.
  - Checks each block until a `\n` line break.
  - Deletes blocks with fewer than 10 characters.
- Removes text containing URLs.

### `./json_clean_summay_4/main_file.py`
- Removes short text (less than 20 characters).
- Deletes Chinese text that appears Japanese-like.
- Removes text containing specific phrases.
- Processes text blocks.

### `./json_clean_summay_5/main_file.py`
- Removes short text (less than 20 characters).
- Deletes shorter sentences that start the same but continue with different content.

## Processing Steps

1. **First Processing**
   - Run `json_clean_summay_1`
   Or
   - Run `json_clean_summay_2` → Then run `json_clean_summay_1`

2. **Second Processing**
   - Run `json_clean_summay_3`

3. **Third Processing**
   - Run `json_clean_summay_4`

4. **Fourth Processing**
   - Run `json_clean_summay_5`

## Data Splitting & Integration

### `./summary_split/text_split_capacity.py`
A script to split specified JSON files.

### `./summary_split/text_summary.py`
A script to integrate multiple JSON files in a folder into a single JSON file.

## Environment
- Python 3.x

## Notes
- Back up original data before running the cleaning process.
- Modify filter conditions in each script if needed to change text deletion rules.
- Be mindful of memory usage when processing large amounts of data.

## License
This project is provided under the MIT License.

