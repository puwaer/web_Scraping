# web_get_url_text

## Overview
This project is a set of tools that recursively collect links starting from a specified URL and save the webpage content in JSON files.

## File Structure

### 1. `./search_url/main.py`
A script that recursively collects links starting with the specified URL.

### 2. `./array_web/file_json_main.py`
A script that scrapes text from collected URLs and compiles it into a JSON file.

### 3. `./class_url/class_url.py`
A script that reads URLs and creates individual JSON files based on the second path segment.

### 4. `./class_url/url_check.py`
A script that reads all JSON files in the specified folder, compiles URLs into a single list, and removes duplicates.

### 5. `./class_url/url_split.py`
A script that splits the collected URLs stored in JSON files.

## Usage
1. Run `./search_url/main.py` to collect links from the target URL.
2. Run `./array_web/file_json_main.py` to scrape text data from the collected URLs.
3. Run `./class_url/class_url.py` to organize the collected data.
4. Run `./class_url/url_check.py` to create a deduplicated URL list.
5. Run `./class_url/url_split.py` to split a large number of URLs into manageable parts.

## Environment
- Python 3.x

## Notes
- When scraping, check the `robots.txt` of the target site and follow its terms of use.
- Sending a large number of requests in a short time may result in IP blocking. Set appropriate delays.

## License
This project is provided under the MIT license.

