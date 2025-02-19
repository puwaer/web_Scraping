# make_dataset

## Overview
This repository provides tools to create datasets from specific websites for building specialized Large Language Models (LLMs).

## Purpose
The goal is to retrieve data from specific URLs and create training datasets for domain-specific LLMs.

## Features

### `data_clean`
- Recursively collects URLs starting from a specified URL and extracts text data from those pages.

### `web_get_url_text`
- Filters the collected text data to create a clean dataset suitable for LLM training.

## Detailed Documentation
For more details on each feature, refer to the following:

- `data_clean` → `./data_clean/README.md`
- `web_get_url_text` → `./web_get_url_text/README.md`

## Generated Datasets
The datasets created using this program are available on Hugging Face:
- [dlsite-jp-v1](https://huggingface.co/datasets/puwaer/dlsite-jp-v1)
- [dmm-fanza-jp-v1](https://huggingface.co/datasets/puwaer/dmm-fanza-jp-v1)

## License
This project is provided under the MIT License.

