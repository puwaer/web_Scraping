import os
import json

def merge_json_files(input_folder):
    # Determine the parent folder as the output folder
    output_folder = os.path.dirname(input_folder)

    # Get the input folder name to use as the output file name
    folder_name = os.path.basename(os.path.normpath(input_folder))
    output_file = os.path.join(output_folder, f"{folder_name}.json")

    # Initialize an empty list to store the data
    merged_data = []

    # Iterate over all JSON files in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.endswith('.json'):
            file_path = os.path.join(input_folder, file_name)
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Append data directly if it's a list, otherwise wrap in a list
                if isinstance(data, list):
                    merged_data.extend(data)  # Extend list
                else:
                    merged_data.append(data)  # Add as a single entry

    # Write the merged data to the output file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=4)

if __name__ == '__main__':
    # Example usage:
    input_folder = './data/text_dmm_1-1000/monthly_text'
    merge_json_files(input_folder)
