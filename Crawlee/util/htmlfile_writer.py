import os
import json


def _save_file(file_path, data):
    chunk_size = 8192  # 8KB chunks
    file_path = file_path.replace("json", "html")
    with open(file_path, "wb") as f:
        print(f"Writing html file to {file_path} ...")
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size].encode(
                "utf-8"
            )  # Convert string chunk to bytes
            f.write(chunk)


def _process_json_file(file_path, target_tag):
    """Process a single JSON file."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            if target_tag in data:
                html = data[target_tag]
                if len(html) > 0:
                    _save_file(file_path, html)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {file_path}: {e}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")


def read_json_files_with_buffer(root_dir, target_tag):
    """Recursively read all JSON files in the given directory."""
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                _process_json_file(file_path, target_tag)


# Example usage
if __name__ == "__main__":
    directory_path = input("Enter the directory path to scan for JSON files: ")
    target_tag = "html"
    read_json_files_with_buffer(directory_path, target_tag)
