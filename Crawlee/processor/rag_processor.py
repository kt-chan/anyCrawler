import requests


def upload_document(api_key, file_path, fodler_name, workspace_name):
    """
    Uploads a PDF document to the specified endpoint with authorization.

    Args:
        api_key (str): The API key for Bearer token authentication.
        file_path (str): Path to the PDF file to be uploaded.

    Returns:
        dict: JSON response from the server if successful.

    Raises:
        Exception: If the file is not found or the request fails.
    """
    url = f"http://localhost:3001/api/v1/document/upload"
    headers = {"accept": "application/json", "Authorization": f"Bearer {api_key}"}
    data = {"addToWorkspaces": workspace_name}

    try:
        with open(file_path, "rb") as f:
            files = {"file": (file_path, f, "application/pdf")}
            response = requests.post(url, headers=headers, data=data, files=files)
        response.raise_for_status()  # Raise error for bad status codes
        return response.json()["success"]
    except FileNotFoundError:
        raise Exception(f"File not found: {file_path}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Request failed: {e}")
    except Exception as e:
        raise Exception(f"File put failed: {e}")
