import requests


def upload_document(api_key, host_url, file_path, fodler_name, workspace_name):
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
    
    """
    curl -X 'POST' \
    'https://104.37.190.226:3001/api/v1/document/upload/WSD-Web-Domain' \
    -H 'accept: application/json' \
    -H 'Authorization: Bearer API-KEY' \
    -H 'Content-Type: multipart/form-data' \
    -F 'file=@水務署 - 水資源資料.pdf;type=application/pdf' \
    -F 'addToWorkspaces=WSD'
    """

    url = f"{host_url}/api/v1/document/upload/{fodler_name}"
    headers = {"accept": "application/json", "Authorization": f"Bearer {api_key}"}
    data = {"addToWorkspaces": workspace_name}

    try:
        with open(file_path, "rb") as f:
            files = {"file": (file_path, f, "application/pdf")}
            response = requests.post(
                url, headers=headers, data=data, files=files, verify=False
            )
        response.raise_for_status()  # Raise error for bad status codes
        return response.json()["success"]
    except FileNotFoundError:
        raise Exception(f"File not found: {file_path}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Request failed: {e}")
    except Exception as e:
        raise Exception(f"File put failed: {e}")
