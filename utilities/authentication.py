def read_api_key(file_path):
    """
    Reads the API key from a text file.

    Args:
        file_path (str): The path to the text file containing the API key.

    Returns:
        str: The API key as a string.
    """
    try:
        with open(file_path, 'r') as file:
            api_key = file.read().strip()
            return api_key
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

