from google.cloud import secretmanager
import yaml



def get_secret(secret_id):
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
        project_id = config["project_id"]
    
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    print(f"Retrieved secret {secret_id} from Google Cloud Secret Manager.")
    return response.payload.data.decode("UTF-8")
