import json

from google.cloud import secretmanager

"""
    Template do retorno esperado!
{
  "host": "host",
  "port": 3306,
  "user": "user",
  "password": "password",
  "database": "database"
}

"""


def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> dict:
    """
    Acessa um segredo no Google Cloud Secret Manager e o retorna como um dicionário.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8")
    return json.loads(payload)
