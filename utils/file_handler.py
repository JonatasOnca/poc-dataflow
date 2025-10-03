import json
import yaml

from apache_beam.io.filesystems import FileSystems

def load_yaml(bucket_yaml: str) -> str:
    """Lê um arquivo .yaml do GCS."""
    with FileSystems.open(bucket_yaml) as f:
        return yaml.safe_load(f)
    
def load_query(bucket_query: str) -> str:
    """Lê um arquivo .sql do GCS."""
    with FileSystems.open(bucket_query) as f:
        return f.read()

def load_schema(bucket_schema: str) -> dict:
    """Lê um arquivo .json do GCS."""
    with FileSystems.open(bucket_schema) as f:
        return json.load(f)