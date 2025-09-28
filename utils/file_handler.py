from pathlib import Path
import json

BASE_DIR = Path(__file__).resolve().parent.parent

def load_query(filename: str) -> str:
    """Lê um arquivo .sql do diretório de queries."""
    query_path = BASE_DIR / "queries" / filename
    with open(query_path, 'r') as f:
        return f.read()

def load_schema(filename: str) -> dict:
    """Lê um arquivo .json de schema do diretório de schemas."""
    schema_path = BASE_DIR / "schemas" / filename
    with open(schema_path, 'r') as f:
        return json.load(f)