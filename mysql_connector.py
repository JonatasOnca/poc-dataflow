# mysql_connector.py

import logging
import apache_beam as beam
import pymysql
from google.cloud import secretmanager

def get_secret(secret_id: str) -> str:
    """
    Busca o valor de um segredo no GCP Secret Manager.
    Esta função é agnóstica ao pipeline e pode ser reutilizada.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={"name": secret_id})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error(f"Não foi possível acessar o segredo {secret_id}. Erro: {e}")
        raise

class ReadFromMySql(beam.DoFn):
    """
    Um DoFn que se conecta ao MySQL e executa uma query, emitindo cada linha como um dicionário.
    A conexão é aberta no setup() e fechada no teardown() para eficiência.
    """
    def __init__(self, host, user, password, db, query):
        self._host = host
        self._user = user
        self._password = password
        self._db = db
        self._query = query
        self.conn = None
        self.cursor = None

    def setup(self):
        """Abre a conexão com o banco de dados em cada worker."""
        logging.info("Abrindo conexão com o MySQL usando PyMySQL...")
        self.conn = pymysql.connect(
            host=self._host,
            user=self._user,
            password=self._password,
            database=self._db,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.conn.cursor()

    def process(self, element):
        """Executa a query e emite (yield) cada linha."""
        self.cursor.execute(self._query)
        for row in self.cursor:
            yield row

    def teardown(self):
        """Fecha a conexão com o banco de dados."""
        logging.info("Fechando conexão com o MySQL.")
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()