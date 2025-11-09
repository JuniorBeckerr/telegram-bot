import boto3

DO_SPACES_KEY = "DO801N79B86DQ6HH43V6"
DO_SPACES_SECRET = "OSKDipRfn34iIgWf1Gb8DQyQzNUuka69ghX+u7nL40E"
DO_SPACES_REGION = "nyc3"
DO_SPACES_BUCKET = "storage-becker"
DO_SPACES_ENDPOINT = f"https://{DO_SPACES_REGION}.digitaloceanspaces.com"


def _get_spaces_client():
    """Cria um cliente boto3 isolado — seguro para uso em multiprocessing."""
    session = boto3.session.Session()
    return session.client(
        "s3",
        region_name=DO_SPACES_REGION,
        endpoint_url=DO_SPACES_ENDPOINT,
        aws_access_key_id=DO_SPACES_KEY,
        aws_secret_access_key=DO_SPACES_SECRET,
    )


class StorageService:
    """Serviço de upload para o DigitalOcean Spaces."""

    def __init__(self):
        # 🔹 Não cria o client aqui — evita erro de pickle no multiprocessing
        self.bucket = DO_SPACES_BUCKET
        self.endpoint = DO_SPACES_ENDPOINT

    def upload_bytes(self, file_bytes: bytes, key: str, mime: str) -> str:
        """Faz upload de bytes diretamente para o Spaces."""
        client = _get_spaces_client()  # cria o client localmente em cada chamada
        client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=file_bytes,
            ContentType=mime,
            ACL="public-read",
        )
        return f"{self.endpoint}/{key}"
