import boto3
from app.repository.models_repository import ModelsRepository

# === CONFIG ===
DO_SPACES_KEY = "DO801N79B86DQ6HH43V6"
DO_SPACES_SECRET = "OSKDipRfn34iIgWf1Gb8DQyQzNUuka69ghX+u7nL40E"
DO_SPACES_REGION = "nyc3"
DO_SPACES_BUCKET = "storage-becker"
DO_SPACES_ENDPOINT = f"https://{DO_SPACES_REGION}.digitaloceanspaces.com"

session = boto3.session.Session()
client = session.client(
    "s3",
    region_name=DO_SPACES_REGION,
    endpoint_url=DO_SPACES_ENDPOINT,
    aws_access_key_id=DO_SPACES_KEY,
    aws_secret_access_key=DO_SPACES_SECRET,
)


def list_subfolders(prefix: str):
    """Lista subpastas diretas dentro de um prefixo (sem barra final)."""
    response = client.list_objects_v2(
        Bucket=DO_SPACES_BUCKET,
        Prefix=prefix,
        Delimiter="/",
    )

    subfolders = response.get("CommonPrefixes", [])
    result = []

    for folder in subfolders:
        raw_path = folder["Prefix"]
        clean_path = raw_path.rstrip("/")  # remove barra final
        name = clean_path.split("/")[-1]
        result.append({"name": name, "path": clean_path})

    return result


def create_models_from_folders():
    prefix = "data-telegram/"
    repo = ModelsRepository()

    folders = list_subfolders(prefix)
    print(f"📦 Encontradas {len(folders)} pastas em {prefix}\n")

    for f in folders:
        stage_name = f["name"]

        # 🔎 Verifica se já existe
        existing = repo.where("stage_name", stage_name).first()
        if existing:
            print(f"⏩ Já existe: {stage_name}, pulando...")
            continue

        data = {
            "stage_name": stage_name,
            "full_name": stage_name,
            "reference_path": f["path"],
        }

        try:
            repo.create(data)
            print(f"✅ Criado: {stage_name} ({f['path']})")
        except Exception as e:
            print(f"⚠️ Erro ao criar {stage_name}: {e}")


if __name__ == "__main__":
    create_models_from_folders()
