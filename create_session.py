import os
import asyncio
from telethon import TelegramClient
from config.settings import Config
from app.repository.credentials_repository import CredentialsRepository

async def create_multiple_sessions(credential_id, num_sessions=12):
    repo = CredentialsRepository()
    credential = repo.find(credential_id)

    if not credential:
        print(f"❌ Credencial com ID {credential_id} não encontrada.")
        return

    os.makedirs(Config.SESSION_PATH, exist_ok=True)

    for i in range(num_sessions):
        session_name = f"{credential['session_name']}_session_{i}"
        session_path = os.path.join(Config.SESSION_PATH, session_name)
        client = TelegramClient(session_path, int(credential['api_id']), credential['api_hash'])

        print(f"⚡ Criando sessão {i+1}/{num_sessions} para {credential['phone']}...")
        await client.start(phone=credential['phone'])
        await client.disconnect()
        print(f"✅ Sessão criada: {session_path}.session")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: python create_session.py <credential_id> [<num_sessions>]")
        sys.exit(1)

    cred_id = int(sys.argv[1])
    num_sessions = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    asyncio.run(create_multiple_sessions(cred_id, num_sessions))
