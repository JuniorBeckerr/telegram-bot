import os
import sys
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

from config.settings import Config
from app.repository.credentials_repository import CredentialsRepository
from app.repository.group_credentials_repository import GroupCredentialsRepository

"""
Gera UMA session_string para group_credentials e salva direto no DB.
Fluxo atualizado:
- Usa sessão LOCAL (arquivo) para evitar ResendCodeRequest
- Se sessão não existir, faz login UMA VEZ via client.start()
- Depois extrai a StringSession sem pedir SMS
Uso:

    python generate_group_session.py <group_credential_id>
"""

async def create_session_for_group_credential(credential_id: int):
    # buscar registro em group_credentials
    gc_repo = GroupCredentialsRepository()
    group_cred = gc_repo.where("id", credential_id).first()

    if not group_cred:
        print(f"❌ group_credential com ID {credential_id} não encontrado.")
        return

    # buscar credentials geral
    cred = CredentialsRepository().find(group_cred["credential_id"])
    if not cred:
        print(f"❌ credentials com ID {group_cred['credential_id']} não encontrado.")
        return

    api_id = int(cred["api_id"])
    api_hash = cred["api_hash"]
    phone = cred["phone"]
    group_id = group_cred["group_id"]

    print(f"\n🔑 Credencial carregada:")
    print(f"   → group_credential.id: {credential_id}")
    print(f"   → credentials.id: {group_cred['credential_id']}")
    print(f"   → API_ID: {api_id}")
    print(f"   → API_HASH: {api_hash}")
    print(f"   → Phone: {phone}")
    print(f"   → Group ID: {group_id}")

    print("\n⚡ Preparando sessão local…")

    # caminho onde ficará a sessão local
    os.makedirs(Config.SESSION_PATH, exist_ok=True)
    session_name = f"group_cred_{credential_id}"
    session_path = os.path.join(Config.SESSION_PATH, session_name)

    # cria client usando sessão física
    client = TelegramClient(session_path, api_id, api_hash)

    # se arquivo de sessão não existir → precisa logar uma vez
    need_login = not os.path.exists(session_path + ".session")

    async with client:
        if need_login:
            print("\n📱 Primeira vez usando este número. Será necessário login UMA VEZ.")
            print(f"→ Número: {phone}")

            # fluxo oficial do Telethon (NÃO usa send_code_request)
            await client.start(phone=phone)

            print("✅ Sessão local criada com sucesso!")
        else:
            print("🔁 Sessão local encontrada. Conectando sem pedir código…")
            await client.connect()

        # agora gera a session string
        session_string = StringSession.save(client.session)

    print("\n===== SESSION STRING GERADA =====")
    print(session_string)
    print("=================================\n")

    # salva no banco
    gc_repo.update(credential_id, {"session_string": session_string})

    print(f"💾 Session salva no banco (group_credentials.session_string)")
    print(f"   → ID: {credential_id}")
    print("\n🚀 Finalizado com sucesso!")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python generate_group_session.py <group_credential_id>")
        sys.exit(1)

    cred_id = int(sys.argv[1])
    asyncio.run(create_session_for_group_credential(cred_id))
