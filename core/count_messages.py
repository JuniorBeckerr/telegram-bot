import os
import asyncio
import json
from telethon import TelegramClient
from config.settings import API_ID, API_HASH, PHONE, GROUP_NAME, SESSION_PATH

LAST_PROCESSED_PATH = os.path.join("storage", "last_processed.json")


def load_last_processed():
    """Lê o último ID processado do grupo, se existir."""
    if os.path.exists(LAST_PROCESSED_PATH):
        try:
            with open(LAST_PROCESSED_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


async def count_messages():
    """Conta o total de mensagens e calcula quantas são novas desde o último processamento."""
    session_path = os.path.join(SESSION_PATH, "sessao_base")
    client = TelegramClient(session_path, API_ID, API_HASH)

    await client.connect()
    if not await client.is_user_authorized():
        print("⚠️ Sessão não autenticada. Rode create_session.py primeiro.")
        await client.disconnect()
        return

    dialogs = await client.get_dialogs()
    chat = next((d.entity for d in dialogs if GROUP_NAME in d.name), None)
    if not chat:
        print(f"❌ Grupo '{GROUP_NAME}' não encontrado.")
        await client.disconnect()
        return

    # 🔍 Busca a mensagem mais recente
    last_msg = await client.get_messages(chat, limit=1)
    if not last_msg:
        print(f"📭 O grupo '{GROUP_NAME}' está vazio.")
        await client.disconnect()
        return

    last_msg_id = last_msg[0].id
    print(f"📊 O grupo '{GROUP_NAME}' tem aproximadamente {last_msg_id:,} mensagens no total.")

    # 📦 Verifica último ID processado
    last_data = load_last_processed()
    last_id = last_data.get(GROUP_NAME, {}).get("last_id", 0)

    if last_id > 0:
        diff = last_msg_id - last_id
        if diff > 0:
            print(f"🆕 Há {diff:,} mensagens novas desde o último processamento (último ID: {last_id}).")
        else:
            print("✅ Nenhuma mensagem nova desde o último processamento.")
    else:
        print("ℹ️ Nenhum histórico anterior encontrado (primeira execução).")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(count_messages())
