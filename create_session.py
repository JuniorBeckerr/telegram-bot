from telethon import TelegramClient
from config.settings import API_ID, API_HASH, PHONE, SESSION_PATH
import os, asyncio

async def init_session():
    base_path = os.path.join(SESSION_PATH, "sessao_base")
    client = TelegramClient(base_path, API_ID, API_HASH)
    await client.start(phone=PHONE)
    await client.disconnect()
    print(f"✅ Sessão base criada em: {base_path}.session")

if __name__ == "__main__":
    asyncio.run(init_session())
