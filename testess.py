"""
Teste simples de envio de álbum via Bot API Local
"""
import asyncio
import aiohttp
import json

# Configurações
BOT_TOKEN = "8541185101:AAGqEe1nxmD9HXlbz8ATx27YC3hU79FEbKQ"
BASE_URL = f"http://154.38.174.118:9000"
CHAT_ID = -1003391602003

MEDIA_URLS = [
    # "https://storage-becker.nyc3.digitaloceanspaces.com/data-telegram/sabrinademartini/2025-09-03_sabrinademartini_522",
    # "https://storage-becker.nyc3.digitaloceanspaces.com/data-telegram/sabrinademartini/2025-09-03_sabrinademartini_521",
    # "https://storage-becker.nyc3.digitaloceanspaces.com/data-telegram/sabrinademartini/2025-09-16_sabrinademartini_5460",
    # "https://storage-becker.nyc3.digitaloceanspaces.com/data-telegram/sabrinademartini/2025-09-16_sabrinademartini_5462",
    # "https://storage-becker.nyc3.digitaloceanspaces.com/data-telegram/sabrinademartini/2025-09-16_sabrinademartini_5461",
    "https://storage-becker.nyc3.digitaloceanspaces.com/data-telegram/jenneferkaroline_/2025-09-03_jenneferkaroline__509",
]


async def test_send_media_group():
    """Testa envio de álbum"""

    url = f"{BASE_URL}/bot{BOT_TOKEN}/sendMediaGroup"

    # Monta lista de mídias
    media = []
    for i, media_url in enumerate(MEDIA_URLS):
        item = {
            "type": "video",
            "media": media_url
        }
        if i == 0:
            item["caption"] = "#teste - Sabrina De Martini"
        media.append(item)

    print(f"📤 Enviando {len(media)} mídias para {CHAT_ID}")
    print(f"🌐 URL: {url}")
    print(f"📋 Media: {json.dumps(media, indent=2)}")

    async with aiohttp.ClientSession() as session:
        # Teste 1: Enviar como JSON
        print("\n--- Teste 1: Enviando como JSON ---")
        payload = {
            "chat_id": CHAT_ID,
            "media": media
        }

        try:
            async with session.post(url, json=payload) as response:
                result = await response.json()
                print(f"Status: {response.status}")
                print(f"Response: {json.dumps(result, indent=2)}")

                if result.get("ok"):
                    print("✅ Sucesso com JSON!")
                    return
                else:
                    print(f"❌ Erro: {result.get('description')}")
        except Exception as e:
            print(f"❌ Exceção: {e}")

        # Teste 2: Enviar como FormData com media como string JSON
        print("\n--- Teste 2: Enviando como FormData ---")
        data = aiohttp.FormData()
        data.add_field("chat_id", str(CHAT_ID))
        data.add_field("media", json.dumps(media))

        try:
            async with session.post(url, data=data) as response:
                result = await response.json()
                print(f"Status: {response.status}")
                print(f"Response: {json.dumps(result, indent=2)}")

                if result.get("ok"):
                    print("✅ Sucesso com FormData!")
                    return
                else:
                    print(f"❌ Erro: {result.get('description')}")
        except Exception as e:
            print(f"❌ Exceção: {e}")

        # Teste 3: Enviar como query parameters na URL
        print("\n--- Teste 3: Enviando como Query Parameters ---")
        params = {
            "chat_id": str(CHAT_ID),
            "media": json.dumps(media)
        }

        try:
            async with session.post(url, params=params) as response:
                result = await response.json()
                print(f"Status: {response.status}")
                print(f"Response: {json.dumps(result, indent=2)}")

                if result.get("ok"):
                    print("✅ Sucesso com Query Parameters!")
                    return
                else:
                    print(f"❌ Erro: {result.get('description')}")
        except Exception as e:
            print(f"❌ Exceção: {e}")

        # Teste 4: GET com query parameters
        print("\n--- Teste 4: GET com Query Parameters ---")
        try:
            async with session.get(url, params=params) as response:
                result = await response.json()
                print(f"Status: {response.status}")
                print(f"Response: {json.dumps(result, indent=2)}")

                if result.get("ok"):
                    print("✅ Sucesso com GET!")
                    return
                else:
                    print(f"❌ Erro: {result.get('description')}")
        except Exception as e:
            print(f"❌ Exceção: {e}")

        # Teste 5: multipart/form-data com content-type explícito
        print("\n--- Teste 5: multipart/form-data explícito ---")
        data = aiohttp.FormData()
        data.add_field("chat_id", str(CHAT_ID), content_type="text/plain")
        data.add_field("media", json.dumps(media), content_type="application/json")

        try:
            async with session.post(url, data=data) as response:
                result = await response.json()
                print(f"Status: {response.status}")
                print(f"Response: {json.dumps(result, indent=2)}")

                if result.get("ok"):
                    print("✅ Sucesso com multipart explícito!")
                    return
                else:
                    print(f"❌ Erro: {result.get('description')}")
        except Exception as e:
            print(f"❌ Exceção: {e}")


async def test_get_me():
    """Testa se o bot está funcionando"""
    url = f"{BASE_URL}/bot{BOT_TOKEN}/getMe"

    print(f"🤖 Testando bot...")

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            result = await response.json()
            print(f"Response: {json.dumps(result, indent=2)}")

            if result.get("ok"):
                print(f"✅ Bot OK: @{result['result']['username']}")
            else:
                print(f"❌ Erro: {result.get('description')}")


if __name__ == "__main__":
    print("=" * 50)
    print("TESTE BOT API LOCAL")
    print("=" * 50)

    asyncio.run(test_get_me())
    print("\n")
    asyncio.run(test_send_media_group())

