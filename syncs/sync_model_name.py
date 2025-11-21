import asyncio
import aiohttp
import re
import json
import sys

from app.repository.models_repository import ModelsRepository


async def fetch(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=15) as r:
            return await r.text()


def extract_user_info(html: str):
    match = re.search(r'user="([^"]+)"', html)
    if not match:
        return None

    raw_json = match.group(1)
    raw_json = raw_json.replace("&quot;", '"')

    return json.loads(raw_json)


async def process_model(model, repo):
    name = model["stage_name"]
    model_id = model["id"]
    url = f"https://privacy.com.br/checkout/{name}"

    print(f"\n🔎 Buscando: {url}")

    html = await fetch(url)
    if not html:
        print("❌ Não retornou HTML")
        return None

    user = extract_user_info(html)
    if not user:
        print(f"❌ Não encontrou user=\"...\" no HTML para {name}")
        return None

    full_name = user.get("name")

    print(f"➡ Atualizando registro {model_id} | full_name = {full_name}")

    try:
        repo.update(model_id, {
            "full_name": full_name
        })
    except Exception as e:
        print("❌ Erro ao atualizar no banco:", e)

    return {
        "id": model_id,
        "stage_name": name,
        "privacy_url": url,
        "full_name": full_name,
        "username": user.get("profileName"),
        "location": user.get("location"),
    }


async def main():
    repo = ModelsRepository()
    models = repo.all()

    results = []

    for m in models:
        data = await process_model(m, repo)
        if data:
            results.append(data)

    with open("resultado_privacy.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    print("\n💾 Arquivo salvo: resultado_privacy.json")
    print(f"Total de registros processados: {len(results)}")


asyncio.run(main())
