from openai import OpenAI
import os
from dotenv import load_dotenv

API_KEY1= os.getenv('API_KEY_2')
client = OpenAI(api_key=API_KEY1)

def extract_name_from_image(img_b64: str):
    prompt = (
        "Analise cuidadosamente o texto na imagem. "
        "Procure apenas por endereços visíveis de sites ou perfis, como: "
        "'onlyfans.com/...', 'privacy.com.br/profile/...', 'instagram.com/...', 'x.com/...', etc. "
        "Se encontrar uma URL ou nome de perfil legível, extraia **somente o nome exato** após a última barra '/'. "
        "O texto retornado deve ser exatamente como aparece na imagem, sem inventar, corrigir, nem completar. "
        "Não traduza, não corrija, não adivinhe. "
        "Se houver dúvida, texto ilegível ou ausência total de URL, responda exatamente: nenhuma. "
        "Jamais crie nomes imaginários ou tente deduzir o nome — só retorne se o texto for 100% visível e legível. "
        "Responda apenas com o nome puro (sem espaços, sem @, sem links, sem comentários)."
    )

    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        max_completion_tokens=2000,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}
                    },
                ],
            }
        ],
    )

    result = response.choices[0].message.content.strip().lower()

    # Tokens e custo
    usage = response.usage
    prompt_tokens = usage.prompt_tokens
    completion_tokens = usage.completion_tokens
    total_tokens = usage.total_tokens

    input_price_per_1k = 0.00040
    output_price_per_1k = 0.00160
    cost = ((prompt_tokens / 1000) * input_price_per_1k) + \
           ((completion_tokens / 1000) * output_price_per_1k)

    # 🔹 Sanitiza saída: remove prefixos acidentais
    result = result.strip().replace("https://", "").replace("http://", "")
    if "/" in result:
        result = result.split("/")[-1]
    result = result.replace("@", "").replace(" ", "").replace("\n", "")

    if not result or len(result) < 2:
        result = "nenhuma"
    return result, total_tokens, cost
