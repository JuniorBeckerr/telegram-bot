import os
import re
import json
import time
from difflib import SequenceMatcher

CACHE_PATH = os.path.join("storage", "name_cache.json")


# ======================================================
# 🧠 Fonética leve + normalização
# ======================================================
def soundex_simple(name: str) -> str:
    """
    Gera uma assinatura fonética leve que tolera erros comuns
    (i/l, o/0, s/z, c/k/q).
    """
    name = name.lower()
    name = re.sub(r'[^a-z0-9]', '', name)

    # substituições fonéticas comuns
    name = re.sub(r'(ph|f)', 'f', name)
    name = re.sub(r'(c|k|q)', 'k', name)
    name = re.sub(r'(s|z|x)', 's', name)
    name = re.sub(r'(m|n)', 'm', name)
    name = re.sub(r'(l|i|1)', 'l', name)  # 🔥 chave: trata l/i/1 como iguais
    name = re.sub(r'(r+)', 'r', name)
    name = re.sub(r'(o|0)', 'o', name)

    # remove vogais exceto a primeira consoante
    name = re.sub(r'[aeiouy]', '', name)

    # mantém até 6 caracteres
    return name[:6]



# ======================================================
# 📦 Cache
# ======================================================
def ensure_folder(path):
    for _ in range(5):
        try:
            os.makedirs(path, exist_ok=True)
            if os.path.isdir(path):
                return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError(f"❌ Falha ao criar pasta {path}")


def load_name_cache():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_name_cache(cache):
    ensure_folder(os.path.dirname(CACHE_PATH))
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=4)


_name_cache = load_name_cache()


# ======================================================
# 🔡 Normalização
# ======================================================
def normalize_name(name: str) -> str:
    n = name.strip().lower()
    n = re.sub(r"https?://", "", n)
    n = re.sub(r"privacy\.com(\.[a-z]{2})?/profile/", "", n)
    n = re.sub(r"onlyfans\.com/", "", n)
    n = re.sub(r"[^a-z0-9]", "", n)
    n = re.sub(r"(.)\1{2,}", r"\1\1", n)
    return n


# ======================================================
# 🔍 Comparação de similaridade
# ======================================================
def find_similar_name(name: str):
    """Compara nome com cache usando fonética + distância textual combinada."""
    key = soundex_simple(name)

    best_match = None
    best_score = 0.0

    for seen in _name_cache.keys():
        seen_key = soundex_simple(seen)

        # Similaridade fonética (peso 0.5)
        phonetic_score = 1.0 if key == seen_key else SequenceMatcher(None, key, seen_key).ratio()

        # Similaridade textual (peso 0.5)
        text_score = SequenceMatcher(None, name, seen).ratio()

        # Score combinado
        combined_score = (phonetic_score * 0.5) + (text_score * 0.5)

        # Limite dinâmico: nomes curtos exigem mais precisão
        if len(name) < 6:
            threshold = 0.86
        elif len(name) < 9:
            threshold = 0.82
        else:
            threshold = 0.78

        if combined_score >= threshold and combined_score > best_score:
            best_match = _name_cache[seen]
            best_score = combined_score

    return best_match

