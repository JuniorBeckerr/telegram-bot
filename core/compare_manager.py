import os
import shutil
from itertools import combinations
from services.compare.compare_images import compute_image_phash
from services.compare.compare_videos import compute_video_phash
import imagehash

# --- CONFIG ---
BASE_PATH = "storage/downloads/nadineborges"
EXTENSIONS_IMG = (".jpg", ".jpeg", ".png", ".webp")
EXTENSIONS_VID = (".mp4", ".mov", ".avi", ".mkv")
SIMILARITY_THRESHOLD = 3  # tolerância


def compute_hash(file_path):
    """Decide se é imagem ou vídeo e calcula o hash apropriado."""
    ext = os.path.splitext(file_path)[1].lower()

    if ext in EXTENSIONS_IMG:
        return compute_image_phash(file_path)
    elif ext in EXTENSIONS_VID:
        return compute_video_phash(file_path)
    else:
        return None


def get_model_folders(base_path):
    """Lista as pastas diretas dentro de downloads."""
    return [
        os.path.join(base_path, d)
        for d in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, d))
    ]


def group_by_hash(folder_path):
    """Agrupa arquivos similares dentro da pasta."""
    files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.lower().endswith(EXTENSIONS_IMG + EXTENSIONS_VID)
    ]

    hashes = {}
    for path in files:
        h = compute_hash(path)
        if h:
            hashes[path] = h

    groups = []
    for (f1, h1), (f2, h2) in combinations(hashes.items(), 2):
        diff = h1 - h2
        if diff <= SIMILARITY_THRESHOLD:
            found_group = next((g for g in groups if f1 in g or f2 in g), None)
            if found_group:
                found_group.update([f1, f2])
            else:
                groups.append({f1, f2})

    return groups


def move_groups_to_temp(folder_path, groups):
    """Cria temp/hash_xxx e move duplicatas pra dentro."""
    temp_path = os.path.join(folder_path, "temp")
    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)
    os.makedirs(temp_path, exist_ok=True)

    for i, group in enumerate(groups, start=1):
        hash_folder = os.path.join(temp_path, f"hash_{i:03d}")
        os.makedirs(hash_folder, exist_ok=True)

        print(f"📦 Grupo {i} ({len(group)} similares) → {hash_folder}")
        for file in group:
            dst = os.path.join(hash_folder, os.path.basename(file))
            try:
                shutil.move(file, dst)
            except Exception as e:
                print(f"⚠️ Erro ao mover {file}: {e}")


def process_folder(folder_path):
    print(f"\n📂 Analisando '{folder_path}'...")
    groups = group_by_hash(folder_path)

    if not groups:
        print("✅ Nenhuma duplicata encontrada.")
        return

    move_groups_to_temp(folder_path, groups)
    print(f"🧩 {len(groups)} grupo(s) movidos para temp/.")


if __name__ == "__main__":
    print(f"🔍 Procurando imagens/vídeos duplicados em '{BASE_PATH}'...")

    model_folders = get_model_folders(BASE_PATH)
    print(f"📁 {len(model_folders)} pastas encontradas para analisar.\n")

    for folder in model_folders:
        process_folder(folder)

    print("\n🏁 Finalizado.")
