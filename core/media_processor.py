import os
import re
import cv2
import base64
import time
import shutil
from datetime import datetime

from services.ia_service import extract_name_from_image
from data.database import add_entry
from config.settings import DOWNLOAD_PATH
from core.utils import (
    ensure_folder, find_similar_name,
    normalize_name, save_name_cache, _name_cache
)

FRAMES_PATH = os.path.join("storage", "frames")

# =========================================================
# 🔹 Utilitários básicos
# =========================================================
def _safe_cleanup(path: str):
    """Remove o arquivo de forma segura."""
    for _ in range(3):
        if not os.path.exists(path):
            return
        try:
            os.remove(path)
            print(f"🧹 Limpando arquivo temporário: {os.path.basename(path)}")
            return
        except (PermissionError, FileNotFoundError):
            time.sleep(0.2)
        except Exception as e:
            print(f"⚠️ Falha ao remover {path}: {e}")
            return


def _wait_for_stable_download(path: str) -> bool:
    """Espera o arquivo estabilizar o tamanho (garantir que terminou o download)."""
    for _ in range(10):
        try:
            size1 = os.path.getsize(path)
            time.sleep(0.1)
            size2 = os.path.getsize(path)
            if size1 == size2 and size1 > 1024:
                return True
        except FileNotFoundError:
            return False
    return False


def _read_image_or_video_frame(path: str):
    """Lê a imagem ou frame central de um vídeo."""
    ext = os.path.splitext(path)[1].lower()
    if ext in [".mp4", ".mov", ".avi", ".mkv"]:
        cap = cv2.VideoCapture(path)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        cap.set(cv2.CAP_PROP_POS_FRAMES, total_frames // 2)
        ret, frame = cap.read()
        cap.release()
        return frame if ret else None
    return cv2.imread(path)


def _crop_region(img):
    """Corta a região inferior direita da imagem."""
    h, w, _ = img.shape
    start_x = 0.80 if w > 1080 else 0.55 if w > 920 else 0.30
    return img[int(h * 0.93):h, int(w * start_x):w]


def _save_frame(roi, media_id, date):
    """Salva a imagem cortada pura (colorida)."""
    try:
        os.makedirs(FRAMES_PATH, exist_ok=True)
        filename = f"{datetime.strftime(date, '%Y-%m-%d')}_{media_id}_frame.jpg"
        path = os.path.join(FRAMES_PATH, filename)
        cv2.imwrite(path, roi)
        return path
    except Exception as e:
        print(f"⚠️ Falha ao salvar frame: {e}")
        return None


def _encode_for_ia(roi):
    """Converte a imagem em base64 para enviar à IA."""
    _, buffer = cv2.imencode(".jpg", roi, [cv2.IMWRITE_JPEG_QUALITY, 90])
    return base64.b64encode(buffer).decode("utf-8")


# =========================================================
# 🚀 Função principal
# =========================================================
def process_media(file_path, media_id, date, worker_id=None):
    start_time = time.time()

    if not os.path.exists(file_path):
        print(f"[W{worker_id}] ⚠️ Arquivo não encontrado: {file_path}")
        return

    # Garante que o download terminou
    if not _wait_for_stable_download(file_path):
        print(f"[W{worker_id}] ⚠️ Arquivo instável: {file_path}")
        return

    # Lê imagem ou vídeo
    img = _read_image_or_video_frame(file_path)
    if img is None:
        _safe_cleanup(file_path)
        return

    # Recorte
    roi = _crop_region(img)

    # Salva o frame puro (colorido)
    # _save_frame(roi, media_id, date)

    # Codifica e envia para IA
    t_ia_start = time.time()
    img_b64 = _encode_for_ia(roi)
    try:
        name_raw, tokens, cost = extract_name_from_image(img_b64)
    except Exception as e:
        print(f"[W{worker_id}] ❌ Erro IA: {e}")
        _safe_cleanup(file_path)
        return

    # Normaliza e corrige nome
    clean_name = normalize_name(name_raw)
    if not clean_name or len(clean_name) < 3:
        print(f"[W{worker_id}] ⚠️ Nome inválido retornado pela IA: '{name_raw}'")
        return

    similar = find_similar_name(clean_name)
    name = similar if similar else clean_name
    if not similar:
        _name_cache[clean_name] = clean_name
        save_name_cache(_name_cache)

    # Caminho final
    safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", name)
    folder_path = os.path.join(DOWNLOAD_PATH, safe_name)
    ensure_folder(folder_path)
    date_prefix = datetime.strftime(date, "%Y-%m-%d")
    new_filename = f"{date_prefix}_{media_id}_{safe_name}{os.path.splitext(file_path)[1]}"
    new_path = os.path.join(folder_path, new_filename)

    # Move o arquivo original
    moved = False
    try:
        if os.path.exists(file_path):
            try:
                os.rename(file_path, new_path)
                moved = True
            except OSError:
                shutil.move(file_path, new_path)
                moved = True
        else:
            print(f"[W{worker_id}] ⚠️ Arquivo desapareceu antes do move: {file_path}")
            return
    except Exception as e:
        print(f"[W{worker_id}] ⚠️ Falha ao mover {file_path}: {e}")
    finally:
        if not moved:
            _safe_cleanup(file_path)

    # Registro no banco e log
    if moved:
        add_entry(media_id, name, tokens, cost, safe_name)
        total_time = time.time() - start_time
        ia_time = time.time() - t_ia_start
        print(f"[W{worker_id}] ✅ {new_filename} | {name} | custo {cost:.6f}$ | ⏱️ {total_time:.2f}s (IA={ia_time:.2f}s)")
