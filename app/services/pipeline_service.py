import hashlib
import io
import imagehash
import base64
import cv2
import tempfile
import json
import time
from datetime import datetime
from PIL import Image

from app.services.classification_service import ClassificationService
from app.services.costs_service import CostsService
from app.services.ia_service import extract_name_from_image
from app.services.media_service import MediaService
from app.services.models_service import ModelsService
from app.services.storage_service import StorageService


class PipelineService:
    def __init__(self):
        self.media_service = MediaService()
        self.classification_service = ClassificationService()
        self.models_service = ModelsService()
        self.cost_service = CostsService()
        self.storage = StorageService()

    async def process_message(self, msg, file_bytes, mime, group, worker_id=None):
        """Processa uma mensagem com mídia do Telegram."""
        prefix = f"[W{worker_id or '-'}]"  # prefixo p/ logs do worker
        start_total = time.time()

        try:
            media_hash = hashlib.sha256(file_bytes).hexdigest()
            phash = self._compute_phash(file_bytes, mime)
            print(f"{prefix} 🧩 Processando msg {msg.id} (mime={mime}, size={len(file_bytes)} bytes)")

            # Evita duplicadas
            existing = self.media_service.get_classifications(media_hash)
            if existing:
                print(f"{prefix} ⚠️ Mídia duplicada ignorada: {media_hash[:10]}")
                return

            # 🔹 Extrai nome via IA
            roi_b64 = self._extract_roi(file_bytes, mime)
            print(f"{prefix} 🧠 Iniciando análise de imagem...")
            t0 = time.time()
            label, tokens, cost = extract_name_from_image(roi_b64)
            print(f"{prefix} 🧠 Finalizado em {time.time() - t0:.2f}s → label='{label}'")
            normalized_label = label.strip().lower()

            # 🔹 Busca modelo existente ou por alias
            t1 = time.time()
            model = self.models_service.find_by_name_or_alias(normalized_label)
            model_id = None
            stage_name = label
            reference_path = f"data-telegram/{label}"
            status = "pending"

            if model:
                aliases = self._parse_aliases(model.get("aliases"))
                print(f"{prefix} 🔍 Modelo encontrado: {model['stage_name']} | Aliases={aliases}")

                # se o nome reconhecido é um alias, troca pro stage_name oficial
                if normalized_label in [a.lower() for a in aliases] or normalized_label == model["stage_name"].lower():
                    stage_name = model["stage_name"]
                    model_id = model["id"]
                    reference_path = model.get("reference_path", f"data-telegram/{stage_name}")
                    status = "approved"

            print(f"{prefix} 🔎 Model lookup em {time.time() - t1:.2f}s → {status.upper()} ({stage_name})")

            # 🔹 Cria mídia
            media, created = self.media_service.register_media(
                group_id=group["id"],
                telegram_message_id=msg.id,
                sha256_hex=media_hash,
                phash=phash,
                mime=mime,
                size=len(file_bytes)
            )

            if created:
                # Usa data da mensagem (não do sistema)
                date_str = msg.date.strftime("%Y-%m-%d")

                folder = reference_path
                filename = f"{date_str}_{stage_name}_{msg.id}"
                remote_key = f"{folder}/{filename}"

                print(f"{prefix} ☁️ Fazendo upload para Spaces: {remote_key}")
                t2 = time.time()
                remote_url = self.storage.upload_bytes(file_bytes, remote_key, mime)
                print(f"{prefix} ☁️ Upload concluído em {time.time() - t2:.2f}s")

                # Atualiza mídia
                self.media_service.mark_uploaded(
                    media_id=media["id"],
                    url=remote_url,
                    storage_key=remote_key,
                    group_id=group["id"],
                    etag=media_hash[:16]
                )

                # Define estado final
                final_state = "approved" if model_id else "needs_review"
                self.media_service.media.update(media["id"], {"state": final_state})
            else:
                print(f"{prefix} ⚠️ Mídia duplicada ignorada: {media_hash[:10]}")
                return

            # 🔹 Cria classificação (com model_id correto se houver)
            self.classification_service.classifications.create({
                "media_id": media["id"],
                "model_id": model_id,
                "label": stage_name,
                "confidence": 1.0,
                "source": "openai-vision"
            })

            # 🔹 Registra custo
            self.cost_service.record_cost(
                step="classify",
                media_id=media["id"],
                model_id=model_id,
                group_id=group["id"],
                amount=cost,
                meta={
                    "tokens": tokens,
                    "model": "gpt-4.1-mini"
                }
            )

            total_elapsed = time.time() - start_total
            print(f"{prefix} ✅ [{msg.id}] {stage_name} ({status}) em {total_elapsed:.2f}s")

        except Exception as e:
            print(f"{prefix} ❌ Erro processando msg {msg.id}: {e}")

    # ==============================================================
    # UTILITÁRIOS DE IMAGEM
    # ==============================================================

    def _extract_roi(self, file_bytes, mime):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.write(file_bytes)
        tmp.flush()

        frame = self._read_frame(tmp.name, mime)
        h, w, _ = frame.shape
        start_x = 0.8 if w > 1080 else 0.55
        roi = frame[int(h * 0.93):h, int(w * start_x):w]
        _, buf = cv2.imencode(".jpg", roi)
        tmp.close()
        return base64.b64encode(buf).decode()

    def _read_frame(self, path, mime):
        if "video" in mime:
            cap = cv2.VideoCapture(path)
            cap.set(cv2.CAP_PROP_POS_FRAMES, cap.get(cv2.CAP_PROP_FRAME_COUNT) // 2)
            ret, frame = cap.read()
            cap.release()
            return frame
        return cv2.imread(path)

    def _compute_phash(self, file_bytes, mime):
        try:
            if "video" in mime:
                tmp = tempfile.NamedTemporaryFile(delete=False)
                tmp.write(file_bytes)
                tmp.flush()
                frame = self._read_frame(tmp.name, mime)
                tmp.close()
                if frame is None:
                    return None
                img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
            else:
                img = Image.open(io.BytesIO(file_bytes))
            return int(str(imagehash.phash(img)), 16)
        except Exception:
            return None

    # ==============================================================
    # PARSE ROBUSTO DE ALIASES
    # ==============================================================

    def _parse_aliases(self, raw_aliases):
        """Garante conversão correta e faz debug de aliases."""
        if not raw_aliases:
            return []

        aliases = []

        # já é lista
        if isinstance(raw_aliases, list):
            aliases = raw_aliases

        # string malformada ou JSON escapado
        elif isinstance(raw_aliases, str):
            raw = raw_aliases.strip()

            # remove aspas duplas externas
            if raw.startswith('"') and raw.endswith('"'):
                raw = raw[1:-1]

            # remove barra invertida de escape
            raw = raw.replace('\\"', '"')

            # se veio com {}
            if raw.startswith("{") and raw.endswith("}"):
                raw = raw.strip("{}")
                aliases = [p.strip().strip('"').strip("'") for p in raw.split(",") if p.strip()]
            else:
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        aliases = parsed
                    elif isinstance(parsed, str):
                        aliases = [parsed]
                except Exception:
                    aliases = [p.strip() for p in raw.split(",") if p.strip()]

        # normaliza
        aliases = [a.lower().strip() for a in aliases if a]
        print(f"🧩 [DEBUG] Aliases normalizados: {aliases}")
        return aliases
