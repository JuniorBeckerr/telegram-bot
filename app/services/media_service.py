import sys

from app.repository.media_repository import MediaRepository
from app.repository.models_repository import ModelsRepository
from app.repository.media_classifications_repository import MediaClassificationsRepository
from app.repository.media_approvals_repository import MediaApprovalsRepository
from app.repository.costs_repository import CostsRepository
from app.repository.process_logs_repository import ProcessLogsRepository
from app.services.storage_service import StorageService
import json
import os

class MediaService:
    def __init__(self):
        self.media = MediaRepository()
        self.models = ModelsRepository()
        self.classifications = MediaClassificationsRepository()
        self.approvals = MediaApprovalsRepository()
        self.costs = CostsRepository()
        self.storage = StorageService()
        self.logs = ProcessLogsRepository()

    def register_media(self, group_id: int, telegram_message_id: int, sha256_hex: str, phash: int, mime: str, size: int):
        """Cria registro de mídia se não existir (deduplicação)."""
        existing = self.media.find_by_hash(sha256_hex)
        if existing:
            return existing, False

        data = {
            "group_id": group_id,
            "telegram_message_id": telegram_message_id,
            "sha256_hex": sha256_hex,
            "phash": phash,
            "mime": mime,
            "size_bytes": size,
            "state": "pending_classification"
        }
        created = self.media.create(data)
        return created, True

    def mark_uploaded(self, media_id: int, url: str, storage_key: str, etag: str, group_id):
        """Atualiza estado e metadados após upload."""
        self.media.update(media_id, {
            "remote_url": url,
            "storage_key": storage_key,
            "state": "uploaded"
        })
        self.logs.log_success(media_id=media_id, group_id=group_id, step="upload", message=f"Upload concluído: {etag}")

    def mark_error(self, media_id: int, step: str, message: str):
        """Marca erro no log."""
        self.media.update(media_id, {"state": "error"})
        self.logs.log_error(media_id, None, step, message)

    def get_classifications(self, media_hash):
        return self.media.where("sha256_hex", media_hash).first()


    def index(self, status_list=None):
        if status_list:
            query = self.media.index(status_list)
        else:
            query = self.media.query().order_by("created_at", "desc").get()
        for r in query:
            r["public_url"] = self.storage.build_public_url(r)

        return query

    def show(self, id):
        media = self.media.show(id)
        media["public_url"] = self.storage.build_public_url(media)
        return media

    async def approve_related_media(self, model_id: int, media_id: int, reviewed_by: str):
        # 🔹 1. Busca model e mídia base
        model = self.models.find(model_id)
        media = self.media.find(media_id)
        if not model or not media:
            raise Exception("Model ou mídia não encontrada")
        # 🔹 2. Busca label dessa mídia
        classification = self.classifications.where("media_id", media_id).first()
        if not classification:
            raise Exception("Classificação não encontrada para a mídia")

        label = classification["label"].strip().lower()

        # 🔹 3. Busca todas as classificações com a mesma label
        all_class = self.classifications.where("label", label).get()
        media_ids = [c["media_id"] for c in all_class]

        moved = []
        skipped = []

        for mid in media_ids:
            m = self.media.find(mid)
            if not m:
                continue

            current_path = m["storage_key"]
            current_folder = os.path.dirname(current_path)

            target_path = model["reference_path"]
            mime = m["mime"]

            if current_folder == target_path:
                self.media.update(mid, {"state": "approved"})
                self.classifications.where("media_id", mid).update({"model_id": model_id})

                self.approvals.create({
                    "media_id": mid,
                    "approved": True,
                    "reviewed_by": reviewed_by,
                    "comment": "Já estava na pasta correta — apenas marcada como aprovada"
                })
                skipped.append(mid)
                continue

            # 🚀 move no Spaces
            new_key = f"{target_path}/{current_path.split('/')[-1]}"

            self.storage.move_file(m["storage_key"], new_key)

            # Atualiza no banco
            new_url = f"{self.storage.endpoint}/{new_key}"
            self.media.update(mid, {
                "storage_key": new_key,
                "remote_url": new_url,
                "state": "approved"
            })
            self.classifications.where("media_id", mid).update({"model_id": model_id})
            moved.append(mid)

            # Cria registro de aprovação
            self.approvals.create({
                "media_id": mid,
                "approved": True,
                "reviewed_by": reviewed_by,
                "comment": f"Movida para {target_path}"
            })

        # 🔹 4. Atualiza aliases se label ≠ stage_name
        if label != model["stage_name"].lower():
            aliases = []
            raw = model.get("aliases")
            if raw:
                try:
                    aliases = json.loads(raw)
                except Exception:
                    aliases = [raw]

            if label not in [a.lower() for a in aliases]:
                aliases.append(label)
                self.models.update(model_id, {"aliases": json.dumps(list(set(aliases)))})

        return {
            "label": label,
            "moved": moved,
            "skipped": skipped,
            "total_processed": len(media_ids)
        }
