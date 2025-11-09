from app.repository.media_repository import MediaRepository
from app.repository.models_repository import ModelsRepository
from app.repository.media_classifications_repository import MediaClassificationsRepository
from app.repository.media_approvals_repository import MediaApprovalsRepository
from app.repository.costs_repository import CostsRepository
from app.repository.process_logs_repository import ProcessLogsRepository


class MediaService:
    def __init__(self):
        self.media = MediaRepository()
        self.models = ModelsRepository()
        self.classifications = MediaClassificationsRepository()
        self.approvals = MediaApprovalsRepository()
        self.costs = CostsRepository()
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
