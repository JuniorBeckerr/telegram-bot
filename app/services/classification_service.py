from app.repository.media_classifications_repository import MediaClassificationsRepository
from app.repository.media_repository import MediaRepository
from app.repository.media_approvals_repository import MediaApprovalsRepository
from app.services.models_service import ModelsService


class ClassificationService:
    def __init__(self):
        self.classifications = MediaClassificationsRepository()
        self.media = MediaRepository()
        self.modelsService = ModelsService()
        self.approvals = MediaApprovalsRepository()

    def classify_media(self, media_id: int, label: str, confidence: float, source: str):
        """Salva resultado de classificação e define status da mídia."""
        model = self.modelsService.find_by_name_or_alias(label)
        model_id = model["id"] if model else None

        # Cria classificação
        self.classifications.create({
            "media_id": media_id,
            "model_id": model_id,
            "label": label,
            "confidence": confidence,
            "source": source
        })


        self.media.update(media_id, {"state": "needs_review"})
        self.approvals.create({"media_id": media_id, "approved": False})
