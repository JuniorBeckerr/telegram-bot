from app.repository.costs_repository import CostsRepository
import json

class CostsService:
    def __init__(self):
        self.repo = CostsRepository()

    def record_cost(self, step: str, media_id: int, amount: float, meta: dict, currency: str = "USD", model_id = None, group_id=None):
        data = {
            "step": step,
            "media_id": media_id,
            "amount": amount,
            "group_id": group_id,
            "model_id": model_id,
            "currency": currency,
            "meta": json.dumps(meta)
        }
        return self.repo.create(data)

    def total_media(self, media_id: int):
        return self.repo.total_by_media(media_id)
