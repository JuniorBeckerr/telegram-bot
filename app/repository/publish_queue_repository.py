"""
Repositories para o Sistema de Publicação
"""
from config.mysql_repository import BaseRepository


class PublishQueueRepository(BaseRepository):
    """Repository para tabela publish_queue"""

    def __init__(self):
        super().__init__("publish_queue")

    def get_pending(self, limit: int = 100):
        import datetime
        now = datetime.datetime.now().isoformat()

        return (
            self.where("status", "pending")
            .whereRaw("(scheduled_at IS NULL OR scheduled_at <= %s)", now)
            .whereColumn("attempts", "max_attempts", "<")  # agora funciona!
            .order_by("priority", "DESC")
            .order_by("created_at", "ASC")
            .limit(limit)
            .get()
        )

    def get_pending_for_group(self, group_id: int, limit: int = 50):
        """Retorna itens pendentes de um grupo"""
        return (
            self.where("group_id", group_id)
            .where("status", "pending")
            .order_by("priority", "DESC")
            .limit(limit)
            .get()
        )

    def add_to_queue(self, group_id: int, media_id: int, priority: int = 0, scheduled_at: str = None):
        """Adiciona item à fila"""
        return self.create({
            "group_id": group_id,
            "media_id": media_id,
            "priority": priority,
            "scheduled_at": scheduled_at,
            "status": "pending",
            "attempts": 0
        })

    def add_batch_to_queue(self, items: list):
        """Adiciona múltiplos itens à fila"""
        for item in items:
            try:
                self.add_to_queue(
                    group_id=item["group_id"],
                    media_id=item["media_id"],
                    priority=item.get("priority", 0),
                    scheduled_at=item.get("scheduled_at")
                )
            except Exception:
                pass  # Ignora duplicados

    def mark_processing(self, queue_id: int):
        """Marca item como em processamento"""
        import datetime
        now = datetime.datetime.now().isoformat()

        # increment attempts usando o próprio builder
        self.increment({"id": queue_id}, "attempts", 1)

        return self.update(queue_id, {
            "status": "processing",
            "last_attempt_at": now
        })

    def mark_completed(self, queue_id: int):
        """Marca item como completado"""
        return self.update(queue_id, {"status": "completed"})

    def mark_failed(self, queue_id: int, error_message: str):
        """Marca item como falha"""
        return self.update(queue_id, {
            "status": "failed",
            "error_message": error_message
        })

    def retry_item(self, queue_id: int):
        """Recoloca item na fila para retry"""
        return self.update(queue_id, {"status": "pending"})

    def is_in_queue(self, group_id: int, media_id: int) -> bool:
        """Verifica se mídia já está na fila do grupo"""
        return (
            self.where("group_id", group_id)
            .where("media_id", media_id)
            .where("status", "pending")
            .exists()
        )

    def clear_completed(self, days_old: int = 7):
        """Remove itens completados antigos"""
        import datetime
        cutoff = (datetime.datetime.now() - datetime.timedelta(days=days_old)).isoformat()

        return (
            self.where("status", "completed")
            .where("updated_at", cutoff, "<")
            .delete()
        )

    def get_queue_stats(self):
        """Retorna estatísticas da fila"""

        return (
            self.select("status", "COUNT(*) as count")
            .group_by("status")
            .get()
        )
