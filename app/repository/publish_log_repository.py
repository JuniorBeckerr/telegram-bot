"""
Repositories para Publish Log e Publish Stats
"""
from config.mysql_repository import BaseRepository
from datetime import datetime, date


class PublishLogRepository(BaseRepository):
    """Repository para tabela publish_log"""

    def __init__(self):
        super().__init__("publish_log")

    def log_action(self, publish_id: int, action: str, details: dict = None):
        """Registra uma ação"""
        import json
        return self.create({
            "group_publish_id": publish_id,
            "action": action,
            "details": json.dumps(details) if details else None,
            "created_at": datetime.now().isoformat()
        })

    def get_logs_for_publish(self, publish_id: int):
        """Retorna logs de uma publicação"""
        return self.where("group_publish_id", publish_id).get()

    def get_recent_logs(self, limit: int = 100):
        """Retorna logs recentes"""
        return self.order_by("created_at", "DESC").limit(limit).get()

    def get_logs_by_action(self, action: str, limit: int = 100):
        """Retorna logs por tipo de ação"""
        return (self.where("action", action)
                .order_by("created_at", "DESC")
                .limit(limit)
                .get())
