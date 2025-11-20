"""
Repositories para o Sistema de Publicação
"""
from config.mysql_repository import BaseRepository


class GroupPublishRepository(BaseRepository):
    """Repository para tabela group_publish"""

    def __init__(self):
        super().__init__("group_publish")

    def get_by_group(self, group_id: int):
        """Retorna todas as publicações de um grupo"""
        return self.where("group_id", group_id).get()

    def get_by_media(self, media_id: int):
        """Retorna todas as publicações de uma mídia"""
        return self.where("media_id", media_id).get()

    def get_by_status(self, status: str):
        """Retorna publicações por status"""
        return self.where("status", status).get()

    def is_media_published(self, group_id: int, media_id: int) -> bool:
        """Verifica se mídia já foi publicada no grupo"""
        result = (self.where("group_id", group_id)
                  .where("media_id", media_id)
                  .where("status", "published")
                  .first())
        return result is not None

    def get_published_count_today(self, group_id: int) -> int:
        """Conta publicações do dia"""
        import datetime
        today = datetime.date.today().isoformat()

        query = """
                SELECT COUNT(*) as count
                FROM group_publish
                WHERE group_id = ?
                  AND status = 'published'
                  AND DATE(published_at) = ? \
                """
        result = self.raw(query, [group_id, today])
        return result[0]["count"] if result else 0

    def get_published_count_hour(self, group_id: int) -> int:
        """Conta publicações da última hora"""
        import datetime
        one_hour_ago = (datetime.datetime.now() - datetime.timedelta(hours=1)).isoformat()

        query = """
                SELECT COUNT(*) as count
                FROM group_publish
                WHERE group_id = ?
                  AND status = 'published'
                  AND published_at >= ? \
                """
        result = self.raw(query, [group_id, one_hour_ago])
        return result[0]["count"] if result else 0

    def mark_as_published(self, publish_id: int, telegram_message_id: int = None, file_id: str = None):
        """Marca como publicado"""
        import datetime
        data = {
            "status": "published",
            "published_at": datetime.datetime.now().isoformat()
        }
        if telegram_message_id:
            data["telegram_message_id"] = telegram_message_id
        if file_id:
            data["file_id"] = file_id
        return self.update(publish_id, data)

    def mark_as_failed(self, publish_id: int, error_message: str):
        """Marca como falha"""
        return self.update(publish_id, {
            "status": "failed",
            "error_message": error_message,
            "retry_count": self.raw(
                "SELECT retry_count + 1 as count FROM group_publish WHERE id = ?",
                [publish_id]
            )[0]["count"]
        })

    def get_recent_publications(self, group_id: int, limit: int = 50):
        """Retorna publicações recentes de um grupo"""
        return (self.where("group_id", group_id)
                .where("status", "published")
                .order_by("published_at", "DESC")
                .limit(limit)
                .get())
