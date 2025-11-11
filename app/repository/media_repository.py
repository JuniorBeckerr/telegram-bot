# app/repository/media_repository.py
from config.mysql_repository import BaseRepository

class MediaRepository(BaseRepository):
    def __init__(self):
        super().__init__("media", primary_key="id")

    def find_by_hash(self, sha256_hex: str):
        return self.where("sha256_hex", sha256_hex).first()

    def pending_classification(self):
        return self.where("state", "pending_classification").get()

    def update_state(self, media_id: int, state: str):
        return self.update(media_id, {"state": state})


    def index(self, status=None):
        """
        Lista mídias com join do nome do grupo e filtro opcional por estado.
        """
        query = (
            self.query()
            .select(
                "media.*",
                "g.title AS group_name"
            )
            .left_join("`groups` AS g", "g.id", "=", "media.group_id")
            .order_by("media.created_at", "DESC")
        )
        if status:
            query.where("media.state", status)
        return query.get()

    def show(self, id):
        """
        Lista mídias com join do nome do grupo e filtro opcional por estado.
        """
        query = (
            self.query()
            .select(
                "media.*",
                "g.title AS group_name"
            )
            .left_join("`groups` AS g", "g.id", "=", "media.group_id")
            .where("media.id", id)
            .order_by("media.created_at", "DESC")
        )
        return query.first()