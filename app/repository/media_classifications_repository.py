# app/repository/media_classifications_repository.py
from config.mysql_repository import BaseRepository

class MediaClassificationsRepository(BaseRepository):
    def __init__(self):
        super().__init__("media_classifications", primary_key="id")

    def by_media(self, media_id: int):
        return self.where("media_id", media_id).get()
