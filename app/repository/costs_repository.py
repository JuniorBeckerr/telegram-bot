# app/repository/costs_repository.py
from config.mysql_repository import BaseRepository

class CostsRepository(BaseRepository):
    def __init__(self):
        super().__init__("costs", primary_key="id")

    def total_by_group(self, group_id: int):
        return self.where("group_id", group_id).sum("amount")

    def total_by_media(self, media_id: int):
        return self.where("media_id", media_id).sum("amount")
