# app/repository/media_approvals_repository.py
from config.mysql_repository import BaseRepository

class MediaApprovalsRepository(BaseRepository):
    def __init__(self):
        super().__init__("media_approvals", primary_key="id")

    def approved(self):
        return self.where("approved", True).get()

    def pending(self):
        return self.where("approved", False).get()
