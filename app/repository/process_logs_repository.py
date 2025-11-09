# app/repository/process_logs_repository.py
from config.mysql_repository import BaseRepository

class ProcessLogsRepository(BaseRepository):
    def __init__(self):
        super().__init__("process_logs", primary_key="id")

    def log_success(self, media_id: int, group_id: int, step: str, message: str = None):
        return self.create({
            "media_id": media_id,
            "group_id": group_id,
            "step": step,
            "status": "success",
            "message": message or ""
        })

    def log_error(self, media_id: int, group_id: int, step: str, message: str):
        return self.create({
            "media_id": media_id,
            "group_id": group_id,
            "step": step,
            "status": "error",
            "message": message
        })
