from app.repository.process_logs_repository import ProcessLogsRepository

class LogsService:
    def __init__(self):
        self.repo = ProcessLogsRepository()

    def success(self, media_id: int, group_id: int, step: str, msg: str = None):
        return self.repo.log_success(media_id, group_id, step, msg)

    def error(self, media_id: int, group_id: int, step: str, msg: str):
        return self.repo.log_error(media_id, group_id, step, msg)
