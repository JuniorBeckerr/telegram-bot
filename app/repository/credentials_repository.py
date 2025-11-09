from config.mysql_repository import BaseRepository

class CredentialsRepository(BaseRepository):
    def __init__(self):
        super().__init__("credentials")

    def active(self):
        return self.where("active", True).get()
