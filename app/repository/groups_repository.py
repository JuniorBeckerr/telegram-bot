from config.mysql_repository import BaseRepository

class GroupsRepository(BaseRepository):
    def __init__(self):
        super().__init__("groups")

    def find_by_username(self, username: str):
        return self.where("username", username).first()

    def active(self):
        return self.where("enabled", True).get()
