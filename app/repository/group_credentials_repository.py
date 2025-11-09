from config.mysql_repository import BaseRepository

class GroupCredentialsRepository(BaseRepository):
    def __init__(self):
        super().__init__("group_credentials")

    def find_by_group(self, group_id: int):
        return self.where("group_id", group_id).get()

    def find_by_credential(self, credential_id: int):
        return self.where("credential_id", credential_id).get()
