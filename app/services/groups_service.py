from app.repository.groups_repository import GroupsRepository
from app.repository.group_credentials_repository import GroupCredentialsRepository
from app.repository.credentials_repository import CredentialsRepository


class GroupsService:
    def __init__(self):
        self.groups = GroupsRepository()
        self.group_credentials = GroupCredentialsRepository()
        self.credentials = CredentialsRepository()

    def index(self):
        return self.groups.all()

    def show(self, id):
        return self.groups.find(id)

    def store(self, data):
        return self.groups.create(data)

    def update(self, data, id):
        return self.groups.update(id, data)

    def destroy(self, id):
        return self.groups.delete(id)


    def get_enabled_groups(self):
        """Retorna todos os grupos ativos."""
        return self.groups.active()

    def get_group_with_credentials(self, group_id: int):
        """Retorna o grupo com suas credenciais associadas."""
        group = self.groups.find(group_id)
        if not group:
            return None

        links = self.group_credentials.where("group_id", group_id).get()
        cred_ids = [l["credential_id"] for l in links]

        creds = self.credentials.query().where_in("id", cred_ids).get() if cred_ids else []
        group["credentials"] = creds
        return group
