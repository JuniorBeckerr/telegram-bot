from app.repository.credentials_repository import CredentialsRepository


class CredentialsService:
    def __init__(self):
        self.repo = CredentialsRepository()

    def get_active(self):
        return self.repo.active()

    def get_by_id(self, cred_id: int):
        return self.repo.find(cred_id)
