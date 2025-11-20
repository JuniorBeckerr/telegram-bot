"""
Repositories para o Sistema de Publicação
"""
from config.mysql_repository import BaseRepository


class BotsRepository(BaseRepository):
    """Repository para tabela bots"""

    def __init__(self):
        super().__init__("bots")

    def get_active(self):
        """Retorna todos os bots ativos"""
        return self.where("active", 1).get()

    def get_by_username(self, username: str):
        """Busca bot por username"""
        return self.where("username", username).first()

    def get_by_token(self, token: str):
        """Busca bot por token"""
        return self.where("token", token).first()

    def deactivate(self, bot_id: int):
        """Desativa um bot"""
        return self.update(bot_id, {"active": 0})

    def update_last_used(self, bot_id: int):
        """Atualiza timestamp de último uso"""
        import datetime
        return self.update(bot_id, {
            "last_used_at": datetime.datetime.now().isoformat()
        })
