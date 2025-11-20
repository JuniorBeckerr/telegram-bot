"""
Repositories para o Sistema de Publicação
"""
from config.mysql_repository import BaseRepository


class GroupBotsRepository(BaseRepository):
    """Repository para tabela group_bots"""

    def __init__(self):
        super().__init__("group_bots")

    def get_bots_for_group(self, group_id: int):
        """Retorna todos os bots vinculados a um grupo"""
        return self.where("group_id", group_id).get()

    def get_publisher_bot(self, group_id: int):
        """Retorna o bot publicador de um grupo"""
        return (self.where("group_id", group_id)
                .where("is_publisher", 1)
                .first())

    def get_groups_for_bot(self, bot_id: int):
        """Retorna todos os grupos onde um bot está vinculado"""
        return self.where("bot_id", bot_id).get()

    def link_bot_to_group(self, group_id: int, bot_id: int, is_publisher: bool = True):
        """Vincula um bot a um grupo"""
        return self.create({
            "group_id": group_id,
            "bot_id": bot_id,
            "is_publisher": 1 if is_publisher else 0
        })

    def unlink_bot_from_group(self, group_id: int, bot_id: int):
        """Remove vínculo entre bot e grupo"""
        return (self.where("group_id", group_id)
                .where("bot_id", bot_id)
                .delete())
