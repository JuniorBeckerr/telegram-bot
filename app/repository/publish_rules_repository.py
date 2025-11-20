"""
Repositories para o Sistema de Publicação
"""
from config.mysql_repository import BaseRepository

class PublishRulesRepository(BaseRepository):
    """Repository para tabela publish_rules"""

    def __init__(self):
        super().__init__("publish_rules")

    def get_active_rules(self):
        """Retorna todas as regras ativas"""
        return self.where("active", 1).get()

    def get_rules_for_group(self, group_id: int):
        """Retorna regras de um grupo destino"""
        return (self.where("group_id", group_id)
                .where("active", 1)
                .order_by("priority", "DESC")
                .get())

    def get_rules_by_source(self, source_group_id: int):
        """Retorna regras que usam um grupo como fonte"""
        return (self.where("source_group_id", source_group_id)
                .where("active", 1)
                .get())

    def create_rule(self, group_id: int, source_group_id: int = None, **kwargs):
        """Cria uma nova regra"""
        data = {
            "group_id": group_id,
            "source_group_id": source_group_id,
            "approval_required": kwargs.get("approval_required", 1),
            "auto_publish": kwargs.get("auto_publish", 0),
            "daily_limit": kwargs.get("daily_limit"),
            "hourly_limit": kwargs.get("hourly_limit"),
            "classification_filter": kwargs.get("classification_filter"),
            "caption_template": kwargs.get("caption_template"),
            "priority": kwargs.get("priority", 0),
            "active": 1
        }
        return self.create(data)

    def deactivate_rule(self, rule_id: int):
        """Desativa uma regra"""
        return self.update(rule_id, {"active": 0})
