from config.mysql_repository import BaseRepository

class ModelsRepository(BaseRepository):
    def __init__(self):
        super().__init__("models", primary_key="id")

    def find_by_stage_name(self, stage_name: str):
        return self.where("stage_name", stage_name).first()

    def find_by_alias(self, alias: str):
        query = """
                SELECT * FROM models
                WHERE JSON_CONTAINS(aliases, JSON_QUOTE(%s)) \
                """
        return self.execute_raw(query, (alias,))
