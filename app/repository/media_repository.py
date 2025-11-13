# app/repository/media_repository.py
from config.mysql_repository import BaseRepository

class MediaRepository(BaseRepository):
    def __init__(self):
        super().__init__("media", primary_key="id")

    def find_by_hash(self, sha256_hex: str):
        return self.where("sha256_hex", sha256_hex).first()

    def exists_msg_id(self,group, id):
        return self.where("group_id", group).where("telegram_message_id", id).first()

    def pending_classification(self):
        return self.where("state", "pending_classification").get()

    def update_state(self, media_id: int, state: str):
        return self.update(media_id, {"state": state})


    def index(self, status=None, mime=None, search=None, page=1, limit=48):
        """
        Lista mídias com join do nome do grupo e filtros opcionais.
        Pagina os resultados.
        """
        query = (
            self.query()
            .select(
                "media.*",
                "g.title AS group_name"
            )
            .left_join("`groups` AS g", "g.id", "=", "media.group_id")
            .order_by("media.created_at", "DESC")
        )

        # filtros opcionais
        if status:
            query.where("media.state", status)

        if mime:
            if mime == "video":
                query.where_like("media.mime", "video/")
            elif mime == "image":
                query.where_like("media.mime", "image/")

        if search:
            query.where_like("g.title", search)

        # paginação usando o QueryBuilder
        return query.paginate(page=page, per_page=limit)

    def show(self, id):
        """
        Lista mídias com join do nome do grupo e filtro opcional por estado.
        """
        query = (
            self.query()
            .select(
                "media.*",
                "g.title AS group_name"
            )
            .left_join("`groups` AS g", "g.id", "=", "media.group_id")
            .where("media.id", id)
            .order_by("media.created_at", "DESC")
        )
        return query.first()