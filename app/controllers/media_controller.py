import logging

from app.services.media_service import MediaService
from app.utils.response_handler import ResponseHandler

logger = logging.getLogger(__name__)

class MediaController:
    def __init__(self):
        self.service = MediaService()

    def index(self, request):
        try:
            status_param = request.query_params.get("status")
            data = self.service.index(status_list=status_param)
            return ResponseHandler.success(data, "Listagem obtida com sucesso", 200)
        except Exception as e:
            logger.error(f"Erro ao listar funis: {str(e)}")
            return ResponseHandler.error("Erro ao listar funis", 500)

    def show(self, id):
        try:
            data = self.service.show(id)
            if not data:
                return ResponseHandler.error("Item não encontrado", 404)
            return ResponseHandler.success(data, "Item encontrado", 200)
        except Exception as e:
            logger.error(f"Erro ao buscar Item: {str(e)}")
            return ResponseHandler.error("Erro ao buscar Item", 500)


    async def approve_related_media(self, model_id: int, media_id: int, reviewed_by: str):
        try:
            data = await self.service.approve_related_media(model_id, media_id, reviewed_by)
            if not data:
                return ResponseHandler.error("Item não encontrado", 404)
            return ResponseHandler.success(data, "Item encontrado", 200)
        except Exception as e:
            logger.error(f"Erro ao buscar Item: {str(e)}")
            return ResponseHandler.error("Erro ao buscar Item", 500)
