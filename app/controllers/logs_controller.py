import logging

from app.services.logs_service import LogsService
from app.utils.response_handler import ResponseHandler

logger = logging.getLogger(__name__)

class LogsController:
    def __init__(self):
        self.service = LogsService()

    def index(self):
        try:
            data = self.service.index()
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
