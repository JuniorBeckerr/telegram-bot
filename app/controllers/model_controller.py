import logging

from app.services.models_service import ModelsService
from app.utils.response_handler import ResponseHandler

logger = logging.getLogger(__name__)

class ModelController:
    def __init__(self):
        self.service = ModelsService()

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

    def store(self, payload):
        try:
            data = self.service.store(payload)
            return ResponseHandler.success(data, "Criado com sucesso!", 201)
        except Exception as e:
            logger.error(f"Erro ao criar Item: {str(e)}")
            return ResponseHandler.error("Erro ao criar Item", 500)

    def update(self, payload, id):
        try:
            data = self.service.update(payload, id)
            return ResponseHandler.success(data, "Atualizado com sucesso!", 200)
        except Exception as e:
            logger.error(f"Erro ao atualizar Item: {str(e)}")
            return ResponseHandler.error("Erro ao atualizar Item", 500)

    def destroy(self, id):
        try:
            self.service.destroy(id)
            return ResponseHandler.success(None, "Removido com sucesso!", 200)
        except Exception as e:
            logger.error(f"Erro ao remover Item: {str(e)}")
            return ResponseHandler.error("Erro ao remover Item", 500)
