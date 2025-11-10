from fastapi import FastAPI, Request, Depends
from app.controllers.model_controller import ModelController
import logging

logger = logging.getLogger(__name__)

class ModelRoute:
    def __init__(self, app: FastAPI):
        self.app = app
        self.controller = ModelController()

    def register_routes(self):
        """Registra todas as rotas de Funis."""

        @self.app.get("/models", tags=["Telegram-bot - models"])
        async def index():
            return self.controller.index()

        @self.app.get("/models/{id}", tags=["Telegram-bot - models"])
        async def show(
                id: int
        ):
            return self.controller.show(id)

        @self.app.post("/models", tags=["Telegram-bot - models"])
        async def store(
                request: Request,
        ):
            body = await request.json()
            return self.controller.store(body)

        @self.app.put("/models/{id}", tags=["Telegram-bot - models"])
        async def update(
                request: Request,
                id: int
        ):
            body = await request.json()
            return self.controller.update(body, id)

        @self.app.delete("/models/{id}", tags=["Telegram-bot - models"])
        async def delete(
                id: int
        ):
            return self.controller.destroy(id)