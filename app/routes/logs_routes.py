from fastapi import FastAPI, Request, Depends
from app.controllers.logs_controller import LogsController
import logging

logger = logging.getLogger(__name__)

class LogsRoute:
    def __init__(self, app: FastAPI):
        self.app = app
        self.controller = LogsController()

    def register_routes(self):
        """Registra todas as rotas de Funis."""

        @self.app.get("/logs", tags=["Telegram-bot - logs"])
        async def index():
            return self.controller.index()

        @self.app.get("/logs/{id}", tags=["Telegram-bot - logs"])
        async def show(
                id: int
        ):
            return self.controller.show(id)
