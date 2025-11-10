from fastapi import FastAPI, Request, Depends
import logging

from app.controllers.groups_controller import GroupsController

logger = logging.getLogger(__name__)

class GroupsRoute:
    def __init__(self, app: FastAPI):
        self.app = app
        self.controller = GroupsController()

    def register_routes(self):
        """Registra todas as rotas de Funis."""

        @self.app.get("/groups", tags=["Telegram-bot - groups"])
        async def index():
            return self.controller.index()

        @self.app.get("/groups/{id}", tags=["Telegram-bot - groups"])
        async def show(
                id: int
        ):
            return self.controller.show(id)

        @self.app.post("/groups", tags=["Telegram-bot - groups"])
        async def store(
                request: Request,
        ):
            body = await request.json()
            return self.controller.store(body)

        @self.app.put("/groups/{id}", tags=["Telegram-bot - groups"])
        async def update(
                request: Request,
                id: int
        ):
            body = await request.json()
            return self.controller.update(body, id)

        @self.app.delete("/groups/{id}", tags=["Telegram-bot - groups"])
        async def delete(
                id: int
        ):
            return self.controller.destroy(id)