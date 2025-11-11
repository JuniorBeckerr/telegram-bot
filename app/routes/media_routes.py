from fastapi import FastAPI, Request, Depends
import logging

from app.controllers.media_controller import MediaController

logger = logging.getLogger(__name__)

class MediaRoute:
    def __init__(self, app: FastAPI):
        self.app = app
        self.controller = MediaController()

    def register_routes(self):
        """Registra todas as rotas de Funis."""

        @self.app.get("/media", tags=["Telegram-bot - media"])
        async def index(request: Request):
            return self.controller.index(request)

        @self.app.get("/media/{id}", tags=["Telegram-bot - media"])
        async def show(
                id: int
        ):
            return self.controller.show(id)

        @self.app.post("/media/approve", tags=["Telegram-bot - media"])
        async def approve_media(payload: dict):
            """
            Aprova todas as mídias relacionadas a uma label.
            payload = { "model_id": 91, "media_id": 78, "reviewed_by": "admin" }
            """
            model_id = payload.get("model_id")
            media_id = payload.get("media_id")
            reviewed_by = payload.get("reviewed_by", "system")

            return await self.controller.approve_related_media(model_id, media_id, reviewed_by)
