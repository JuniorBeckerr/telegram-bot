import logging
import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.routes.route_manager import RouteManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class API:
    """Main FastAPI application class for JoinChat microservice."""

    def __init__(self):
        """Initialize the FastAPI application with all configurations."""
        tags_metadata = [
            {
                "name": "System",
                "description": "System health and status endpoints"
            },
            {
                "name": "Broadcast",
                "description": "Broadcast messaging and campaign management"
            },
            {
                "name": "Contacts",
                "description": "Contact management and subscriber operations"
            },
            {
                "name": "Analytics",
                "description": "Analytics, reporting, and performance metrics"
            },
            {
                "name": "ManyChat - Webhooks",
                "description": "Facebook webhook integration endpoints"
            },
            {
                "name": "ManyChat - Pages",
                "description": "Facebook page management and listing"
            },
            {
                "name": "ManyChat - Funnels",
                "description": "Funnel creation, management, and flow operations"
            },
            {
                "name": "ManyChat - Nodes",
                "description": "Node management for funnel flows"
            },
            {
                "name": "ManyChat - Relations",
                "description": "Node relationship and connection management"
            },
            {
                "name": "ManyChat - Tags",
                "description": "Tag management for subscriber segmentation"
            },
            {
                "name": "ManyChat - Leads",
                "description": "Lead management and qualification"
            },
            {
                "name": "ManyChat - Media",
                "description": "Media upload and management for messages"
            },
            {
                "name": "ManyChat - Actions",
                "description": "Active actions and trigger management"
            },
            {
                "name": "Tracking",
                "description": "Click tracking and redirect management"
            }
        ]

        self.app = FastAPI(
            title="JoinChat API",
            version="1.0.0",
            description="Microservice for managing broadcast messages, contacts, and ManyChat integrations",
            docs_url="/docs",
            redoc_url="/redoc",
            openapi_url="/openapi.json",
            openapi_tags=tags_metadata,
            debug=False
        )
        self.setup_middleware()
        self.setup_routes()
        self.setup_static_files()

    def setup_routes(self):
        """Register all application routes."""
        route_manager = RouteManager(self.app)
        route_manager.register_all_routes()
        logger.info("All routes registered successfully")

    def setup_static_files(self):
        """Configure static file serving for images, audio, and videos."""
        try:
            # Get absolute path to project root
            current_dir = Path(__file__).parent.absolute()

            # Static directories - usar caminho do volume montado para images
            sessions = Path(f"{current_dir}/storage")

            # Create directories if they don't exist
            sessions.mkdir(parents=True, exist_ok=True)

            # Mount static file handlers
            self.app.mount("/sessions", StaticFiles(directory=str(sessions)), name="sessions")

            logger.info(f"Static files mounted: /sessions -> {sessions}")

        except Exception as e:
            logger.error(f"Error mounting static files: {e}")


    def setup_middleware(self):
        """Configure application middleware."""
        # CORS Middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
            allow_headers=["*"],
            expose_headers=["*"],
            max_age=3600
        )

# Create application instance
api = API()
app = api.app

# Add startup and shutdown events
async def lifespan(app: FastAPI):
    print("Iniciando a aplicação")
    yield
    print("Finalizando a aplicação")

# Health check endpoint
@app.get("/health", tags=["System"])
async def health_check():
    """Health check endpoint for monitoring."""
    return {
        "status": "healthy",
        "service": "telegram-bot",
        "version": "1.0.0"
    }

if __name__ == "__main__":
    # Run with uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8080,
        reload=False,
        workers=1,
        timeout_keep_alive=1200,
        timeout_graceful_shutdown=30,
        log_level="info"
    )
