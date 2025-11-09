# from core.telegram_downloader import run_workers
# from config.settings import NUM_WORKERS
#
# if __name__ == "__main__":
#     print(f"🚀 Iniciando processamento com {NUM_WORKERS} worker(s)...")
#     run_workers()
#     print("🏁 Finalizado com sucesso.")
#

import asyncio
import time
import logging
from app.services.telegram_service import TelegramService
from app.repository.groups_repository import GroupsRepository

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


async def main():
    start_time = time.time()
    logger.info("🚀 Inicializando pipeline de coleta do Telegram...")

    try:
        telegram_service = TelegramService()
        await telegram_service.run_all_groups()

    except KeyboardInterrupt:
        logger.warning("🟥 Execução interrompida manualmente.")
    except Exception as e:
        logger.error(f"❌ Erro fatal no pipeline: {e}", exc_info=True)
    finally:
        elapsed = time.time() - start_time
        logger.info(f"🏁 Execução finalizada em {elapsed:.2f}s.")


if __name__ == "__main__":
    asyncio.run(main())
