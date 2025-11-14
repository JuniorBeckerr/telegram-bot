import asyncio
import time
import logging
from app.services.telegram_service import TelegramService

# Configura logging com mais detalhes
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


async def main():
    start_time = time.time()
    logger.info("🚀 Inicializando pipeline OTIMIZADO de coleta do Telegram...")
    logger.info("="*60)

    try:
        telegram_service = TelegramService()
        await telegram_service.run_all_groups()

    except KeyboardInterrupt:
        logger.warning("\n🟥 Execução interrompida manualmente.")
    except Exception as e:
        logger.error(f"\n❌ Erro fatal no pipeline: {e}", exc_info=True)
    finally:
        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)

        logger.info("="*60)
        logger.info(f"🏁 Execução finalizada em {minutes}m {seconds}s")
        logger.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())