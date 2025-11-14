import asyncio
import time
import logging
from app.services.telegram_service import TelegramServiceBalanced

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


async def main():
    start_time = time.time()

    logger.info("="*60)
    logger.info("🚀 MODO BALANCEADO - 20k msgs/dia")
    logger.info("="*60)

    try:
        service = TelegramServiceBalanced()
        await service.run_all_groups()

    except KeyboardInterrupt:
        logger.warning("\n🟥 Interrompido")
    except Exception as e:
        logger.error(f"\n❌ Erro: {e}", exc_info=True)
    finally:
        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)

        logger.info("")
        logger.info("="*60)
        logger.info(f"🏁 Concluído em {minutes}m {seconds}s")
        logger.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())