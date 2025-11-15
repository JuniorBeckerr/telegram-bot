import asyncio
import time
import logging
from datetime import datetime
from app.services.telegram_service import TelegramServiceProduction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


async def run_batch():
    """Executa um batch de processamento"""
    service = TelegramServiceProduction()
    await service.run_all_groups()


async def main_continuous():
    """Roda continuamente com intervalo"""
    logger.info("="*60)
    logger.info("🚀 PRODUCTION MODE - 24/7")
    logger.info("="*60)

    batch_count = 0

    while True:
        try:
            batch_count += 1
            batch_start = time.time()

            logger.info(f"\n{'#'*60}")
            logger.info(f"📦 BATCH #{batch_count}")
            logger.info(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"{'#'*60}\n")

            await run_batch()

            batch_elapsed = time.time() - batch_start
            logger.info(f"\n✅ Batch #{batch_count} finalizado em {batch_elapsed/60:.1f} min")

            # Aguarda 2 horas
            logger.info(f"⏳ Aguardando 2 horas para próximo batch...")
            logger.info(f"   Próximo batch: ~{datetime.now().strftime('%H:%M')} + 2h")
            await asyncio.sleep(7200)  # 2 horas

        except KeyboardInterrupt:
            logger.warning("\n🛑 Interrompido pelo usuário")
            break
        except Exception as e:
            logger.error(f"\n❌ Erro no batch: {e}", exc_info=True)
            logger.info("⏳ Aguardando 5 minutos antes de tentar novamente...")
            await asyncio.sleep(300)


async def main_once():
    """Executa apenas uma vez"""
    start_time = time.time()

    logger.info("="*60)
    logger.info("🚀 PRODUCTION MODE")
    logger.info("="*60)

    try:
        await run_batch()
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
    import sys

    # Use --continuous para rodar 24/7
    if "--continuous" in sys.argv or "-c" in sys.argv:
        asyncio.run(main_continuous())
    else:
        asyncio.run(main_once())