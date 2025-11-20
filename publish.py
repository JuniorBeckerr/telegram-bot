"""
Main Publisher V2
Processa a fila publish_queue usando download workers e FormData
"""
import asyncio
import argparse
import logging

# Importa a nova versão do service
from app.services.publisher_service import PublisherServiceV2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(
        description="Publisher V2 - Processa fila com download paralelo e FormData"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Limite de itens por execução (padrão: 10)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Número de workers para download paralelo (padrão: 5)"
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Executa em loop contínuo"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Intervalo entre execuções em segundos (padrão: 60)"
    )
    parser.add_argument(
        "--group",
        type=int,
        default=None,
        help="Processa apenas um grupo específico"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Ativa modo debug com logs detalhados"
    )

    args = parser.parse_args()

    # Configura nível de log
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("🔍 Modo debug ativado")

    # Inicializa o publisher
    publisher = PublisherServiceV2(download_workers=args.workers)

    try:
        logger.info(f"🚀 Publisher V2 iniciado")
        logger.info(f"   📥 Download workers: {args.workers}")
        logger.info(f"   📊 Limite por execução: {args.limit}")

        if args.group:
            logger.info(f"   🎯 Grupo específico: {args.group}")

        if args.loop:
            # Modo loop contínuo
            logger.info(f"   🔄 Modo loop (intervalo: {args.interval}s)")

            iteration = 0
            while True:
                iteration += 1
                logger.info(f"\n{'='*50}")
                logger.info(f"📍 Iteração #{iteration}")

                status = publisher.get_queue_status()
                pending = status.get("pending", 0)
                processing = status.get("processing", 0)

                logger.info(f"📋 Status: {pending} pendentes, {processing} processando")

                if pending > 0:
                    await publisher.process_queue(
                        group_id=args.group,
                        limit=args.limit
                    )
                else:
                    logger.info("📭 Fila vazia")

                logger.info(f"💤 Aguardando {args.interval}s...")
                await asyncio.sleep(args.interval)
        else:
            # Execução única
            status = publisher.get_queue_status()
            logger.info(f"📊 Status da fila: {status}")

            pending = status.get("pending", 0)
            if pending == 0:
                logger.info("📭 Nenhum item pendente na fila")
                return

            await publisher.process_queue(
                group_id=args.group,
                limit=args.limit
            )

            # Status final
            final_status = publisher.get_queue_status()
            logger.info(f"📊 Status final: {final_status}")

    except KeyboardInterrupt:
        logger.info("\n🛑 Interrompido pelo usuário")
    except Exception as e:
        logger.error(f"❌ Erro fatal: {e}", exc_info=True)
        raise
    finally:
        logger.info("🧹 Limpando recursos...")
        await publisher.close()
        logger.info("✅ Finalizado")


if __name__ == "__main__":
    asyncio.run(main())