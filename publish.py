"""
Main Publisher V3 - Máxima Performance
Processa múltiplos modelos em paralelo usando todos os recursos do servidor
"""
import asyncio
import argparse
import logging
import os

# Importa a versão otimizada
from app.services.publisher_service import PublisherServiceV3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


def get_optimal_workers():
    """Calcula workers ótimos baseado no hardware"""
    cpu_count = os.cpu_count() or 4

    # Recomendações para servidor com 8 cores, 24GB RAM:
    # - download_workers: 12-16 (I/O bound)
    # - model_workers: 4-6 (parallel albums)
    # - thumb_workers: 6-8 (CPU bound FFmpeg)

    return {
        "download": min(cpu_count * 2, 16),
        "model": min(cpu_count // 2, 6),
        "thumb": min(cpu_count, 8)
    }


async def main():
    parser = argparse.ArgumentParser(
        description="Publisher V3 - Máxima Performance com Processamento Paralelo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  # Execução padrão otimizada
  python main_publisher_v3.py

  # Máxima performance para servidor potente
  python main_publisher_v3.py --download-workers 16 --model-workers 6 --thumb-workers 8

  # Loop contínuo
  python main_publisher_v3.py --loop --interval 20

  # Conservador (menos recursos)
  python main_publisher_v3.py --download-workers 6 --model-workers 2 --thumb-workers 4
        """
    )

    # Calcula valores ótimos
    optimal = get_optimal_workers()

    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Limite de itens por execução (padrão: 100)"
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        default=optimal["download"],
        help=f"Workers para download paralelo (padrão: {optimal['download']})"
    )
    parser.add_argument(
        "--model-workers",
        type=int,
        default=optimal["model"],
        help=f"Modelos processados em paralelo (padrão: {optimal['model']})"
    )
    parser.add_argument(
        "--thumb-workers",
        type=int,
        default=optimal["thumb"],
        help=f"Threads para FFmpeg/thumbnails (padrão: {optimal['thumb']})"
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Executa em loop contínuo"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=20,
        help="Intervalo entre execuções em segundos (padrão: 20)"
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
        help="Ativa logs detalhados"
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Inicializa o publisher
    publisher = PublisherServiceV3(
        download_workers=args.download_workers,
        model_workers=args.model_workers,
        thumb_workers=args.thumb_workers
    )

    try:
        logger.info("=" * 60)
        logger.info("🚀 PUBLISHER V3 - MÁXIMA PERFORMANCE")
        logger.info("=" * 60)
        logger.info(f"⚙️  Configuração:")
        logger.info(f"   • Download workers: {args.download_workers}")
        logger.info(f"   • Model workers: {args.model_workers} (paralelo)")
        logger.info(f"   • Thumb workers: {args.thumb_workers}")
        logger.info(f"   • Limite: {args.limit} itens")

        if args.group:
            logger.info(f"   • Grupo: {args.group}")
        logger.info("=" * 60)

        if args.loop:
            logger.info(f"🔄 Modo loop (intervalo: {args.interval}s)")

            iteration = 0
            while True:
                iteration += 1

                status = publisher.get_queue_status()
                pending = status.get("pending", 0)

                if pending > 0:
                    logger.info(f"\n📍 Iteração #{iteration} - {pending} pendentes")

                    import time
                    start = time.time()

                    await publisher.process_queue(
                        group_id=args.group,
                        limit=args.limit
                    )

                    elapsed = time.time() - start
                    logger.info(f"⏱️  Tempo: {elapsed:.1f}s")
                else:
                    logger.info(f"📭 Fila vazia (#{iteration})")

                await asyncio.sleep(args.interval)
        else:
            # Execução única
            status = publisher.get_queue_status()
            pending = status.get("pending", 0)

            logger.info(f"📊 Status: {status}")

            if pending == 0:
                logger.info("📭 Nenhum item pendente")
                return

            import time
            start = time.time()

            await publisher.process_queue(
                group_id=args.group,
                limit=args.limit
            )

            elapsed = time.time() - start

            final_status = publisher.get_queue_status()
            logger.info("=" * 60)
            logger.info(f"📊 Status final: {final_status}")
            logger.info(f"⏱️  Tempo total: {elapsed:.1f}s")
            logger.info(f"📈 Performance: {pending/elapsed:.1f} mídias/s")
            logger.info("=" * 60)

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