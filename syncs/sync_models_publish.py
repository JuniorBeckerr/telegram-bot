"""
Comando para adicionar mídias aprovadas de modelos específicas na fila de publicação

Uso:
    python sync_models_publish.py <target_group_id> [--model_id=84] [--priority=5] [--scheduled_at="YYYY-MM-DD HH:MM:SS"] [--workers=4]

Exemplo:
    # Processar todas as models
    python sync_models_publish.py -1003391602003

    # Processar modelo específica
    python sync_models_publish.py -1003391602003 --model_id=84

    # Com prioridade e workers
    python sync_models_publish.py -1003391602003 --priority=10 --workers=8

    # Com agendamento
    python sync_models_publish.py -1003391602003 --model_id=84 --priority=8 --scheduled_at="2025-11-21 10:00:00"
"""

import argparse
import logging
import sys
from typing import Optional, List
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from app.repository.media_classifications_repository import MediaClassificationsRepository
from app.repository.media_repository import MediaRepository
from app.repository.publish_queue_repository import PublishQueueRepository
from app.repository.models_repository import ModelsRepository

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)s] - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Lock para operações thread-safe
stats_lock = Lock()


class SyncModelsPublish:
    """Classe para sincronizar mídias aprovadas de modelos na fila de publicação"""

    def __init__(self):
        self.media_repo = MediaRepository()
        self.classification_repo = MediaClassificationsRepository()
        self.queue_repo = PublishQueueRepository()
        self.models_repo = ModelsRepository()

    def get_all_models(self) -> List[dict]:
        """
        Busca todas as models cadastradas

        Returns:
            Lista de models com seus IDs
        """
        logger.info("Buscando todas as models cadastradas")
        models = self.models_repo.query().whereNotIn('id', [84]).get()

        if not models:
            logger.warning("Nenhuma model encontrada no banco de dados")
            return []

        logger.info(f"Encontradas {len(models)} models cadastradas")
        return models

    def get_approved_media_for_model(self, model_id: int) -> List[int]:
        """
        Busca todas as mídias aprovadas para uma modelo específica

        Args:
            model_id: ID da modelo (model_id na tabela media_classifications)

        Returns:
            Lista de IDs de mídias aprovadas
        """
        logger.info(f"Buscando mídias aprovadas para model_id={model_id}")

        # Busca as classificações da modelo
        classifications = (
            self.classification_repo
            .where("model_id", model_id)
            .get()
        )

        if not classifications:
            logger.warning(f"Nenhuma classificação encontrada para model_id={model_id}")
            return []

        # Extrai os media_ids
        media_ids = [c['media_id'] for c in classifications]

        logger.info(f"Encontradas {len(media_ids)} classificações para model_id={model_id}")

        # Busca mídias aprovadas com esses IDs
        approved_media = (
            self.media_repo.query()
            .where_in("id", media_ids)
            .where("state", "approved")
            .get()
        )

        approved_count = len(approved_media)
        logger.info(f"Encontradas {approved_count} mídias aprovadas para model_id={model_id}")

        return [media['id'] for media in approved_media]

    def process_single_media(
            self,
            media_id: int,
            target_group_id: int,
            priority: int,
            scheduled_at: Optional[str]
    ) -> dict:
        """
        Processa uma única mídia (adiciona na fila)

        Args:
            media_id: ID da mídia
            target_group_id: ID do grupo de destino
            priority: Prioridade
            scheduled_at: Data/hora agendada

        Returns:
            Dicionário com resultado da operação
        """
        result = {'added': 0, 'skipped': 0, 'errors': 0}

        try:
            # Verifica se já existe na fila (evita duplicatas)
            already_in_queue = self.queue_repo.is_in_queue(target_group_id, media_id)

            if already_in_queue:
                logger.debug(f"Mídia {media_id} já está na fila do grupo {target_group_id}, pulando...")
                result['skipped'] = 1
                return result

            # Adiciona na fila
            self.queue_repo.add_to_queue(
                group_id=target_group_id,
                media_id=media_id,
                priority=priority,
                scheduled_at=scheduled_at
            )

            result['added'] = 1
            logger.debug(f"Mídia {media_id} adicionada à fila com sucesso")

        except Exception as e:
            logger.error(f"Erro ao adicionar mídia {media_id} na fila: {e}")
            result['errors'] = 1

        return result

    def add_to_queue_parallel(
            self,
            model_id: int,
            target_group_id: int,
            priority: int = 5,
            scheduled_at: Optional[str] = None,
            workers: int = 4
    ) -> dict:
        """
        Adiciona mídias aprovadas de uma modelo na fila de publicação usando workers paralelos

        Args:
            model_id: ID da modelo
            target_group_id: ID do grupo de destino onde será publicado
            priority: Prioridade (1-10, default=5)
            scheduled_at: Data/hora agendada (opcional, formato: YYYY-MM-DD HH:MM:SS)
            workers: Número de workers paralelos

        Returns:
            Dicionário com estatísticas da operação
        """
        stats = {
            'model_id': model_id,
            'total_found': 0,
            'added': 0,
            'skipped': 0,
            'errors': 0
        }

        # Busca mídias aprovadas
        approved_media_ids = self.get_approved_media_for_model(model_id)
        stats['total_found'] = len(approved_media_ids)

        if not approved_media_ids:
            logger.warning(f"Nenhuma mídia aprovada encontrada para model_id={model_id}")
            return stats

        logger.info(f"Processando {len(approved_media_ids)} mídias da model {model_id} com {workers} workers")

        # Processa mídias em paralelo
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"Model-{model_id}") as executor:
            futures = {
                executor.submit(
                    self.process_single_media,
                    media_id,
                    target_group_id,
                    priority,
                    scheduled_at
                ): media_id
                for media_id in approved_media_ids
            }

            # Aguarda conclusão e agrega resultados
            for future in as_completed(futures):
                media_id = futures[future]
                try:
                    result = future.result()
                    with stats_lock:
                        stats['added'] += result['added']
                        stats['skipped'] += result['skipped']
                        stats['errors'] += result['errors']
                except Exception as e:
                    logger.error(f"Erro ao processar mídia {media_id}: {e}")
                    with stats_lock:
                        stats['errors'] += 1

        return stats

    def add_all_models_to_queue(
            self,
            target_group_id: int,
            priority: int = 5,
            scheduled_at: Optional[str] = None,
            workers: int = 4
    ) -> dict:
        """
        Adiciona mídias aprovadas de TODAS as models na fila de publicação

        Args:
            target_group_id: ID do grupo de destino onde será publicado
            priority: Prioridade (1-10, default=5)
            scheduled_at: Data/hora agendada (opcional)
            workers: Número de workers paralelos

        Returns:
            Dicionário com estatísticas da operação
        """
        global_stats = {
            'total_models': 0,
            'models_processed': 0,
            'total_media_found': 0,
            'total_added': 0,
            'total_skipped': 0,
            'total_errors': 0,
            'models_stats': []
        }

        # Busca todas as models
        models = self.get_all_models()
        global_stats['total_models'] = len(models)

        if not models:
            logger.warning("Nenhuma model encontrada para processar")
            return global_stats

        logger.info(f"Iniciando processamento de {len(models)} models")

        # Processa cada model
        for model in models:
            model_id = model['id']
            model_name = model.get('name', f'Model {model_id}')

            logger.info(f"Processando model: {model_name} (ID: {model_id})")

            try:
                # Processa essa model
                model_stats = self.add_to_queue_parallel(
                    model_id=model_id,
                    target_group_id=target_group_id,
                    priority=priority,
                    scheduled_at=scheduled_at,
                    workers=workers
                )

                # Agrega estatísticas
                global_stats['models_processed'] += 1
                global_stats['total_media_found'] += model_stats['total_found']
                global_stats['total_added'] += model_stats['added']
                global_stats['total_skipped'] += model_stats['skipped']
                global_stats['total_errors'] += model_stats['errors']

                # Adiciona stats individuais da model
                model_stats['model_name'] = model_name
                global_stats['models_stats'].append(model_stats)

                logger.info(
                    f"Model {model_name} concluída: "
                    f"{model_stats['added']} adicionadas, "
                    f"{model_stats['skipped']} puladas, "
                    f"{model_stats['errors']} erros"
                )

            except Exception as e:
                logger.error(f"Erro ao processar model {model_name} (ID: {model_id}): {e}")
                global_stats['total_errors'] += 1

        return global_stats


def main():
    """Função principal do comando"""
    parser = argparse.ArgumentParser(
        description='Adiciona mídias aprovadas de modelos na fila de publicação',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Processar todas as models
  python sync_models_publish.py -1003391602003
  
  # Processar modelo específica
  python sync_models_publish.py -1003391602003 --model_id=84
  
  # Com workers paralelos
  python sync_models_publish.py -1003391602003 --workers=8
  
  # Com prioridade e agendamento
  python sync_models_publish.py -1003391602003 --model_id=84 --priority=10 --scheduled_at="2025-11-21 10:00:00"
        """
    )

    parser.add_argument(
        'target_group_id',
        type=int,
        help='ID do grupo de destino onde as mídias serão publicadas'
    )

    parser.add_argument(
        '--model_id',
        type=int,
        default=None,
        help='ID da modelo (opcional - se não informado, processa todas)'
    )

    parser.add_argument(
        '--priority',
        type=int,
        default=5,
        help='Prioridade da publicação (1-10, padrão: 5)'
    )

    parser.add_argument(
        '--scheduled_at',
        type=str,
        default=None,
        help='Data/hora agendada para publicação (formato: YYYY-MM-DD HH:MM:SS)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Número de workers paralelos (padrão: 4)'
    )

    args = parser.parse_args()

    # Valida prioridade
    if not 1 <= args.priority <= 10:
        logger.warning(f"Prioridade {args.priority} fora do range 1-10, ajustando para 5")
        args.priority = 5

    # Valida workers
    if args.workers < 1:
        logger.warning(f"Número de workers {args.workers} inválido, ajustando para 4")
        args.workers = 4

    # Valida scheduled_at se fornecido
    if args.scheduled_at:
        try:
            datetime.strptime(args.scheduled_at, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            logger.error(f"Formato de data inválido: {args.scheduled_at}. Use: YYYY-MM-DD HH:MM:SS")
            sys.exit(1)

    # Log dos parâmetros
    logger.info("=" * 60)
    logger.info("Iniciando sincronização de mídias para publicação")
    logger.info("=" * 60)
    logger.info(f"Model ID: {args.model_id or 'TODAS AS MODELS'}")
    logger.info(f"Grupo de destino: {args.target_group_id}")
    logger.info(f"Prioridade: {args.priority}")
    logger.info(f"Workers: {args.workers}")
    logger.info(f"Agendamento: {args.scheduled_at or 'Imediato'}")
    logger.info("=" * 60)

    try:
        syncer = SyncModelsPublish()

        # Processa model específica ou todas
        if args.model_id:
            # Processa apenas uma model
            stats = syncer.add_to_queue_parallel(
                model_id=args.model_id,
                target_group_id=args.target_group_id,
                priority=args.priority,
                scheduled_at=args.scheduled_at,
                workers=args.workers
            )

            # Exibe resultado
            logger.info("=" * 60)
            logger.info("RESUMO DA OPERAÇÃO")
            logger.info("=" * 60)
            logger.info(f"Model ID: {args.model_id}")
            logger.info(f"Total de mídias aprovadas encontradas: {stats['total_found']}")
            logger.info(f"Adicionadas à fila: {stats['added']}")
            logger.info(f"Já existiam na fila (puladas): {stats['skipped']}")
            logger.info(f"Erros: {stats['errors']}")
            logger.info("=" * 60)

            if stats['errors'] > 0:
                logger.warning("A operação foi concluída com erros. Verifique os logs acima.")
                sys.exit(1)

            if stats['added'] == 0:
                logger.warning("Nenhuma mídia foi adicionada à fila.")
                sys.exit(0)

        else:
            # Processa todas as models
            stats = syncer.add_all_models_to_queue(
                target_group_id=args.target_group_id,
                priority=args.priority,
                scheduled_at=args.scheduled_at,
                workers=args.workers
            )

            # Exibe resultado
            logger.info("=" * 60)
            logger.info("RESUMO GERAL DA OPERAÇÃO")
            logger.info("=" * 60)
            logger.info(f"Total de models processadas: {stats['models_processed']}/{stats['total_models']}")
            logger.info(f"Total de mídias encontradas: {stats['total_media_found']}")
            logger.info(f"Total adicionadas à fila: {stats['total_added']}")
            logger.info(f"Total já existiam (puladas): {stats['total_skipped']}")
            logger.info(f"Total de erros: {stats['total_errors']}")
            logger.info("=" * 60)

            # Exibe detalhes por model
            if stats['models_stats']:
                logger.info("\nDETALHES POR MODEL:")
                logger.info("-" * 60)
                for model_stat in stats['models_stats']:
                    if model_stat['total_found'] > 0:
                        logger.info(
                            f"{model_stat['model_name']} (ID: {model_stat['model_id']}): "
                            f"{model_stat['added']} adicionadas, "
                            f"{model_stat['skipped']} puladas, "
                            f"{model_stat['errors']} erros"
                        )
                logger.info("=" * 60)

            if stats['total_errors'] > 0:
                logger.warning("A operação foi concluída com erros. Verifique os logs acima.")
                sys.exit(1)

            if stats['total_added'] == 0:
                logger.warning("Nenhuma mídia foi adicionada à fila.")
                sys.exit(0)

        logger.info("Sincronização concluída com sucesso!")
        sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("\nOperação cancelada pelo usuário")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Erro fatal: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

    # python sync_models_publish.py -1003391602003 --priority=10 --workers=16