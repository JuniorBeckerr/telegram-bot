"""
Comando para adicionar mídias aprovadas de modelos específicas na fila de publicação

Uso:
    python sync_models_publish.py <model_id> <target_group_id> [--priority=5] [--scheduled_at="YYYY-MM-DD HH:MM:SS"]

Exemplo:
    python sync_models_publish.py 84 -1003391602003
    python sync_models_publish.py 84 -1003391602003 --priority=10
    python sync_models_publish.py 84 -1003391602003 --priority=8 --scheduled_at="2025-11-21 10:00:00"
"""

import argparse
import logging
import sys
from typing import Optional
from datetime import datetime

from app.repository.media_classifications_repository import MediaClassificationsRepository
from app.repository.media_repository import MediaRepository
from app.repository.publish_queue_repository import PublishQueueRepository

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SyncModelsPublish:
    """Classe para sincronizar mídias aprovadas de modelos na fila de publicação"""

    def __init__(self):
        self.media_repo = MediaRepository()
        self.classification_repo = MediaClassificationsRepository()
        self.queue_repo = PublishQueueRepository()

    def get_approved_media_for_model(self, model_id: int):
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

    def add_to_queue(
            self,
            model_id: int,
            target_group_id: int,
            priority: int = 10,
            scheduled_at: Optional[str] = None
    ) -> dict:
        """
        Adiciona mídias aprovadas de uma modelo na fila de publicação

        Args:
            model_id: ID da modelo
            target_group_id: ID do grupo de destino onde será publicado
            priority: Prioridade (1-10, default=5)
            scheduled_at: Data/hora agendada (opcional, formato: YYYY-MM-DD HH:MM:SS)

        Returns:
            Dicionário com estatísticas da operação
        """
        stats = {
            'total_found': 0,
            'added': 0,
            'skipped': 0,
            'errors': 0
        }

        # Valida prioridade
        if not 1 <= priority <= 10:
            logger.warning(f"Prioridade {priority} fora do range 1-10, ajustando para 5")
            priority = 5

        # Valida scheduled_at se fornecido
        if scheduled_at:
            try:
                datetime.strptime(scheduled_at, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                logger.error(f"Formato de data inválido: {scheduled_at}. Use: YYYY-MM-DD HH:MM:SS")
                return stats

        # Busca mídias aprovadas
        approved_media_ids = self.get_approved_media_for_model(model_id)
        stats['total_found'] = len(approved_media_ids)

        if not approved_media_ids:
            logger.warning(f"Nenhuma mídia aprovada encontrada para model_id={model_id}")
            return stats

        logger.info(f"Processando {len(approved_media_ids)} mídias para adicionar na fila")

        # Adiciona cada mídia na fila
        for media_id in approved_media_ids:
            try:
                # Verifica se já existe na fila (evita duplicatas)
                already_in_queue = self.queue_repo.is_in_queue(target_group_id, media_id)

                if already_in_queue:
                    logger.debug(f"Mídia {media_id} já está na fila do grupo {target_group_id}, pulando...")
                    stats['skipped'] += 1
                    continue

                # Adiciona na fila
                self.queue_repo.add_to_queue(
                    group_id=target_group_id,
                    media_id=media_id,
                    priority=priority,
                    scheduled_at=scheduled_at
                )

                stats['added'] += 1
                logger.debug(f"Mídia {media_id} adicionada à fila com sucesso")

            except Exception as e:
                logger.error(f"Erro ao adicionar mídia {media_id} na fila: {e}")
                stats['errors'] += 1

        return stats


def main():
    """Função principal do comando"""
    parser = argparse.ArgumentParser(
        description='Adiciona mídias aprovadas de uma modelo na fila de publicação',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python sync_models_publish.py 84 -1003391602003
  python sync_models_publish.py 84 -1003391602003 --priority=10
  python sync_models_publish.py 84 -1003391602003 --priority=8 --scheduled_at="2025-11-21 10:00:00"
        """
    )

    parser.add_argument(
        'model_id',
        type=int,
        help='ID da modelo (model_id na tabela media_classifications)'
    )

    parser.add_argument(
        'target_group_id',
        type=int,
        help='ID do grupo de destino onde as mídias serão publicadas'
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

    args = parser.parse_args()

    # Log dos parâmetros
    logger.info("=" * 60)
    logger.info("Iniciando sincronização de mídias para publicação")
    logger.info("=" * 60)
    logger.info(f"Model ID: {args.model_id}")
    logger.info(f"Grupo de destino: {args.target_group_id}")
    logger.info(f"Prioridade: {args.priority}")
    logger.info(f"Agendamento: {args.scheduled_at or 'Imediato'}")
    logger.info("=" * 60)

    try:
        # Executa a sincronização
        syncer = SyncModelsPublish()
        stats = syncer.add_to_queue(
            model_id=args.model_id,
            target_group_id=args.target_group_id,
            priority=args.priority,
            scheduled_at=args.scheduled_at
        )

        # Exibe resultado
        logger.info("=" * 60)
        logger.info("RESUMO DA OPERAÇÃO")
        logger.info("=" * 60)
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