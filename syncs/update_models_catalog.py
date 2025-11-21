"""
Comando para criar ou atualizar catálogo de modelos no Telegram

Modos de operação:
1. CRIAR (primeira execução): Cria 3 mensagens fixas e retorna seus IDs
2. ATUALIZAR (execuções seguintes): Atualiza mensagens existentes por seus IDs

Mensagens:
- Mensagem 1: A-H
- Mensagem 2: I-P
- Mensagem 3: Q-Z

Uso:
    # Criar mensagens (primeira vez)
    python update_models_catalog.py <chat_id> --create [--bot_token=TOKEN]

    # Atualizar mensagens existentes
    python update_models_catalog.py <chat_id> --message_id_ah=668 --message_id_ip=669 --message_id_qz=670 [--bot_token=TOKEN]

Exemplos:
    # Criar mensagens iniciais
    python update_models_catalog.py -1003391602003 --create

    # Atualizar mensagens existentes
    python update_models_catalog.py -1003391602003 --message_id_ah=668 --message_id_ip=669 --message_id_qz=670
"""

import argparse
import logging
import sys
import os
from typing import List, Dict, Optional
from collections import defaultdict
from datetime import datetime

from app.repository.models_repository import ModelsRepository
from app.repository.media_classifications_repository import MediaClassificationsRepository
from app.repository.publish_queue_repository import PublishQueueRepository
from app.services.bot_service import BotServiceV2, BotApiError

import asyncio

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UpdateModelsCatalog:
    """Classe para criar ou atualizar catálogo de modelos no Telegram"""

    def __init__(self, bot_token: str):
        self.models_repo = ModelsRepository()
        self.classification_repo = MediaClassificationsRepository()
        self.queue_repo = PublishQueueRepository()
        self.bot_token = bot_token
        self.bot_service = None

    def escape_model_name(self, name: str) -> str:
        """Escapa apenas caracteres que quebram Markdown nos nomes das modelos."""
        return name.replace("_", "\\_").replace("*", "\\*")

    def get_published_models(self) -> List[str]:
        """
        Busca modelos que têm publicações completadas

        Lógica:
        1. Busca registros de publish_queue com status='completed'
        2. Pega os media_ids dessas publicações
        3. Busca model_ids através de media_classifications
        4. Busca stage_names dos modelos
        5. Retorna lista única ordenada alfabeticamente

        Returns:
            Lista de stage_names únicos em ordem alfabética
        """
        logger.info("Buscando modelos com publicações completadas...")

        # 1. Busca publicações completadas
        completed_queue = (
            self.queue_repo
            .where("status", "completed")
            .get()
        )

        if not completed_queue:
            logger.warning("Nenhuma publicação completada encontrada")
            return []

        logger.info(f"Encontradas {len(completed_queue)} publicações completadas")

        # 2. Extrai media_ids únicos
        media_ids = list(set([item['media_id'] for item in completed_queue]))
        logger.info(f"Total de {len(media_ids)} mídias únicas publicadas")

        # 3. Busca classificações para essas mídias
        classifications = (
            self.classification_repo.query()
            .where_in("media_id", media_ids)
            .get()
        )

        if not classifications:
            logger.warning("Nenhuma classificação encontrada para as mídias publicadas")
            return []

        logger.info(f"Encontradas {len(classifications)} classificações")

        # 4. Extrai model_ids únicos
        model_ids = list(set([c['model_id'] for c in classifications]))
        logger.info(f"Total de {len(model_ids)} modelos classificados")

        # 5. Busca stage_names dos modelos
        models = (
            self.models_repo.query()
            .where_in("id", model_ids)
            .where_not_null("stage_name")
            .get()
        )

        if not models:
            logger.warning("Nenhum modelo encontrado com stage_name")
            return []

        # 6. Extrai e ordena stage_names
        stage_names = sorted(list(set([m['stage_name'] for m in models])))

        logger.info(f"✓ Total de {len(stage_names)} modelos únicos para publicar no catálogo")

        return stage_names

    def group_models_by_letter_range(self, models: List[str]) -> Dict[str, List[str]]:
        """
        Agrupa modelos por faixa de letras (A-H, I-P, Q-Z)

        Args:
            models: Lista de stage_names

        Returns:
            Dicionário com grupos {range: [models]}
        """
        groups = {
            'A-H': [],
            'I-P': [],
            'Q-Z': []
        }

        for model in models:
            if not model:
                continue

            first_char = model[0].upper()

            if 'A' <= first_char <= 'H':
                groups['A-H'].append(model)
            elif 'I' <= first_char <= 'P':
                groups['I-P'].append(model)
            elif 'Q' <= first_char <= 'Z':
                groups['Q-Z'].append(model)
            else:
                # Números ou caracteres especiais vão para Q-Z
                groups['Q-Z'].append(model)

        return groups

    def format_message(self, letter_range: str, models: List[str]) -> str:
        """
        Formata a mensagem para o Telegram

        Args:
            letter_range: Faixa de letras (ex: "A-H")
            models: Lista de stage_names

        Returns:
            String formatada em Markdown
        """
        if not models:
            return (
                f"📋 **CATÁLOGO DE MODELOS ({letter_range})**\n\n"
                f"_Nenhum modelo disponível nesta categoria._\n\n"
                f"🕐 Atualizado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
            )

        # Header
        message = f"📋 CATÁLOGO DE MODELOS {letter_range}\n"
        message += f"_Total: {len(models)} modelo{'s' if len(models) != 1 else ''}_\n\n"

        # Agrupa por letra inicial
        by_letter = defaultdict(list)
        for model in sorted(models):
            letter = model[0].upper()
            by_letter[letter].append(model)

        # Formata por letra
        for letter in sorted(by_letter.keys()):
            message += f"**{letter}**\n"
            for model in sorted(by_letter[letter]):
                message += f"  #{self.escape_model_name(model)}\n"
            message += "\n"

        # Footer
        message += f"🕐 Atualizado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}"

        return message

    async def create_catalog(self, chat_id: int) -> Dict[str, any]:
        """
        Cria o catálogo completo no Telegram (primeira vez)

        Args:
            chat_id: ID do chat/grupo

        Returns:
            Dicionário com estatísticas da operação e IDs das mensagens criadas
        """
        stats = {
            'total_models': 0,
            'groups': {},
            'created': 0,
            'errors': 0,
            'message_ids': {}
        }

        try:
            # 1. Busca modelos publicados
            logger.info("=" * 60)
            models = self.get_published_models()
            stats['total_models'] = len(models)

            if not models:
                logger.warning("Nenhum modelo encontrado para criar catálogo")
                return stats

            # 2. Agrupa por faixa de letras
            grouped_models = self.group_models_by_letter_range(models)

            logger.info("=" * 60)
            logger.info("DISTRIBUIÇÃO DOS MODELOS:")
            for group, model_list in grouped_models.items():
                stats['groups'][group] = len(model_list)
                logger.info(f"  {group}: {len(model_list)} modelo(s)")

            # 3. Cria cada mensagem
            logger.info("=" * 60)
            logger.info("CRIANDO MENSAGENS NO TELEGRAM:")

            for letter_range in ['A-H', 'I-P', 'Q-Z']:
                model_list = grouped_models.get(letter_range, [])

                # Formata mensagem
                message_text = self.format_message(letter_range, model_list)

                # Cria no Telegram
                message_id = await self.send_message(
                    chat_id=chat_id,
                    text=message_text
                )

                if message_id:
                    logger.info(f"  ✓ {letter_range} criado (msg_id={message_id})")
                    stats['created'] += 1
                    stats['message_ids'][letter_range] = message_id

                    # Tenta fixar a mensagem
                    pinned = await self.pin_message(chat_id, message_id)
                    if pinned:
                        logger.info(f"    → Mensagem fixada com sucesso")
                    else:
                        logger.warning(f"    → Não foi possível fixar a mensagem")
                else:
                    logger.error(f"  ✗ Falha ao criar {letter_range}")
                    stats['errors'] += 1

            return stats

        except Exception as e:
            logger.error(f"Erro ao criar catálogo: {e}", exc_info=True)
            stats['errors'] += 1
            return stats
        finally:
            # Fecha conexão do bot service
            if self.bot_service:
                await self.bot_service.close()

    async def update_catalog(
            self,
            chat_id: int,
            message_ids: Dict[str, int]
    ) -> Dict[str, any]:
        """
        Atualiza o catálogo completo no Telegram

        Args:
            chat_id: ID do chat/grupo
            message_ids: Dict com IDs das mensagens {'A-H': 123, 'I-P': 124, 'Q-Z': 125}

        Returns:
            Dicionário com estatísticas da operação
        """
        stats = {
            'total_models': 0,
            'groups': {},
            'updated': 0,
            'errors': 0
        }

        try:
            # 1. Busca modelos publicados
            logger.info("=" * 60)
            models = self.get_published_models()
            stats['total_models'] = len(models)

            if not models:
                logger.warning("Nenhum modelo encontrado para atualizar")
                return stats

            # 2. Agrupa por faixa de letras
            grouped_models = self.group_models_by_letter_range(models)

            logger.info("=" * 60)
            logger.info("DISTRIBUIÇÃO DOS MODELOS:")
            for group, model_list in grouped_models.items():
                stats['groups'][group] = len(model_list)
                logger.info(f"  {group}: {len(model_list)} modelo(s)")

            # 3. Atualiza cada mensagem
            logger.info("=" * 60)
            logger.info("ATUALIZANDO MENSAGENS NO TELEGRAM:")

            for letter_range, model_list in grouped_models.items():
                message_id = message_ids.get(letter_range)

                if not message_id:
                    logger.warning(f"  ⚠ Message ID não fornecido para {letter_range}")
                    stats['errors'] += 1
                    continue

                # Formata mensagem
                message_text = self.format_message(letter_range, model_list)

                # Atualiza no Telegram (apenas edita, não cria nova)
                success = await self.edit_message(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=message_text
                )

                if success:
                    logger.info(f"  ✓ {letter_range} atualizado (msg_id={message_id})")
                    stats['updated'] += 1
                else:
                    logger.error(f"  ✗ Falha ao atualizar {letter_range} (msg_id={message_id})")
                    stats['errors'] += 1

            return stats

        except Exception as e:
            logger.error(f"Erro ao atualizar catálogo: {e}", exc_info=True)
            stats['errors'] += 1
            return stats
        finally:
            # Fecha conexão do bot service
            if self.bot_service:
                await self.bot_service.close()

    async def send_message(self, chat_id: int, text: str) -> Optional[int]:
        """
        Envia uma nova mensagem no Telegram

        Args:
            chat_id: ID do chat
            text: Texto da mensagem

        Returns:
            ID da mensagem criada ou None se erro
        """
        try:
            if not self.bot_service:
                self.bot_service = BotServiceV2(token=self.bot_token)

            response = await self.bot_service._request(
                "sendMessage",
                chat_id=chat_id,
                text=text,
                parse_mode="Markdown"
            )

            return response.get('message_id')

        except Exception as e:
            logger.error(f"Erro ao enviar mensagem: {e}")
            return None

    async def pin_message(self, chat_id: int, message_id: int) -> bool:
        """
        Fixa uma mensagem no chat

        Args:
            chat_id: ID do chat
            message_id: ID da mensagem a fixar

        Returns:
            True se sucesso, False se erro
        """
        try:
            if not self.bot_service:
                self.bot_service = BotServiceV2(token=self.bot_token)

            await self.bot_service._request(
                "pinChatMessage",
                chat_id=chat_id,
                message_id=message_id,
                disable_notification=True
            )

            return True

        except Exception as e:
            logger.error(f"Erro ao fixar mensagem {message_id}: {e}")
            return False

    async def edit_message(self, chat_id: int, message_id: int, text: str) -> bool:
        """
        Edita uma mensagem existente (não cria nova)

        Args:
            chat_id: ID do chat
            message_id: ID da mensagem a editar
            text: Novo texto

        Returns:
            True se sucesso, False se erro
        """
        try:
            if not self.bot_service:
                self.bot_service = BotServiceV2(token=self.bot_token)

            # Apenas edita a mensagem existente
            await self.bot_service._request(
                "editMessageText",
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode="Markdown"
            )

            return True

        except Exception as e:
            logger.error(f"Erro ao editar mensagem {message_id}: {e}")
            return False


async def async_main(args):
    """Função principal assíncrona"""

    # Validações
    bot_token = args.bot_token or os.getenv("TELEGRAM_BOT_TOKEN") or "8541185101:AAGqEe1nxmD9HXlbz8ATx27YC3hU79FEbKQ"
    if not bot_token:
        logger.error("Bot token não fornecido. Use --bot_token ou configure TELEGRAM_BOT_TOKEN")
        sys.exit(1)

    # Log dos parâmetros
    logger.info("=" * 60)
    if args.create:
        logger.info("CRIANDO CATÁLOGO DE MODELOS NO TELEGRAM")
    else:
        logger.info("ATUALIZANDO CATÁLOGO DE MODELOS NO TELEGRAM")
    logger.info("=" * 60)
    logger.info(f"Chat ID: {args.chat_id}")

    if not args.create:
        logger.info(f"Message ID A-H: {args.message_id_ah}")
        logger.info(f"Message ID I-P: {args.message_id_ip}")
        logger.info(f"Message ID Q-Z: {args.message_id_qz}")

    logger.info("=" * 60)

    try:
        updater = UpdateModelsCatalog(bot_token)

        if args.create:
            # Modo criação
            stats = await updater.create_catalog(chat_id=args.chat_id)

            # Exibe resultado
            logger.info("=" * 60)
            logger.info("RESUMO DA OPERAÇÃO - CRIAÇÃO")
            logger.info("=" * 60)
            logger.info(f"Total de modelos encontrados: {stats['total_models']}")

            for group, count in stats['groups'].items():
                logger.info(f"  {group}: {count} modelo(s)")

            logger.info(f"\nMensagens criadas: {stats['created']}/3")
            logger.info(f"Erros: {stats['errors']}")

            if stats['message_ids']:
                logger.info("\n📝 IDs DAS MENSAGENS CRIADAS (salve para próximas atualizações):")
                logger.info("=" * 60)
                for letter_range in ['A-H', 'I-P', 'Q-Z']:
                    msg_id = stats['message_ids'].get(letter_range, 'N/A')
                    logger.info(f"  {letter_range}: {msg_id}")
                logger.info("\n💡 Use estes IDs no próximo comando:")
                logger.info(f"  python update_models_catalog.py {args.chat_id} \\")
                logger.info(f"    --message_id_ah={stats['message_ids'].get('A-H', 'XXX')} \\")
                logger.info(f"    --message_id_ip={stats['message_ids'].get('I-P', 'XXX')} \\")
                logger.info(f"    --message_id_qz={stats['message_ids'].get('Q-Z', 'XXX')}")

            logger.info("=" * 60)

            if stats['errors'] > 0:
                logger.warning("A operação foi concluída com erros. Verifique os logs acima.")
                sys.exit(1)

            if stats['created'] == 0:
                logger.warning("Nenhuma mensagem foi criada.")
                sys.exit(1)

            logger.info("✅ Catálogo criado com sucesso!")

        else:
            # Modo atualização
            message_ids = {
                'A-H': args.message_id_ah,
                'I-P': args.message_id_ip,
                'Q-Z': args.message_id_qz
            }

            stats = await updater.update_catalog(
                chat_id=args.chat_id,
                message_ids=message_ids
            )

            # Exibe resultado
            logger.info("=" * 60)
            logger.info("RESUMO DA OPERAÇÃO - ATUALIZAÇÃO")
            logger.info("=" * 60)
            logger.info(f"Total de modelos encontrados: {stats['total_models']}")

            for group, count in stats['groups'].items():
                logger.info(f"  {group}: {count} modelo(s)")

            logger.info(f"\nMensagens atualizadas: {stats['updated']}/3")
            logger.info(f"Erros: {stats['errors']}")
            logger.info("=" * 60)

            if stats['errors'] > 0:
                logger.warning("A operação foi concluída com erros. Verifique os logs acima.")
                sys.exit(1)

            if stats['updated'] == 0:
                logger.warning("Nenhuma mensagem foi atualizada.")
                sys.exit(1)

            logger.info("✅ Catálogo atualizado com sucesso!")

        sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("\nOperação cancelada pelo usuário")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Erro fatal: {e}", exc_info=True)
        sys.exit(1)


def main():
    """Função principal do comando"""
    parser = argparse.ArgumentParser(
        description='Cria ou atualiza catálogo de modelos em mensagens do Telegram',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:

  1. CRIAR mensagens (primeira vez):
     python update_models_catalog.py -1003391602003 --create
     
  2. ATUALIZAR mensagens existentes:
     python update_models_catalog.py -1003391602003 \\
       --message_id_ah=668 --message_id_ip=669 --message_id_qz=670

O script busca modelos que têm mídias publicadas (status='completed' em publish_queue)
e cria ou atualiza 3 mensagens fixas no Telegram com a lista ordenada alfabeticamente.
        """
    )

    parser.add_argument(
        'chat_id',
        type=int,
        help='ID do chat/grupo do Telegram'
    )

    parser.add_argument(
        '--create',
        action='store_true',
        help='Cria as mensagens iniciais (primeira execução)'
    )

    parser.add_argument(
        '--message_id_ah',
        type=int,
        default=None,
        help='ID da mensagem para modelos A-H (obrigatório se não usar --create)'
    )

    parser.add_argument(
        '--message_id_ip',
        type=int,
        default=None,
        help='ID da mensagem para modelos I-P (obrigatório se não usar --create)'
    )

    parser.add_argument(
        '--message_id_qz',
        type=int,
        default=None,
        help='ID da mensagem para modelos Q-Z (obrigatório se não usar --create)'
    )

    parser.add_argument(
        '--bot_token',
        type=str,
        default=None,
        help='Token do bot do Telegram (ou use a variável TELEGRAM_BOT_TOKEN)'
    )

    args = parser.parse_args()

    # Validações
    if not args.create:
        if not all([args.message_id_ah, args.message_id_ip, args.message_id_qz]):
            parser.error("Quando não usar --create, todos os message_ids são obrigatórios (--message_id_ah, --message_id_ip, --message_id_qz)")

    # Executa de forma assíncrona
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()

# EXEMPLOS DE USO:
#
# Criar mensagens (primeira vez):
# python -m syncs.update_models_catalog -1003391602003 --create
#
# Atualizar mensagens existentes:
# python -m syncs.update_models_catalog -1003391602003 --message_id_ah=1387 --message_id_ip=1389 --message_id_qz=1391