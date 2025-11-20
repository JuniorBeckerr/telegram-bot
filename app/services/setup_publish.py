"""
Setup e Exemplo de Uso do Sistema de Publicação

Este arquivo demonstra como:
1. Configurar o banco de dados
2. Cadastrar bots
3. Configurar grupos para publicação
4. Criar regras de publicação
5. Executar o publisher
"""
import asyncio
import logging
from app.repository.groups_repository import GroupsRepository
from app.repository.publisher_repositories import (
    BotsRepository,
    GroupBotsRepository,
    PublishRulesRepository,
    PublishQueueRepository,
    GroupPublishRepository
)
from app.services.publisher_service import PublisherService
from app.services.bot_service import BotService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class PublisherSetup:
    """Utilitário para configurar o sistema de publicação"""

    def __init__(self):
        self.groups_repo = GroupsRepository()
        self.bots_repo = BotsRepository()
        self.group_bots_repo = GroupBotsRepository()
        self.rules_repo = PublishRulesRepository()
        self.queue_repo = PublishQueueRepository()
        self.publish_repo = GroupPublishRepository()

    # =====================================================
    # CONFIGURAÇÃO DE GRUPOS
    # =====================================================

    def set_group_as_owner(self, group_id: int,
                           publish_enabled: bool = True,
                           publish_interval_minutes: int = 60):
        """
        Marca um grupo como sendo de sua propriedade.

        Args:
            group_id: ID do grupo no Telegram
            publish_enabled: Habilitar publicação
            publish_interval_minutes: Intervalo mínimo entre publicações
        """
        group = self.groups_repo.find(group_id)

        if not group:
            logger.error(f"❌ Grupo {group_id} não encontrado")
            return False

        self.groups_repo.update(group_id, {
            "is_owner": 1,
            "publish_enabled": 1 if publish_enabled else 0,
            "publish_interval_minutes": publish_interval_minutes
        })

        logger.info(f"✅ Grupo '{group.get('title', group_id)}' configurado como owner")
        return True

    def disable_publish(self, group_id: int):
        """Desabilita publicação em um grupo"""
        self.groups_repo.update(group_id, {"publish_enabled": 0})
        logger.info(f"⏸️ Publicação desabilitada para grupo {group_id}")

    # =====================================================
    # CONFIGURAÇÃO DE BOTS
    # =====================================================

    async def add_bot(self, token: str, name: str = None) -> int:
        """
        Adiciona um bot ao sistema.

        Args:
            token: Token do bot (obtido do @BotFather)
            name: Nome descritivo (opcional)

        Returns:
            ID do bot criado
        """
        # Valida o token consultando a API
        bot_service = BotService(token)

        try:
            me = await bot_service.get_me()
            username = me.get("username", "")

            if not name:
                name = me.get("first_name", username)

            # Verifica se já existe
            existing = self.bots_repo.get_by_token(token)
            if existing:
                logger.warning(f"⚠️ Bot @{username} já cadastrado (ID: {existing['id']})")
                return existing["id"]

            # Cadastra
            bot_id = self.bots_repo.create({
                "name": name,
                "token": token,
                "username": username,
                "active": 1
            })

            logger.info(f"✅ Bot @{username} cadastrado (ID: {bot_id})")
            return bot_id

        except Exception as e:
            logger.error(f"❌ Erro ao validar bot: {e}")
            raise
        finally:
            await bot_service.close()

    def link_bot_to_group(self, group_id: int, bot_id: int):
        """
        Vincula um bot a um grupo para publicação.

        Args:
            group_id: ID do grupo
            bot_id: ID do bot
        """
        # Verifica se já existe vínculo
        existing = (self.group_bots_repo
                    .where("group_id", group_id)
                    .where("bot_id", bot_id)
                    .first())

        if existing:
            logger.info(f"✅ Vínculo já existe (ID: {existing['id']})")
            return existing["id"]

        link_id = self.group_bots_repo.create({
            "group_id": group_id,
            "bot_id": bot_id,
            "is_publisher": 1
        })

        logger.info(f"✅ Bot {bot_id} vinculado ao grupo {group_id}")
        return link_id

    # =====================================================
    # CONFIGURAÇÃO DE REGRAS
    # =====================================================

    def create_publish_rule(self,
                            destination_group_id: int,
                            source_group_id: int = None,
                            **kwargs) -> int:
        """
        Cria uma regra de publicação.

        Args:
            destination_group_id: Grupo onde publicar
            source_group_id: Grupo fonte das mídias (opcional)
            **kwargs: Opções adicionais:
                - approval_required: bool - Só publica aprovadas
                - auto_publish: bool - Enfileira automaticamente
                - daily_limit: int - Limite diário
                - hourly_limit: int - Limite por hora
                - classification_filter: str - Filtro de classificação
                - caption_template: str - Template de caption
                - priority: int - Prioridade

        Returns:
            ID da regra criada
        """
        rule_id = self.rules_repo.create_rule(
            group_id=destination_group_id,
            source_group_id=source_group_id,
            **kwargs
        )

        logger.info(f"✅ Regra {rule_id} criada para grupo {destination_group_id}")
        return rule_id

    # =====================================================
    # UTILITÁRIOS
    # =====================================================

    def list_owner_groups(self):
        """Lista todos os grupos onde você é owner"""
        groups = self.groups_repo.where("is_owner", 1).get()

        logger.info(f"\n📋 {len(groups)} grupo(s) próprio(s):")
        for group in groups:
            status = "✅" if group.get("publish_enabled") else "⏸️"
            logger.info(f"  {status} {group['title']} (ID: {group['id']})")

        return groups

    def list_bots(self):
        """Lista todos os bots cadastrados"""
        bots = self.bots_repo.get_active()

        logger.info(f"\n🤖 {len(bots)} bot(s) ativo(s):")
        for bot in bots:
            logger.info(f"  • @{bot['username']} - {bot['name']} (ID: {bot['id']})")

        return bots

    def get_queue_status(self):
        """Mostra status da fila de publicação"""
        stats = self.queue_repo.get_queue_stats()

        logger.info("\n📊 Status da Fila:")
        for stat in stats:
            logger.info(f"  • {stat['status']}: {stat['count']}")

        return stats


# =====================================================
# EXEMPLOS DE USO
# =====================================================

async def example_full_setup():
    """
    Exemplo completo de configuração do sistema.

    Este exemplo mostra como:
    1. Marcar grupo como owner
    2. Cadastrar bot
    3. Vincular bot ao grupo
    4. Criar regra de publicação
    """

    setup = PublisherSetup()

    # =====================================================
    # 1. CONFIGURAR GRUPO COMO OWNER
    # =====================================================

    # Substitua pelo ID real do seu grupo
    MY_GROUP_ID = -1001234567890

    # Marca como owner e habilita publicação
    setup.set_group_as_owner(
        group_id=MY_GROUP_ID,
        publish_enabled=True,
        publish_interval_minutes=30  # Mínimo 30min entre publicações
    )

    # =====================================================
    # 2. CADASTRAR BOT
    # =====================================================

    # Substitua pelo token real do seu bot
    BOT_TOKEN = "123456789:ABCdefGHIjklMNOpqrsTUVwxyz"

    bot_id = await setup.add_bot(
        token=BOT_TOKEN,
        name="Publisher Bot"
    )

    # =====================================================
    # 3. VINCULAR BOT AO GRUPO
    # =====================================================

    setup.link_bot_to_group(
        group_id=MY_GROUP_ID,
        bot_id=bot_id
    )

    # =====================================================
    # 4. CRIAR REGRA DE PUBLICAÇÃO
    # =====================================================

    # Grupo fonte (de onde vêm as mídias)
    SOURCE_GROUP_ID = -1009876543210

    setup.create_publish_rule(
        destination_group_id=MY_GROUP_ID,
        source_group_id=SOURCE_GROUP_ID,
        approval_required=True,     # Só publica mídias aprovadas
        auto_publish=True,          # Enfileira automaticamente
        daily_limit=50,             # Máximo 50 por dia
        hourly_limit=10,            # Máximo 10 por hora
        priority=0
    )

    # =====================================================
    # 5. VERIFICAR CONFIGURAÇÃO
    # =====================================================

    logger.info("\n" + "=" * 60)
    logger.info("✅ CONFIGURAÇÃO CONCLUÍDA")
    logger.info("=" * 60)

    setup.list_owner_groups()
    setup.list_bots()
    setup.get_queue_status()

    logger.info("\n📝 Próximos passos:")
    logger.info("  1. Adicione o bot como admin no grupo")
    logger.info("  2. Execute: python main_publisher.py")


async def example_manual_publish():
    """
    Exemplo de publicação manual de mídias.

    Útil para publicar mídias específicas sem usar regras automáticas.
    """

    publisher = PublisherService()

    try:
        # IDs de exemplo
        GROUP_ID = -1001234567890
        MEDIA_IDS = [1, 2, 3, 4, 5]  # IDs das mídias a publicar

        # Enfileira manualmente
        for media_id in MEDIA_IDS:
            publisher.enqueue_media(
                group_id=GROUP_ID,
                media_id=media_id,
                priority=5  # Prioridade alta
            )

        # Processa a fila
        await publisher.process_queue(limit=len(MEDIA_IDS))

    finally:
        await publisher.close()


async def example_check_bot():
    """
    Exemplo de verificação de bot.

    Testa se o bot está funcionando e tem acesso ao grupo.
    """

    BOT_TOKEN = "123456789:ABCdefGHIjklMNOpqrsTUVwxyz"
    GROUP_ID = -1001234567890

    bot = BotService(BOT_TOKEN)

    try:
        # Verifica bot
        me = await bot.get_me()
        logger.info(f"✅ Bot: @{me['username']}")

        # Verifica acesso ao grupo
        chat = await bot.get_chat(GROUP_ID)
        logger.info(f"✅ Grupo: {chat['title']}")

        # Verifica se é admin
        admins = await bot.get_chat_administrators(GROUP_ID)
        is_admin = any(a['user']['id'] == me['id'] for a in admins)

        if is_admin:
            logger.info("✅ Bot é administrador do grupo")
        else:
            logger.warning("⚠️ Bot NÃO é administrador do grupo")

        # Envia mensagem de teste
        result = await bot.send_message(
            chat_id=GROUP_ID,
            text="🤖 Teste de conexão do Publisher Bot"
        )
        logger.info(f"✅ Mensagem de teste enviada (ID: {result['message_id']})")

    except Exception as e:
        logger.error(f"❌ Erro: {e}")
    finally:
        await bot.close()


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "setup":
            asyncio.run(example_full_setup())
        elif command == "publish":
            asyncio.run(example_manual_publish())
        elif command == "check":
            asyncio.run(example_check_bot())
        else:
            print("Comandos disponíveis:")
            print("  python setup_publisher.py setup   - Configuração completa")
            print("  python setup_publisher.py publish - Publicação manual")
            print("  python setup_publisher.py check   - Verificar bot")
    else:
        # Por padrão, mostra status
        setup = PublisherSetup()
        setup.list_owner_groups()
        setup.list_bots()
        setup.get_queue_status()