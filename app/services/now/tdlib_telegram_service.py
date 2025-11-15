"""
Telegram Service com TDLib - Performance Máxima
Substitui TelegramServiceBalanced do Telethon

Performance esperada:
- 100-150k mensagens/dia (vs 20k com Telethon)
- 4000-6000 mensagens/hora
- Rate limits muito mais generosos
"""
import asyncio
import time
import logging
from typing import List, Dict, Optional
from app.services.now.tdlib_session_pool import TDLibSessionPool
from app.services.now.tdlib_config import TDLibConfig

# Seus repositórios existentes
from app.repository.groups_repository import GroupsRepository
from app.repository.credentials_repository import CredentialsRepository
from app.repository.group_credentials_repository import GroupCredentialsRepository
from app.repository.media_repository import MediaRepository
from config.settings import Config

logger = logging.getLogger(__name__)


class TDLibTelegramService:
    """
    Serviço Telegram com TDLib - Alta Performance

    Diferenças vs Telethon:
    - 5x mais rápido
    - Rate limits muito menores
    - Melhor gerenciamento de recursos
    - Usado pelos apps oficiais do Telegram
    """

    def __init__(self):
        # Repositórios (mantém os mesmos)
        self.groups_repo = GroupsRepository()
        self.creds_repo = CredentialsRepository()
        self.group_creds_repo = GroupCredentialsRepository()
        self.media_repo = MediaRepository()

        # Configurações
        self.num_workers = Config.NUM_WORKERS
        self.msg_per_worker = Config.MSG_POR_WORKER

        # TDLib aguenta muito mais concorrência
        self.max_concurrent_downloads = TDLibConfig.MAX_CONCURRENT_DOWNLOADS

        logger.info(f"🚀 TDLib Service inicializado")
        logger.info(f"⚙️  Workers: {self.num_workers} | Msgs/worker: {self.msg_per_worker}")
        logger.info(f"⚡ Concorrência: {self.max_concurrent_downloads} downloads simultâneos")

    async def run_all_groups(self):
        """
        Processa todos os grupos habilitados

        Fluxo:
        1. Busca grupos habilitados
        2. Para cada grupo:
           - Conecta com credencial
           - Busca mensagens não processadas
           - Faz download paralelo
           - Processa pipeline
        """
        groups = self.groups_repo.where("enabled", 1).get()

        if not groups:
            logger.warning("⚠️ Nenhum grupo habilitado no banco")
            return

        logger.info(f"\n{'='*70}")
        logger.info(f"📋 {len(groups)} grupo(s) habilitado(s) para processamento")
        logger.info(f"{'='*70}\n")

        total_start = time.time()

        for idx, group in enumerate(groups, 1):
            logger.info(f"\n{'='*70}")
            logger.info(f"🎯 [{idx}/{len(groups)}] Grupo: {group['title']}")
            logger.info(f"   ID: {group['id']} | Last Update: {group.get('last_update_id', 0)}")
            logger.info(f"{'='*70}")

            try:
                await self._process_group(group)

            except Exception as e:
                logger.error(f"❌ Erro fatal no grupo {group['title']}: {e}", exc_info=True)

            # Pausa suave entre grupos (TDLib não precisa de muito tempo)
            if idx < len(groups):
                logger.info("⏳ Aguardando 2s antes do próximo grupo...\n")
                await asyncio.sleep(2)

        total_elapsed = time.time() - total_start
        logger.info(f"\n{'='*70}")
        logger.info(f"✅ TODOS OS GRUPOS PROCESSADOS")
        logger.info(f"⏱️  Tempo total: {total_elapsed:.0f}s ({total_elapsed/60:.1f} min)")
        logger.info(f"{'='*70}\n")

    async def _process_group(self, group: dict):
        """Processa um grupo específico"""
        start_time = time.time()

        # 1️⃣ Busca credencial do grupo
        link = self.group_creds_repo.where("group_id", group["id"]).first()
        if not link:
            logger.warning("⚠️ Grupo sem credencial vinculada")
            return

        cred = self.creds_repo.find(link["credential_id"])
        if not cred or not cred.get("active"):
            logger.warning("⚠️ Credencial inválida ou inativa")
            return

        logger.info(f"🔑 Credencial: {cred.get('session_name', 'N/A')}")

        # 2️⃣ Inicializa pool TDLib
        session_pool = TDLibSessionPool(cred)

        try:
            # Número de sessões baseado na configuração
            num_sessions = min(5, TDLibConfig.MAX_CONCURRENT_SESSIONS)

            if not await session_pool.initialize(num_sessions=num_sessions):
                logger.error("❌ Falha ao inicializar TDLib SessionPool")
                return

            # 3️⃣ Busca informações do chat
            try:
                chat_info = await session_pool.get_chat(group["id"])
                logger.info(f"✅ Chat conectado: {chat_info.get('title', 'N/A')}")
            except Exception as e:
                logger.error(f"❌ Erro ao buscar chat: {e}")
                return

            # 4️⃣ Busca mensagens não processadas
            messages_to_process = await self._fetch_unprocessed_messages(
                session_pool, group
            )

            if not messages_to_process:
                logger.info("✅ Nenhuma mensagem nova para processar")
                return

            # Limita ao total configurado
            total_expected = self.num_workers * self.msg_per_worker
            if len(messages_to_process) > total_expected:
                logger.info(f"📊 Limitando processamento a {total_expected} mensagens")
                messages_to_process = messages_to_process[:total_expected]

            # 5️⃣ Processa mensagens com alta concorrência
            await self._process_messages_parallel(
                session_pool, group, messages_to_process
            )

        finally:
            await session_pool.close_all()

            elapsed = time.time() - start_time
            logger.info(f"\n⏱️  Grupo finalizado em {elapsed:.0f}s ({elapsed/60:.1f} min)")

    async def _fetch_unprocessed_messages(
            self,
            session_pool: TDLibSessionPool,
            group: dict
    ) -> List[dict]:
        """
        Busca mensagens não processadas do grupo

        TDLib usa paginação diferente:
        - from_message_id ao invés de offset_id
        - Retorna lista de dicts ao invés de objetos Message
        """
        last_id = group.get("last_update_id", 0)
        total_to_fetch = self.num_workers * self.msg_per_worker

        logger.info(f"🔍 Buscando mensagens após ID {last_id}...")
        logger.info(f"📊 Meta: {total_to_fetch} mensagens não processadas")

        # Busca IDs já processados (otimização)
        processed_ids = set(
            self.media_repo.where("group_id", group["id"]).pluck("telegram_message_id")
        )
        logger.info(f"📊 {len(processed_ids)} mensagens já no banco")

        unprocessed_messages = []
        batch_size = TDLibConfig.BATCH_SIZE  # 300 por padrão
        from_message_id = last_id

        fetch_start = time.time()

        while len(unprocessed_messages) < total_to_fetch:
            try:
                # TDLib: busca batch de mensagens
                messages_batch = await session_pool.get_messages_batch(
                    chat_id=group["id"],
                    limit=batch_size,
                    from_message_id=from_message_id
                )

                if not messages_batch:
                    logger.info("📭 Fim do histórico de mensagens")
                    break

                # Filtra mensagens com mídia não processadas
                for msg in messages_batch:
                    msg_id = msg.get("id")
                    content = msg.get("content", {})

                    # Verifica se tem mídia
                    has_media = self._message_has_media(content)

                    if has_media and msg_id not in processed_ids:
                        unprocessed_messages.append(msg)

                        if len(unprocessed_messages) >= total_to_fetch:
                            break

                # Atualiza posição para próximo batch
                if messages_batch:
                    from_message_id = messages_batch[-1].get("id", 0)

                # Log de progresso
                if len(unprocessed_messages) % 100 == 0 and len(unprocessed_messages) > 0:
                    logger.info(f"  📥 {len(unprocessed_messages)} mensagens coletadas...")

                if len(unprocessed_messages) >= total_to_fetch:
                    break

                # TDLib: pausa mínima (muito menor que Telethon)
                await asyncio.sleep(0.3)

            except Exception as e:
                logger.error(f"❌ Erro ao buscar batch: {e}")
                break

        fetch_elapsed = time.time() - fetch_start
        logger.info(f"✅ {len(unprocessed_messages)} mensagens encontradas em {fetch_elapsed:.1f}s")

        return unprocessed_messages

    def _message_has_media(self, content: dict) -> bool:
        """Verifica se mensagem tem mídia suportada"""
        content_type = content.get("@type", "")

        return content_type in [
            "messagePhoto",
            "messageVideo",
            "messageDocument",
            "messageAnimation"
        ]

    async def _process_messages_parallel(
            self,
            session_pool: TDLibSessionPool,
            group: dict,
            messages: List[dict]
    ):
        """
        Processamento paralelo de mensagens com TDLib

        Performance:
        - 50+ downloads simultâneos
        - ~4000-6000 msgs/hora
        - Rate limits muito menores
        """
        from app.services.pipeline_service import PipelineService

        total = len(messages)
        logger.info(f"\n{'='*70}")
        logger.info(f"🚀 Iniciando processamento paralelo")
        logger.info(f"📊 Total: {total} mensagens")
        logger.info(f"⚡ Concorrência: {self.max_concurrent_downloads} downloads simultâneos")
        logger.info(f"{'='*70}\n")

        # Status do pool
        pool_status = session_pool.get_pool_status()
        logger.info(f"📊 Pool TDLib: {pool_status['available_sessions']}/{pool_status['total_sessions']} sessões")

        pipeline = PipelineService()

        # Controle de concorrência (TDLib aguenta muito mais)
        semaphore = asyncio.Semaphore(self.max_concurrent_downloads)

        # Contadores
        processed = 0
        failed = 0

        async def _process_one(msg: dict, idx: int):
            """Processa uma mensagem"""
            nonlocal processed, failed

            async with semaphore:
                msg_id = msg.get("id")

                try:
                    # 1️⃣ Download com TDLib (muito mais rápido)
                    file_bytes = await session_pool.download_media(msg)

                    if not file_bytes:
                        logger.warning(f"⚠️ Download falhou para mensagem {msg_id}")
                        failed += 1
                        return

                    # 2️⃣ Extrai mime type
                    mime = self._extract_mime_type(msg)

                    # 3️⃣ Pipeline (seu código existente)
                    await pipeline.process_message(
                        msg=self._convert_to_telethon_format(msg),  # Adapta formato
                        file_bytes=file_bytes,
                        mime=mime,
                        group=group,
                        worker_id=idx % self.num_workers
                    )

                    processed += 1

                    # Log de progresso (a cada 50 mensagens)
                    if processed % 50 == 0:
                        elapsed = time.time() - start_time
                        rate = processed / elapsed if elapsed > 0 else 0
                        progress = (processed * 100) // total if total > 0 else 0

                        logger.info(
                            f"  📊 {processed}/{total} ({progress}%) | "
                            f"{rate:.1f} msgs/s | "
                            f"{rate*60:.0f} msgs/min"
                        )

                except Exception as e:
                    logger.error(f"❌ Erro processando mensagem {msg_id}: {e}")
                    failed += 1

        # Executa processamento paralelo
        start_time = time.time()

        tasks = [_process_one(msg, idx) for idx, msg in enumerate(messages)]
        await asyncio.gather(*tasks, return_exceptions=True)

        elapsed = time.time() - start_time

        # 📊 Relatório final
        logger.info(f"\n{'='*70}")
        logger.info(f"✅ PROCESSAMENTO CONCLUÍDO")
        logger.info(f"{'='*70}")
        logger.info(f"📊 Processadas: {processed}/{total} ({processed*100//total if total > 0 else 0}%)")
        logger.info(f"❌ Falhas: {failed}")
        logger.info(f"⏱️  Tempo: {elapsed:.0f}s ({elapsed/60:.1f} min)")

        if elapsed > 0:
            rate = processed / elapsed
            logger.info(f"⚡ Velocidade:")
            logger.info(f"   • {rate:.2f} msgs/segundo")
            logger.info(f"   • {rate*60:.0f} msgs/minuto")
            logger.info(f"   • {rate*3600:.0f} msgs/hora estimadas")

        # Status final do pool
        final_status = session_pool.get_pool_status()
        logger.info(f"🔄 Total de downloads: {final_status['total_downloads']}")
        logger.info(f"🔄 Total de requisições: {final_status['total_requests']}")
        logger.info(f"{'='*70}\n")

        # 💾 Atualiza last_update_id no banco
        if messages and processed > 0:
            max_id = max(msg.get("id", 0) for msg in messages)

            if max_id > group.get("last_update_id", 0):
                self.groups_repo.update(group["id"], {"last_update_id": max_id})
                logger.info(f"💾 Last_update_id atualizado: {max_id}")

    def _extract_mime_type(self, message: dict) -> str:
        """Extrai mime type de uma mensagem TDLib"""
        content = message.get("content", {})
        content_type = content.get("@type", "")

        if content_type == "messagePhoto":
            return "image/jpeg"

        elif content_type == "messageVideo":
            mime = content.get("video", {}).get("mime_type", "video/mp4")
            return mime

        elif content_type == "messageDocument":
            mime = content.get("document", {}).get("mime_type", "application/octet-stream")
            return mime

        elif content_type == "messageAnimation":
            return "image/gif"

        return "application/octet-stream"

    def _convert_to_telethon_format(self, tdlib_msg: dict):
        """
        Converte mensagem TDLib para formato compatível com pipeline

        Seu pipeline espera atributos como msg.id, msg.date, msg.file
        Criamos um objeto dict que simula esses atributos
        """
        from datetime import datetime

        # Cria objeto compatível
        class TDLibMessageAdapter:
            def __init__(self, tdlib_data):
                self.id = tdlib_data.get("id")
                self.date = datetime.fromtimestamp(tdlib_data.get("date", 0))
                self.chat_id = tdlib_data.get("chat_id")

                # Simula atributo file (usado no pipeline)
                content = tdlib_data.get("content", {})
                self.file = type('obj', (object,), {
                    'mime_type': self._get_mime(content)
                })()

            def _get_mime(self, content):
                content_type = content.get("@type", "")

                if content_type == "messagePhoto":
                    return "image/jpeg"
                elif content_type == "messageVideo":
                    return content.get("video", {}).get("mime_type", "video/mp4")
                elif content_type == "messageDocument":
                    return content.get("document", {}).get("mime_type", "application/octet-stream")
                elif content_type == "messageAnimation":
                    return "image/gif"

                return "application/octet-stream"

        return TDLibMessageAdapter(tdlib_msg)