import asyncio
import time
import logging
from typing import List
from app.services.session_pool import SessionPoolBalanced
from app.repository.groups_repository import GroupsRepository
from app.repository.credentials_repository import CredentialsRepository
from app.repository.group_credentials_repository import GroupCredentialsRepository
from app.repository.media_repository import MediaRepository
from config.settings import Config

logger = logging.getLogger(__name__)


class TelegramServiceBalanced:
    """Serviço BALANCEADO - 20k mensagens/dia sem FloodWait excessivo"""

    def __init__(self):
        self.groups_repo = GroupsRepository()
        self.creds_repo = CredentialsRepository()
        self.group_creds_repo = GroupCredentialsRepository()
        self.media_repo = MediaRepository()

        self.num_workers = Config.NUM_WORKERS
        self.msg_per_worker = Config.MSG_POR_WORKER
        self.session_path = Config.SESSION_PATH

        # CONCORRÊNCIA BALANCEADA: 10 downloads simultâneos globais
        self.max_concurrent_downloads = 10

    async def run_all_groups(self):
        groups = self.groups_repo.where("enabled", 1).get()
        if not groups:
            logger.warning("⚠️ Nenhum grupo habilitado")
            return

        logger.info(f"📋 {len(groups)} grupo(s) habilitado(s)")

        for idx, group in enumerate(groups, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"🎯 [{idx}/{len(groups)}] {group['title']}")
            logger.info(f"{'='*60}")

            try:
                await self._process_group(group)
            except Exception as e:
                logger.error(f"❌ Erro: {e}", exc_info=True)

            # Pausa entre grupos
            if idx < len(groups):
                logger.info("⏳ Aguardando 3s antes do próximo grupo...")
                await asyncio.sleep(3)

    async def _process_group(self, group):
        start_time = time.time()

        link = self.group_creds_repo.where("group_id", group["id"]).first()
        if not link:
            logger.warning("⚠️ Sem credencial")
            return

        cred = self.creds_repo.find(link["credential_id"])
        if not cred or not cred["active"]:
            logger.warning("⚠️ Credencial inválida")
            return

        session_pool = SessionPoolBalanced(cred, self.session_path)

        try:
            if not await session_pool.initialize():
                logger.error("❌ Falha ao inicializar pool")
                return

            entity = await session_pool.get_entity(group["id"])
            logger.info(f"✅ Entidade: {entity.title}")

            messages_to_process = await self._fetch_unprocessed_messages(
                session_pool, entity, group
            )

            if not messages_to_process:
                logger.info("✅ Nenhuma mensagem nova")
                return

            total_expected = self.num_workers * self.msg_per_worker
            if len(messages_to_process) > total_expected:
                logger.info(f"📊 Limitando a {total_expected} mensagens")
                messages_to_process = messages_to_process[:total_expected]

            await self._process_messages_controlled(
                session_pool, group, messages_to_process
            )

        finally:
            await session_pool.close_all()
            elapsed = time.time() - start_time
            logger.info(f"⏱️ Finalizado em {elapsed:.0f}s ({elapsed/60:.1f} min)")

    async def _fetch_unprocessed_messages(self, session_pool, entity, group):
        last_id = group.get("last_update_id", 0)
        total_to_fetch = self.num_workers * self.msg_per_worker

        logger.info(f"🔍 Buscando {total_to_fetch} mensagens após ID {last_id}...")

        processed_ids = set(
            self.media_repo.where("group_id", group["id"]).pluck("telegram_message_id")
        )
        logger.info(f"📊 {len(processed_ids)} já processadas")

        unprocessed_messages = []
        batch_size = 150
        offset_id = last_id

        while len(unprocessed_messages) < total_to_fetch:
            try:
                messages_batch = await session_pool.iter_messages_batch(
                    entity,
                    limit=batch_size,
                    offset_id=offset_id
                )

                if not messages_batch:
                    break

                for msg in messages_batch:
                    if msg.media and msg.id not in processed_ids:
                        unprocessed_messages.append(msg)

                        if len(unprocessed_messages) >= total_to_fetch:
                            break

                offset_id = messages_batch[-1].id

                if len(unprocessed_messages) >= total_to_fetch:
                    break

                # Pausa entre batches de busca
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"❌ Erro ao buscar: {e}")
                break

        logger.info(f"✅ {len(unprocessed_messages)} mensagens encontradas")
        return unprocessed_messages

    async def _process_messages_controlled(self, session_pool, group, messages: List):
        """Processamento CONTROLADO - evita FloodWait"""
        from app.services.pipeline_service import PipelineService

        total = len(messages)
        logger.info(f"🚀 Processando {total} mensagens (concorrência: {self.max_concurrent_downloads})")

        pool_status = session_pool.get_pool_status()
        logger.info(f"📊 Pool: {pool_status['available']}/{pool_status['total']} sessões disponíveis")

        pipeline = PipelineService()

        semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        processed = 0
        failed = 0

        async def _process_one(msg, idx):
            nonlocal processed, failed

            async with semaphore:
                try:
                    # Download
                    file_bytes = await session_pool.download_media(msg)

                    if not file_bytes:
                        failed += 1
                        return

                    # Pipeline
                    mime = msg.file.mime_type or "application/octet-stream"
                    await pipeline.process_message(msg, file_bytes, mime, group, worker_id=1)

                    processed += 1

                    # Log a cada 25
                    if processed % 25 == 0:
                        elapsed = time.time() - start_time
                        rate = processed / elapsed if elapsed > 0 else 0
                        logger.info(f"  📊 {processed}/{total} ({processed*100//total}%) | {rate:.1f} msgs/s")

                except Exception as e:
                    failed += 1

        start_time = time.time()
        tasks = [_process_one(msg, idx) for idx, msg in enumerate(messages)]

        await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.time() - start_time

        logger.info(f"\n{'='*60}")
        logger.info(f"✅ Concluído:")
        logger.info(f"  📊 Processadas: {processed}/{total} ({processed*100//total if total > 0 else 0}%)")
        logger.info(f"  ❌ Falhas: {failed}")
        logger.info(f"  ⏱️ Tempo: {elapsed:.0f}s ({elapsed/60:.1f} min)")

        if elapsed > 0:
            rate = processed / elapsed
            logger.info(f"  ⚡ Taxa: {rate:.2f} msgs/s | {rate*60:.0f} msgs/min | {rate*3600:.0f} msgs/hora")

        final_status = session_pool.get_pool_status()
        logger.info(f"  🔄 Total de requisições: {final_status['total_requests']}")
        logger.info(f"{'='*60}")

        # Atualiza last_update_id
        if messages:
            max_id = max(msg.id for msg in messages)
            if max_id > group.get("last_update_id", 0):
                self.groups_repo.update(group["id"], {"last_update_id": max_id})
                logger.info(f"💾 Last_update_id: {max_id}")