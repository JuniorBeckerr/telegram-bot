import asyncio
import time
import logging
from typing import List
from app.services.session_pool import SessionPoolProduction
from app.repository.groups_repository import GroupsRepository
from app.repository.credentials_repository import CredentialsRepository
from app.repository.group_credentials_repository import GroupCredentialsRepository
from app.repository.media_repository import MediaRepository
from config.settings import Config

logger = logging.getLogger(__name__)


class TelegramServiceProduction:
    """Serviço PRODUCTION - processamento SEQUENCIAL estável"""

    def __init__(self):
        self.groups_repo = GroupsRepository()
        self.creds_repo = CredentialsRepository()
        self.group_creds_repo = GroupCredentialsRepository()
        self.media_repo = MediaRepository()

        self.num_workers = Config.NUM_WORKERS
        self.msg_per_worker = Config.MSG_POR_WORKER
        self.session_path = Config.SESSION_PATH

    async def run_all_groups(self):
        groups = self.groups_repo.where("enabled", 1).get()
        if not groups:
            logger.warning("⚠️ Nenhum grupo habilitado")
            return

        logger.info(f"📋 {len(groups)} grupo(s)")

        for idx, group in enumerate(groups, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"🎯 [{idx}/{len(groups)}] {group['title']}")
            logger.info(f"{'='*60}")

            try:
                await self._process_group(group)
            except Exception as e:
                logger.error(f"❌ Erro: {e}", exc_info=True)

            if idx < len(groups):
                logger.info("⏳ Aguardando 5s...")
                await asyncio.sleep(5)

    async def _process_group(self, group):
        start_time = time.time()

        link = self.group_creds_repo.where("group_id", group["id"]).first()
        if not link:
            return

        cred = self.creds_repo.find(link["credential_id"])
        if not cred or not cred["active"]:
            return

        session_pool = SessionPoolProduction(cred, self.session_path)

        try:
            if not await session_pool.initialize():
                return

            entity = await session_pool.get_entity(group["id"])
            logger.info(f"✅ Entidade: {entity.title}")

            messages = await self._fetch_unprocessed_messages(
                session_pool, entity, group
            )

            if not messages:
                logger.info("✅ Nenhuma mensagem nova")
                return

            total_expected = self.num_workers * self.msg_per_worker
            if len(messages) > total_expected:
                logger.info(f"📊 Limitando a {total_expected}")
                messages = messages[:total_expected]

            # Processa SEQUENCIALMENTE
            await self._process_messages_sequential(
                session_pool, group, messages
            )

        finally:
            await session_pool.close_all()
            elapsed = time.time() - start_time
            logger.info(f"⏱️ Finalizado em {elapsed:.0f}s ({elapsed/60:.1f} min)")

    async def _fetch_unprocessed_messages(self, session_pool, entity, group):
        last_id = group.get("last_update_id", 0)
        total_to_fetch = self.num_workers * self.msg_per_worker

        logger.info(f"🔍 Buscando {total_to_fetch} mensagens NOVAS após ID {last_id}...")

        # Busca IDs já processados deste grupo (telegram_message_id + group_id)
        processed_message_ids = set(
            self.media_repo
            .where("group_id", group["id"])
            .pluck("telegram_message_id")
        )

        logger.info(f"📊 {len(processed_message_ids)} IDs já processados neste grupo")

        unprocessed = []
        batch_size = 100
        offset_id = last_id
        total_fetched = 0
        total_skipped = 0

        while len(unprocessed) < total_to_fetch:
            try:
                batch = await session_pool.iter_messages_batch(
                    entity, limit=batch_size, offset_id=offset_id
                )

                if not batch:
                    logger.info("📭 Fim das mensagens do grupo")
                    break

                total_fetched += len(batch)

                for msg in batch:
                    # Só mensagens com mídia
                    if not msg.media:
                        continue

                    # Verifica se o ID da mensagem JÁ foi processado
                    if msg.id in processed_message_ids:
                        total_skipped += 1
                        continue

                    # Adiciona apenas se for NOVA
                    unprocessed.append(msg)

                    if len(unprocessed) >= total_to_fetch:
                        break

                offset_id = batch[-1].id

                # Log de progresso
                if total_fetched % 100 == 0:
                    logger.info(
                        f"  📦 Verificadas: {total_fetched} | "
                        f"Novas: {len(unprocessed)} | "
                        f"Ignoradas: {total_skipped}"
                    )

                if len(unprocessed) >= total_to_fetch:
                    break

                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"❌ Erro busca: {e}")
                break

        logger.info(f"\n✅ Resultado:")
        logger.info(f"  📥 Verificadas: {total_fetched}")
        logger.info(f"  ✨ NOVAS: {len(unprocessed)}")
        logger.info(f"  ⏭️ Ignoradas: {total_skipped}")

        return unprocessed
    async def _process_messages_sequential(self, session_pool, group, messages: List):
        """Processa SEQUENCIALMENTE - 1 por vez"""
        from app.services.pipeline_service import PipelineService

        total = len(messages)

        if total == 0:
            logger.info("⚠️ Nenhuma mensagem nova para processar")
            return

        logger.info(f"🚀 Processando {total} mensagens NOVAS (sequencial)")
        logger.info(f"⏱️ Estimativa: ~{total * 12 / 60:.0f} min (12s/msg)")

        pipeline = PipelineService()

        processed = 0
        failed = 0
        duplicates_skipped = 0
        start_time = time.time()

        for idx, msg in enumerate(messages, 1):
            msg_start = time.time()

            try:
                # Download
                file_bytes = await session_pool.download_media(msg)

                if not file_bytes:
                    failed += 1
                    logger.warning(f"  [{idx}/{total}] ⚠️ Falha download msg {msg.id}")
                    continue

                # Pipeline (pode detectar duplicata por phash)
                mime = msg.file.mime_type or "application/octet-stream"
                result = await pipeline.process_message(msg, file_bytes, mime, group, worker_id=1)

                # Verifica se foi duplicata
                if result and "duplicada" in str(result).lower():
                    duplicates_skipped += 1
                else:
                    processed += 1

                msg_elapsed = time.time() - msg_start

                # Log a cada 5 (já que são poucas mensagens)
                if idx % 5 == 0 or idx == total:
                    elapsed = time.time() - start_time
                    rate = (processed + duplicates_skipped) / elapsed if elapsed > 0 else 0
                    remaining = total - idx
                    eta = remaining / rate if rate > 0 else 0

                    logger.info(
                        f"  📊 [{idx}/{total}] Processadas: {processed} | "
                        f"Duplicadas: {duplicates_skipped} | Falhas: {failed} | "
                        f"Taxa: {rate*60:.1f}/min | ETA: {eta/60:.0f}min"
                    )

            except Exception as e:
                failed += 1
                logger.error(f"  [{idx}/{total}] ❌ Erro msg {msg.id}: {str(e)[:100]}")

            # Salva progresso a cada 25
            if idx % 25 == 0:
                max_id = max(m.id for m in messages[:idx])
                if max_id > group.get("last_update_id", 0):
                    self.groups_repo.update(group["id"], {"last_update_id": max_id})
                    logger.info(f"  💾 Progresso salvo: ID {max_id}")

        elapsed = time.time() - start_time

        logger.info(f"\n{'='*60}")
        logger.info(f"✅ Concluído:")
        logger.info(f"  ✨ Novas processadas: {processed}")
        logger.info(f"  ⏭️ Duplicadas ignoradas: {duplicates_skipped}")
        logger.info(f"  ❌ Falhas: {failed}")
        logger.info(f"  📊 Total verificadas: {total}")
        logger.info(f"  ⏱️ Tempo: {elapsed:.0f}s ({elapsed/60:.1f} min)")

        if elapsed > 0:
            rate = total / elapsed
            logger.info(f"  ⚡ Taxa: {rate:.2f} msgs/s | {rate*60:.0f} msgs/min | {rate*3600:.0f} msgs/hora")

        final_status = session_pool.get_pool_status()
        logger.info(f"  🔄 Requisições: {final_status['total_requests']}")
        logger.info(f"{'='*60}")

        # Atualiza final
        if messages:
            max_id = max(msg.id for msg in messages)
            if max_id > group.get("last_update_id", 0):
                self.groups_repo.update(group["id"], {"last_update_id": max_id})
                logger.info(f"💾 Last_update_id final: {max_id}")