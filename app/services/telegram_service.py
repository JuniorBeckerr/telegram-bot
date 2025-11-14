import asyncio
import time
import logging
from typing import List
from app.services.session_pool import SessionPool
from app.repository.groups_repository import GroupsRepository
from app.repository.credentials_repository import CredentialsRepository
from app.repository.group_credentials_repository import GroupCredentialsRepository
from app.repository.media_repository import MediaRepository
from config.settings import Config

logger = logging.getLogger(__name__)


class TelegramService:
    def __init__(self):
        self.groups_repo = GroupsRepository()
        self.creds_repo = CredentialsRepository()
        self.group_creds_repo = GroupCredentialsRepository()
        self.media_repo = MediaRepository()

        # Configurações
        self.num_workers = Config.NUM_WORKERS
        self.msg_per_worker = Config.MSG_POR_WORKER
        self.session_path = Config.SESSION_PATH

        # Controle de concorrência
        self.max_concurrent_downloads = 5  # Downloads simultâneos total

    async def run_all_groups(self):
        """Processa todos os grupos habilitados sequencialmente."""
        groups = self.groups_repo.where("enabled", 1).get()
        if not groups:
            logger.warning("⚠️ Nenhum grupo habilitado encontrado.")
            return

        logger.info(f"📋 {len(groups)} grupo(s) habilitado(s) encontrado(s).")

        for idx, group in enumerate(groups, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"🎯 [{idx}/{len(groups)}] Processando: {group['title']}")
            logger.info(f"{'='*60}")

            try:
                await self._process_group(group)
            except Exception as e:
                logger.error(f"❌ Erro ao processar grupo {group['title']}: {e}", exc_info=True)

            # Pausa entre grupos
            if idx < len(groups):
                logger.info(f"⏳ Aguardando 5s antes do próximo grupo...")
                await asyncio.sleep(5)

    async def _process_group(self, group):
        """Processa um grupo usando pool de sessões."""
        start_time = time.time()

        # Busca credencial vinculada
        link = self.group_creds_repo.where("group_id", group["id"]).first()
        if not link:
            logger.warning(f"⚠️ Nenhuma credencial vinculada ao grupo {group['title']}")
            return

        cred = self.creds_repo.find(link["credential_id"])
        if not cred or not cred["active"]:
            logger.warning(f"⚠️ Credencial inválida para grupo {group['title']}")
            return

        # Inicializa pool de sessões
        session_pool = SessionPool(cred, self.session_path)

        try:
            # Conecta todas as sessões
            if not await session_pool.initialize():
                logger.error(f"❌ Falha ao inicializar pool de sessões")
                return

            # Busca entidade do grupo
            entity = await session_pool.get_entity(group["id"])
            logger.info(f"✅ Entidade do grupo obtida: {entity.title}")

            # Busca mensagens não processadas
            messages_to_process = await self._fetch_unprocessed_messages(
                session_pool, entity, group
            )

            if not messages_to_process:
                logger.info(f"✅ Nenhuma mensagem nova para processar em {group['title']}")
                return

            # Limita ao total esperado (NUM_WORKERS × MSG_POR_WORKER)
            total_expected = self.num_workers * self.msg_per_worker
            if len(messages_to_process) > total_expected:
                logger.info(f"📊 Limitando processamento a {total_expected} mensagens")
                messages_to_process = messages_to_process[:total_expected]

            # Processa mensagens em paralelo com workers
            await self._process_messages_parallel(
                session_pool, group, messages_to_process
            )

        finally:
            await session_pool.close_all()
            elapsed = time.time() - start_time
            logger.info(f"⏱️ Grupo {group['title']} finalizado em {elapsed:.2f}s")

    async def _fetch_unprocessed_messages(self, session_pool, entity, group):
        """Busca mensagens com mídia que ainda não foram processadas."""
        last_id = group.get("last_update_id", 0)
        total_to_fetch = self.num_workers * self.msg_per_worker

        logger.info(f"🔍 Buscando até {total_to_fetch} mensagens após ID {last_id}...")

        # Busca IDs já processados deste grupo
        processed_ids = set(
            self.media_repo.where("group_id", group["id"]).pluck("telegram_message_id")
        )
        logger.info(f"📊 {len(processed_ids)} mensagens já processadas anteriormente")

        # Busca mensagens em lotes usando o pool
        unprocessed_messages = []
        batch_size = 100
        offset_id = last_id

        while len(unprocessed_messages) < total_to_fetch:
            try:
                # Busca batch de mensagens
                messages_batch = await session_pool.iter_messages_batch(
                    entity,
                    limit=batch_size,
                    offset_id=offset_id
                )

                if not messages_batch:
                    logger.info("📭 Não há mais mensagens para buscar")
                    break

                # Filtra mensagens com mídia não processadas
                for msg in messages_batch:
                    if msg.media and msg.id not in processed_ids:
                        unprocessed_messages.append(msg)

                        if len(unprocessed_messages) >= total_to_fetch:
                            break

                # Atualiza offset para próximo batch
                offset_id = messages_batch[-1].id

                logger.info(
                    f"  📦 Batch processado: {len(unprocessed_messages)}/{total_to_fetch} "
                    f"encontradas (último ID: {offset_id})"
                )

                # Para se já encontrou o suficiente
                if len(unprocessed_messages) >= total_to_fetch:
                    break

                # Pequeno delay entre batches de busca
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"❌ Erro ao buscar mensagens: {e}")
                break

        logger.info(
            f"✅ {len(unprocessed_messages)} mensagens novas encontradas para processar"
        )
        return unprocessed_messages

    async def _process_messages_parallel(self, session_pool, group, messages: List):
        """Processa mensagens em paralelo com workers."""
        from app.services.pipeline_service import PipelineService

        total = len(messages)
        logger.info(f"🚀 Processando {total} mensagens com {self.num_workers} workers")

        # Divide mensagens em chunks por worker
        chunks = self._split_into_chunks(messages, self.num_workers)

        # Mostra status do pool
        pool_status = session_pool.get_pool_status()
        logger.info(
            f"📊 Pool: {pool_status['available']}/{pool_status['total']} sessões disponíveis"
        )

        # Processa cada worker em paralelo
        tasks = []
        for worker_id, chunk in enumerate(chunks, 1):
            if not chunk:
                continue

            task = asyncio.create_task(
                self._process_worker_chunk(
                    session_pool=session_pool,
                    worker_id=worker_id,
                    messages=chunk,
                    group=group,
                    total_workers=self.num_workers
                )
            )
            tasks.append(task)

        # Aguarda todos os workers terminarem
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Conta sucessos e falhas
        total_processed = sum(r for r in results if isinstance(r, int))
        total_failed = sum(1 for r in results if isinstance(r, Exception))

        logger.info(f"\n{'='*60}")
        logger.info(f"✅ Processamento concluído:")
        logger.info(f"  📊 Processadas: {total_processed}/{total}")
        logger.info(f"  ❌ Falhas: {total_failed}")

        # Mostra estatísticas finais do pool
        final_status = session_pool.get_pool_status()
        logger.info(f"  🔄 Total de requisições: {final_status['total_requests']}")
        logger.info(f"{'='*60}")

        # Atualiza last_update_id para o maior ID processado
        if messages:
            max_id = max(msg.id for msg in messages)
            if max_id > group.get("last_update_id", 0):
                self.groups_repo.update(group["id"], {"last_update_id": max_id})
                logger.info(f"💾 Last_update_id atualizado: {max_id}")

    async def _process_worker_chunk(
            self,
            session_pool: SessionPool,
            worker_id: int,
            messages: List,
            group: dict,
            total_workers: int
    ) -> int:
        """Processa um chunk de mensagens em um worker."""
        from app.services.pipeline_service import PipelineService

        pipeline = PipelineService()
        processed = 0
        chunk_size = len(messages)

        logger.info(f"👷 Worker {worker_id}/{total_workers}: {chunk_size} mensagens")

        for idx, msg in enumerate(messages, 1):
            try:
                logger.info(
                    f"  [{worker_id}] [{idx}/{chunk_size}] Processando msg ID {msg.id}..."
                )

                # Download usando pool (rotação automática de sessões)
                file_bytes = await session_pool.download_media(msg)

                if not file_bytes:
                    logger.warning(f"  [{worker_id}] ⚠️ Falha no download msg {msg.id}")
                    continue

                # Processa através do pipeline
                mime = msg.file.mime_type or "application/octet-stream"
                await pipeline.process_message(msg, file_bytes, mime, group, worker_id)

                processed += 1

                # Pequeno delay entre mensagens do mesmo worker
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"  [{worker_id}] ❌ Erro msg {msg.id}: {e}")
                continue

        logger.info(f"✅ Worker {worker_id} finalizado: {processed}/{chunk_size} processadas")
        return processed

    def _split_into_chunks(self, items: List, num_chunks: int) -> List[List]:
        """Divide lista em N chunks equilibrados."""
        chunk_size = len(items) // num_chunks
        remainder = len(items) % num_chunks

        chunks = []
        start = 0

        for i in range(num_chunks):
            # Distribui o resto entre os primeiros chunks
            end = start + chunk_size + (1 if i < remainder else 0)
            chunks.append(items[start:end])
            start = end

        return chunks