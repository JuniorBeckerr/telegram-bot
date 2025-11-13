import os
import asyncio
import time
import logging
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
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

        # Configurações do settings
        self.num_workers = Config.NUM_WORKERS
        self.msg_per_worker = Config.MSG_POR_WORKER
        self.session_path = Config.SESSION_PATH

        # Delays anti-flood (ajustáveis)
        self.delay_between_messages = 2.5  # segundos entre cada mensagem
        self.delay_between_batches = 10    # segundos entre cada batch
        self.max_retries = 5

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
            await self._process_group(group)

            # Pausa entre grupos para evitar sobrecarga
            if idx < len(groups):
                logger.info(f"⏳ Aguardando {self.delay_between_batches}s antes do próximo grupo...")
                await asyncio.sleep(self.delay_between_batches)

    async def _process_group(self, group):
        """Processa um grupo específico."""
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

        # Seleciona sessão disponível
        session_file = self._get_available_session(cred)
        if not session_file:
            logger.error(f"❌ Nenhuma sessão disponível para {group['title']}")
            return

        # Conecta ao Telegram
        client = TelegramClient(session_file, cred["api_id"], cred["api_hash"])

        try:
            await client.start(phone=cred["phone"])
            logger.info(f"✅ Conectado com sessão: {os.path.basename(session_file)}")

            # Busca mensagens não processadas
            messages_to_process = await self._fetch_unprocessed_messages(client, group)

            if not messages_to_process:
                logger.info(f"✅ Nenhuma mensagem nova para processar em {group['title']}")
                return

            # Processa mensagens em batches
            await self._process_messages_in_batches(client, group, messages_to_process)

        except Exception as e:
            logger.error(f"❌ Erro ao processar grupo {group['title']}: {e}", exc_info=True)
        finally:
            await client.disconnect()
            elapsed = time.time() - start_time
            logger.info(f"⏱️ Grupo {group['title']} finalizado em {elapsed:.2f}s")

    def _get_available_session(self, cred):
        """Retorna a primeira sessão disponível da credencial."""
        cred_dir = os.path.join(self.session_path, str(cred["session_name"]))

        if not os.path.exists(cred_dir):
            logger.error(f"❌ Diretório de sessões não encontrado: {cred_dir}")
            return None

        session_files = sorted([
            os.path.join(cred_dir, f)
            for f in os.listdir(cred_dir)
            if f.endswith(".session")
        ])

        if not session_files:
            logger.error(f"❌ Nenhuma sessão encontrada em {cred_dir}")
            return None

        # Retorna a primeira sessão (você pode implementar rotação se quiser)
        return session_files[0]

    async def _fetch_unprocessed_messages(self, client, group):
        """Busca mensagens com mídia que ainda não foram processadas."""
        last_id = group.get("last_update_id", 0)
        total_to_fetch = self.num_workers * self.msg_per_worker

        logger.info(f"🔍 Buscando até {total_to_fetch} mensagens após ID {last_id}...")

        # Busca IDs já processados deste grupo
        processed_ids = set(
            self.media_repo.where("group_id", group["id"]).pluck("telegram_message_id")
        )
        logger.info(f"📊 {len(processed_ids)} mensagens já processadas anteriormente")

        # Busca mensagens do Telegram
        entity = await self._safe_call(client.get_entity, group["id"])

        unprocessed_messages = []
        fetched_count = 0

        async for msg in client.iter_messages(
                entity,
                limit=total_to_fetch * 2,  # Busca 2x para compensar mensagens já processadas
                offset_id=last_id,
                reverse=True
        ):
            fetched_count += 1

            # Filtra apenas mensagens com mídia não processadas
            if msg.media and msg.id not in processed_ids:
                unprocessed_messages.append(msg)

            # Para quando atingir o limite desejado
            if len(unprocessed_messages) >= total_to_fetch:
                break

        logger.info(f"✅ {len(unprocessed_messages)} mensagens novas encontradas (de {fetched_count} verificadas)")
        return unprocessed_messages

    async def _process_messages_in_batches(self, client, group, messages):
        """Processa mensagens em batches sequenciais."""
        from app.services.pipeline_service import PipelineService

        pipeline = PipelineService()
        total = len(messages)
        batch_size = self.msg_per_worker

        logger.info(f"🚀 Iniciando processamento de {total} mensagens em batches de {batch_size}")

        processed_count = 0
        last_processed_id = group.get("last_update_id", 0)

        # Divide em batches
        for batch_idx in range(0, total, batch_size):
            batch = messages[batch_idx:batch_idx + batch_size]
            batch_num = (batch_idx // batch_size) + 1
            total_batches = (total + batch_size - 1) // batch_size

            logger.info(f"\n📦 Batch {batch_num}/{total_batches} ({len(batch)} mensagens)")

            # Processa cada mensagem do batch sequencialmente
            for msg_idx, msg in enumerate(batch, 1):
                try:
                    logger.info(f"  [{msg_idx}/{len(batch)}] Processando msg ID {msg.id}...")

                    # Download com retry
                    file_bytes = await self._download_with_retry(client, msg)

                    if not file_bytes:
                        logger.warning(f"  ⚠️ Não foi possível baixar msg {msg.id}")
                        continue

                    # Processa através do pipeline
                    mime = msg.file.mime_type or "application/octet-stream"
                    await pipeline.process_message(msg, file_bytes, mime, group, worker_id=batch_num)

                    processed_count += 1
                    last_processed_id = max(last_processed_id, msg.id)

                    # Delay entre mensagens
                    await asyncio.sleep(self.delay_between_messages)

                except Exception as e:
                    logger.error(f"  ❌ Erro processando msg {msg.id}: {e}")
                    continue

            # Salva progresso após cada batch
            if last_processed_id > group.get("last_update_id", 0):
                self.groups_repo.update(group["id"], {"last_update_id": last_processed_id})
                logger.info(f"💾 Progresso salvo: last_update_id={last_processed_id}")

            # Pausa entre batches (exceto no último)
            if batch_idx + batch_size < total:
                logger.info(f"⏳ Aguardando {self.delay_between_batches}s antes do próximo batch...")
                await asyncio.sleep(self.delay_between_batches)

        logger.info(f"\n✅ Processamento concluído: {processed_count}/{total} mensagens")

        # Atualização final
        if last_processed_id > group.get("last_update_id", 0):
            self.groups_repo.update(group["id"], {"last_update_id": last_processed_id})
            logger.info(f"💾 Last_update_id final: {last_processed_id}")

    async def _download_with_retry(self, client, msg):
        """Baixa mídia com retry e backoff exponencial."""
        for attempt in range(self.max_retries):
            try:
                file_bytes = await self._safe_call(msg.download_media, bytes)
                return file_bytes

            except FloodWaitError as e:
                wait_time = e.seconds + 5
                logger.warning(f"  ⏳ FloodWait: aguardando {wait_time}s...")
                await asyncio.sleep(wait_time)

            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # backoff exponencial: 1s, 2s, 4s, 8s
                    logger.warning(f"  ⚠️ Tentativa {attempt + 1}/{self.max_retries} falhou: {e}")
                    logger.info(f"  ⏳ Aguardando {wait_time}s antes de tentar novamente...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"  ❌ Falha permanente após {self.max_retries} tentativas")
                    raise

        return None

    async def _safe_call(self, func, *args, **kwargs):
        """Wrapper para chamadas ao Telegram com retry automático."""
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)

            except FloodWaitError as e:
                wait_time = e.seconds + 2
                logger.warning(f"⏳ FloodWait detectado: aguardando {wait_time}s...")
                await asyncio.sleep(wait_time)

            except RPCError as e:
                if "429" in str(e) or "flood" in str(e).lower():
                    wait_time = 5 * (attempt + 1)
                    logger.warning(f"⚠️ Rate limit ({e}). Tentativa {attempt + 1}/{self.max_retries}")
                    await asyncio.sleep(wait_time)
                else:
                    raise

            except Exception as e:
                if "429" in str(e) or "flood" in str(e).lower():
                    wait_time = 5 * (attempt + 1)
                    logger.warning(f"⚠️ Erro 429 detectado. Aguardando {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    raise

        raise Exception(f"❌ Falha após {self.max_retries} tentativas em {func.__name__}")