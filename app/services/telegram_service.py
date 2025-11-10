import os
import shutil
import asyncio
import time
import multiprocessing
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from app.repository.groups_repository import GroupsRepository
from app.repository.credentials_repository import CredentialsRepository
from app.repository.group_credentials_repository import GroupCredentialsRepository
from config.settings import Config

# 🚦 Limite global de chamadas Telegram simultâneas
TELEGRAM_SEMAPHORE = asyncio.Semaphore(5)


async def safe_telegram_call(func, *args, retries=3, **kwargs):
    """Executa chamadas Telegram com limite global e retry/backoff em 429."""
    async with TELEGRAM_SEMAPHORE:
        for attempt in range(retries):
            try:
                return await func(*args, **kwargs)
            except FloodWaitError as e:
                print(f"⚠️ FloodWait: aguardando {e.seconds}s antes de tentar novamente...")
                await asyncio.sleep(e.seconds + 1)
            except RPCError as e:
                if "429" in str(e) or "flood" in str(e).lower():
                    wait_time = 5 * (attempt + 1)
                    print(f"⚠️ Flood control detectado ({e}). Tentando novamente em {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    raise
            except Exception as e:
                if "429" in str(e) or "flood" in str(e).lower():
                    wait_time = 5 * (attempt + 1)
                    print(f"⚠️ Erro 429: aguardando {wait_time}s antes de tentar novamente...")
                    await asyncio.sleep(wait_time)
                else:
                    raise
        raise Exception(f"❌ Falha após {retries} tentativas em {func.__name__}")


class TelegramService:
    def __init__(self):
        self.groups_repo = GroupsRepository()
        self.creds_repo = CredentialsRepository()
        self.group_creds_repo = GroupCredentialsRepository()

        self.num_workers = Config.NUM_WORKERS
        self.msg_per_worker = Config.MSG_POR_WORKER
        self.session_path = Config.SESSION_PATH

    async def run_all_groups(self):
        await self._ensure_base_sessions()
        groups = self.groups_repo.where("enabled", 1).get()
        if not groups:
            print("⚠️ Nenhum grupo habilitado encontrado.")
            return

        for group in groups:
            await self._dispatch_group(group)

    async def _ensure_base_sessions(self):
        """Cria a sessão base e duplica para o número de workers configurado."""
        os.makedirs(self.session_path, exist_ok=True)
        base_name = "sessao_base"
        base_path = os.path.join(self.session_path, base_name)
        base_file = f"{base_path}.session"

        # Cria sessão base se não existir
        if not os.path.exists(base_file):
            print("⚙️ Criando sessão base...")
            cred = self.creds_repo.where("active", 1).first()
            if not cred:
                print("❌ Nenhuma credencial ativa encontrada.")
                return
            client = TelegramClient(base_path, cred["api_id"], cred["api_hash"])
            await client.start(phone=cred["phone"])
            await client.disconnect()
            print(f"✅ Sessão base criada em: {base_file}")

        # Duplica para cada worker
        for i in range(self.num_workers):
            target_path = os.path.join(self.session_path, f"sessao_{i}.session")
            if not os.path.exists(target_path):
                shutil.copy2(base_file, target_path)
                print(f"🧩 Sessão clonada: sessao_{i}.session")

    async def _prefetch_valid_messages(self, client, group):
        """Busca mensagens válidas (com mídia) após o último ID processado."""
        total_to_fetch = self.num_workers * self.msg_per_worker * 2
        last_id = group.get("last_update_id", 0)
        print(f"📦 Buscando {total_to_fetch} mensagens após ID {last_id} do grupo {group['title']}...")

        entity = await safe_telegram_call(client.get_entity, group["id"])
        msgs = []
        async for m in client.iter_messages(entity, limit=total_to_fetch, offset_id=last_id, reverse=True):
            if m.media:
                msgs.append(m)

        print(f"✅ {len(msgs)} mensagens válidas encontradas no grupo {group['title']}")
        return msgs

    def _worker_process(self, idx, group, msg_ids, cred, result_queue):
        """Processo individual de um worker (executa mensagens em paralelo)."""

        async def job():
            from app.services.pipeline_service import PipelineService
            pipeline = PipelineService()
            session_path = os.path.join(self.session_path, f"sessao_{idx}")
            client = TelegramClient(session_path, cred["api_id"], cred["api_hash"])

            # reconexão robusta
            async def connect_client():
                for attempt in range(3):
                    try:
                        await client.connect()
                        if await client.is_user_authorized():
                            return True
                    except Exception as e:
                        print(f"[W{idx}] ⚠️ Erro conectando Telegram: {e}")
                    await asyncio.sleep(3 * (attempt + 1))
                return False

            if not await connect_client():
                print(f"[W{idx}] ❌ Falha ao autenticar sessão.")
                result_queue.put(0)
                return

            entity = await safe_telegram_call(client.get_entity, group["id"])
            start_time = time.time()
            semaphore = asyncio.Semaphore(2)
            processed_count = 0
            last_processed_id = 0

            async def process_single(msg_id):
                nonlocal last_processed_id, processed_count
                async with semaphore:
                    try:
                        msg = await safe_telegram_call(client.get_messages, entity, ids=msg_id)
                        if not msg or not msg.media:
                            return

                        file = await safe_telegram_call(msg.download_media, bytes)
                        mime = msg.file.mime_type or "application/octet-stream"
                        await pipeline.process_message(msg, file, mime, group, worker_id=idx)

                        last_processed_id = max(last_processed_id, msg.id)
                        processed_count += 1
                    except Exception as e:
                        print(f"[W{idx}] ⚠️ Erro processando msg {msg_id}: {e}")

            await asyncio.gather(*[process_single(m) for m in msg_ids])
            await client.disconnect()

            elapsed = time.time() - start_time
            print(f"[W{idx}] ✅ Processadas {processed_count}/{len(msg_ids)} mensagens em {elapsed:.2f}s")
            result_queue.put(last_processed_id)

        asyncio.run(job())

    async def _dispatch_group(self, group):
        """Cria os workers e despacha o processamento."""
        link = self.group_creds_repo.where("group_id", group["id"]).first()
        if not link:
            print(f"⚠️ Nenhuma credencial vinculada ao grupo {group['title']}")
            return

        cred = self.creds_repo.find(link["credential_id"])
        if not cred or not cred["active"]:
            print(f"⚠️ Credencial inválida para grupo {group['title']}")
            return

        base_session = os.path.join(self.session_path, "sessao_base")
        client = TelegramClient(base_session, cred["api_id"], cred["api_hash"])
        await client.start(phone=cred["phone"])

        msgs = await self._prefetch_valid_messages(client, group)
        await client.disconnect()

        if not msgs:
            print(f"⚠️ Nenhuma mensagem nova com mídia em {group['title']}")
            return

        msg_ids = [m.id for m in msgs]
        chunk_size = self.msg_per_worker
        chunks = [msg_ids[i:i + chunk_size] for i in range(0, len(msg_ids), chunk_size)]

        print(f"👷 Iniciando {min(len(chunks), self.num_workers)} worker(s)...")

        start_time = time.time()
        processes = []
        result_queue = multiprocessing.Queue()

        for i, chunk in enumerate(chunks[:self.num_workers]):
            p = multiprocessing.Process(target=self._worker_process, args=(i, group, chunk, cred, result_queue))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        processed_ids = []
        while not result_queue.empty():
            processed_ids.append(result_queue.get())

        if processed_ids:
            last_id = max(processed_ids)
            self.groups_repo.update(group["id"], {"last_update_id": last_id})
            print(f"💾 Atualizado last_update_id={last_id} para {group['title']}")
        else:
            print(f"⚠️ Nenhuma mensagem processada com sucesso em {group['title']}")

        elapsed = time.time() - start_time
        print(f"🏁 Execução finalizada em {elapsed:.2f}s.\n")
