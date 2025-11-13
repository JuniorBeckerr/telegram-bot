# import os
# import asyncio
# import time
# import multiprocessing
# from telethon import TelegramClient
# from telethon.errors import FloodWaitError, RPCError
# from app.repository.groups_repository import GroupsRepository
# from app.repository.credentials_repository import CredentialsRepository
# from app.repository.group_credentials_repository import GroupCredentialsRepository
# from app.repository.media_repository import MediaRepository
# from config.settings import Config
#
# TELEGRAM_SEMAPHORE = asyncio.Semaphore(5)
#
#
# async def safe_telegram_call(func, *args, retries=3, **kwargs):
#     async with TELEGRAM_SEMAPHORE:
#         for attempt in range(retries):
#             try:
#                 return await func(*args, **kwargs)
#             except FloodWaitError as e:
#                 print(f"⚠️ FloodWait: aguardando {e.seconds}s antes de tentar novamente...")
#                 await asyncio.sleep(e.seconds + 1)
#             except RPCError as e:
#                 if "429" in str(e) or "flood" in str(e).lower():
#                     wait_time = 5 * (attempt + 1)
#                     print(f"⚠️ Flood control detectado ({e}). Tentando novamente em {wait_time}s...")
#                     await asyncio.sleep(wait_time)
#                 else:
#                     raise
#             except Exception as e:
#                 if "429" in str(e) or "flood" in str(e).lower():
#                     wait_time = 5 * (attempt + 1)
#                     print(f"⚠️ Erro 429: aguardando {wait_time}s antes de tentar novamente...")
#                     await asyncio.sleep(wait_time)
#                 else:
#                     raise
#         raise Exception(f"❌ Falha após {retries} tentativas em {func.__name__}")
#
#
# class TelegramService:
#     def __init__(self):
#         self.groups_repo = GroupsRepository()
#         self.creds_repo = CredentialsRepository()
#         self.group_creds_repo = GroupCredentialsRepository()
#         self.media_repo = MediaRepository()  # Repositório de media
#
#         self.num_workers = Config.NUM_WORKERS
#         self.msg_per_worker = Config.MSG_POR_WORKER
#         self.session_path = Config.SESSION_PATH
#
#     async def run_all_groups(self):
#         groups = self.groups_repo.where("enabled", 1).get()
#         if not groups:
#             print("⚠️ Nenhum grupo habilitado encontrado.")
#             return
#
#         for group in groups:
#             await self._dispatch_group(group)
#
#     async def _prefetch_valid_messages(self, client, group):
#         total_to_fetch = self.num_workers * self.msg_per_worker * 2
#         last_id = group.get("last_update_id", 0)
#         print(f"📦 Buscando {total_to_fetch} mensagens após ID {last_id} do grupo {group['title']}...")
#
#         entity = await safe_telegram_call(client.get_entity, group["id"])
#         msgs = []
#         async for m in client.iter_messages(entity, limit=total_to_fetch, offset_id=last_id, reverse=True):
#             if m.media:
#                 msgs.append(m)
#
#         print(f"✅ {len(msgs)} mensagens válidas encontradas no grupo {group['title']}")
#         return msgs
#
#     def _worker_process(self, idx, group, msg_ids, session_file, cred, result_queue):
#         async def job():
#             from app.services.pipeline_service import PipelineService
#             pipeline = PipelineService()
#
#             client = TelegramClient(session_file, cred["api_id"], cred["api_hash"])
#             await client.connect()
#
#             if not await client.is_user_authorized():
#                 print(f"[W{idx}] ❌ Sessão não autenticada: {session_file}")
#                 result_queue.put(0)
#                 return
#
#             entity = await client.get_entity(group["id"])
#             start_time = time.time()
#             last_processed_id = 0
#             processed_count = 0
#
#             async def safe_call(func, *args, **kwargs):
#                 for attempt in range(5):
#                     try:
#                         return await func(*args, **kwargs)
#                     except FloodWaitError as e:
#                         await asyncio.sleep(e.seconds + 1)
#                     except RPCError as e:
#                         if "disconnected" in str(e).lower() or not client.is_connected():
#                             await client.disconnect()
#                             await asyncio.sleep(3)
#                             await client.connect()
#                         else:
#                             raise
#                     except Exception as e:
#                         if attempt == 4:
#                             print(f"[W{idx}] ❌ Erro persistente: {e}")
#                         else:
#                             await asyncio.sleep(3)
#
#             async def process_single(msg_id):
#                 nonlocal last_processed_id, processed_count
#                 try:
#                     msg = await safe_call(client.get_messages, entity, ids=msg_id)
#                     if not msg or not msg.media:
#                         return
#
#                     file = await safe_call(msg.download_media, bytes)
#                     if not file:
#                         return
#
#                     mime = msg.file.mime_type or "application/octet-stream"
#                     await pipeline.process_message(msg, file, mime, group, worker_id=idx)
#                     last_processed_id = max(last_processed_id, msg.id)
#                     processed_count += 1
#                 except Exception as e:
#                     print(f"[W{idx}] ⚠️ Erro processando msg {msg_id}: {e}")
#
#             # ⚡ Processa sequencialmente
#             for m in msg_ids:
#                 await process_single(m)
#                 await asyncio.sleep(2)
#             await client.disconnect()
#             result_queue.put(last_processed_id)
#             elapsed = time.time() - start_time
#             print(f"[W{idx}] ✅ Processadas {processed_count}/{len(msg_ids)} mensagens em {elapsed:.2f}s")
#
#         asyncio.run(job())
#
#     async def _dispatch_group(self, group):
#         link = self.group_creds_repo.where("group_id", group["id"]).first()
#         if not link:
#             print(f"⚠️ Nenhuma credencial vinculada ao grupo {group['title']}")
#             return
#
#         cred = self.creds_repo.find(link["credential_id"])
#         if not cred or not cred["active"]:
#             print(f"⚠️ Credencial inválida para grupo {group['title']}")
#             return
#
#         # Lista todas as sessões individuais da credencial
#         cred_dir = os.path.join(self.session_path, str(cred["session_name"]))
#         session_files = sorted([
#             os.path.join(cred_dir, f)
#             for f in os.listdir(cred_dir)
#             if f.endswith(".session")
#         ])
#
#         if not session_files:
#             print(f"❌ Nenhuma sessão encontrada em {cred_dir}")
#             return
#
#         # Prefetch usando a primeira sessão disponível
#         client = TelegramClient(session_files[0], cred["api_id"], cred["api_hash"])
#         await client.start(phone=cred["phone"])
#         msgs = await self._prefetch_valid_messages(client, group)
#         await client.disconnect()
#
#         if not msgs:
#             print(f"⚠️ Nenhuma mensagem nova com mídia em {group['title']}")
#             return
#
#         # ⚡ Filtra mensagens já processadas
#         processed_ids_set = set(
#             self.media_repo.where("group_id", group["id"]).pluck("telegram_message_id")
#         )
#         msg_ids = [m.id for m in msgs if m.id not in processed_ids_set]
#
#         if not msg_ids:
#             print(f"⚠️ Todas as mensagens já foram processadas em {group['title']}")
#             return
#
#         # Divide em chunks para os workers
#         chunk_size = self.msg_per_worker
#         chunks = [msg_ids[i:i + chunk_size] for i in range(0, len(msg_ids), chunk_size)]
#
#         print(f"👷 Iniciando {min(len(chunks), len(session_files))} worker(s)...")
#         start_time = time.time()
#         processes = []
#         result_queue = multiprocessing.Queue()
#
#         for i, chunk in enumerate(chunks[:len(session_files)]):
#             p = multiprocessing.Process(
#                 target=self._worker_process,
#                 args=(i, group, chunk, session_files[i], cred, result_queue)
#             )
#             p.start()
#             processes.append(p)
#
#         for p in processes:
#             p.join()
#
#         processed_ids = [result_queue.get() for _ in range(result_queue.qsize())]
#         if processed_ids:
#             last_id = max(processed_ids)
#             self.groups_repo.update(group["id"], {"last_update_id": last_id})
#             print(f"💾 Atualizado last_update_id={last_id} para {group['title']}")
#
#         elapsed = time.time() - start_time
#         print(f"🏁 Execução finalizada em {elapsed:.2f}s.\n")

import os
import asyncio
import time
import multiprocessing
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from app.repository.groups_repository import GroupsRepository
from app.repository.credentials_repository import CredentialsRepository
from app.repository.group_credentials_repository import GroupCredentialsRepository
from app.repository.media_repository import MediaRepository
from config.settings import Config

TELEGRAM_SEMAPHORE = asyncio.Semaphore(5)


async def safe_telegram_call(func, *args, retries=3, **kwargs):
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
        self.media_repo = MediaRepository()

        self.num_workers = Config.NUM_WORKERS
        self.msg_per_worker = Config.MSG_POR_WORKER
        self.session_path = Config.SESSION_PATH

    async def run_all_groups(self):
        groups = self.groups_repo.where("enabled", 1).get()
        if not groups:
            print("⚠️ Nenhum grupo habilitado encontrado.")
            return

        for group in groups:
            await self._dispatch_group(group)

    async def _prefetch_valid_messages(self, client, group):
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

    def _worker_process(self, idx, group, msg_ids, session_file, cred, result_queue):
        async def job():
            from app.services.pipeline_service import PipelineService
            pipeline = PipelineService()

            client = TelegramClient(session_file, cred["api_id"], cred["api_hash"])
            await client.connect()

            if not await client.is_user_authorized():
                print(f"[W{idx}] ❌ Sessão não autenticada: {session_file}")
                result_queue.put(0)
                return

            entity = await client.get_entity(group["id"])
            start_time = time.time()
            last_processed_id = 0
            processed_count = 0

            async def safe_call(func, *args, **kwargs):
                for attempt in range(5):
                    try:
                        return await func(*args, **kwargs)
                    except FloodWaitError as e:
                        print(f"[W{idx}] ⚠️ FloodWait {e.seconds}s")
                        raise  # Sinaliza para o supervisor que houve flood
                    except RPCError as e:
                        if "disconnected" in str(e).lower() or not client.is_connected():
                            await client.disconnect()
                            await asyncio.sleep(3)
                            await client.connect()
                        else:
                            raise
                    except Exception as e:
                        if attempt == 4:
                            print(f"[W{idx}] ❌ Erro persistente: {e}")
                        else:
                            await asyncio.sleep(3)

            async def process_single(msg_id):
                nonlocal last_processed_id, processed_count
                msg = await safe_call(client.get_messages, entity, ids=msg_id)
                if not msg or not msg.media:
                    return
                file = await safe_call(msg.download_media, bytes)
                if not file:
                    return
                mime = msg.file.mime_type or "application/octet-stream"
                await pipeline.process_message(msg, file, mime, group, worker_id=idx)
                last_processed_id = max(last_processed_id, msg.id)
                processed_count += 1

            for m in msg_ids:
                await process_single(m)
                await asyncio.sleep(2)

            await client.disconnect()
            result_queue.put(last_processed_id)
            elapsed = time.time() - start_time
            print(f"[W{idx}] ✅ Processadas {processed_count}/{len(msg_ids)} mensagens em {elapsed:.2f}s")

        try:
            asyncio.run(job())
        except FloodWaitError:
            print(f"[W{idx}] ⚠️ Worker pausado devido a FloodWait")
            result_queue.put("FLOOD")
        except Exception as e:
            print(f"[W{idx}] ❌ Worker terminou com erro: {e}")
            result_queue.put("ERROR")

    async def _dispatch_group(self, group):
        link = self.group_creds_repo.where("group_id", group["id"]).first()
        if not link:
            print(f"⚠️ Nenhuma credencial vinculada ao grupo {group['title']}")
            return

        cred = self.creds_repo.find(link["credential_id"])
        if not cred or not cred["active"]:
            print(f"⚠️ Credencial inválida para grupo {group['title']}")
            return

        cred_dir = os.path.join(self.session_path, str(cred["session_name"]))
        session_files = sorted([
            os.path.join(cred_dir, f)
            for f in os.listdir(cred_dir)
            if f.endswith(".session")
        ])

        if not session_files:
            print(f"❌ Nenhuma sessão encontrada em {cred_dir}")
            return

        client = TelegramClient(session_files[0], cred["api_id"], cred["api_hash"])
        await client.start(phone=cred["phone"])
        msgs = await self._prefetch_valid_messages(client, group)
        await client.disconnect()

        if not msgs:
            print(f"⚠️ Nenhuma mensagem nova com mídia em {group['title']}")
            return

        processed_ids_set = set(
            self.media_repo.where("group_id", group["id"]).pluck("telegram_message_id")
        )
        msg_ids = [m.id for m in msgs if m.id not in processed_ids_set]

        if not msg_ids:
            print(f"⚠️ Todas as mensagens já foram processadas em {group['title']}")
            return

        chunk_size = self.msg_per_worker
        chunks = [msg_ids[i:i + chunk_size] for i in range(0, len(msg_ids), chunk_size)]

        num_active_workers = min(len(session_files) - 1, len(chunks))
        reserve_worker = len(session_files) - 1

        start_time = time.time()
        result_queue = multiprocessing.Queue()
        active_processes = []

        # Função para iniciar worker
        def start_worker(idx, chunk_idx):
            p = multiprocessing.Process(
                target=self._worker_process,
                args=(idx, group, chunks[chunk_idx], session_files[idx], cred, result_queue)
            )
            p.start()
            active_processes.append((p, idx, chunk_idx))
            return p

        # Inicia workers ativos
        for i in range(num_active_workers):
            start_worker(i, i)

        # Monitoramento de workers
        while active_processes:
            for p, idx, chunk_idx in active_processes.copy():
                p.join(timeout=0.1)  # Não bloqueia
                if not p.is_alive():
                    active_processes.remove((p, idx, chunk_idx))
                    result = result_queue.get()
                    if result in ["FLOOD", "ERROR"] and reserve_worker >= 0:
                        print(f"🛡️ Worker reserva assumindo posição de W{idx}")
                        start_worker(reserve_worker, chunk_idx)
                        reserve_worker = -1  # Apenas 1 reserva
                    elif result != "FLOOD" and result != "ERROR":
                        print(f"💾 Worker {idx} terminou, last_id={result}")

        # Atualiza last_update_id
        processed_ids = [result_queue.get() for _ in range(result_queue.qsize())]
        processed_ids = [i for i in processed_ids if isinstance(i, int)]
        if processed_ids:
            last_id = max(processed_ids)
            self.groups_repo.update(group["id"], {"last_update_id": last_id})
            print(f"💾 Atualizado last_update_id={last_id} para {group['title']}")

        elapsed = time.time() - start_time
        print(f"🏁 Execução finalizada em {elapsed:.2f}s.\n")
