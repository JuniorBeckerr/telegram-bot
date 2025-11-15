import os
import asyncio
import logging
import time
from typing import Dict, List, Optional
from collections import deque
from telethon import TelegramClient
from telethon.errors import FloodWaitError

logger = logging.getLogger(__name__)


class SessionInfo:
    def __init__(self, client: TelegramClient, session_path: str, index: int):
        self.client = client
        self.session_path = session_path
        self.index = index
        self.is_connected = False
        self.last_request_time = 0
        self.requests_count = 0
        self.flood_wait_until = 0
        self.consecutive_errors = 0

    def is_available(self) -> bool:
        now = time.time()
        if self.flood_wait_until > now:
            return False
        if self.consecutive_errors >= 3:
            return False
        return self.is_connected

    def get_cooldown(self) -> float:
        now = time.time()
        if self.flood_wait_until > now:
            return self.flood_wait_until - now

        # COOLDOWN REAL: 2s entre requests da mesma sessão
        time_since_last = now - self.last_request_time
        if time_since_last < 2.0:
            return 2.0 - time_since_last
        return 0

    def mark_request(self):
        self.last_request_time = time.time()
        self.requests_count += 1

    def mark_flood_wait(self, seconds: int):
        self.flood_wait_until = time.time() + seconds + 5
        logger.warning(f"  🔴 Sessão {self.index} FloodWait {seconds}s")

    def mark_error(self):
        self.consecutive_errors += 1

    def reset_errors(self):
        self.consecutive_errors = 0


class SessionPoolProduction:
    """Pool PRODUCTION - funciona 24/7 sem travar"""

    def __init__(self, credential: dict, session_path: str):
        self.credential = credential
        self.session_path = session_path
        self.sessions: List[SessionInfo] = []
        self.session_queue: deque = deque()
        self.lock = asyncio.Lock()
        self.global_last_download = 0

        # CONFIGURAÇÃO PRODUCTION: 1 download por vez
        self.max_concurrent = 1
        self.global_semaphore = asyncio.Semaphore(1)

        # DELAY ENTRE DOWNLOADS: 2s mínimo
        self.min_delay_between_downloads = 2.0

    async def initialize(self) -> bool:
        cred_dir = os.path.join(self.session_path, str(self.credential["session_name"]))

        if not os.path.exists(cred_dir):
            logger.error(f"❌ Diretório não encontrado: {cred_dir}")
            return False

        session_files = sorted([
            os.path.join(cred_dir, f)
            for f in os.listdir(cred_dir)
            if f.endswith(".session")
        ])

        if not session_files:
            logger.error(f"❌ Nenhuma sessão encontrada")
            return False

        logger.info(f"🔧 Inicializando {len(session_files)} sessões (PRODUCTION)...")

        # Conecta sequencialmente
        for idx, session_file in enumerate(session_files):
            result = await self._connect_session(idx, session_file)
            if result:
                self.sessions.append(result)
                self.session_queue.append(result)

        if not self.sessions:
            logger.error("❌ Nenhuma sessão conectada")
            return False

        logger.info(f"✅ Pool PRODUCTION: {len(self.sessions)} sessões | 1 download por vez | 2s entre downloads")
        return True

    async def _connect_session(self, idx: int, session_file: str) -> Optional[SessionInfo]:
        try:
            client = TelegramClient(
                session_file,
                self.credential["api_id"],
                self.credential["api_hash"]
            )

            await client.start(phone=self.credential["phone"])

            session_info = SessionInfo(client, session_file, idx)
            session_info.is_connected = True

            logger.info(f"  ✅ Sessão {idx}")
            return session_info

        except Exception as e:
            logger.error(f"  ❌ Sessão {idx}: {e}")
            return None

    async def get_next_session(self, max_wait: float = 20.0) -> Optional[SessionInfo]:
        start_time = time.time()

        while time.time() - start_time < max_wait:
            async with self.lock:
                for _ in range(len(self.session_queue)):
                    session = self.session_queue[0]
                    self.session_queue.rotate(-1)

                    if session.is_available():
                        cooldown = session.get_cooldown()
                        if cooldown > 0:
                            await asyncio.sleep(cooldown)
                        return session

            await asyncio.sleep(0.3)

        return None

    async def download_media(self, message):
        """Download com ESPERA GLOBAL entre cada request"""
        async with self.global_semaphore:
            # Garante 2s entre QUALQUER download
            now = time.time()
            time_since_last = now - self.global_last_download
            if time_since_last < self.min_delay_between_downloads:
                wait = self.min_delay_between_downloads - time_since_last
                await asyncio.sleep(wait)

            for attempt in range(2):
                session = await self.get_next_session()

                if not session:
                    if attempt < 1:
                        await asyncio.sleep(5)
                        continue
                    raise Exception("Nenhuma sessão disponível")

                try:
                    session.mark_request()
                    result = await message.download_media(file=bytes)
                    session.reset_errors()
                    self.global_last_download = time.time()
                    return result

                except FloodWaitError as e:
                    session.mark_flood_wait(e.seconds)
                    logger.warning(f"  ⏳ Aguardando FloodWait: {e.seconds}s")
                    await asyncio.sleep(e.seconds + 3)
                    if attempt < 1:
                        continue
                    raise

                except Exception as e:
                    session.mark_error()
                    if attempt < 1:
                        await asyncio.sleep(2)
                        continue
                    raise

            raise Exception("Falha após tentativas")

    async def get_entity(self, entity_id):
        session = await self.get_next_session()
        if not session:
            raise Exception("Nenhuma sessão disponível")

        try:
            session.mark_request()
            result = await session.client.get_entity(entity_id)
            session.reset_errors()
            return result
        except FloodWaitError as e:
            session.mark_flood_wait(e.seconds)
            await asyncio.sleep(e.seconds + 3)
            raise

    async def iter_messages_batch(self, entity, limit: int, offset_id: int = 0):
        session = await self.get_next_session()

        if not session:
            raise Exception("Nenhuma sessão disponível")

        messages = []

        try:
            async for msg in session.client.iter_messages(
                    entity,
                    limit=limit,
                    offset_id=offset_id,
                    reverse=True
            ):
                messages.append(msg)

                # Pausa a cada 50
                if len(messages) % 50 == 0:
                    await asyncio.sleep(1)

            session.reset_errors()
            return messages

        except FloodWaitError as e:
            session.mark_flood_wait(e.seconds)
            raise
        except Exception as e:
            session.mark_error()
            raise

    async def close_all(self):
        logger.info("🔌 Fechando sessões...")
        for s in self.sessions:
            try:
                await s.client.disconnect()
            except:
                pass

    def get_pool_status(self) -> dict:
        available = sum(1 for s in self.sessions if s.is_available())
        in_flood = sum(1 for s in self.sessions if s.flood_wait_until > time.time())

        return {
            "total": len(self.sessions),
            "available": available,
            "in_flood_wait": in_flood,
            "total_requests": sum(s.requests_count for s in self.sessions)
        }