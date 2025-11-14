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

        # Cooldown BALANCEADO: 0.8s (não muito alto, não muito baixo)
        time_since_last = now - self.last_request_time
        if time_since_last < 0.8:
            return 0.8 - time_since_last
        return 0

    def mark_request(self):
        self.last_request_time = time.time()
        self.requests_count += 1

    def mark_flood_wait(self, seconds: int):
        self.flood_wait_until = time.time() + seconds + 3
        logger.warning(f"  🔴 Sessão {self.index} em FloodWait por {seconds}s")

    def mark_error(self):
        self.consecutive_errors += 1

    def reset_errors(self):
        self.consecutive_errors = 0


class SessionPoolBalanced:
    """Pool de sessões BALANCEADO - evita FloodWait sem ser lento"""

    def __init__(self, credential: dict, session_path: str):
        self.credential = credential
        self.session_path = session_path
        self.sessions: List[SessionInfo] = []
        self.session_queue: deque = deque()
        self.lock = asyncio.Lock()

        # CONFIGURAÇÃO BALANCEADA
        self.max_concurrent_per_session = 3  # 3 downloads por sessão (balanceado)
        self.download_semaphores: Dict[int, asyncio.Semaphore] = {}

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

        logger.info(f"🔧 Inicializando {len(session_files)} sessões (modo BALANCEADO)...")

        # Conecta sessões em paralelo
        tasks = []
        for idx, session_file in enumerate(session_files):
            tasks.append(self._connect_session(idx, session_file))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, SessionInfo):
                self.sessions.append(result)
                self.session_queue.append(result)
                self.download_semaphores[result.index] = asyncio.Semaphore(
                    self.max_concurrent_per_session
                )

        if not self.sessions:
            logger.error("❌ Nenhuma sessão conectada")
            return False

        logger.info(f"✅ Pool com {len(self.sessions)} sessões | {self.max_concurrent_per_session} downloads/sessão")
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

            logger.info(f"  ✅ Sessão {idx}: {os.path.basename(session_file)}")
            return session_info

        except Exception as e:
            logger.error(f"  ❌ Sessão {idx}: {e}")
            return None

    async def get_next_session(self, max_wait: float = 15.0) -> Optional[SessionInfo]:
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

            await asyncio.sleep(0.2)

        return None

    async def download_media(self, message):
        """Download com retry e controle de FloodWait"""
        for attempt in range(2):
            session = await self.get_next_session()

            if not session:
                if attempt < 1:
                    await asyncio.sleep(3)
                    continue
                raise Exception("Nenhuma sessão disponível")

            try:
                async with self.download_semaphores[session.index]:
                    session.mark_request()
                    result = await message.download_media(file=bytes)
                    session.reset_errors()
                    return result

            except FloodWaitError as e:
                session.mark_flood_wait(e.seconds)
                if attempt < 1:
                    await asyncio.sleep(1)
                    continue
                raise

            except Exception as e:
                session.mark_error()
                if attempt < 1:
                    await asyncio.sleep(1)
                    continue
                raise

        raise Exception("Falha após tentativas")

    async def get_entity(self, entity_id):
        async def _get_entity(client, eid):
            return await client.get_entity(eid)

        session = await self.get_next_session()
        if not session:
            raise Exception("Nenhuma sessão disponível")

        try:
            session.mark_request()
            result = await _get_entity(session.client, entity_id)
            session.reset_errors()
            return result
        except FloodWaitError as e:
            session.mark_flood_wait(e.seconds)
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

                # Pausa suave a cada 100 mensagens
                if len(messages) % 100 == 0:
                    await asyncio.sleep(0.5)

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
        tasks = [s.client.disconnect() for s in self.sessions]
        await asyncio.gather(*tasks, return_exceptions=True)

    def get_pool_status(self) -> dict:
        available = sum(1 for s in self.sessions if s.is_available())
        in_flood = sum(1 for s in self.sessions if s.flood_wait_until > time.time())

        return {
            "total": len(self.sessions),
            "available": available,
            "in_flood_wait": in_flood,
            "total_requests": sum(s.requests_count for s in self.sessions)
        }