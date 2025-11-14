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
    """Informações de controle de cada sessão."""
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
        """Verifica se a sessão está disponível para uso."""
        now = time.time()

        # Verifica se está em flood wait
        if self.flood_wait_until > now:
            return False

        # Verifica se tem muitos erros consecutivos
        if self.consecutive_errors >= 3:
            return False

        return self.is_connected

    def get_cooldown(self) -> float:
        """Retorna tempo de espera necessário em segundos."""
        now = time.time()

        if self.flood_wait_until > now:
            return self.flood_wait_until - now

        # Cooldown base após requisição: 1.5s
        time_since_last = now - self.last_request_time
        if time_since_last < 1.5:
            return 1.5 - time_since_last

        return 0

    def mark_request(self):
        """Marca que uma requisição foi feita."""
        self.last_request_time = time.time()
        self.requests_count += 1

    def mark_flood_wait(self, seconds: int):
        """Marca que a sessão entrou em flood wait."""
        self.flood_wait_until = time.time() + seconds
        logger.warning(f"  🔴 Sessão {self.index} em FloodWait por {seconds}s")

    def mark_error(self):
        """Marca um erro consecutivo."""
        self.consecutive_errors += 1

    def reset_errors(self):
        """Reseta contador de erros."""
        self.consecutive_errors = 0


class SessionPool:
    """Pool de sessões com rotação e rate limiting inteligente."""

    def __init__(self, credential: dict, session_path: str):
        self.credential = credential
        self.session_path = session_path
        self.sessions: List[SessionInfo] = []
        self.session_queue: deque = deque()  # Fila circular para rotação
        self.lock = asyncio.Lock()

        # Configurações de rate limiting
        self.max_concurrent_downloads = 3  # Downloads simultâneos por sessão
        self.download_semaphores: Dict[int, asyncio.Semaphore] = {}

    async def initialize(self) -> bool:
        """Inicializa todas as sessões disponíveis."""
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
            logger.error(f"❌ Nenhuma sessão encontrada em {cred_dir}")
            return False

        logger.info(f"🔧 Inicializando {len(session_files)} sessões...")

        for idx, session_file in enumerate(session_files):
            try:
                client = TelegramClient(
                    session_file,
                    self.credential["api_id"],
                    self.credential["api_hash"]
                )

                await client.start(phone=self.credential["phone"])

                session_info = SessionInfo(client, session_file, idx)
                session_info.is_connected = True

                self.sessions.append(session_info)
                self.session_queue.append(session_info)
                self.download_semaphores[idx] = asyncio.Semaphore(self.max_concurrent_downloads)

                logger.info(f"  ✅ Sessão {idx} conectada: {os.path.basename(session_file)}")

            except Exception as e:
                logger.error(f"  ❌ Erro ao conectar sessão {idx}: {e}")

        if not self.sessions:
            logger.error("❌ Nenhuma sessão conectada com sucesso")
            return False

        logger.info(f"✅ Pool inicializado com {len(self.sessions)} sessões")
        return True

    async def get_next_session(self, max_wait: float = 30.0) -> Optional[SessionInfo]:
        """Obtém a próxima sessão disponível (com espera se necessário)."""
        start_time = time.time()

        while time.time() - start_time < max_wait:
            async with self.lock:
                # Tenta encontrar sessão disponível imediatamente
                for _ in range(len(self.session_queue)):
                    session = self.session_queue[0]
                    self.session_queue.rotate(-1)  # Move para o fim

                    if session.is_available():
                        cooldown = session.get_cooldown()
                        if cooldown > 0:
                            await asyncio.sleep(cooldown)
                        return session

            # Se nenhuma sessão disponível, aguarda um pouco
            await asyncio.sleep(0.5)

        logger.warning(f"⚠️ Timeout: nenhuma sessão disponível após {max_wait}s")
        return None

    async def execute_with_session(self, func, *args, max_retries: int = 3, **kwargs):
        """Executa uma função usando uma sessão do pool com retry."""
        for attempt in range(max_retries):
            session = await self.get_next_session()

            if not session:
                if attempt < max_retries - 1:
                    logger.warning(f"  ⚠️ Tentativa {attempt + 1}/{max_retries}: aguardando sessões...")
                    await asyncio.sleep(5)
                    continue
                raise Exception("❌ Nenhuma sessão disponível após múltiplas tentativas")

            try:
                # Marca requisição
                session.mark_request()

                # Executa a função
                result = await func(session.client, *args, **kwargs)

                # Sucesso: reseta erros
                session.reset_errors()
                return result

            except FloodWaitError as e:
                session.mark_flood_wait(e.seconds + 5)

                if attempt < max_retries - 1:
                    logger.warning(f"  ⚠️ FloodWait na sessão {session.index}. Tentando outra...")
                    await asyncio.sleep(1)
                    continue
                raise

            except Exception as e:
                session.mark_error()

                if attempt < max_retries - 1:
                    logger.warning(f"  ⚠️ Erro na sessão {session.index}: {e}. Tentando outra...")
                    await asyncio.sleep(2)
                    continue
                raise

        raise Exception(f"❌ Falha após {max_retries} tentativas")

    async def download_media(self, message, session_hint: Optional[SessionInfo] = None):
        """Baixa mídia usando o pool de sessões."""
        async def _download(client, msg):
            return await msg.download_media(file=bytes)

        if session_hint and session_hint.is_available():
            # Tenta usar a sessão sugerida primeiro
            try:
                async with self.download_semaphores[session_hint.index]:
                    session_hint.mark_request()
                    result = await _download(session_hint.client, message)
                    session_hint.reset_errors()
                    return result
            except FloodWaitError as e:
                session_hint.mark_flood_wait(e.seconds + 5)
                logger.warning(f"  🔄 Sessão {session_hint.index} em FloodWait, usando outra...")
            except Exception as e:
                session_hint.mark_error()
                logger.warning(f"  🔄 Erro na sessão {session_hint.index}, usando outra...")

        # Usa qualquer sessão disponível do pool
        return await self.execute_with_session(_download, message)

    async def get_entity(self, entity_id):
        """Obtém entidade do Telegram usando o pool."""
        async def _get_entity(client, eid):
            return await client.get_entity(eid)

        return await self.execute_with_session(_get_entity, entity_id)

    async def iter_messages_batch(self, entity, limit: int, offset_id: int = 0):
        """Itera mensagens usando uma sessão do pool."""
        session = await self.get_next_session()

        if not session:
            raise Exception("❌ Nenhuma sessão disponível para iterar mensagens")

        messages = []

        try:
            async for msg in session.client.iter_messages(
                    entity,
                    limit=limit,
                    offset_id=offset_id,
                    reverse=True
            ):
                messages.append(msg)

                # Rate limiting suave
                if len(messages) % 50 == 0:
                    await asyncio.sleep(0.5)

            session.reset_errors()
            return messages

        except FloodWaitError as e:
            session.mark_flood_wait(e.seconds + 5)
            raise
        except Exception as e:
            session.mark_error()
            raise

    async def close_all(self):
        """Desconecta todas as sessões."""
        logger.info("🔌 Fechando todas as sessões do pool...")

        for session in self.sessions:
            try:
                await session.client.disconnect()
                logger.info(f"  ✅ Sessão {session.index} desconectada")
            except Exception as e:
                logger.error(f"  ❌ Erro ao desconectar sessão {session.index}: {e}")

    def get_pool_status(self) -> dict:
        """Retorna status do pool de sessões."""
        available = sum(1 for s in self.sessions if s.is_available())
        in_flood = sum(1 for s in self.sessions if s.flood_wait_until > time.time())

        return {
            "total": len(self.sessions),
            "available": available,
            "in_flood_wait": in_flood,
            "total_requests": sum(s.requests_count for s in self.sessions)
        }