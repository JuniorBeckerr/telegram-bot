"""
TDLib Session Pool - Performance Máxima
Substitui SessionPoolBalanced do Telethon
"""
import asyncio
import logging
import time
from typing import Dict, List, Optional
from collections import deque
from telegram.client import Telegram
from telegram.client import AsyncResult
from app.services.now.tdlib_config import TDLibConfig

logger = logging.getLogger(__name__)


class TDLibSessionInfo:
    """Informações de uma sessão TDLib"""

    def __init__(self, client: Telegram, index: int, credential: dict):
        self.client = client
        self.index = index
        self.credential = credential
        self.is_connected = False
        self.is_authorized = False

        # Estatísticas
        self.last_request_time = 0
        self.requests_count = 0
        self.downloads_count = 0
        self.failed_count = 0

        # Controle de rate limit (TDLib é mais generoso)
        self.cooldown_until = 0

    def is_available(self) -> bool:
        """Verifica se sessão está disponível"""
        now = time.time()
        if self.cooldown_until > now:
            return False
        return self.is_connected and self.is_authorized

    def get_cooldown(self) -> float:
        """Retorna tempo de cooldown necessário"""
        now = time.time()
        if self.cooldown_until > now:
            return self.cooldown_until - now

        # TDLib: cooldown muito menor (50ms)
        time_since_last = now - self.last_request_time
        if time_since_last < TDLibConfig.DOWNLOAD_DELAY:
            return TDLibConfig.DOWNLOAD_DELAY - time_since_last
        return 0

    def mark_request(self):
        """Registra uma requisição"""
        self.last_request_time = time.time()
        self.requests_count += 1

    def mark_download_success(self):
        """Registra download bem-sucedido"""
        self.downloads_count += 1
        self.failed_count = 0  # Reset contador de falhas

    def mark_download_failed(self):
        """Registra falha no download"""
        self.failed_count += 1

        # Se muitas falhas, aumenta cooldown
        if self.failed_count >= 3:
            self.cooldown_until = time.time() + 10
            logger.warning(f"⚠️ Sessão {self.index} com {self.failed_count} falhas - cooldown 10s")


class TDLibSessionPool:
    """
    Pool de sessões TDLib otimizado para download em massa
    
    Performance esperada:
    - 100-150k mensagens/dia
    - 50+ downloads simultâneos
    - Rate limits muito mais generosos que Telethon
    """

    def __init__(self, credential: dict):
        self.credential = credential
        self.sessions: List[TDLibSessionInfo] = []
        self.session_queue: deque = deque()
        self.lock = asyncio.Lock()

        # Semáforo global para controlar concorrência
        self.download_semaphore = asyncio.Semaphore(TDLibConfig.MAX_CONCURRENT_DOWNLOADS)

        # Cache de chats
        self.chat_cache: Dict[int, dict] = {}

    async def initialize(self, num_sessions: int = 5) -> bool:
        """
        Inicializa pool de sessões TDLib
        
        Args:
            num_sessions: Número de sessões a criar (TDLib pode usar menos sessões)
        """
        logger.info(f"🚀 Inicializando TDLib com {num_sessions} sessões...")
        logger.info(f"📊 Concorrência: {TDLibConfig.MAX_CONCURRENT_DOWNLOADS} downloads simultâneos")

        tasks = []
        for idx in range(num_sessions):
            tasks.append(self._create_session(idx))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, TDLibSessionInfo) and result.is_authorized:
                self.sessions.append(result)
                self.session_queue.append(result)

        if not self.sessions:
            logger.error("❌ Nenhuma sessão TDLib conectada")
            return False

        logger.info(f"✅ {len(self.sessions)} sessões TDLib prontas")
        logger.info(f"⚡ Performance estimada: ~{TDLibConfig.MAX_CONCURRENT_DOWNLOADS * 3600} msgs/hora")
        return True

    async def _create_session(self, index: int) -> Optional[TDLibSessionInfo]:
        """Cria e autentica uma sessão TDLib"""
        try:
            params = TDLibConfig.get_session_params(self.credential, index)

            # Cria cliente TDLib
            tg = Telegram(
                api_id=params["api_id"],
                api_hash=params["api_hash"],
                phone=params["phone_number"],
                database_encryption_key=params.get("database_encryption_key", ""),
                files_directory=params["files_directory"],
                tdlib_verbosity=0  # Desabilita logs verbosos
            )

            # Inicia cliente
            tg.login()

            # Aguarda autorização
            await asyncio.sleep(2)  # TDLib precisa de tempo para carregar

            session_info = TDLibSessionInfo(tg, index, self.credential)
            session_info.is_connected = True
            session_info.is_authorized = True

            logger.info(f"  ✅ Sessão TDLib {index} autenticada")
            return session_info

        except Exception as e:
            logger.error(f"  ❌ Erro na sessão {index}: {e}")
            return None

    async def get_next_session(self, max_wait: float = 30.0) -> Optional[TDLibSessionInfo]:
        """Obtém próxima sessão disponível com load balancing"""
        start_time = time.time()

        while time.time() - start_time < max_wait:
            async with self.lock:
                # Tenta todas as sessões no queue
                for _ in range(len(self.session_queue)):
                    session = self.session_queue[0]
                    self.session_queue.rotate(-1)

                    if session.is_available():
                        cooldown = session.get_cooldown()
                        if cooldown > 0:
                            await asyncio.sleep(cooldown)
                        return session

            # Se nenhuma disponível, aguarda um pouco
            await asyncio.sleep(0.1)

        logger.warning("⚠️ Timeout aguardando sessão disponível")
        return None

    async def get_chat(self, chat_id: int):
        """Obtém informações de um chat (com cache)"""
        if chat_id in self.chat_cache:
            return self.chat_cache[chat_id]

        session = await self.get_next_session()
        if not session:
            raise Exception("Nenhuma sessão disponível")

        try:
            session.mark_request()
            result: AsyncResult = session.client.get_chat(chat_id)
            chat_info = await result.wait()

            self.chat_cache[chat_id] = chat_info
            return chat_info

        except Exception as e:
            logger.error(f"❌ Erro ao buscar chat {chat_id}: {e}")
            raise

    async def get_messages_batch(
            self,
            chat_id: int,
            limit: int = 300,
            from_message_id: int = 0
    ) -> List[dict]:
        """
        Busca mensagens de um chat
        
        TDLib usa paginação diferente: from_message_id em vez de offset_id
        """
        session = await self.get_next_session()
        if not session:
            raise Exception("Nenhuma sessão disponível")

        try:
            session.mark_request()

            result: AsyncResult = session.client.get_chat_history(
                chat_id=chat_id,
                from_message_id=from_message_id,
                limit=limit,
                offset=0,
                only_local=False
            )

            response = await result.wait()
            messages = response.get("messages", [])

            logger.debug(f"📥 Buscadas {len(messages)} mensagens do chat {chat_id}")
            return messages

        except Exception as e:
            logger.error(f"❌ Erro ao buscar mensagens: {e}")
            raise

    async def download_media(self, message: dict) -> Optional[bytes]:
        """
        Download de mídia com TDLib (muito mais rápido que Telethon)
        
        Args:
            message: Dicionário com dados da mensagem do TDLib
            
        Returns:
            bytes da mídia ou None se falhar
        """
        # Extrai file_id da mensagem
        file_id = self._extract_file_id(message)
        if not file_id:
            return None

        # Controla concorrência global
        async with self.download_semaphore:
            for attempt in range(2):
                session = await self.get_next_session()

                if not session:
                    if attempt < 1:
                        await asyncio.sleep(1)
                        continue
                    raise Exception("Nenhuma sessão disponível")

                try:
                    session.mark_request()

                    # Download com TDLib
                    result: AsyncResult = session.client.download_file(
                        file_id=file_id,
                        priority=16,  # Alta prioridade
                        synchronous=True  # Download síncrono (mais rápido)
                    )

                    file_info = await asyncio.wait_for(
                        result.wait(),
                        timeout=TDLibConfig.DOWNLOAD_TIMEOUT
                    )

                    # Lê arquivo baixado
                    local_path = file_info.get("local", {}).get("path")
                    if not local_path:
                        raise Exception("Caminho do arquivo não encontrado")

                    # Lê bytes
                    with open(local_path, "rb") as f:
                        file_bytes = f.read()

                    session.mark_download_success()
                    return file_bytes

                except asyncio.TimeoutError:
                    logger.warning(f"⏱️ Timeout no download (tentativa {attempt + 1}/2)")
                    session.mark_download_failed()
                    if attempt < 1:
                        await asyncio.sleep(1)
                        continue
                    return None

                except Exception as e:
                    logger.error(f"❌ Erro no download (tentativa {attempt + 1}/2): {e}")
                    session.mark_download_failed()
                    if attempt < 1:
                        await asyncio.sleep(1)
                        continue
                    return None

        return None

    def _extract_file_id(self, message: dict) -> Optional[int]:
        """Extrai file_id de uma mensagem TDLib"""
        content = message.get("content", {})
        content_type = content.get("@type", "")

        # Foto
        if content_type == "messagePhoto":
            sizes = content.get("photo", {}).get("sizes", [])
            if sizes:
                return sizes[-1].get("photo", {}).get("id")

        # Vídeo
        elif content_type == "messageVideo":
            return content.get("video", {}).get("video", {}).get("id")

        # Documento
        elif content_type == "messageDocument":
            return content.get("document", {}).get("document", {}).get("id")

        # Animação (GIF)
        elif content_type == "messageAnimation":
            return content.get("animation", {}).get("animation", {}).get("id")

        return None

    async def close_all(self):
        """Fecha todas as sessões"""
        logger.info("🔌 Fechando sessões TDLib...")

        for session in self.sessions:
            try:
                session.client.stop()
            except Exception as e:
                logger.error(f"Erro ao fechar sessão {session.index}: {e}")

        logger.info("✅ Sessões fechadas")

    def get_pool_status(self) -> dict:
        """Retorna status do pool"""
        available = sum(1 for s in self.sessions if s.is_available())
        total_downloads = sum(s.downloads_count for s in self.sessions)
        total_requests = sum(s.requests_count for s in self.sessions)

        return {
            "total_sessions": len(self.sessions),
            "available_sessions": available,
            "total_downloads": total_downloads,
            "total_requests": total_requests,
            "avg_downloads_per_session": total_downloads / len(self.sessions) if self.sessions else 0
        }