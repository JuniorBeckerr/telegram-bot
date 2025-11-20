"""
Telethon Sender Service - Para envio de arquivos grandes (>50MB)
Usa a API do Telegram diretamente, sem limite de tamanho
"""
import logging
import os
from typing import List, Optional, Union
from telethon import TelegramClient
from telethon.sessions import StringSession

logger = logging.getLogger(__name__)


class TelethonSenderService:
    """
    Serviço para envio de arquivos grandes via Telethon.
    Usado quando o arquivo excede o limite de 50MB do Bot API.
    """

    def __init__(self, api_id: int, api_hash: str, session_string: str):
        """
        Inicializa o cliente Telethon.
        
        Args:
            api_id: API ID do Telegram
            api_hash: API Hash do Telegram
            session_string: String de sessão do Telethon
        """
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_string = session_string
        self.client: Optional[TelegramClient] = None

    async def connect(self):
        """Conecta ao Telegram"""
        if self.client is None:
            self.client = TelegramClient(
                session=StringSession(self.session_string),
                api_id=self.api_id,
                api_hash=self.api_hash
            )

        if not self.client.is_connected():
            await self.client.connect()
            logger.info("✅ Telethon conectado")

    async def disconnect(self):
        """Desconecta do Telegram"""
        if self.client and self.client.is_connected():
            await self.client.disconnect()
            logger.info("🔌 Telethon desconectado")

    async def send_file(self, chat_id: Union[int, str], file_path: str,
                        caption: str = None) -> dict:
        """
        Envia um único arquivo.
        
        Args:
            chat_id: ID do chat destino
            file_path: Caminho do arquivo local
            caption: Legenda opcional
            
        Returns:
            Mensagem enviada como dict
        """
        await self.connect()

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

        entity = await self.client.get_entity(chat_id)

        message = await self.client.send_file(
            entity=entity,
            file=file_path,
            caption=caption,
            supports_streaming=True,
            force_document=False
        )

        logger.info(f"✅ Arquivo enviado via Telethon: {file_path}")

        return message.to_dict()

    async def send_album(self, chat_id: Union[int, str], files: List[str],
                         caption: str = None) -> List[dict]:
        """
        Envia um álbum de arquivos.
        
        Args:
            chat_id: ID do chat destino
            files: Lista de caminhos de arquivos locais
            caption: Legenda (aparece no primeiro item)
            
        Returns:
            Lista de mensagens enviadas como dicts
        """
        await self.connect()

        # Verifica se todos os arquivos existem
        for file_path in files:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

        entity = await self.client.get_entity(chat_id)

        # Telethon aceita lista de arquivos para álbum
        messages = await self.client.send_file(
            entity=entity,
            file=files,
            caption=caption,
            supports_streaming=True,
            force_document=False
        )

        # send_file retorna lista quando múltiplos arquivos
        if not isinstance(messages, list):
            messages = [messages]

        logger.info(f"✅ Álbum enviado via Telethon: {len(messages)} arquivos")

        return [msg.to_dict() for msg in messages]

    async def send_video(self, chat_id: Union[int, str], file_path: str,
                         caption: str = None,
                         thumb: str = None) -> dict:
        """
        Envia um vídeo com suporte a streaming.
        
        Args:
            chat_id: ID do chat destino
            file_path: Caminho do vídeo
            caption: Legenda opcional
            thumb: Thumbnail opcional
            
        Returns:
            Mensagem enviada como dict
        """
        await self.connect()

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

        entity = await self.client.get_entity(chat_id)

        message = await self.client.send_file(
            entity=entity,
            file=file_path,
            caption=caption,
            supports_streaming=True,
            thumb=thumb,
            force_document=False
        )

        logger.info(f"✅ Vídeo enviado via Telethon: {file_path}")

        return message.to_dict()

    async def __aenter__(self):
        """Context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        await self.disconnect()