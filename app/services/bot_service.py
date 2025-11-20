"""
Bot Service V2 - Comunicação com Telegram Bot API
Com suporte a thumbnails em álbuns
"""
import asyncio
import aiohttp
import logging
import json
import os
from typing import Optional, Dict, Any, Union, List
from pathlib import Path

logger = logging.getLogger(__name__)


class BotServiceV2:
    """
    Serviço para comunicação com a API de Bots do Telegram.
    Versão 2 com suporte a thumbnails em álbuns.
    """

    # Servidor local (sem limite de tamanho)
    BASE_URL = "https://api.telegram.org/bot{token}/{method}"
    # BASE_URL = "http://154.38.174.118:9000/bot{token}/{method}"

    def __init__(self, token: str, timeout: int = 120, base_url: str = None):
        self.token = token
        self.timeout = timeout
        self._session: Optional[aiohttp.ClientSession] = None
        self._rate_limit_delay = 0.05
        self._last_request_time = 0

        if base_url:
            self.base_url = base_url
        else:
            self.base_url = self.BASE_URL

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, method: str, **kwargs) -> Dict[str, Any]:
        import time
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._rate_limit_delay:
            await asyncio.sleep(self._rate_limit_delay - elapsed)

        url = self.base_url.format(token=self.token, method=method)
        session = await self._get_session()

        try:
            data = {k: v for k, v in kwargs.items() if v is not None}

            async with session.post(url, json=data) as response:
                self._last_request_time = time.time()
                result = await response.json()

                if not result.get("ok"):
                    error_code = result.get("error_code", "unknown")
                    description = result.get("description", "Unknown error")

                    if error_code == 429:
                        retry_after = result.get("parameters", {}).get("retry_after", 30)
                        logger.warning(f"Rate limited. Aguardando {retry_after}s...")
                        await asyncio.sleep(retry_after)
                        return await self._request(method, **kwargs)

                    raise BotApiError(f"[{error_code}] {description}")

                return result.get("result", {})

        except aiohttp.ClientError as e:
            raise BotApiError(f"Erro de conexão: {e}")
        except asyncio.TimeoutError:
            raise BotApiError("Timeout na requisição")

    async def _request_with_file(self, method: str, file_field: str,
                                 file_path: str, **kwargs) -> Dict[str, Any]:
        url = self.base_url.format(token=self.token, method=method)
        session = await self._get_session()

        try:
            data = aiohttp.FormData()

            file_path = Path(file_path)
            if not file_path.exists():
                raise BotApiError(f"Arquivo não encontrado: {file_path}")

            data.add_field(
                file_field,
                open(file_path, 'rb'),
                filename=file_path.name
            )

            for key, value in kwargs.items():
                if value is not None:
                    if key == 'thumb' and Path(value).exists():
                        # Thumb é arquivo
                        data.add_field(
                            'thumb',
                            open(value, 'rb'),
                            filename=Path(value).name
                        )
                    else:
                        data.add_field(key, str(value))

            async with session.post(url, data=data) as response:
                self._last_request_time = time.time()
                result = await response.json()

                if not result.get("ok"):
                    error_code = result.get("error_code", "unknown")
                    description = result.get("description", "Unknown error")
                    raise BotApiError(f"[{error_code}] {description}")

                return result.get("result", {})

        except aiohttp.ClientError as e:
            raise BotApiError(f"Erro de conexão: {e}")

    # =====================================================
    # MÉTODOS DE INFORMAÇÃO
    # =====================================================

    async def get_me(self) -> Dict[str, Any]:
        return await self._request("getMe")

    async def get_chat(self, chat_id: Union[int, str]) -> Dict[str, Any]:
        return await self._request("getChat", chat_id=chat_id)

    # =====================================================
    # MÉTODOS DE ENVIO
    # =====================================================

    async def send_message(self, chat_id: Union[int, str], text: str,
                           parse_mode: str = None,
                           disable_notification: bool = False) -> Dict[str, Any]:
        return await self._request(
            "sendMessage",
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            disable_notification=disable_notification
        )

    async def send_photo(self, chat_id: Union[int, str],
                         photo: str,
                         caption: str = None,
                         parse_mode: str = None,
                         disable_notification: bool = False) -> Dict[str, Any]:
        if photo and not photo.startswith(('http', 'https')) and Path(photo).exists():
            return await self._request_with_file(
                "sendPhoto",
                "photo",
                photo,
                chat_id=chat_id,
                caption=caption,
                parse_mode=parse_mode,
                disable_notification=disable_notification
            )

        return await self._request(
            "sendPhoto",
            chat_id=chat_id,
            photo=photo,
            caption=caption,
            parse_mode=parse_mode,
            disable_notification=disable_notification
        )

    async def send_video(self, chat_id: Union[int, str],
                         video: str,
                         caption: str = None,
                         thumb: str = None,
                         duration: int = None,
                         width: int = None,
                         height: int = None,
                         supports_streaming: bool = True,
                         disable_notification: bool = False) -> Dict[str, Any]:
        """Envia vídeo com suporte a thumbnail"""
        if video and not video.startswith(('http', 'https')) and Path(video).exists():
            return await self._request_with_file(
                "sendVideo",
                "video",
                video,
                chat_id=chat_id,
                caption=caption,
                thumb=thumb,
                duration=duration,
                width=width,
                height=height,
                supports_streaming=supports_streaming,
                disable_notification=disable_notification
            )

        return await self._request(
            "sendVideo",
            chat_id=chat_id,
            video=video,
            caption=caption,
            duration=duration,
            width=width,
            height=height,
            supports_streaming=supports_streaming,
            disable_notification=disable_notification
        )

    async def send_document(self, chat_id: Union[int, str],
                            document: str,
                            caption: str = None,
                            thumb: str = None,
                            disable_notification: bool = False) -> Dict[str, Any]:
        if document and not document.startswith(('http', 'https')) and Path(document).exists():
            return await self._request_with_file(
                "sendDocument",
                "document",
                document,
                chat_id=chat_id,
                caption=caption,
                thumb=thumb,
                disable_notification=disable_notification
            )

        return await self._request(
            "sendDocument",
            chat_id=chat_id,
            document=document,
            caption=caption,
            disable_notification=disable_notification
        )

    async def send_media_group_with_thumbs(self, chat_id: Union[int, str],
                                           items: List[Dict],
                                           caption: str = None,
                                           disable_notification: bool = False) -> List[Dict[str, Any]]:
        """
        Envia um grupo de mídias (álbum) com upload direto e suporte a thumbnails.

        Args:
            chat_id: ID do chat destino
            items: Lista de dicts com:
                   - temp_path: caminho do arquivo local
                   - type: 'photo' ou 'video'
                   - thumb_path: (opcional) caminho do thumbnail para vídeos
                   - duration: (opcional) duração em segundos
                   - width: (opcional) largura
                   - height: (opcional) altura
            caption: Caption para o primeiro item

        Returns:
            Lista de mensagens enviadas
        """
        import time
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._rate_limit_delay:
            await asyncio.sleep(self._rate_limit_delay - elapsed)

        url = self.base_url.format(token=self.token, method="sendMediaGroup")
        session = await self._get_session()

        try:
            data = aiohttp.FormData()
            data.add_field("chat_id", str(chat_id))

            if disable_notification:
                data.add_field("disable_notification", "true")

            media_list = []
            file_index = 0

            for i, item in enumerate(items):
                temp_path = item["temp_path"]
                media_type = item["type"]

                # Nome do campo para o arquivo principal
                attach_name = f"file_{file_index}"
                file_index += 1

                # Adiciona arquivo principal ao form
                with open(temp_path, 'rb') as f:
                    file_content = f.read()

                filename = os.path.basename(temp_path)
                data.add_field(
                    attach_name,
                    file_content,
                    filename=filename,
                    content_type='application/octet-stream'
                )

                # Monta item da media list
                media_item = {
                    "type": media_type,
                    "media": f"attach://{attach_name}"
                }

                # Caption só no primeiro
                if i == 0 and caption:
                    media_item["caption"] = caption

                # Para vídeos, adiciona thumbnail e metadados
                if media_type == "video":
                    # Thumbnail
                    thumb_path = item.get("thumb_path")
                    if thumb_path and os.path.exists(thumb_path):
                        thumb_attach_name = f"thumb_{file_index}"
                        file_index += 1

                        with open(thumb_path, 'rb') as f:
                            thumb_content = f.read()

                        data.add_field(
                            thumb_attach_name,
                            thumb_content,
                            filename=os.path.basename(thumb_path),
                            content_type='image/jpeg'
                        )

                        media_item["thumbnail"] = f"attach://{thumb_attach_name}"

                    # Metadados
                    if item.get("duration"):
                        media_item["duration"] = item["duration"]
                    if item.get("width"):
                        media_item["width"] = item["width"]
                    if item.get("height"):
                        media_item["height"] = item["height"]

                    # Importante para streaming
                    media_item["supports_streaming"] = True

                media_list.append(media_item)

            # Adiciona media list como JSON
            data.add_field("media", json.dumps(media_list))

            logger.debug(f"📤 Enviando álbum com {len(items)} mídias")

            async with session.post(url, data=data) as response:
                self._last_request_time = time.time()
                result = await response.json()

                if not result.get("ok"):
                    error_code = result.get("error_code", "unknown")
                    description = result.get("description", "Unknown error")

                    if error_code == 429:
                        retry_after = result.get("parameters", {}).get("retry_after", 30)
                        logger.warning(f"Rate limited. Aguardando {retry_after}s...")
                        await asyncio.sleep(retry_after)
                        return await self.send_media_group_with_thumbs(
                            chat_id, items, caption, disable_notification
                        )

                    raise BotApiError(f"[{error_code}] {description}")

                return result.get("result", [])

        except aiohttp.ClientError as e:
            raise BotApiError(f"Erro de conexão: {e}")
        except asyncio.TimeoutError:
            raise BotApiError("Timeout na requisição")

    # Alias para compatibilidade
    async def send_media_group_upload(self, chat_id: Union[int, str],
                                      items: List[Dict],
                                      caption: str = None,
                                      disable_notification: bool = False) -> List[Dict[str, Any]]:
        """Alias para send_media_group_with_thumbs"""
        return await self.send_media_group_with_thumbs(
            chat_id, items, caption, disable_notification
        )

    # =====================================================
    # MÉTODOS DE EDIÇÃO
    # =====================================================

    async def edit_message_caption(self, chat_id: Union[int, str],
                                   message_id: int,
                                   caption: str,
                                   parse_mode: str = None) -> Dict[str, Any]:
        return await self._request(
            "editMessageCaption",
            chat_id=chat_id,
            message_id=message_id,
            caption=caption,
            parse_mode=parse_mode
        )

    async def delete_message(self, chat_id: Union[int, str],
                             message_id: int) -> bool:
        return await self._request(
            "deleteMessage",
            chat_id=chat_id,
            message_id=message_id
        )

    # =====================================================
    # UTILITÁRIOS
    # =====================================================

    async def get_file(self, file_id: str) -> Dict[str, Any]:
        return await self._request("getFile", file_id=file_id)

    def get_file_url(self, file_path: str) -> str:
        base = self.base_url.replace("/bot{token}/{method}", "")
        return f"{base}/file/bot{self.token}/{file_path}"

    async def download_file(self, file_id: str, dest_path: str) -> str:
        file_info = await self.get_file(file_id)
        file_url = self.get_file_url(file_info["file_path"])

        session = await self._get_session()
        async with session.get(file_url) as response:
            if response.status == 200:
                dest = Path(dest_path)
                dest.parent.mkdir(parents=True, exist_ok=True)

                with open(dest, 'wb') as f:
                    f.write(await response.read())

                return str(dest)
            else:
                raise BotApiError(f"Erro ao baixar arquivo: {response.status}")


class BotApiError(Exception):
    """Erro na API do Bot"""
    pass