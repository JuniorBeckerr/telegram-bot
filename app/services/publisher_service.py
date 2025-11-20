"""
Publisher Service V2 - Com Download Workers e Upload via FormData
100% de acertividade usando FormData para envio de mídias
"""
import asyncio
import aiohttp
import logging
import tempfile
import os
import shutil
import subprocess
from typing import Optional, Dict, Any, List
from datetime import datetime
from collections import defaultdict
from pathlib import Path
import random

from app.repository.models_repository import ModelsRepository
from app.services.bot_service import BotServiceV2, BotApiError
from app.repository.bot_repository import BotsRepository
from app.repository.groups_bot_repository import GroupBotsRepository
from app.repository.groups_publish_repository import GroupPublishRepository
from app.repository.publish_queue_repository import PublishQueueRepository
from app.repository.publish_rules_repository import PublishRulesRepository
from app.repository.publish_log_repository import PublishLogRepository
from app.repository.publish_stats_repository import PublishStatsRepository
from app.repository.groups_repository import GroupsRepository
from app.repository.media_repository import MediaRepository
from app.repository.media_classifications_repository import MediaClassificationsRepository
from config.settings import Config

logger = logging.getLogger(__name__)


class DownloadWorker:
    """Worker para download paralelo de mídias"""

    def __init__(self, worker_id: int, session: aiohttp.ClientSession):
        self.worker_id = worker_id
        self.session = session
        self.downloaded = 0
        self.failed = 0

    async def download(self, url: str, dest_path: str, timeout: int = 120) -> bool:
        """
        Baixa um arquivo da URL para o caminho de destino.

        Args:
            url: URL do arquivo
            dest_path: Caminho local para salvar
            timeout: Timeout em segundos

        Returns:
            True se sucesso, False se falha
        """
        try:
            timeout_config = aiohttp.ClientTimeout(total=timeout)

            async with self.session.get(url, timeout=timeout_config) as response:
                if response.status != 200:
                    logger.error(f"Worker {self.worker_id}: HTTP {response.status} para {url}")
                    self.failed += 1
                    return False

                # Cria diretório se não existir
                Path(dest_path).parent.mkdir(parents=True, exist_ok=True)

                # Baixa em chunks para não estourar memória
                with open(dest_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)

                self.downloaded += 1
                logger.debug(f"Worker {self.worker_id}: Download OK - {dest_path}")
                return True

        except asyncio.TimeoutError:
            logger.error(f"Worker {self.worker_id}: Timeout ao baixar {url}")
            self.failed += 1
            return False
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Erro ao baixar {url}: {e}")
            self.failed += 1
            return False


class DownloadManager:
    """Gerencia pool de workers para download paralelo"""

    def __init__(self, num_workers: int = 5, timeout: int = 120):
        self.num_workers = num_workers
        self.timeout = timeout
        self._session: Optional[aiohttp.ClientSession] = None
        self._workers: List[DownloadWorker] = []

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=self.num_workers * 2)
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def download_batch(self, items: List[Dict]) -> List[Dict]:
        """
        Baixa um batch de itens em paralelo usando workers.

        Args:
            items: Lista de dicts com:
                   - url: URL do arquivo
                   - dest_path: Caminho de destino
                   - (outros campos são preservados)

        Returns:
            Lista de items com campo 'downloaded' (True/False) adicionado
        """
        if not items:
            return []

        session = await self._get_session()

        # Cria workers
        workers = [DownloadWorker(i, session) for i in range(self.num_workers)]

        # Fila de trabalho
        queue = asyncio.Queue()
        for item in items:
            await queue.put(item)

        # Resultados
        results = []
        results_lock = asyncio.Lock()

        async def worker_task(worker: DownloadWorker):
            while True:
                try:
                    item = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                success = await worker.download(
                    item["url"],
                    item["dest_path"],
                    self.timeout
                )

                item["downloaded"] = success

                async with results_lock:
                    results.append(item)

                queue.task_done()

        # Executa workers em paralelo
        tasks = [worker_task(w) for w in workers]
        await asyncio.gather(*tasks)

        # Log estatísticas
        total_ok = sum(1 for r in results if r.get("downloaded"))
        total_fail = len(results) - total_ok
        logger.info(f"📥 Download batch: {total_ok} OK, {total_fail} falhas")

        return results


class PublisherServiceV2:
    """
    Serviço de publicação com download workers e upload via FormData.
    Garante 100% de acertividade usando arquivos locais.
    """

    def __init__(self, download_workers: int = 5):
        # Repositories
        self.groups_repo = GroupsRepository()
        self.bots_repo = BotsRepository()
        self.group_bots_repo = GroupBotsRepository()
        self.group_publish_repo = GroupPublishRepository()
        self.queue_repo = PublishQueueRepository()
        self.rules_repo = PublishRulesRepository()
        self.log_repo = PublishLogRepository()
        self.stats_repo = PublishStatsRepository()
        self.media_repo = MediaRepository()
        self.models_repo = ModelsRepository()
        self.classifications_repo = MediaClassificationsRepository()

        # Configurações
        self.batch_size = getattr(Config, 'PUBLISHER_BATCH_SIZE', 6)
        self.min_interval = getattr(Config, 'PUBLISHER_MIN_INTERVAL', 5)
        self.max_retries = getattr(Config, 'PUBLISHER_MAX_RETRIES', 3)
        self.storage_base_url = getattr(
            Config,
            'STORAGE_BASE_URL',
            'https://storage-becker.nyc3.digitaloceanspaces.com'
        )

        # Download manager
        self.download_manager = DownloadManager(
            num_workers=download_workers,
            timeout=getattr(Config, 'DOWNLOAD_TIMEOUT', 120)
        )

        # Cache de bot services
        self._bot_services: Dict[int, BotServiceV2] = {}

        # Diretório temporário para downloads
        self._temp_dir = tempfile.mkdtemp(prefix="publisher_")
        logger.info(f"📁 Temp dir: {self._temp_dir}")

    async def _get_bot_service(self, bot_id: int) -> Optional[BotServiceV2]:
        """Retorna instância do BotServiceV2 para um bot"""
        if bot_id not in self._bot_services:
            bot = self.bots_repo.find(bot_id)
            if not bot or not bot.get("active"):
                return None

            self._bot_services[bot_id] = BotServiceV2(
                token=bot["token"],
                timeout=getattr(Config, 'BOT_REQUEST_TIMEOUT', 120)
            )

        return self._bot_services[bot_id]

    async def close(self):
        """Fecha todas as conexões e limpa arquivos temporários"""
        # Fecha bot services
        for bot_service in self._bot_services.values():
            await bot_service.close()
        self._bot_services.clear()

        # Fecha download manager
        await self.download_manager.close()

        # Limpa diretório temporário
        if os.path.exists(self._temp_dir):
            shutil.rmtree(self._temp_dir)
            logger.info(f"🗑️ Temp dir removido: {self._temp_dir}")

    def _get_temp_path(self, media_id: int, extension: str) -> str:
        """Gera caminho temporário para uma mídia"""
        return os.path.join(self._temp_dir, f"media_{media_id}{extension}")

    def _get_extension_from_mime(self, mime_type: str) -> str:
        """Retorna extensão baseada no mime type"""
        mime_map = {
            'image/jpeg': '.jpg',
            'image/png': '.png',
            'image/gif': '.gif',
            'image/webp': '.webp',
            'video/mp4': '.mp4',
            'video/quicktime': '.mov',
            'video/webm': '.webm',
            'video/x-matroska': '.mkv',
        }
        return mime_map.get(mime_type, '.bin')

    def _extract_video_thumbnail(self, video_path: str) -> Optional[str]:
        """
        Extrai um frame do vídeo para usar como thumbnail.

        Args:
            video_path: Caminho do vídeo

        Returns:
            Caminho do thumbnail ou None se falhar
        """
        try:
            thumb_path = video_path.rsplit('.', 1)[0] + '_thumb.jpg'

            # Extrai frame em 1 segundo do vídeo
            # -ss 1: vai para 1 segundo
            # -vframes 1: captura 1 frame
            # -vf scale: redimensiona para max 320px mantendo proporção
            cmd = [
                'ffmpeg',
                '-i', video_path,
                '-ss', '1',
                '-vframes', '1',
                '-vf', 'scale=320:-1',
                '-y',  # Sobrescreve se existir
                thumb_path
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=30
            )

            if result.returncode == 0 and os.path.exists(thumb_path):
                logger.debug(f"🖼️ Thumbnail extraído: {thumb_path}")
                return thumb_path
            else:
                # Tenta extrair do início se 1s falhar
                cmd[4] = '0'
                result = subprocess.run(cmd, capture_output=True, timeout=30)

                if result.returncode == 0 and os.path.exists(thumb_path):
                    return thumb_path

                logger.warning(f"⚠️ Falha ao extrair thumbnail: {result.stderr.decode()[:200]}")
                return None

        except subprocess.TimeoutExpired:
            logger.warning(f"⚠️ Timeout ao extrair thumbnail de {video_path}")
            return None
        except Exception as e:
            logger.warning(f"⚠️ Erro ao extrair thumbnail: {e}")
            return None

    def _get_video_metadata(self, video_path: str) -> Dict[str, Any]:
        """
        Obtém metadados do vídeo (duração, dimensões).

        Returns:
            Dict com duration, width, height
        """
        try:
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                video_path
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=30
            )

            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)

                # Busca stream de vídeo
                video_stream = None
                for stream in data.get('streams', []):
                    if stream.get('codec_type') == 'video':
                        video_stream = stream
                        break

                metadata = {}

                # Duração
                if 'format' in data:
                    duration = data['format'].get('duration')
                    if duration:
                        metadata['duration'] = int(float(duration))

                # Dimensões
                if video_stream:
                    metadata['width'] = video_stream.get('width')
                    metadata['height'] = video_stream.get('height')

                return metadata

        except Exception as e:
            logger.warning(f"⚠️ Erro ao obter metadata: {e}")

        return {}

    # =====================================================
    # PROCESSAMENTO PRINCIPAL
    # =====================================================

    async def process_queue(self, group_id: int = None, limit: int = None):
        """Processa itens da fila de publicação."""
        limit = limit or self.batch_size * 10

        logger.info(f"🚀 Processando fila (limit={limit})")

        if group_id:
            pending = self.queue_repo.get_pending_for_group(group_id, limit)
        else:
            pending = self.queue_repo.get_pending(limit)

        if not pending:
            logger.info("📭 Fila vazia")
            return

        logger.info(f"📋 {len(pending)} itens pendentes")

        items_by_group = defaultdict(list)
        for item in pending:
            items_by_group[item["group_id"]].append(item)

        for gid, items in items_by_group.items():
            await self._process_group_items(gid, items)

    async def _process_group_items(self, group_id: int, items: List[Dict]):
        """Processa itens de um grupo específico."""
        logger.info(f"🎯 Processando grupo {group_id} ({len(items)} itens)")

        rules = self.rules_repo.get_rules_for_group(group_id)
        rule = rules[0] if rules else {}

        random_model = rule.get("random_model", False)
        batch_size = rule.get("batch_size", self.batch_size)

        if random_model:
            await self._process_random_model(group_id, items, batch_size)
        else:
            await self._process_sequential(group_id, items, batch_size)

    async def _process_random_model(self, group_id: int, items: List[Dict], batch_size: int):
        """Processa itens agrupados por modelo com download paralelo."""
        logger.info(f"🎲 Modo Random Model (batch_size={batch_size})")

        # Agrupa por model_id
        items_by_model = defaultdict(list)
        model_info = {}

        for item in items:
            media_id = item["media_id"]
            classification = self.classifications_repo.where("media_id", media_id).first()
            model_id = classification.get("model_id") if classification else None

            if model_id:
                model = self.models_repo.find(model_id)
                if model:
                    items_by_model[model_id].append(item)
                    if model_id not in model_info:
                        model_info[model_id] = {
                            "model_id": model_id,
                            "full_name": model.get("full_name", "Unknown"),
                            "stage_name": model.get("stage_name", "unknown")
                        }
                else:
                    items_by_model[0].append(item)
                    if 0 not in model_info:
                        model_info[0] = {"model_id": 0, "full_name": "Unknown", "stage_name": "unknown"}
            else:
                items_by_model[0].append(item)
                if 0 not in model_info:
                    model_info[0] = {"model_id": 0, "full_name": "Unknown", "stage_name": "unknown"}

        logger.info(f"📊 {len(items_by_model)} modelos encontrados")

        # Embaralha modelos
        model_ids = list(items_by_model.keys())
        random.shuffle(model_ids)

        # Busca bot do grupo
        group_bot = self.group_bots_repo.get_publisher_bot(group_id)
        if not group_bot:
            logger.error(f"❌ Nenhum bot para grupo {group_id}")
            return

        bot_id = group_bot["bot_id"]
        bot_service = await self._get_bot_service(bot_id)
        if not bot_service:
            logger.error(f"❌ Bot {bot_id} não disponível")
            return

        total_processed = 0
        total_failed = 0

        # Processa cada modelo
        for mid in model_ids:
            model_items = items_by_model[mid]
            info = model_info[mid]
            batch = model_items[:batch_size]

            logger.info(f"👤 Modelo: {info['full_name']} ({len(batch)} mídias)")

            caption = f"#{info['stage_name']}\n{info['full_name']}"

            processed, failed = await self._publish_album_with_download(
                bot_service, bot_id, group_id, batch, caption
            )

            total_processed += processed
            total_failed += failed

            await asyncio.sleep(self.min_interval)

        logger.info(f"✅ Random Model concluído: {total_processed} ok, {total_failed} falhas")

    # =====================================================
    # DOWNLOAD + PUBLICAÇÃO VIA FORMDATA
    # =====================================================

    async def _publish_album_with_download(self, bot_service: BotServiceV2, bot_id: int,
                                           group_id: int, items: List[Dict],
                                           caption: str) -> tuple:
        """
        Baixa mídias em paralelo e envia álbum via FormData com thumbnails.
        """
        # Prepara lista de downloads
        download_items = []

        for item in items:
            media_id = item["media_id"]
            media = self.media_repo.find(media_id)

            if not media:
                logger.warning(f"⚠️ Mídia {media_id} não encontrada")
                self.queue_repo.mark_failed(item["id"], "Mídia não encontrada")
                continue

            mime_type = media.get("mime", "")
            storage_key = media.get("storage_key", "")

            if not storage_key:
                logger.warning(f"⚠️ Mídia {media_id} sem storage_key")
                self.queue_repo.mark_failed(item["id"], "Sem storage_key")
                continue

            # URL e caminho temporário
            media_url = f"{self.storage_base_url}/{storage_key}"
            extension = self._get_extension_from_mime(mime_type)
            temp_path = self._get_temp_path(media_id, extension)

            # Determina tipo
            if mime_type.startswith("video/"):
                media_type = "video"
            else:
                media_type = "photo"

            download_items.append({
                "item": item,
                "media_id": media_id,
                "url": media_url,
                "dest_path": temp_path,
                "type": media_type,
                "mime": mime_type
            })

        if not download_items:
            return (0, len(items))

        # === FASE 1: Download paralelo ===
        logger.info(f"📥 Baixando {len(download_items)} mídias...")

        downloaded_items = await self.download_manager.download_batch(download_items)

        # Filtra apenas os que baixaram com sucesso
        ready_items = []
        failed_count = 0

        for data in downloaded_items:
            if data.get("downloaded"):
                ready_items.append(data)
            else:
                # Marca como falha
                self.queue_repo.mark_failed(
                    data["item"]["id"],
                    f"Falha no download: {data['url']}"
                )
                self._log_action(None, "download_failed", {
                    "media_id": data["media_id"],
                    "url": data["url"]
                })
                self.stats_repo.increment_failed(group_id)
                failed_count += 1

        if not ready_items:
            logger.error("❌ Nenhuma mídia baixada com sucesso")
            return (0, failed_count)

        # === FASE 2: Extrair thumbnails e metadados de vídeos ===
        logger.info(f"🖼️ Processando thumbnails e metadados...")

        for data in ready_items:
            if data["type"] == "video":
                # Extrai thumbnail
                thumb_path = self._extract_video_thumbnail(data["dest_path"])
                if thumb_path:
                    data["thumb_path"] = thumb_path

                # Obtém metadados
                metadata = self._get_video_metadata(data["dest_path"])
                if metadata.get("duration"):
                    data["duration"] = metadata["duration"]
                if metadata.get("width"):
                    data["width"] = metadata["width"]
                if metadata.get("height"):
                    data["height"] = metadata["height"]

        # === FASE 3: Envio via FormData com thumbnails ===
        logger.info(f"📤 Enviando {len(ready_items)} mídias via FormData...")

        try:
            # Prepara items para send_media_group_with_thumbs
            upload_items = []
            for i, data in enumerate(ready_items):
                upload_item = {
                    "temp_path": data["dest_path"],
                    "type": data["type"],
                    "data": data  # Preserva dados originais
                }

                # Adiciona thumb e metadados para vídeos
                if data["type"] == "video":
                    if data.get("thumb_path"):
                        upload_item["thumb_path"] = data["thumb_path"]
                    if data.get("duration"):
                        upload_item["duration"] = data["duration"]
                    if data.get("width"):
                        upload_item["width"] = data["width"]
                    if data.get("height"):
                        upload_item["height"] = data["height"]

                upload_items.append(upload_item)

            # Envia álbum
            results = await bot_service.send_media_group_with_thumbs(
                chat_id=group_id,
                items=upload_items,
                caption=caption,
                disable_notification=False
            )

            # Registra publicações
            processed = 0
            for i, result in enumerate(results):
                if i < len(ready_items):
                    data = ready_items[i]

                    publish_id = self.group_publish_repo.create({
                        "group_id": group_id,
                        "media_id": data["media_id"],
                        "bot_id": bot_id,
                        "telegram_message_id": result.get("message_id"),
                        "file_id": self._extract_file_id(result),
                        "caption": caption if i == 0 else None,
                        "status": "published",
                        "published_at": datetime.now().isoformat()
                    })

                    self.queue_repo.mark_completed(data["item"]["id"])
                    self._log_action(publish_id["id"], "published", {
                        "via": "formdata",
                        "file_size": os.path.getsize(data["dest_path"]),
                        "has_thumb": bool(data.get("thumb_path"))
                    })
                    processed += 1

            self.stats_repo.increment_published(group_id, processed)
            logger.info(f"✅ Álbum publicado ({processed} mídias)")

            return (processed, failed_count)

        except BotApiError as e:
            error_msg = str(e)
            logger.error(f"❌ Erro ao enviar álbum: {error_msg}")

            # Tenta identificar mídia problemática
            import re
            match = re.search(r'message #(\d+)', error_msg)
            failed_index = int(match.group(1)) - 1 if match else -1

            # Processa falhas
            success_items = []

            for i, data in enumerate(ready_items):
                if i == failed_index:
                    self.queue_repo.mark_failed(data["item"]["id"], error_msg)
                    self._log_action(None, "upload_failed", {
                        "media_id": data["media_id"],
                        "error": error_msg,
                        "temp_path": data["dest_path"]
                    })
                    self.stats_repo.increment_failed(group_id)
                    failed_count += 1
                    logger.error(f"❌ Mídia falhou: {data['media_id']}")
                else:
                    success_items.append(data)

            # Tenta reenviar os restantes
            if success_items and len(success_items) >= 1:
                logger.info(f"🔄 Tentando enviar {len(success_items)} mídias restantes...")

                # Reconstrói items para recursão
                retry_queue_items = [d["item"] for d in success_items]

                # Cria items já baixados
                retry_download_items = []
                for d in success_items:
                    retry_item = {
                        "item": d["item"],
                        "media_id": d["media_id"],
                        "dest_path": d["dest_path"],
                        "type": d["type"],
                        "downloaded": True
                    }
                    # Preserva thumb e metadados
                    if d.get("thumb_path"):
                        retry_item["thumb_path"] = d["thumb_path"]
                    if d.get("duration"):
                        retry_item["duration"] = d["duration"]
                    if d.get("width"):
                        retry_item["width"] = d["width"]
                    if d.get("height"):
                        retry_item["height"] = d["height"]

                    retry_download_items.append(retry_item)

                # Envia diretamente (já baixados)
                processed, more_failed = await self._send_downloaded_album(
                    bot_service, bot_id, group_id, retry_download_items, caption
                )
                return (processed, failed_count + more_failed)

            return (0, failed_count)

        finally:
            # Limpa arquivos temporários deste batch
            for data in ready_items:
                # Remove arquivo principal
                temp_path = data.get("dest_path")
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except Exception as e:
                        logger.warning(f"⚠️ Erro ao remover temp: {temp_path}: {e}")

                # Remove thumbnail
                thumb_path = data.get("thumb_path")
                if thumb_path and os.path.exists(thumb_path):
                    try:
                        os.remove(thumb_path)
                    except Exception as e:
                        logger.warning(f"⚠️ Erro ao remover thumb: {thumb_path}: {e}")

    async def _send_downloaded_album(self, bot_service: BotServiceV2, bot_id: int,
                                     group_id: int, items: List[Dict],
                                     caption: str) -> tuple:
        """
        Envia álbum com arquivos já baixados (incluindo thumbs).
        """
        if not items:
            return (0, 0)

        try:
            upload_items = []
            for data in items:
                upload_item = {
                    "temp_path": data["dest_path"],
                    "type": data["type"],
                    "data": data
                }

                # Adiciona thumb e metadados para vídeos
                if data["type"] == "video":
                    if data.get("thumb_path"):
                        upload_item["thumb_path"] = data["thumb_path"]
                    if data.get("duration"):
                        upload_item["duration"] = data["duration"]
                    if data.get("width"):
                        upload_item["width"] = data["width"]
                    if data.get("height"):
                        upload_item["height"] = data["height"]

                upload_items.append(upload_item)

            results = await bot_service.send_media_group_with_thumbs(
                chat_id=group_id,
                items=upload_items,
                caption=caption,
                disable_notification=False
            )

            processed = 0
            for i, result in enumerate(results):
                if i < len(items):
                    data = items[i]

                    publish_id = self.group_publish_repo.create({
                        "group_id": group_id,
                        "media_id": data["media_id"],
                        "bot_id": bot_id,
                        "telegram_message_id": result.get("message_id"),
                        "file_id": self._extract_file_id(result),
                        "caption": caption if i == 0 else None,
                        "status": "published",
                        "published_at": datetime.now().isoformat()
                    })

                    self.queue_repo.mark_completed(data["item"]["id"])
                    self._log_action(publish_id["id"], "published", {"via": "formdata_retry"})
                    processed += 1

            self.stats_repo.increment_published(group_id, processed)
            return (processed, 0)

        except BotApiError as e:
            logger.error(f"❌ Erro no retry: {e}")

            # Marca todos como falha
            for data in items:
                self.queue_repo.mark_failed(data["item"]["id"], str(e))
                self.stats_repo.increment_failed(group_id)

            return (0, len(items))

    # =====================================================
    # MODO SEQUENCIAL
    # =====================================================

    async def _process_sequential(self, group_id: int, items: List[Dict], batch_size: int):
        """Processa itens sequencialmente com download individual."""
        logger.info(f"📝 Modo Sequencial (batch_size={batch_size})")

        batch = items[:batch_size]
        processed = 0
        failed = 0

        for item in batch:
            try:
                success = await self._publish_single_item(item, group_id)
                if success:
                    processed += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"❌ Erro item {item['id']}: {e}")
                failed += 1

            await asyncio.sleep(self.min_interval)

        logger.info(f"✅ Sequencial concluído: {processed} ok, {failed} falhas")

    async def _publish_single_item(self, queue_item: Dict, group_id: int,
                                   caption: str = None) -> bool:
        """Publica um item individual com download e FormData."""
        queue_id = queue_item["id"]
        media_id = queue_item["media_id"]

        self.queue_repo.mark_processing(queue_id)

        try:
            media = self.media_repo.find(media_id)
            if not media:
                raise PublishError(f"Mídia {media_id} não encontrada")

            group_bot = self.group_bots_repo.get_publisher_bot(group_id)
            if not group_bot:
                raise PublishError(f"Nenhum bot para grupo {group_id}")

            bot_id = group_bot["bot_id"]
            bot_service = await self._get_bot_service(bot_id)
            if not bot_service:
                raise PublishError(f"Bot {bot_id} não disponível")

            mime_type = media.get("mime", "")
            storage_key = media.get("storage_key", "")

            # Download
            media_url = f"{self.storage_base_url}/{storage_key}"
            extension = self._get_extension_from_mime(mime_type)
            temp_path = self._get_temp_path(media_id, extension)

            # Baixa arquivo
            download_result = await self.download_manager.download_batch([{
                "url": media_url,
                "dest_path": temp_path
            }])

            if not download_result or not download_result[0].get("downloaded"):
                raise PublishError(f"Falha no download: {media_url}")

            try:
                # Envia via arquivo local
                if mime_type.startswith("image/"):
                    result = await bot_service.send_photo(
                        chat_id=group_id,
                        photo=temp_path,
                        caption=caption
                    )
                elif mime_type.startswith("video/"):
                    result = await bot_service.send_video(
                        chat_id=group_id,
                        video=temp_path,
                        caption=caption,
                        supports_streaming=True
                    )
                else:
                    result = await bot_service.send_document(
                        chat_id=group_id,
                        document=temp_path,
                        caption=caption
                    )

                publish_id = self.group_publish_repo.create({
                    "group_id": group_id,
                    "media_id": media_id,
                    "bot_id": bot_id,
                    "telegram_message_id": result.get("message_id"),
                    "file_id": self._extract_file_id(result),
                    "caption": caption,
                    "status": "published",
                    "published_at": datetime.now().isoformat()
                })

                self.queue_repo.mark_completed(queue_id)
                self._log_action(publish_id["id"], "published", {"via": "individual_formdata"})
                self.stats_repo.increment_published(group_id)

                return True

            finally:
                # Limpa arquivo temporário
                if os.path.exists(temp_path):
                    os.remove(temp_path)

        except PublishError as e:
            logger.error(f"❌ {e}")
            self.queue_repo.mark_failed(queue_id, str(e))
            self.stats_repo.increment_failed(group_id)
            return False

        except BotApiError as e:
            logger.error(f"❌ API Error: {e}")
            self.queue_repo.mark_failed(queue_id, str(e))
            self.stats_repo.increment_failed(group_id)
            return False

    # =====================================================
    # UTILITÁRIOS
    # =====================================================

    def _extract_file_id(self, result: Dict) -> Optional[str]:
        """Extrai file_id da resposta"""
        for field in ["photo", "video", "document", "animation"]:
            if field in result:
                content = result[field]
                if isinstance(content, list):
                    return content[-1].get("file_id") if content else None
                return content.get("file_id")
        return None

    def _log_action(self, publish_id, action: str, details: dict):
        try:
            import json

            details_str = json.dumps(details) if isinstance(details, dict) else str(details)

            payload = {
                "group_publish_id": publish_id,  # <- AQUI O FIX
                "action": action,
                "details": details_str,
                "created_at": datetime.now().isoformat()
            }

            self.log_repo.create(payload)

        except Exception as e:
            logger.warning(f"⚠️ Erro ao registrar log: {e}")

    # =====================================================
    # MÉTODOS PÚBLICOS
    # =====================================================

    def add_to_queue(self, group_id: int, media_id: int, priority: int = 0):
        """Adiciona mídia à fila."""
        if self.queue_repo.is_in_queue(group_id, media_id):
            return False
        if self.group_publish_repo.is_media_published(group_id, media_id):
            return False
        self.queue_repo.add_to_queue(group_id, media_id, priority)
        return True

    def get_queue_status(self) -> Dict:
        """Retorna status da fila"""
        stats = self.queue_repo.get_queue_stats()
        return {stat["status"]: stat["count"] for stat in stats}


class PublishError(Exception):
    """Erro de publicação"""
    pass


# =====================================================
# MAIN
# =====================================================

async def main():
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="Publisher V2 - Com Download Workers")
    parser.add_argument("--limit", type=int, default=10, help="Limite de itens")
    parser.add_argument("--workers", type=int, default=5, help="Número de download workers")
    parser.add_argument("--loop", action="store_true", help="Modo loop contínuo")
    parser.add_argument("--interval", type=int, default=60, help="Intervalo do loop")

    args = parser.parse_args()

    publisher = PublisherServiceV2(download_workers=args.workers)

    try:
        if args.loop:
            logger.info(f"🔄 Modo loop (intervalo: {args.interval}s, workers: {args.workers})")

            while True:
                status = publisher.get_queue_status()
                pending = status.get("pending", 0)

                if pending > 0:
                    logger.info(f"📋 {pending} itens pendentes")
                    await publisher.process_queue(limit=args.limit)
                else:
                    logger.info("📭 Fila vazia")

                logger.info(f"💤 Aguardando {args.interval}s...")
                await asyncio.sleep(args.interval)
        else:
            status = publisher.get_queue_status()
            logger.info(f"📊 Status: {status}")
            await publisher.process_queue(limit=args.limit)

    except KeyboardInterrupt:
        logger.info("\n🛑 Interrompido")
    finally:
        await publisher.close()


if __name__ == "__main__":
    asyncio.run(main())