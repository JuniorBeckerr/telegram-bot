"""
Publisher Service V3 - Máxima Performance com Workers Paralelos por Modelo
Otimizado para servidores com múltiplos cores e RAM
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
from concurrent.futures import ThreadPoolExecutor

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
        try:
            timeout_config = aiohttp.ClientTimeout(total=timeout)

            async with self.session.get(url, timeout=timeout_config) as response:
                if response.status != 200:
                    logger.error(f"Worker {self.worker_id}: HTTP {response.status} para {url}")
                    self.failed += 1
                    return False

                Path(dest_path).parent.mkdir(parents=True, exist_ok=True)

                with open(dest_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(65536):  # 64KB chunks
                        f.write(chunk)

                self.downloaded += 1
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

    def __init__(self, num_workers: int = 10, timeout: int = 120):
        self.num_workers = num_workers
        self.timeout = timeout
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.num_workers * 3,
                limit_per_host=self.num_workers * 2,
                ttl_dns_cache=300
            )
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def download_batch(self, items: List[Dict]) -> List[Dict]:
        if not items:
            return []

        session = await self._get_session()
        workers = [DownloadWorker(i, session) for i in range(self.num_workers)]

        queue = asyncio.Queue()
        for item in items:
            await queue.put(item)

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

        tasks = [worker_task(w) for w in workers]
        await asyncio.gather(*tasks)

        total_ok = sum(1 for r in results if r.get("downloaded"))
        total_fail = len(results) - total_ok
        logger.debug(f"📥 Download: {total_ok} OK, {total_fail} falhas")

        return results


class PublisherServiceV3:
    """
    Serviço de publicação com máxima performance.
    Processa múltiplos modelos em paralelo.
    """

    def __init__(self,
                 download_workers: int = 12,
                 model_workers: int = 4,
                 thumb_workers: int = 6):
        """
        Args:
            download_workers: Workers para download (recomendado: 10-15)
            model_workers: Modelos processados em paralelo (recomendado: 3-6)
            thumb_workers: Threads para extração de thumbnails (recomendado: 4-8)
        """
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
        self.min_interval = getattr(Config, 'PUBLISHER_MIN_INTERVAL', 3)  # Reduzido
        self.storage_base_url = getattr(
            Config,
            'STORAGE_BASE_URL',
            'https://storage-becker.nyc3.digitaloceanspaces.com'
        )

        # Workers
        self.model_workers = model_workers
        self.thumb_workers = thumb_workers

        # Download manager com mais workers
        self.download_manager = DownloadManager(
            num_workers=download_workers,
            timeout=getattr(Config, 'DOWNLOAD_TIMEOUT', 120)
        )

        # Thread pool para FFmpeg (CPU-bound)
        self._thread_pool = ThreadPoolExecutor(max_workers=thumb_workers)

        # Cache de bot services
        self._bot_services: Dict[int, BotServiceV2] = {}

        # Diretório temporário
        self._temp_dir = tempfile.mkdtemp(prefix="publisher_")

        # Semáforo para controle de envio ao Telegram
        self._send_semaphore = asyncio.Semaphore(model_workers)

        logger.info(f"📁 Temp dir: {self._temp_dir}")
        logger.info(f"⚡ Config: {download_workers} download, {model_workers} model, {thumb_workers} thumb workers")

    async def _get_bot_service(self, bot_id: int) -> Optional[BotServiceV2]:
        if bot_id not in self._bot_services:
            bot = self.bots_repo.find(bot_id)
            if not bot or not bot.get("active"):
                return None

            self._bot_services[bot_id] = BotServiceV2(
                token=bot["token"],
                timeout=getattr(Config, 'BOT_REQUEST_TIMEOUT', 120),
                auto_retry_rate_limit=False
            )

        return self._bot_services[bot_id]

    async def close(self):
        for bot_service in self._bot_services.values():
            await bot_service.close()
        self._bot_services.clear()

        await self.download_manager.close()
        self._thread_pool.shutdown(wait=False)

        if os.path.exists(self._temp_dir):
            shutil.rmtree(self._temp_dir)
            logger.info(f"🗑️ Temp dir removido")

    def _get_temp_path(self, media_id: int, extension: str) -> str:
        return os.path.join(self._temp_dir, f"media_{media_id}{extension}")

    def _get_extension_from_mime(self, mime_type: str) -> str:
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

    def _extract_video_thumbnail_sync(self, video_path: str) -> Optional[str]:
        """Versão síncrona para rodar em thread pool"""
        try:
            thumb_path = video_path.rsplit('.', 1)[0] + '_thumb.jpg'

            cmd = [
                'ffmpeg', '-y',
                '-i', video_path,
                '-ss', '1',
                '-vframes', '1',
                '-vf', 'scale=320:-1',
                '-q:v', '3',
                thumb_path
            ]

            result = subprocess.run(cmd, capture_output=True, timeout=15)

            if result.returncode == 0 and os.path.exists(thumb_path):
                return thumb_path

            # Tenta do início
            cmd[5] = '0'
            result = subprocess.run(cmd, capture_output=True, timeout=15)

            if result.returncode == 0 and os.path.exists(thumb_path):
                return thumb_path

            return None

        except Exception as e:
            logger.warning(f"⚠️ Erro thumb: {e}")
            return None

    def _get_video_metadata_sync(self, video_path: str) -> Dict[str, Any]:
        """Versão síncrona para rodar em thread pool"""
        try:
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                video_path
            ]

            result = subprocess.run(cmd, capture_output=True, timeout=10)

            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)

                video_stream = None
                for stream in data.get('streams', []):
                    if stream.get('codec_type') == 'video':
                        video_stream = stream
                        break

                metadata = {}

                if 'format' in data:
                    duration = data['format'].get('duration')
                    if duration:
                        metadata['duration'] = int(float(duration))

                if video_stream:
                    metadata['width'] = video_stream.get('width')
                    metadata['height'] = video_stream.get('height')

                return metadata

        except Exception as e:
            pass

        return {}

    async def _extract_thumbnails_parallel(self, items: List[Dict]) -> None:
        """Extrai thumbnails em paralelo usando thread pool"""
        loop = asyncio.get_event_loop()

        async def process_item(data):
            if data["type"] == "video":
                # Extrai thumb em thread separada
                thumb_path = await loop.run_in_executor(
                    self._thread_pool,
                    self._extract_video_thumbnail_sync,
                    data["dest_path"]
                )
                if thumb_path:
                    data["thumb_path"] = thumb_path

                # Obtém metadados em thread separada
                metadata = await loop.run_in_executor(
                    self._thread_pool,
                    self._get_video_metadata_sync,
                    data["dest_path"]
                )
                if metadata.get("duration"):
                    data["duration"] = metadata["duration"]
                if metadata.get("width"):
                    data["width"] = metadata["width"]
                if metadata.get("height"):
                    data["height"] = metadata["height"]

        # Processa todos em paralelo
        await asyncio.gather(*[process_item(item) for item in items])

    # =====================================================
    # PROCESSAMENTO PRINCIPAL
    # =====================================================

    async def process_queue(self, group_id: int = None, limit: int = None):
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
        logger.info(f"🎯 Processando grupo {group_id} ({len(items)} itens)")

        rules = self.rules_repo.get_rules_for_group(group_id)
        rule = rules[0] if rules else {}

        random_model = rule.get("random_model", False)
        batch_size = rule.get("batch_size", self.batch_size)

        if random_model:
            await self._process_random_model_parallel(group_id, items, batch_size)
        else:
            await self._process_sequential(group_id, items, batch_size)

    async def _process_random_model_parallel(self, group_id: int, items: List[Dict], batch_size: int):
        """Processa múltiplos modelos em paralelo"""
        logger.info(f"🎲 Modo Random Model PARALELO (batch={batch_size}, workers={self.model_workers})")

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

        logger.info(f"📊 {len(items_by_model)} modelos para processar")

        # Busca TODOS os bots do grupo para rotação
        group_bots = self.group_bots_repo.get_publisher_bots(group_id)
        if not group_bots:
            # Fallback para método antigo
            group_bot = self.group_bots_repo.get_publisher_bot(group_id)
            if group_bot:
                group_bots = [group_bot]
            else:
                logger.error(f"❌ Nenhum bot para grupo {group_id}")
                return

        # Carrega todos os bot services
        bot_services = []
        for gb in group_bots:
            bot_id = gb["bot_id"]
            bot_service = await self._get_bot_service(bot_id)
            if bot_service:
                bot_services.append((bot_id, bot_service))

        if not bot_services:
            logger.error(f"❌ Nenhum bot disponível para grupo {group_id}")
            return

        logger.info(f"🤖 {len(bot_services)} bots disponíveis para rotação")

        # NOVA LÓGICA: Processa modelos completos em paralelo
        # Cada modelo processará TODOS os seus batches antes de terminar

        # Embaralha a ordem dos modelos (mas cada modelo mantém seus batches intactos)
        model_ids = list(items_by_model.keys())
        random.shuffle(model_ids)

        total_processed = 0
        total_failed = 0

        # Processa modelos em chunks (limitado por model_workers)
        for chunk_start in range(0, len(model_ids), self.model_workers):
            chunk_end = min(chunk_start + self.model_workers, len(model_ids))
            chunk_model_ids = model_ids[chunk_start:chunk_end]

            logger.info(f"🔄 Processando chunk de {len(chunk_model_ids)} modelos em paralelo")

            # Processa cada modelo do chunk em paralelo
            results = await asyncio.gather(*[
                self._process_single_model_complete(
                    bot_services[i % len(bot_services)],
                    group_id,
                    items_by_model[model_id],
                    model_info[model_id],
                    batch_size,
                    model_index=chunk_start + i,
                    all_bot_services=bot_services  # Adiciona esta linha
                )
                for i, model_id in enumerate(chunk_model_ids)
            ], return_exceptions=True)

            # Consolida resultados
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"❌ Erro em modelo: {result}")
                    continue
                processed, failed = result
                total_processed += processed
                total_failed += failed

        logger.info(f"✅ Paralelo concluído: {total_processed} ok, {total_failed} falhas")


    async def _process_single_model_complete(self, bot_service_tuple: tuple,
                                             group_id: int, all_items: List[Dict],
                                             info: Dict, batch_size: int, model_index: int,
                                             all_bot_services: List[tuple] = None) -> tuple:
        """Processa TODOS os batches de um modelo de forma sequencial"""

        bot_id, bot_service = bot_service_tuple
        model_name = info['full_name']

        # Divide em batches
        num_batches = (len(all_items) + batch_size - 1) // batch_size

        logger.info(f"👤 [{model_index}] {model_name}: {len(all_items)} mídias em {num_batches} batch(es)")

        total_processed = 0
        total_failed = 0

        # Processa cada batch deste modelo sequencialmente
        for batch_idx in range(num_batches):
            start = batch_idx * batch_size
            end = min(start + batch_size, len(all_items))
            batch_items = all_items[start:end]

            batch_info = f"{batch_idx + 1}/{num_batches}"

            try:
                processed, failed = await self._process_single_batch(
                    bot_service,
                    bot_id,
                    group_id,
                    batch_items,
                    info,
                    model_index,
                    batch_info,
                    all_bot_services  # Passa a lista de bots
                )

                total_processed += processed
                total_failed += failed

                # Intervalo menor entre batches (bots já fazem o controle)
                if batch_idx < num_batches - 1:
                    await asyncio.sleep(self.min_interval)

            except Exception as e:
                logger.error(f"❌ [{model_index}] Erro no batch {batch_info}: {e}")
                total_failed += len(batch_items)

        logger.info(f"✅ [{model_index}] {model_name} completo: {total_processed} ok, {total_failed} falhas")

        return (total_processed, total_failed)

    async def _process_single_batch(self, bot_service: BotServiceV2, bot_id: int,
                                    group_id: int, items: List[Dict],
                                    info: Dict, model_index: int, batch_info: str = "",
                                    all_bot_services: List[tuple] = None) -> tuple:
        """Processa um único batch com rotação de bots apenas para rate limit"""

        model_name = info['full_name']
        batch_label = f" [Batch {batch_info}]" if batch_info else ""
        logger.info(f"📦 [{model_index}] Processando{batch_label}: {len(items)} mídias")

        # Prepara lista de downloads
        download_items = []

        for item in items:
            media_id = item["media_id"]
            media = self.media_repo.find(media_id)

            if not media:
                self.queue_repo.mark_failed(item["id"], "Mídia não encontrada")
                continue

            mime_type = media.get("mime", "")
            storage_key = media.get("storage_key", "")

            if not storage_key:
                self.queue_repo.mark_failed(item["id"], "Sem storage_key")
                continue

            media_url = f"{self.storage_base_url}/{storage_key}"
            extension = self._get_extension_from_mime(mime_type)
            temp_path = self._get_temp_path(media_id, extension)

            media_type = "video" if mime_type.startswith("video/") else "photo"

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

        # FASE 1: Download
        downloaded_items = await self.download_manager.download_batch(download_items)

        ready_items = []
        failed_count = 0

        for data in downloaded_items:
            if data.get("downloaded"):
                ready_items.append(data)
            else:
                self.queue_repo.mark_failed(
                    data["item"]["id"],
                    f"Falha no download"
                )
                self.stats_repo.increment_failed(group_id)
                failed_count += 1

        if not ready_items:
            return (0, failed_count)

        # FASE 2: Extração de thumbnails em paralelo
        await self._extract_thumbnails_parallel(ready_items)

        # FASE 3: Envio com rotação de bots APENAS para rate limit
        async with self._send_semaphore:
            caption = f"#{info['stage_name']}\n{info['full_name']}"

            try:
                upload_items = []
                for data in ready_items:
                    upload_item = {
                        "temp_path": data["dest_path"],
                        "type": data["type"],
                    }

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

                # ESTRATÉGIA: Tenta com TODOS os bots disponíveis para rate limit
                results = None
                used_bot_id = bot_id
                last_error = None

                # Se tem múltiplos bots, cria lista começando pelo bot atual
                bots_to_try = []
                if all_bot_services and len(all_bot_services) > 1:
                    # Bot atual primeiro
                    bots_to_try = [(bid, bsvc) for bid, bsvc in all_bot_services if bid == bot_id]
                    # Depois os outros
                    bots_to_try.extend([(bid, bsvc) for bid, bsvc in all_bot_services if bid != bot_id])
                else:
                    # Só tem um bot
                    bots_to_try = [(bot_id, bot_service)]

                # Tenta com cada bot
                for attempt_bot_id, attempt_bot_service in bots_to_try:
                    try:
                        results = await attempt_bot_service.send_media_group_with_thumbs(
                            chat_id=group_id,
                            items=upload_items,
                            caption=caption,
                            disable_notification=False
                        )
                        used_bot_id = attempt_bot_id

                        if attempt_bot_id != bot_id:
                            logger.info(f"🔄 [{model_index}]{batch_label} Sucesso com bot alternativo {attempt_bot_id}")

                        break  # Sucesso!

                    except BotApiError as e:
                        last_error = e
                        error_msg = str(e).lower()

                        # Se é rate limit, tenta próximo bot (SEM aguardar!)
                        if "too many requests" in error_msg or "429" in error_msg or "rate limit" in error_msg:
                            if len(bots_to_try) > 1 and attempt_bot_id != bots_to_try[-1][0]:
                                logger.warning(
                                    f"🔄 [{model_index}]{batch_label} Rate limit no bot {attempt_bot_id}, "
                                    f"tentando próximo bot imediatamente..."
                                )
                                continue
                            else:
                                # Último bot também deu rate limit
                                logger.warning(
                                    f"⏭️ [{model_index}]{batch_label} Rate limit no bot {attempt_bot_id} "
                                    f"(último bot disponível) - pulando batch"
                                )
                                break

                        # Timeout → ignora e vai para próximo (se houver)
                        elif "timeout" in error_msg:
                            logger.warning(f"⏭️ [{model_index}]{batch_label} Timeout no bot {attempt_bot_id}, pulando batch...")

                            # Marca itens como completed com timeout (não como falha)
                            for data in ready_items:
                                self.queue_repo.mark_completed_with_timeout(
                                    data["item"]["id"],
                                    f"Timeout ao enviar"
                                )

                            return 0, len(ready_items)

                        # Outros erros → para imediatamente
                        else:
                            logger.error(f"❌ [{model_index}]{batch_label} Erro no bot {attempt_bot_id}: {e}")
                            raise

                    except asyncio.TimeoutError:
                        logger.warning(f"⏭️ [{model_index}]{batch_label} Timeout asyncio no bot {attempt_bot_id}, pulando batch...")

                        # Marca itens como completed com timeout (não como falha)
                        for data in ready_items:
                            self.queue_repo.mark_completed_with_timeout(
                                data["item"]["id"],
                                f"Timeout asyncio"
                            )

                        return 0, len(ready_items)

            # Se todos os bots deram rate limit, desiste
                if results is None:
                    error_msg = str(last_error).lower() if last_error else ""

                    if "too many requests" in error_msg or "429" in error_msg or "rate limit" in error_msg:
                        logger.warning(
                            f"🔁 [{model_index}]{batch_label} Todos os bots com rate limit, "
                            f"iniciando retry..."
                        )

                        # Retry com backoff: 10s, 30s, 60s
                        retry_delays = [10, 30, 60]

                        for retry_count, wait_time in enumerate(retry_delays, start=1):
                            # Extrai retry_after do Telegram se disponível
                            retry_after = None
                            if "retry after" in str(last_error).lower():
                                import re
                                match = re.search(r'retry after (\d+)', str(last_error).lower())
                                if match:
                                    retry_after = int(match.group(1))

                            # Usa o maior entre o retry_after e o delay planejado
                            if retry_after:
                                wait_time = max(wait_time, retry_after + 2)

                            logger.warning(
                                f"⏳ [{model_index}]{batch_label} Aguardando {wait_time}s "
                                f"(retry {retry_count}/{len(retry_delays)})"
                            )

                            await asyncio.sleep(wait_time)

                            try:
                                # Tenta com o primeiro bot disponível
                                first_bot_id, first_bot_service = bots_to_try[0]
                                results = await first_bot_service.send_media_group_with_thumbs(
                                    chat_id=group_id,
                                    items=upload_items,
                                    caption=caption,
                                    disable_notification=False
                                )
                                used_bot_id = first_bot_id
                                logger.info(f"✅ [{model_index}]{batch_label} Sucesso após retry {retry_count}")
                                break

                            except BotApiError as e3:
                                last_error = e3
                                error_msg3 = str(e3).lower()

                                # Se ainda tem rate limit, continua retry
                                if "too many requests" in error_msg3 or "429" in error_msg3 or "rate limit" in error_msg3:
                                    if retry_count >= len(retry_delays):
                                        logger.error(
                                            f"❌ [{model_index}]{batch_label} Rate limit persistente "
                                            f"após {len(retry_delays)} retries"
                                        )
                                        break
                                    # Atualiza retry_after para próxima tentativa
                                    continue
                                else:
                                    # Outro erro, para
                                    logger.error(f"❌ [{model_index}]{batch_label} Erro diferente no retry: {e3}")
                                    break

                            except asyncio.TimeoutError:
                                logger.warning(f"⏭️ [{model_index}]{batch_label} Timeout no retry, parando...")
                                last_error = Exception("Timeout")
                                break

                        # Se ainda não tem resultado após todos os retries
                        if results is None:
                            logger.error(
                                f"❌ [{model_index}]{batch_label} Falhou após todos os retries, "
                                f"pulando batch..."
                            )
                            raise last_error or Exception("Todos os bots com rate limit após retries")
                    else:
                        # Outro tipo de erro
                        raise last_error or Exception("Falha após tentar todos os bots")
                # Registra publicações
                processed = 0
                for i, result in enumerate(results):
                    if i < len(ready_items):
                        data = ready_items[i]

                        publish_id = self.group_publish_repo.create({
                            "group_id": group_id,
                            "media_id": data["media_id"],
                            "bot_id": used_bot_id,
                            "telegram_message_id": result.get("message_id"),
                            "file_id": self._extract_file_id(result),
                            "caption": caption if i == 0 else None,
                            "status": "published",
                            "published_at": datetime.now().isoformat()
                        })

                        self.queue_repo.mark_completed(data["item"]["id"])
                        self._log_action(publish_id["id"], "published", {
                            "via": "parallel",
                            "model": info['stage_name'],
                            "batch": batch_info,
                            "bot_id": used_bot_id
                        })
                        processed += 1

                self.stats_repo.increment_published(group_id, processed)
                logger.info(f"✅ [{model_index}]{batch_label} enviado: {processed} mídias (bot {used_bot_id})")

                return (processed, failed_count)

            except (BotApiError, asyncio.TimeoutError, Exception) as e:
                logger.error(f"❌ [{model_index}]{batch_label} Erro final: {e}")

                for data in ready_items:
                    self.queue_repo.mark_failed(data["item"]["id"], str(e))
                    self.stats_repo.increment_failed(group_id)

                return (0, len(ready_items) + failed_count)

            finally:
                # Limpa arquivos
                for data in ready_items:
                    for path_key in ["dest_path", "thumb_path"]:
                        path = data.get(path_key)
                        if path and os.path.exists(path):
                            try:
                                os.remove(path)
                            except:
                                pass


    # =====================================================

    async def _process_sequential(self, group_id: int, items: List[Dict], batch_size: int):
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

        logger.info(f"✅ Sequencial: {processed} ok, {failed} falhas")

    async def _publish_single_item(self, queue_item: Dict, group_id: int) -> bool:
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

            media_url = f"{self.storage_base_url}/{storage_key}"
            extension = self._get_extension_from_mime(mime_type)
            temp_path = self._get_temp_path(media_id, extension)

            download_result = await self.download_manager.download_batch([{
                "url": media_url,
                "dest_path": temp_path
            }])

            if not download_result or not download_result[0].get("downloaded"):
                raise PublishError(f"Falha no download")

            try:
                if mime_type.startswith("image/"):
                    result = await bot_service.send_photo(
                        chat_id=group_id,
                        photo=temp_path
                    )
                elif mime_type.startswith("video/"):
                    result = await bot_service.send_video(
                        chat_id=group_id,
                        video=temp_path,
                        supports_streaming=True
                    )
                else:
                    result = await bot_service.send_document(
                        chat_id=group_id,
                        document=temp_path
                    )

                publish_id = self.group_publish_repo.create({
                    "group_id": group_id,
                    "media_id": media_id,
                    "bot_id": bot_id,
                    "telegram_message_id": result.get("message_id"),
                    "file_id": self._extract_file_id(result),
                    "status": "published",
                    "published_at": datetime.now().isoformat()
                })

                self.queue_repo.mark_completed(queue_id)
                self._log_action(publish_id["id"], "published", {"via": "sequential"})
                self.stats_repo.increment_published(group_id)

                return True

            finally:
                if os.path.exists(temp_path):
                    os.remove(temp_path)

        except (PublishError, BotApiError) as e:
            logger.error(f"❌ {e}")
            self.queue_repo.mark_failed(queue_id, str(e))
            self.stats_repo.increment_failed(group_id)
            return False

    # =====================================================
    # UTILITÁRIOS
    # =====================================================

    def _extract_file_id(self, result: Dict) -> Optional[str]:
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

            self.log_repo.create({
                "group_publish_id": publish_id,
                "action": action,
                "details": details_str,
                "created_at": datetime.now().isoformat()
            })
        except Exception as e:
            logger.warning(f"⚠️ Erro log: {e}")

    def add_to_queue(self, group_id: int, media_id: int, priority: int = 0):
        if self.queue_repo.is_in_queue(group_id, media_id):
            return False
        if self.group_publish_repo.is_media_published(group_id, media_id):
            return False
        self.queue_repo.add_to_queue(group_id, media_id, priority)
        return True

    def get_queue_status(self) -> Dict:
        stats = self.queue_repo.get_queue_stats()
        return {stat["status"]: stat["count"] for stat in stats}


class PublishError(Exception):
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

    parser = argparse.ArgumentParser(description="Publisher V3 - Máxima Performance")
    parser.add_argument("--limit", type=int, default=50, help="Limite de itens")
    parser.add_argument("--download-workers", type=int, default=12, help="Workers de download")
    parser.add_argument("--model-workers", type=int, default=4, help="Modelos em paralelo")
    parser.add_argument("--thumb-workers", type=int, default=6, help="Workers de thumbnail")
    parser.add_argument("--loop", action="store_true", help="Modo loop")
    parser.add_argument("--interval", type=int, default=30, help="Intervalo do loop")

    args = parser.parse_args()

    publisher = PublisherServiceV3(
        download_workers=args.download_workers,
        model_workers=args.model_workers,
        thumb_workers=args.thumb_workers
    )

    try:
        if args.loop:
            logger.info(f"🔄 Loop mode (interval: {args.interval}s)")

            while True:
                status = publisher.get_queue_status()
                pending = status.get("pending", 0)

                if pending > 0:
                    logger.info(f"📋 {pending} itens pendentes")
                    await publisher.process_queue(limit=args.limit)
                else:
                    logger.info("📭 Fila vazia")

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