#!/usr/bin/env python3
"""
Script de teste e debugging para o sistema de download do Telegram

Testa:
- Conexão das sessões
- Pool de sessões
- Download de mensagens
- Rate limiting
- Performance
"""

import asyncio
import time
import logging
from typing import List
from app.services.session_pool import SessionPool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class TelegramSystemTester:
    """Suite de testes para o sistema de download"""

    def __init__(self, credential: dict, session_path: str, test_group_id: int):
        self.credential = credential
        self.session_path = session_path
        self.test_group_id = test_group_id
        self.pool = None

    async def run_all_tests(self):
        """Executa todos os testes"""
        logger.info("="*60)
        logger.info("🧪 INICIANDO SUITE DE TESTES")
        logger.info("="*60)

        tests = [
            ("Teste 1: Inicialização do Pool", self.test_pool_initialization),
            ("Teste 2: Status das Sessões", self.test_session_status),
            ("Teste 3: Rotação de Sessões", self.test_session_rotation),
            ("Teste 4: Busca de Mensagens", self.test_fetch_messages),
            ("Teste 5: Download Único", self.test_single_download),
            ("Teste 6: Downloads Paralelos", self.test_parallel_downloads),
            ("Teste 7: Recuperação de FloodWait", self.test_floodwait_recovery),
            ("Teste 8: Performance", self.test_performance),
        ]

        results = []

        for test_name, test_func in tests:
            logger.info(f"\n{'='*60}")
            logger.info(f"▶️  {test_name}")
            logger.info(f"{'='*60}")

            try:
                start = time.time()
                await test_func()
                elapsed = time.time() - start

                logger.info(f"✅ {test_name} - PASSOU ({elapsed:.2f}s)")
                results.append((test_name, "PASSOU", elapsed))

            except Exception as e:
                logger.error(f"❌ {test_name} - FALHOU: {e}", exc_info=True)
                results.append((test_name, "FALHOU", 0))

        # Resumo
        logger.info(f"\n{'='*60}")
        logger.info("📊 RESUMO DOS TESTES")
        logger.info(f"{'='*60}")

        passed = sum(1 for _, status, _ in results if status == "PASSOU")
        failed = len(results) - passed

        for test_name, status, elapsed in results:
            emoji = "✅" if status == "PASSOU" else "❌"
            time_str = f"({elapsed:.2f}s)" if elapsed > 0 else ""
            logger.info(f"{emoji} {test_name} {time_str}")

        logger.info(f"\n📈 Resultado: {passed}/{len(results)} testes passaram")

        if self.pool:
            await self.pool.close_all()

        return passed == len(results)

    async def test_pool_initialization(self):
        """Testa inicialização do pool de sessões"""
        self.pool = SessionPool(self.credential, self.session_path)

        success = await self.pool.initialize()

        if not success:
            raise Exception("Falha ao inicializar pool")

        if len(self.pool.sessions) == 0:
            raise Exception("Nenhuma sessão conectada")

        logger.info(f"✅ {len(self.pool.sessions)} sessões conectadas")

    async def test_session_status(self):
        """Testa status das sessões"""
        status = self.pool.get_pool_status()

        logger.info(f"📊 Status do Pool:")
        logger.info(f"  Total: {status['total']}")
        logger.info(f"  Disponíveis: {status['available']}")
        logger.info(f"  Em FloodWait: {status['in_flood_wait']}")

        if status['available'] == 0:
            raise Exception("Nenhuma sessão disponível")

    async def test_session_rotation(self):
        """Testa rotação de sessões"""
        sessions_used = []

        for i in range(10):
            session = await self.pool.get_next_session(max_wait=5)

            if not session:
                raise Exception(f"Falha ao obter sessão na tentativa {i+1}")

            sessions_used.append(session.index)
            await asyncio.sleep(0.1)

        unique_sessions = len(set(sessions_used))
        logger.info(f"✅ Rotação: {unique_sessions} sessões diferentes usadas em 10 requisições")
        logger.info(f"   Sequência: {sessions_used}")

        if unique_sessions < 2:
            logger.warning("⚠️ Pouca rotação detectada (pode ser normal com poucas sessões)")

    async def test_fetch_messages(self):
        """Testa busca de mensagens"""
        entity = await self.pool.get_entity(self.test_group_id)
        logger.info(f"✅ Entidade obtida: {entity.title}")

        messages = await self.pool.iter_messages_batch(entity, limit=10)

        logger.info(f"✅ {len(messages)} mensagens recuperadas")

        with_media = sum(1 for m in messages if m.media)
        logger.info(f"   {with_media} mensagens com mídia")

        if len(messages) == 0:
            logger.warning("⚠️ Nenhuma mensagem encontrada (grupo vazio?)")

    async def test_single_download(self):
        """Testa download de uma única mídia"""
        entity = await self.pool.get_entity(self.test_group_id)
        messages = await self.pool.iter_messages_batch(entity, limit=50)

        # Encontra primeira mensagem com mídia
        media_msg = next((m for m in messages if m.media), None)

        if not media_msg:
            logger.warning("⚠️ Nenhuma mensagem com mídia encontrada para testar")
            return

        logger.info(f"📥 Testando download da mensagem ID {media_msg.id}")

        start = time.time()
        file_bytes = await self.pool.download_media(media_msg)
        elapsed = time.time() - start

        if not file_bytes:
            raise Exception("Download retornou None")

        size_mb = len(file_bytes) / (1024 * 1024)
        speed_mbps = size_mb / elapsed if elapsed > 0 else 0

        logger.info(f"✅ Download concluído:")
        logger.info(f"   Tamanho: {size_mb:.2f} MB")
        logger.info(f"   Tempo: {elapsed:.2f}s")
        logger.info(f"   Velocidade: {speed_mbps:.2f} MB/s")

    async def test_parallel_downloads(self):
        """Testa downloads paralelos"""
        entity = await self.pool.get_entity(self.test_group_id)
        messages = await self.pool.iter_messages_batch(entity, limit=100)

        # Filtra mensagens com mídia
        media_messages = [m for m in messages if m.media][:5]  # Testa com 5

        if len(media_messages) < 2:
            logger.warning("⚠️ Poucas mensagens com mídia para testar paralelismo")
            return

        logger.info(f"📥 Testando {len(media_messages)} downloads paralelos")

        start = time.time()

        # Downloads paralelos
        tasks = [self.pool.download_media(m) for m in media_messages]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        elapsed = time.time() - start

        # Analisa resultados
        successful = sum(1 for r in results if r and not isinstance(r, Exception))
        failed = len(results) - successful

        total_size = sum(len(r) for r in results if r and not isinstance(r, Exception))
        size_mb = total_size / (1024 * 1024)

        logger.info(f"✅ Downloads paralelos concluídos:")
        logger.info(f"   Sucesso: {successful}/{len(results)}")
        logger.info(f"   Falhas: {failed}")
        logger.info(f"   Total: {size_mb:.2f} MB")
        logger.info(f"   Tempo: {elapsed:.2f}s")
        logger.info(f"   Velocidade média: {size_mb/elapsed:.2f} MB/s")

        if successful == 0:
            raise Exception("Todos os downloads paralelos falharam")

    async def test_floodwait_recovery(self):
        """Testa recuperação de FloodWait (simulado)"""
        logger.info("🔄 Simulando situação de FloodWait...")

        # Marca uma sessão como em FloodWait
        if self.pool.sessions:
            session = self.pool.sessions[0]
            session.mark_flood_wait(5)
            logger.info(f"   Sessão {session.index} marcada em FloodWait (5s)")

        # Verifica se pool ainda consegue obter sessões
        available_session = await self.pool.get_next_session(max_wait=2)

        if not available_session:
            raise Exception("Pool não conseguiu fornecer sessão alternativa")

        logger.info(f"✅ Pool retornou sessão {available_session.index} (rotação funcionando)")

        # Verifica status
        status = self.pool.get_pool_status()
        logger.info(f"   Sessões disponíveis após FloodWait: {status['available']}")

    async def test_performance(self):
        """Testa performance geral do sistema"""
        logger.info("📊 Testando performance com carga...")

        entity = await self.pool.get_entity(self.test_group_id)

        # Teste 1: Busca de mensagens
        start = time.time()
        messages = await self.pool.iter_messages_batch(entity, limit=100)
        fetch_time = time.time() - start

        logger.info(f"   Busca de 100 mensagens: {fetch_time:.2f}s")

        # Teste 2: Downloads sequenciais
        media_messages = [m for m in messages if m.media][:3]

        if media_messages:
            start = time.time()
            for msg in media_messages:
                await self.pool.download_media(msg)
            sequential_time = time.time() - start

            logger.info(f"   {len(media_messages)} downloads sequenciais: {sequential_time:.2f}s")
            logger.info(f"   Média por download: {sequential_time/len(media_messages):.2f}s")

        # Estatísticas finais do pool
        final_status = self.pool.get_pool_status()
        logger.info(f"\n✅ Estatísticas finais:")
        logger.info(f"   Total de requisições no pool: {final_status['total_requests']}")
        logger.info(f"   Média por sessão: {final_status['total_requests']/final_status['total']:.1f}")


async def main():
    """Executa os testes"""

    # ⚠️ CONFIGURE AQUI SUAS CREDENCIAIS
    credential = {
        "api_id": 12345,  # Seu API ID
        "api_hash": "your_api_hash_here",  # Seu API Hash
        "phone": "+5500000000000",  # Seu telefone
        "session_name": "nome_da_credencial",  # Nome da pasta de sessões
        "active": True
    }

    session_path = "./sessions"  # Caminho das sessões
    test_group_id = -1001234567890  # ID do grupo para testar

    # Validação
    if credential["api_id"] == 12345:
        logger.error("❌ Configure suas credenciais no arquivo de teste!")
        logger.error("   Edite: api_id, api_hash, phone, session_name, test_group_id")
        return

    # Executa testes
    tester = TelegramSystemTester(credential, session_path, test_group_id)

    try:
        all_passed = await tester.run_all_tests()

        if all_passed:
            logger.info("\n🎉 TODOS OS TESTES PASSARAM!")
            logger.info("✅ Sistema pronto para produção")
        else:
            logger.warning("\n⚠️ ALGUNS TESTES FALHARAM")
            logger.warning("Verifique os logs acima para detalhes")

    except KeyboardInterrupt:
        logger.warning("\n⚠️ Testes interrompidos pelo usuário")
    except Exception as e:
        logger.error(f"\n❌ Erro fatal nos testes: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())