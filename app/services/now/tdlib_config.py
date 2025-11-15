"""
Configurações TDLib - Performance Máxima
"""
import os
from pathlib import Path


class TDLibConfig:
    """Configurações otimizadas para TDLib"""

    # Diretórios
    BASE_DIR = Path(__file__).parent
    TDLIB_DATABASE_DIR = BASE_DIR / "tdlib_data"
    TDLIB_FILES_DIR = BASE_DIR / "tdlib_files"

    # Parâmetros de performance
    TDLIB_PARAMS = {
        "api_id": None,  # Será preenchido dinamicamente
        "api_hash": None,
        "database_directory": str(TDLIB_DATABASE_DIR),
        "files_directory": str(TDLIB_FILES_DIR),
        "use_file_database": True,
        "use_chat_info_database": True,
        "use_message_database": False,  # Desabilita cache de mensagens (economiza RAM)
        "use_secret_chats": False,
        "system_language_code": "pt-BR",
        "device_model": "Server",
        "application_version": "1.0",
        "enable_storage_optimizer": True,

        # 🔥 CONFIGURAÇÕES DE PERFORMANCE
        "use_test_dc": False,
        "database_encryption_key": "",  # Adicione uma chave se quiser criptografia
    }

    # Limites de download
    MAX_CONCURRENT_DOWNLOADS = 50  # TDLib aguenta muito mais que Telethon
    MAX_CONCURRENT_SESSIONS = 10   # Número de sessões simultâneas

    # Rate limits suaves (TDLib é mais generoso)
    DOWNLOAD_DELAY = 0.05  # 50ms entre downloads (muito mais rápido)
    BATCH_SIZE = 300       # Busca 300 mensagens por vez

    # Timeouts
    DOWNLOAD_TIMEOUT = 80  # 60s para downloads grandes
    CONNECTION_TIMEOUT = 30

    @classmethod
    def ensure_directories(cls):
        """Cria diretórios necessários"""
        cls.TDLIB_DATABASE_DIR.mkdir(parents=True, exist_ok=True)
        cls.TDLIB_FILES_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_session_params(cls, credential: dict, session_index: int = 0):
        """Retorna parâmetros para uma sessão específica"""
        cls.ensure_directories()

        # Cria diretório único para cada sessão
        session_db_dir = cls.TDLIB_DATABASE_DIR / f"session_{session_index}"
        session_db_dir.mkdir(parents=True, exist_ok=True)

        params = cls.TDLIB_PARAMS.copy()
        params["api_id"] = credential["api_id"]
        params["api_hash"] = credential["api_hash"]
        params["database_directory"] = str(session_db_dir)
        params["phone_number"] = credential["phone"]

        return params