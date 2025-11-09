import os
from dotenv import load_dotenv

class ConfigSingleton:
    """
    Singleton para armazenar configurações globais.

    Este design pattern garante que há apenas uma instância
    das configurações em toda a aplicação.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigSingleton, cls).__new__(cls)
            load_dotenv()
            cls._instance.load_config()
        return cls._instance

    def load_config(self):
        """
        Carrega as configurações a partir de variáveis de ambiente.
        """
        self._initialize()

    def _initialize(self):
        """
        Inicializa as variáveis de ambiente.
        """
        # MySQL
        self.MYSQL_HOST = os.getenv('DB_HOST', 'localhost')
        self.MYSQL_PORT = int(os.getenv('DB_PORT', 3306))
        self.MYSQL_USER = os.getenv('DB_USER', 'root')
        self.MYSQL_PASSWORD = os.getenv('DB_PASSWORD', 'root')
        self.MYSQL_DB = os.getenv('DB_NAME', 'primehot')

        self.BASE_DIR = os.path.dirname(os.path.dirname(__file__))

        self.STORAGE_PATH = os.path.join(self.BASE_DIR, "storage")
        self.SESSION_PATH = os.path.join(self.STORAGE_PATH, "sessions")
        self.DOWNLOAD_PATH = os.path.join(self.STORAGE_PATH, "downloads")
        self.FRAMES_PATH = os.path.join(self.STORAGE_PATH, "frames")
        self.JSON_DB = os.path.join(self.BASE_DIR, "processed.json")

        # --- CONFIGURAÇÕES DE PROCESSAMENTO ---
        self.NUM_WORKERS = 10          # número de processos simultâneos
        self.MSG_POR_WORKER = 2       # quantas mensagens cada worker processa
        self.RETRIES = 3              # número de tentativas em caso de erro
        self.OFFSET_INICIAL = 0      # deslocamento inicial de mensagens no Telegram


        os.makedirs(self.SESSION_PATH, exist_ok=True)
        os.makedirs(self.DOWNLOAD_PATH, exist_ok=True)
        os.makedirs(self.FRAMES_PATH, exist_ok=True)

Config = ConfigSingleton()
