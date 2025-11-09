from fastapi import FastAPI
from typing import Dict, Any, List


class RouteManager:
    """
    Gerenciador central de todas as rotas.
    Facilita o registro e organização das rotas.
    """

    def __init__(self, app: FastAPI):
        """
        Inicializa o gerenciador de rotas.

        Args:
            app: Instância do FastAPI
            controllers: Dicionário com todos os controllers
        """
        self.app = app
        self.routes: List = []

    def register_all_routes(self):
        """
        Registra todas as rotas disponíveis.
        """

        # Lista de classes de rotas disponíveis
        route_classes = []

        # Registra cada conjunto de rotas
        for route_class in route_classes:
            try:
                route_instance = route_class(self.app)
                route_instance.register_routes()
                self.routes.append(route_instance)
                print(f"✅ Rotas {route_class.__name__} registradas com sucesso")
            except Exception as e:
                print(f"❌ Erro ao registrar {route_class.__name__}: {str(e)}")

    def get_registered_routes(self) -> List:
        """
        Retorna lista de rotas registradas.
        """
        return self.routes
