from fastapi.responses import ORJSONResponse

class ResponseHandler:
    @staticmethod
    def success(data=None, message="Operação realizada com sucesso", code=200):
        return ORJSONResponse(
            status_code=code,
            content={
                "status": "success",
                "code": code,
                "message": message,
                "data": data or {}
            }
        )

    @staticmethod
    def error(message="Erro interno do servidor", code=500, data=None):
        return ORJSONResponse(
            status_code=code,
            content={
                "status": "error",
                "code": code,
                "message": message,
                "data": data or {}
            }
        )
