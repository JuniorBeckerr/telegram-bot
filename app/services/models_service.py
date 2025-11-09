import json
from app.repository.models_repository import ModelsRepository


class ModelsService:
    def __init__(self):
        self.repo = ModelsRepository()

    def get_or_create(self, stage_name: str):
        """Busca um modelo existente ou cria novo."""
        return self.repo.first_or_create({"stage_name": stage_name})

    def find_by_name_or_alias(self, name: str):
        """Procura modelo por nome principal ou alias, com parsing robusto."""
        normalized = name.strip().lower()

        # 🔹 Primeiro tenta achar pelo stage_name diretamente
        found = self.repo.find_by_stage_name(normalized)
        if found:
            return found

        # 🔹 Busca todos modelos e faz parsing local dos aliases
        all_models = self.repo.all()
        for model in all_models:
            aliases_raw = model.get("aliases")
            aliases = self._parse_aliases(aliases_raw)
            if normalized in aliases:
                return model

        return None

    # ==============================================================
    # Função auxiliar pra tratar qualquer formato salvo no aliases
    # ==============================================================
    def _parse_aliases(self, raw_aliases):
        """Normaliza e corrige qualquer formato do campo aliases."""
        if not raw_aliases:
            return []

        # já é lista
        if isinstance(raw_aliases, list):
            return [a.strip().lower() for a in raw_aliases if a]

        if isinstance(raw_aliases, str):
            raw = raw_aliases.strip()

            # remove aspas duplas externas se vier "\"alias\""
            if raw.startswith('"') and raw.endswith('"'):
                raw = raw[1:-1]

            # remove escapes de aspas internas
            raw = raw.replace('\\"', '"')

            # se veio como { "a", "b" }
            if raw.startswith("{") and raw.endswith("}"):
                raw = raw.strip("{}")
                aliases = [p.strip().strip('"').strip("'").lower() for p in raw.split(",") if p.strip()]
                return aliases

            # tenta decodificar JSON válido
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [str(a).strip().lower() for a in parsed if a]
                elif isinstance(parsed, str):
                    return [parsed.strip().lower()]
            except Exception:
                pass

            # fallback: vírgula separada simples
            return [p.strip().lower() for p in raw.split(",") if p.strip()]

        return []
