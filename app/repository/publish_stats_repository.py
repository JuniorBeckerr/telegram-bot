from config.mysql_repository import BaseRepository
from datetime import datetime, date

class PublishStatsRepository(BaseRepository):
    """Repository para tabela publish_stats"""

    def __init__(self):
        super().__init__("publish_stats")

    def increment_published(self, group_id: int, count: int = 1):
        """Incrementa contador de publicações"""
        today = date.today().isoformat()

        existing = (self.where("group_id", group_id)
                    .where("date", today)
                    .first())

        if existing:
            new_count = existing.get("total_published", 0) + count
            return self.where("id", existing["id"]).update({
                "total_published": new_count,
                "updated_at": datetime.now().isoformat()
            })
        else:
            return self.create({
                "group_id": group_id,
                "date": today,
                "total_published": count,
                "total_failed": 0,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            })

    def increment_failed(self, group_id: int, count: int = 1):
        """Incrementa contador de falhas"""
        today = date.today().isoformat()

        existing = (self.where("group_id", group_id)
                    .where("date", today)
                    .first())

        if existing:
            new_count = existing.get("total_failed", 0) + count
            return self.where("id", existing["id"]).update({
                "total_failed": new_count,
                "updated_at": datetime.now().isoformat()
            })
        else:
            return self.create({
                "group_id": group_id,
                "date": today,
                "total_published": 0,
                "total_failed": count,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            })

    def get_stats_for_group(self, group_id: int, days: int = 7):
        """Retorna stats dos últimos N dias"""
        from datetime import timedelta
        start_date = (date.today() - timedelta(days=days)).isoformat()

        return (self.where("group_id", group_id)
                .where("date", ">=", start_date)
                .order_by("date", "DESC")
                .get())

    def get_total_stats(self, group_id: int):
        """Retorna totais de um grupo"""
        stats = self.where("group_id", group_id).get()

        total_published = sum(s.get("total_published", 0) for s in stats)
        total_failed = sum(s.get("total_failed", 0) for s in stats)

        return {
            "total_published": total_published,
            "total_failed": total_failed
        }

    def get_today_stats(self, group_id: int):
        """Retorna stats de hoje"""
        today = date.today().isoformat()

        stat = (self.where("group_id", group_id)
                .where("date", today)
                .first())

        if stat:
            return {
                "total_published": stat.get("total_published", 0),
                "total_failed": stat.get("total_failed", 0)
            }

        return {"total_published": 0, "total_failed": 0}