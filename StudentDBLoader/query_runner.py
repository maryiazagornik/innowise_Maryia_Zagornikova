# query_runner.py

class QueryRunner:
    """
    Отвечает за выполнение всех бизнес-запросов к БД.
    Вся "математика" (расчет возраста, разницы, аггрегация)
    происходит на стороне БД.
    """

    SQL_QUERIES = {
        "room_student_count": """
                              SELECT r.name, COUNT(s.id) AS student_count
                              FROM rooms r
                                       LEFT JOIN students s ON r.id = s.room_id
                              GROUP BY r.id, r.name
                              ORDER BY student_count DESC;
                              """,
        "top5_min_avg_age": """
                            SELECT r.name,
                                   AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birthday))) AS avg_age
                            FROM rooms r
                                     JOIN students s ON r.id = s.room_id
                            GROUP BY r.id, r.name
                            HAVING COUNT(s.id) > 0 -- Убедимся, что комната не пустая
                            ORDER BY avg_age ASC LIMIT 5;
                            """,
        "top5_max_age_diff": """
                             SELECT r.name,
                                    MAX(EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birthday))) -
                                    MIN(EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birthday))) AS age_difference
                             FROM rooms r
                                      JOIN students s ON r.id = s.room_id
                             GROUP BY r.id, r.name
                             HAVING COUNT(s.id) > 1 -- Нужны минимум 2 студента для разницы
                             ORDER BY age_difference DESC LIMIT 5;
                             """,
        "mixed_gender_rooms": """
                              SELECT r.name
                              FROM rooms r
                                       JOIN students s ON r.id = s.room_id
                              GROUP BY r.id, r.name
                              HAVING COUNT(DISTINCT s.sex) > 1;
                              """
    }

    def __init__(self, connector: 'DatabaseConnector'):
        self.connector = connector

    def _format_results(self, records, column_names):
        """Вспомогательный метод для преобразования (('Val1', 10),) в [{'col1': 'Val1', 'col2': 10}]."""
        if not records:
            return []
        return [dict(zip(column_names, row)) for row in records]

    def get_room_student_count(self):
        records = self.connector.execute_query(
            self.SQL_QUERIES["room_student_count"],
            fetch_many=True
        )
        return self._format_results(records, ["room_name", "student_count"])

    def get_top5_min_avg_age(self):
        records = self.connector.execute_query(
            self.SQL_QUERIES["top5_min_avg_age"],
            fetch_many=True
        )
        return self._format_results(records, ["room_name", "avg_age"])

    def get_top5_max_age_diff(self):
        records = self.connector.execute_query(
            self.SQL_QUERIES["top5_max_age_diff"],
            fetch_many=True
        )
        return self._format_results(records, ["room_name", "age_difference"])

    def get_mixed_gender_rooms(self):
        records = self.connector.execute_query(
            self.SQL_QUERIES["mixed_gender_rooms"],
            fetch_many=True
        )
        return self._format_results(records, ["room_name"])

    def run_all_queries(self):
        """Собирает результаты всех запросов в один словарь."""
        return {
            "task_1_room_student_count": self.get_room_student_count(),
            "task_2_top5_min_avg_age": self.get_top5_min_avg_age(),
            "task_3_top5_max_age_diff": self.get_top5_max_age_diff(),
            "task_4_mixed_gender_rooms": self.get_mixed_gender_rooms(),
        }