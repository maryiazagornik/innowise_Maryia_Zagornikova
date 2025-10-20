# data_loader.py
import json
import sys
from datetime import datetime


class DataLoader:
    """
    Отвечает за чтение данных из JSON-файлов
    и их загрузку в базу данных.
    """

    def __init__(self, connector: 'DatabaseConnector'):
        self.connector = connector

    def _read_json_file(self, filepath):
        """Приватный метод для чтения JSON."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Ошибка: Файл не найден {filepath}", file=sys.stderr)
            return None
        except json.JSONDecodeError:
            print(f"Ошибка: Не удалось декодировать JSON из {filepath}", file=sys.stderr)
            return None

    def load_rooms(self, rooms_path):
        """Загружает комнаты в таблицу rooms."""
        rooms_data = self._read_json_file(rooms_path)
        if rooms_data is None:
            return

        query = "INSERT INTO rooms (id, name) VALUES (%(id)s, %(name)s) ON CONFLICT (id) DO NOTHING"

        # Преобразуем данные для psycopg2.extras.execute_batch
        # Он ожидает список кортежей или словарей
        self.connector.execute_many(query, rooms_data)
        print(f"Загружено {len(rooms_data)} комнат.")

    def load_students(self, students_path):
        """Загружает студентов в таблицу students."""
        students_data = self._read_json_file(students_path)
        if students_data is None:
            return

        query = """
                INSERT INTO students (id, name, birthday, sex, room_id)
                VALUES (%(id)s, %(name)s, %(birthday)s, %(sex)s, %(room)s) ON CONFLICT (id) DO NOTHING \
                """

        # psycopg2 может не понять "room", поэтому лучше переименовать ключ
        # или использовать %s плейсхолдеры и список кортежей.
        # Для execute_batch с именованными аргументами,
        # нам нужно, чтобы ключи словаря соответствовали.

        prepared_data = []
        for student in students_data:
            # Преобразуем дату из строки ISO в объект date
            # '2011-08-22T00:00:00.000000' -> '2011-08-22'
            try:
                bday_dt = datetime.fromisoformat(student['birthday'])
                student['birthday'] = bday_dt.date()
                student['room_id'] = student.pop('room')  # Переименовываем ключ
                prepared_data.append(student)
            except (ValueError, TypeError):
                print(f"Пропуск студента с ID {student.get('id')}: неверный формат даты.", file=sys.stderr)

        # Вставка через execute_many (используя execute_batch)
        query = """
                INSERT INTO students (id, name, birthday, sex, room_id)
                VALUES (%(id)s, %(name)s, %(birthday)s, %(sex)s, %(room_id)s) ON CONFLICT (id) DO NOTHING \
                """
        self.connector.execute_many(query, prepared_data)
        print(f"Загружено {len(prepared_data)} студентов.")