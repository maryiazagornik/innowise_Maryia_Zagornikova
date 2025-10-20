# db_connector.py
import psycopg2
import psycopg2.extras  # Добавьте этот импорт
import sys


class DatabaseConnector:
    """
    Отвечает за подключение к базе данных PostgreSQL
    и выполнение SQL-запросов.
    """

    def __init__(self, dbname, user, password, host, port):
        try:
            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            self.conn.autocommit = False
        except psycopg2.OperationalError as e:
            print(f"Ошибка подключения к БД: {e}", file=sys.stderr)
            sys.exit(1)

    def create_tables(self):
        """Создает таблицы и индексы."""
        try:
            with self.conn.cursor() as cursor:
                # Удаление таблиц, если они существуют
                cursor.execute("""
                               DROP TABLE IF EXISTS students;
                               DROP TABLE IF EXISTS rooms CASCADE;

                               CREATE TABLE rooms
                               (
                                   id   INT PRIMARY KEY,
                                   name VARCHAR(100) NOT NULL
                               );

                               CREATE TABLE students
                               (
                                   id       INT PRIMARY KEY,
                                   name     VARCHAR(255) NOT NULL,
                                   birthday DATE         NOT NULL,
                                   sex      CHAR(1)      NOT NULL,
                                   room_id  INT,
                                   CONSTRAINT fk_room
                                       FOREIGN KEY (room_id)
                                           REFERENCES rooms (id)
                                           ON DELETE SET NULL
                               );

                               -- Создаем индексы
                               CREATE INDEX IF NOT EXISTS idx_students_room_id ON students(room_id);
                               CREATE INDEX IF NOT EXISTS idx_students_birthday ON students(birthday);
                               CREATE INDEX IF NOT EXISTS idx_students_sex ON students(sex);
                               """)
                self.conn.commit()
                print("Таблицы и индексы успешно созданы.")

        except Exception as e:
            print(f"Ошибка при создании таблиц: {e}", file=sys.stderr)
            self.conn.rollback()
            raise

    def execute_query(self, query, params=None, fetch_many=False):
        """Выполняет SQL-запрос и возвращает результат."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params or ())
                if fetch_many:
                    return cursor.fetchall()
                return cursor.fetchone()
        except Exception as e:
            print(f"Ошибка выполнения запроса: {e}", file=sys.stderr)
            self.conn.rollback()
            raise

    def execute_many(self, query, data_list):
        """Выполняет пакетную вставку данных."""
        try:
            with self.conn.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, query, data_list)
                self.conn.commit()
        except Exception as e:
            print(f"Ошибка пакетной вставки: {e}", file=sys.stderr)
            self.conn.rollback()
            raise

    def close(self):
        """Закрывает соединение с БД."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()