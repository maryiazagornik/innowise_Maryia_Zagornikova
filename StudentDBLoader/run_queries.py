from db_connector import DatabaseConnector
from query_runner import QueryRunner
import json
from datetime import datetime

# Настройки подключения к БД
db_config = {
    "dbname": "university_db",
    "user": "postgres",
    "password": "7413",
    "host": "localhost",
    "port": "5432"
}


def print_results(results):
    """Функция для форматированного вывода результатов"""
    print("\n" + "=" * 50)
    print("РЕЗУЛЬТАТЫ ЗАПРОСОВ")
    print("=" * 50)

    # 1. Количество студентов в комнатах
    print("\n1. Количество студентов в комнатах:")
    for room in results["task_1_room_student_count"]:
        print(f"  - {room['room_name']}: {room['student_count']} студентов")

    # 2. Топ-5 комнат с самым маленьким средним возрастом
    print("\n2. Топ-5 комнат с самым маленьким средним возрастом:")
    for i, room in enumerate(results["task_2_top5_min_avg_age"], 1):
        print(f"  {i}. {room['room_name']}: средний возраст {room['avg_age']:.1f} лет")

    # 3. Топ-5 комнат с наибольшей разницей в возрасте
    print("\n3. Топ-5 комнат с наибольшей разницей в возрасте:")
    for i, room in enumerate(results["task_3_top5_max_age_diff"], 1):
        print(f"  {i}. {room['room_name']}: разница {room['age_difference']:.1f} лет")

    # 4. Комнаты с разнополыми студентами
    print("\n4. Комнаты, где живут разнополые студенты:")
    if results["task_4_mixed_gender_rooms"]:
        for room in results["task_4_mixed_gender_rooms"]:
            print(f"  - {room['room_name']}")
    else:
        print("  Таких комнат не найдено")


def save_to_json(results, filename="query_results.json"):
    """Сохранение результатов в JSON файл"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nРезультаты сохранены в файл: {filename}")


def main():
    try:
        # Подключение к БД
        print("Подключение к базе данных...")
        db = DatabaseConnector(**db_config)

        # Создаем экземпляр QueryRunner
        query_runner = QueryRunner(db)

        # Запускаем все запросы
        print("Выполнение запросов...")
        results = query_runner.run_all_queries()

        # Выводим результаты
        print_results(results)

        # Сохраняем результаты в файл
        save_to_json(results)

    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        if 'db' in locals():
            db.close()
            print("\nСоединение с базой данных закрыто.")


if __name__ == "__main__":
    main()