"""
Генератор сообщений для системы контроля домашних заданий
"""
import json
import random
from datetime import datetime, timedelta
from typing import Dict, Any


class MessageGenerator:
    """Генерация сообщений о домашних заданиях"""

    @staticmethod
    def generate_homework_message() -> Dict[str, Any]:
        """
        Генерирует сообщение о событии с домашним заданием
        """
        # Список учеников
        students = [
            {"id": f"ST{random.randint(100, 999)}", "name": f"Ученик_{i}",
             "class": f"{random.randint(5, 11)}{random.choice(['А', 'Б', 'В'])}"}
            for i in range(1, 11)
        ]

        # Список учителей
        teachers = [
            {"id": f"TC{random.randint(100, 999)}", "name": f"Учитель_{i}", "subject": subject}
            for i, subject in
            enumerate(["Математика", "Русский язык", "Физика", "Химия", "История", "Английский язык"], 1)
        ]

        # Предметы
        subjects = ["Математика", "Русский язык", "Физика", "Химия", "История", "Английский язык", "Литература",
                    "Биология"]

        # Типы заданий
        task_types = ["Упражнение", "Задача", "Сочинение", "Реферат", "Лабораторная", "Тест"]

        # Статусы заданий
        statuses = ["выдано", "сдано", "проверено", "просрочено", "возвращено_на_доработку"]

        # Оценки
        grades = [2, 3, 4, 5, None]  # None значит еще не оценено

        # Выбираем случайного ученика и учителя
        student = random.choice(students)
        teacher = random.choice(teachers)

        # Генерируем дату выдачи (от 7 дней назад до сегодня)
        issue_date = datetime.now() - timedelta(days=random.randint(0, 7))

        # Дедлайн (через 1-5 дней после выдачи)
        deadline = issue_date + timedelta(days=random.randint(1, 5))

        # Дата сдачи (если задание сдано)
        submission_date = None
        status = random.choice(statuses)
        if status in ["сдано", "проверено"]:
            submission_date = (deadline - timedelta(days=random.randint(0, 2))).isoformat()
        elif status == "просрочено":
            submission_date = (deadline + timedelta(days=random.randint(1, 3))).isoformat()

        message = {
            "event_id": f"HW{random.randint(10000, 99999)}",
            "timestamp": datetime.now().isoformat(),
            "event_type": "homework_event",
            "data": {
                "homework": {
                    "id": f"HWK{random.randint(1000, 9999)}",
                    "title": f"{random.choice(task_types)} по {random.choice(subjects)}",
                    "subject": random.choice(subjects),
                    "description": f"Выполнить задание №{random.randint(1, 50)} на странице {random.randint(10, 150)}",
                    "task_type": random.choice(task_types),
                    "difficulty": random.choice(["легкое", "среднее", "сложное"]),
                    "max_score": random.choice([5, 10, 20, 100])
                },
                "student": student,
                "teacher": teacher,
                "dates": {
                    "issued": issue_date.isoformat(),
                    "deadline": deadline.isoformat(),
                    "submitted": submission_date
                },
                "status": status,
                "grade": random.choice(grades) if status == "проверено" else None,
                "feedback": random.choice(
                    ["Отлично!", "Есть ошибки", "Нужно доработать", "Молодец", None]) if status == "проверено" else None
            },
            "metadata": {
                "version": "1.0",
                "source": "school_diary_system"
            }
        }

        return message

    @staticmethod
    def generate_invalid_message() -> Dict[str, Any]:
        """
        Генерирует невалидное сообщение для тестирования
        """
        return {
            "event_id": 12345,  # Должен быть строкой
            "timestamp": "неправильная_дата",
            "event_type": None,
            "data": {
                "homework": "не объект",  # Должен быть объект
                "student": None,
                # Отсутствуют обязательные поля
            }
        }


# Тестирование
if __name__ == "__main__":
    generator = MessageGenerator()

    print("=" * 60)
    print("ГЕНЕРАЦИЯ СООБЩЕНИЯ О ДОМАШНЕМ ЗАДАНИИ")
    print("=" * 60)

    message = generator.generate_homework_message()
    print(json.dumps(message, indent=2, ensure_ascii=False))

    print("\n" + "=" * 60)
    print("СТАТИСТИКА СООБЩЕНИЯ:")
    print("=" * 60)
    print(f"Ученик: {message['data']['student']['name']} ({message['data']['student']['class']})")
    print(f"Предмет: {message['data']['homework']['subject']}")
    print(f"Задание: {message['data']['homework']['title']}")
    print(f"Учитель: {message['data']['teacher']['name']} ({message['data']['teacher']['subject']})")
    print(f"Статус: {message['data']['status']}")
    print(f"Дедлайн: {message['data']['dates']['deadline']}")
    print(f"Оценка: {message['data']['grade'] if message['data']['grade'] else 'не оценено'}")