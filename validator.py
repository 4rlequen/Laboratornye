"""
Валидатор сообщений для системы контроля домашних заданий
"""
import json
from datetime import datetime
from typing import Dict, Any, Tuple


class MessageValidator:
    """Валидация сообщений о домашних заданиях"""

    @staticmethod
    def validate_homework_message(message: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Валидация сообщения о домашнем задании
        """
        try:
            # Проверка обязательных полей верхнего уровня
            required_top_fields = ["event_id", "timestamp", "event_type", "data"]
            for field in required_top_fields:
                if field not in message:
                    return False, f"Отсутствует обязательное поле: {field}"

            # Проверка типов данных
            if not isinstance(message["event_id"], str):
                return False, "event_id должен быть строкой"

            if not message["event_id"].startswith("HW"):
                return False, "event_id должен начинаться с 'HW'"

            # Проверка timestamp
            try:
                datetime.fromisoformat(message["timestamp"])
            except (ValueError, TypeError):
                return False, "Некорректный формат timestamp"

            # Проверка event_type
            if message["event_type"] != "homework_event":
                return False, f"Некорректный event_type: {message['event_type']}"

            # Проверка поля data
            if not isinstance(message["data"], dict):
                return False, "Поле data должно быть объектом"

            # Проверка homework
            if "homework" not in message["data"]:
                return False, "Отсутствует поле data.homework"

            homework = message["data"]["homework"]
            if not isinstance(homework, dict):
                return False, "homework должен быть объектом"

            required_homework_fields = ["id", "title", "subject", "task_type"]
            for field in required_homework_fields:
                if field not in homework:
                    return False, f"Отсутствует поле homework.{field}"

            # Проверка student
            if "student" not in message["data"]:
                return False, "Отсутствует поле data.student"

            student = message["data"]["student"]
            if not isinstance(student, dict):
                return False, "student должен быть объектом"

            required_student_fields = ["id", "name", "class"]
            for field in required_student_fields:
                if field not in student:
                    return False, f"Отсутствует поле student.{field}"

            # Проверка teacher
            if "teacher" in message["data"]:
                teacher = message["data"]["teacher"]
                if not isinstance(teacher, dict):
                    return False, "teacher должен быть объектом"

                if "id" not in teacher or "name" not in teacher:
                    return False, "teacher должен содержать id и name"

            # Проверка dates
            if "dates" not in message["data"]:
                return False, "Отсутствует поле data.dates"

            dates = message["data"]["dates"]
            required_date_fields = ["issued", "deadline"]
            for field in required_date_fields:
                if field not in dates:
                    return False, f"Отсутствует поле dates.{field}"

            # Проверка формата дат
            try:
                datetime.fromisoformat(dates["issued"])
                datetime.fromisoformat(dates["deadline"])
            except (ValueError, TypeError):
                return False, "Некорректный формат дат в dates"

            # Проверка статуса
            if "status" not in message["data"]:
                return False, "Отсутствует поле data.status"

            valid_statuses = ["выдано", "сдано", "проверено", "просрочено", "возвращено_на_доработку"]
            if message["data"]["status"] not in valid_statuses:
                return False, f"Некорректный статус: {message['data']['status']}"

            # Логическая проверка: если статус "проверено", должна быть оценка
            if message["data"]["status"] == "проверено" and message["data"].get("grade") is None:
                return False, "Для статуса 'проверено' требуется оценка"

            # Проверка оценки (если есть)
            if "grade" in message["data"] and message["data"]["grade"] is not None:
                grade = message["data"]["grade"]
                if not isinstance(grade, int) or grade < 2 or grade > 5:
                    return False, f"Некорректная оценка: {grade}"

            return True, "Сообщение успешно прошло валидацию"

        except Exception as e:
            return False, f"Ошибка при валидации: {str(e)}"

    @staticmethod
    def print_validation_result(is_valid: bool, message: str):
        """Вывод результата валидации"""
        if is_valid:
            print(f"✅ {message}")
        else:
            print(f"❌ NOT VALID: {message}")


# Тестирование
if __name__ == "__main__":
    from message_generator import MessageGenerator

    validator = MessageValidator()
    generator = MessageGenerator()

    # Тест валидного сообщения
    valid_msg = generator.generate_homework_message()
    is_valid, result = validator.validate_homework_message(valid_msg)
    print("Тест 1 - Валидное сообщение:")
    validator.print_validation_result(is_valid, result)

    # Тест невалидного сообщения
    invalid_msg = generator.generate_invalid_message()
    is_valid, result = validator.validate_homework_message(invalid_msg)
    print("\nТест 2 - Невалидное сообщение:")
    validator.print_validation_result(is_valid, result)