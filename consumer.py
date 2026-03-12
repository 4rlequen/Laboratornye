"""
Consumer для получения сообщений о домашних заданиях из Kafka
"""
import json
import logging
from kafka import KafkaConsumer

from typing import Optional


from validator import MessageValidator

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HomeworkConsumer:
    """
    Consumer для получения сообщений о домашних заданиях
    """

    def __init__(self,
                 bootstrap_servers: str = 'localhost:9092',
                 topic: str = 'homework-events',
                 group_id: str = 'homework-consumer-group'):
        """
        Инициализация consumer
        """
        self.topic = topic
        self.validator = MessageValidator()

        # Статистика
        self.stats = {
            "total": 0,
            "valid": 0,
            "invalid": 0,
            "by_status": {},
            "by_subject": {},
            "grades": []
        }

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None
        )

        logger.info(f"Consumer инициализирован. Топик: {topic}")

    def update_stats(self, message):
        """Обновление статистики"""
        data = message.value['data']

        # По статусу
        status = data['status']
        self.stats["by_status"][status] = self.stats["by_status"].get(status, 0) + 1

        # По предмету
        subject = data['homework']['subject']
        self.stats["by_subject"][subject] = self.stats["by_subject"].get(subject, 0) + 1

        # Оценки
        if data.get('grade'):
            self.stats["grades"].append(data['grade'])

    def print_message_info(self, message):
        """Красивый вывод информации о сообщении"""
        value = message.value
        data = value['data']

        print("\n" + "=" * 60)
        print(f"📋 ДОМАШНЕЕ ЗАДАНИЕ #{value['event_id']}")
        print("=" * 60)
        print(f"👨‍🎓 Ученик: {data['student']['name']} ({data['student']['class']})")
        print(f"👨‍🏫 Учитель: {data['teacher']['name']}")
        print(f"📚 Предмет: {data['homework']['subject']}")
        print(f"📝 Задание: {data['homework']['title']}")
        print(f"📊 Статус: {data['status']}")
        print(f"📅 Выдано: {data['dates']['issued']}")
        print(f"⏰ Дедлайн: {data['dates']['deadline']}")
        if data.get('grade'):
            print(f"🎓 Оценка: {data['grade']}")
        if data.get('feedback'):
            print(f"💬 Отзыв: {data['feedback']}")
        print(f"📎 Offset: {message.offset}")

    def print_stats(self):
        """Вывод статистики"""
        print("\n" + "=" * 60)
        print("📊 СТАТИСТИКА ОБРАБОТКИ")
        print("=" * 60)
        print(f"Всего сообщений: {self.stats['total']}")
        print(f"✅ Валидных: {self.stats['valid']}")
        print(f"❌ Невалидных: {self.stats['invalid']}")

        print("\n📈 По статусам:")
        for status, count in self.stats["by_status"].items():
            print(f"  {status}: {count}")

        print("\n📚 По предметам:")
        for subject, count in self.stats["by_subject"].items():
            print(f"  {subject}: {count}")

        if self.stats["grades"]:
            avg_grade = sum(self.stats["grades"]) / len(self.stats["grades"])
            print(f"\n🎓 Средняя оценка: {avg_grade:.2f}")

    def process_message(self, message):
        """Обработка сообщения"""
        try:
            self.stats["total"] += 1

            # Вывод информации
            self.print_message_info(message)

            # Валидация
            is_valid, validation_msg = self.validator.validate_homework_message(message.value)

            if is_valid:
                self.stats["valid"] += 1
                self.update_stats(message)
                print(f"\n✅ РЕЗУЛЬТАТ: {validation_msg}")
            else:
                self.stats["invalid"] += 1
                print(f"\n❌ РЕЗУЛЬТАТ: {validation_msg}")

        except Exception as e:
            logger.error(f"Ошибка при обработке: {e}")

    def consume(self, timeout: Optional[float] = None):
        """Запуск потребления"""
        import time

        logger.info(f"🚀 Начинаем прослушивание топика '{self.topic}'...")
        logger.info("Ожидание сообщений (Ctrl+C для остановки)...")

        start_time = time.time()

        try:
            for message in self.consumer:
                self.process_message(message)

                if timeout and (time.time() - start_time) > timeout:
                    logger.info(f"⏰ Таймаут {timeout} секунд")
                    break

        except KeyboardInterrupt:
            logger.info("\n⚠️ Получен сигнал прерывания")
        finally:
            self.print_stats()
            self.close()

    def close(self):
        """Закрытие соединения"""
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer закрыт")


def main():
    """Основная функция"""
    consumer = None
    try:
        consumer = HomeworkConsumer()
        consumer.consume(timeout=60)
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
    finally:
        if consumer:
            consumer.close()


if __name__ == "__main__":
    main()