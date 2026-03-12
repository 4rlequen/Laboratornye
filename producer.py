"""
Producer для отправки сообщений о домашних заданиях в Kafka
"""
import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from typing import Optional, Dict, Any

from message_generator import MessageGenerator

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HomeworkProducer:
    """
    Producer для отправки сообщений о домашних заданиях в Kafka
    """

    def __init__(self,
                 bootstrap_servers: str = 'localhost:9092',
                 topic: str = 'homework-events'):
        """
        Инициализация producer
        """
        self.topic = topic
        self.generator = MessageGenerator()

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3
        )

        logger.info(f"Producer инициализирован. Топик: {topic}")

    def send_message(self, key: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Генерация и отправка одного сообщения
        """
        try:
            # Генерация сообщения
            message = self.generator.generate_homework_message()

            # Логирование
            logger.info("=" * 60)
            logger.info("📚 НОВОЕ ДОМАШНЕЕ ЗАДАНИЕ")
            logger.info("=" * 60)
            logger.info(f"Ученик: {message['data']['student']['name']} ({message['data']['student']['class']})")
            logger.info(f"Предмет: {message['data']['homework']['subject']}")
            logger.info(f"Задание: {message['data']['homework']['title']}")
            logger.info(f"Статус: {message['data']['status']}")
            logger.info(f"Дедлайн: {message['data']['dates']['deadline']}")
            logger.info("-" * 60)
            logger.info("Полное сообщение:")
            logger.info(json.dumps(message, indent=2, ensure_ascii=False))

            # Определение ключа
            if not key:
                key = message['event_id']

            # Отправка
            future = self.producer.send(
                topic=self.topic,
                value=message,
                key=key
            )

            record_metadata = future.get(timeout=10)

            logger.info(f"✅ Отправлено: offset={record_metadata.offset}")

            return message

        except KafkaError as e:
            logger.error(f"❌ Ошибка Kafka: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return None

    def send_messages(self, count: int = 5, delay: float = 2.0):
        """
        Отправка нескольких сообщений
        """
        logger.info(f"🚀 Начинаем отправку {count} сообщений...")

        successful = 0
        for i in range(count):
            logger.info(f"\n📨 Сообщение {i + 1} из {count}")

            result = self.send_message()
            if result:
                successful += 1

            if i < count - 1:
                logger.info(f"⏳ Ожидание {delay} секунд...")
                time.sleep(delay)

        logger.info(f"\n📊 Отправка завершена. Успешно: {successful}/{count}")

    def close(self):
        """Закрытие соединения"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer закрыт")


def main():
    """Основная функция"""
    producer = None
    try:
        producer = HomeworkProducer()

        # Отправка сообщений
        producer.send_messages(count=5, delay=2)

    except KeyboardInterrupt:
        logger.info("\n⚠️ Получен сигнал прерывания")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()