import os
import logging
from kafka import KafkaConsumer, KafkaProducer
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ECCentral:
    def __init__(self):
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.producer = None
        self.consumer = None

    def connect_kafka(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            self.consumer = KafkaConsumer(
                'customer_requests',
                bootstrap_servers=self.kafka_bootstrap_servers,
                group_id='central_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info("Successfully connected to Kafka")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False

    def run(self):
        if not self.connect_kafka():
            return

        logger.info("EC_Central is running...")
        try:
            # Simple test message
            self.producer.send('test_topic', {'message': 'EC_Central is online'})
            
            # Start listening for messages
            for message in self.consumer:
                logger.info(f"Received message: {message.value}")
        except KeyboardInterrupt:
            logger.info("Shutting down EC_Central...")
        finally:
            if self.producer:
                self.producer.close()
            if self.consumer:
                self.consumer.close()

if __name__ == "__main__":
    central = ECCentral()
    central.run()