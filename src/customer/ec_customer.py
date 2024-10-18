import sys
import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

class Customer:
    def __init__(self, kafka_broker, customer_id, services_file):
        self.kafka_broker = kafka_broker
        self.customer_id = customer_id
        self.services_file = services_file
        self.producer = None
        
        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - Customer %(customer_id)s: %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.setup_kafka()

    def setup_kafka(self):
        retry_count = 0
        while retry_count < 5:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[self.kafka_broker],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=3
                )
                self.logger.info("Kafka producer set up successfully")
                return
            except KafkaError as e:
                retry_count += 1
                self.logger.error(f"Failed to set up Kafka: {e}. Retrying in 5 seconds...")
                time.sleep(5)
        
        self.logger.critical("Failed to set up Kafka after 5 attempts. Exiting.")
        sys.exit(1)

    def read_services(self):
        try:
            with open(self.services_file, 'r') as file:
                services = file.readlines()
            return [service.strip() for service in services if service.strip()]
        except IOError as e:
            self.logger.error(f"Error reading services file: {e}")
            return []

    def request_service(self, destination):
        request = {
            'customer_id': self.customer_id,
            'destination': destination,
            'timestamp': time.time()
        }
        try:
            self.producer.send('customer_requests', request)
            self.producer.flush()
            self.logger.info(f"Sent service request to destination: {destination}")
        except KafkaError as e:
            self.logger.error(f"Failed to send service request: {e}")

    def run(self):
        services = self.read_services()
        if not services:
            self.logger.warning("No services found in the file. Exiting.")
            return

        for service in services:
            self.request_service(service)
            time.sleep(4)  # Wait 4 seconds before next request

        self.logger.info("All services requested. Exiting.")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python EC_Customer.py <Kafka_Broker> <Customer_ID> <Services_File>")
        sys.exit(1)

    kafka_broker = sys.argv[1]
    customer_id = sys.argv[2]
    services_file = sys.argv[3]

    customer = Customer(kafka_broker, customer_id, services_file)
    customer.run()
