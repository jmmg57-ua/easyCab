import sys
import json
import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

class Customer:
    def __init__(self, kafka_broker, customer_id, services_file, customer_location):
        self.kafka_broker = kafka_broker
        self.customer_id = customer_id
        self.services_file = services_file
        self.producer = None
        self.consumer = None
        self.customer_location = [int(coord) for coord in customer_location.split(',')]  # Almacena como lista
        
        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - Customer %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.setup_kafka()

    def setup_kafka(self):
        retry_count = 0
        while retry_count < 5:
            try:
                # Set up Kafka Producer
                self.producer = KafkaProducer(
                    bootstrap_servers=[self.kafka_broker],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=3
                )
                self.logger.info("Kafka producer set up successfully")

                # Set up Kafka Consumer for responses
                self.consumer = KafkaConsumer(
                    'taxi_responses',
                    bootstrap_servers=[self.kafka_broker],
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    group_id=f'customer_{self.customer_id}',  # Unique group ID for this customer
                    auto_offset_reset='earliest'
                )
                self.logger.info("Kafka consumer set up successfully")
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
            'customer_location': self.customer_location      #Añadida la localización del cliente al mensaje del request
        }
        try:
            self.producer.send('taxi_requests', request)
            self.producer.flush()
            self.logger.info(f"Sent service request to destination: {destination}")
        except KafkaError as e:
            self.logger.error(f"Failed to send service request: {e}")

    def wait_for_confirmation(self):
        """
        Wait for a confirmation from CENTRAL, either 'OK' or 'KO' for the current service request.
        """
        self.logger.info("Waiting for confirmation from CENTRAL...")
        for message in self.consumer:
            response = message.value
            if response['customer_id'] == self.customer_id:
                status = response['status']
                if status == 'OK':
                    self.logger.info(f"Service accepted: {response}")
                    return True
                elif status == 'KO':
                    self.logger.info(f"Service rejected: {response}")
                    return False
                
    def wait_till_finished(self):
        """Espera hasta recibir una confirmación de finalización en 'taxi_responses'."""
        self.logger.info("Waiting for service completion confirmation...")

        # Escuchar indefinidamente hasta que el servicio se complete
        for message in self.consumer:
            response = message.value
            
            self.logger.info(f"Received response: {response}")
            
            # Comprobar si la respuesta es para este cliente y el servicio ha finalizado
            if response.get('customer_id') == self.customer_id and response.get('status') == "END":
                self.logger.info("Service completed.")
                return True  # El servicio ha finalizado correctamente
            
        # Si el bucle se rompe inesperadamente (por manejo de excepciones externas)
        self.logger.warning("Listener stopped unexpectedly.")
        return False


    def run(self):
        services = self.read_services()
        if not services:
            self.logger.warning("No services found in the file. Exiting.")
            return

        for service in services:
            
            self.request_service(service)
            
            confirmation = self.wait_for_confirmation()

            if confirmation:
                self.logger.info(f"Service to {service} asigned successfully.")
                
                completed = self.wait_till_finished()
                if completed:
                    self.logger.info(f"Service to {service} completed")
                    self.logger.info(f"Wait 4 seconds before requesting the next service")
                    time.sleep(4)
                else:
                    self.logger.warning(f"Service to {service} haven't been completed.")

            else:
                self.logger.warning(f"Service to {service} was rejected.")
                
        self.logger.info("All services requested. Exiting.")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python EC_Customer.py <Kafka_Broker> <Customer_ID> <Services_File>")
        sys.exit(1)

    kafka_broker = sys.argv[1]
    customer_id = sys.argv[2]
    services_file = sys.argv[3]
    customer_location = sys.argv[4]

    customer = Customer(kafka_broker, customer_id, services_file, customer_location)
    customer.run()
