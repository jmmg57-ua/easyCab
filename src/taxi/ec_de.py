import socket
import sys
import threading
import time
import json
import logging
from kafka import KafkaConsumer, KafkaProducer

# Configurar el logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

class DigitalEngine:
    def __init__(self, ec_central_ip, ec_central_port, kafka_broker, ec_s_ip, ec_s_port, taxi_id):
        self.ec_central_ip = ec_central_ip
        self.ec_central_port = ec_central_port
        self.kafka_broker = kafka_broker
        self.ec_s_ip = ec_s_ip
        self.ec_s_port = ec_s_port
        self.taxi_id = taxi_id
        self.status = "FREE"   
        self.color = "RED"     
        self.position = [1, 1]  # Initial position
        self.destination = None
        
        # Connect to EC_Central
        self.central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.central_socket.connect((self.ec_central_ip, self.ec_central_port))
        
        # Set up Kafka producer and consumer                        COMRPOBAR QUE SE CONECTA A KAFKA
        self.producer = KafkaProducer(bootstrap_servers=[self.kafka_broker],
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.consumer = KafkaConsumer('taxi_instructions',
                                      bootstrap_servers=[self.kafka_broker],
                                      value_deserializer=lambda v: json.loads(v.decode('utf-8')))
        
        
        # Set up socket for EC_S
        self.sensor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sensor_socket.bind((self.ec_s_ip, self.ec_s_port))
        self.sensor_socket.listen(1)
        
        logger.info(f"Digital Engine for Taxi {self.taxi_id} initialized")

    def authenticate(self):
        auth_message = f"{self.taxi_id}"
        self.central_socket.send(auth_message.encode())
        response = self.central_socket.recv(1024).decode()
        if response == "OK":
            logger.info(f"Taxi {self.taxi_id} authenticated successfully")
            return True
        else:
            logger.warning(f"Authentication failed for Taxi {self.taxi_id}")
            return False

    def listen_for_instructions(self):
        for message in self.consumer:
            instruction = message.value
            if instruction['taxi_id'] == self.taxi_id:
                self.process_instruction(instruction)

    def process_instruction(self, instruction):
        if instruction['type'] == 'MOVE':
            self.destination = instruction['destination']
            self.color = "GREEN"
            self.move_to_destination()
        elif instruction['type'] == 'STOP':
            self.color = "RED"
        elif instruction['type'] == 'RESUME':
            self.color = "GREEN"
        elif instruction['type'] == 'RETURN_TO_BASE':
            self.destination = [1, 1]
            self.color = "GREEN"
            self.move_to_destination()

    def move_to_destination(self):
        while self.position != self.destination and self.color == "GREEN":
            # Simple movement logic
            if self.position[0] < self.destination[0]:
                self.position[0] += 1
            elif self.position[0] > self.destination[0]:
                self.position[0] -= 1
            elif self.position[1] < self.destination[1]:
                self.position[1] += 1
            elif self.position[1] > self.destination[1]:
                self.position[1] -= 1
            
            self.send_position_update()
            time.sleep(1)  # Wait for 1 second between movements
        
        if self.position == self.destination:
            self.color = "RED"
            self.send_position_update()

    def send_position_update(self):
        update = {
            'taxi_id': self.taxi_id,
            'status': self.status,
            'color': self.color,
            'position': self.position
        }
        self.producer.send('taxi_updates', update)

    def listen_for_sensor_data(self):
        conn, addr = self.sensor_socket.accept()
        logger.info(f"Connected to Sensors at {addr}")
        while True:
            data = conn.recv(1024).decode()
            if data == "KO":
                self.color = "RED"
                self.send_position_update()
            elif data == "OK" and self.color == "RED":
                self.color = "GREEN"
                self.send_position_update()

    def run(self):
        if not self.authenticate():
            return
        
        threading.Thread(target=self.listen_for_instructions, daemon=True).start()
        threading.Thread(target=self.listen_for_sensor_data, daemon=True).start()
        
        while True:
            time.sleep(1)  # Keep the main thread alive

if __name__ == "__main__":

    if len(sys.argv) != 7:
        logger.error(f"Arguments received: {sys.argv}")
        logger.error("Usage: python EC_DE.py <EC_Central_IP> <EC_Central_Port> <Kafka_Broker> <EC_S_IP> <EC_S_Port> <Taxi_ID>")
        sys.exit(1)
    
    ec_central_ip = sys.argv[1]
    ec_central_port = int(sys.argv[2])
    kafka_broker = sys.argv[3]
    ec_s_ip = sys.argv[4]
    ec_s_port = int(sys.argv[5])
    taxi_id = int(sys.argv[6])
    
    digital_engine = DigitalEngine(ec_central_ip, ec_central_port, kafka_broker, ec_s_ip, ec_s_port, taxi_id)
    digital_engine.run()

