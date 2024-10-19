import os
import socket
import sys
import threading
import time
import json
from kafka import KafkaConsumer, KafkaProducer

class DigitalEngine:
    def __init__(self, ec_central_ip, ec_central_port, kafka_broker, ec_s_ip, ec_s_port, taxi_id):
        self.ec_central_ip = ec_central_ip
        self.ec_central_port = ec_central_port
        self.kafka_broker = kafka_broker
        self.ec_s_ip = ec_s_ip
        self.ec_s_port = ec_s_port
        self.taxi_id = taxi_id
        self.position = [1, 1]  # Initial position
        self.state = "IDLE"
        self.destination = None
        
        # Connect to EC_Central
        self.central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.central_socket.connect((self.ec_central_ip, self.ec_central_port))
        
        # Set up Kafka producer and consumer
        self.producer = KafkaProducer(bootstrap_servers=[self.kafka_broker],
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.consumer = KafkaConsumer('taxi_instructions',
                                      bootstrap_servers=[self.kafka_broker],
                                      value_deserializer=lambda v: json.loads(v.decode('utf-8')))
        
        # Set up socket for EC_S
        self.sensor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sensor_socket.bind((self.ec_s_ip, self.ec_s_port))
        self.sensor_socket.listen(1)
        
        print(f"Digital Engine for Taxi {self.taxi_id} initialized")

    def authenticate(self):
        auth_message = f"AUTH#{self.taxi_id}"
        self.central_socket.send(auth_message.encode())
        response = self.central_socket.recv(1024).decode()
        if response == "OK":
            print(f"Taxi {self.taxi_id} authenticated successfully")
            return True
        else:
            print(f"Authentication failed for Taxi {self.taxi_id}")
            return False

    def listen_for_instructions(self):
        for message in self.consumer:
            instruction = message.value
            if instruction['taxi_id'] == self.taxi_id:
                self.process_instruction(instruction)

    def process_instruction(self, instruction):
        if instruction['type'] == 'MOVE':
            self.destination = instruction['destination']
            self.state = "MOVING"
            self.move_to_destination()
        elif instruction['type'] == 'STOP':
            self.state = "STOPPED"
        elif instruction['type'] == 'RESUME':
            self.state = "MOVING"
        elif instruction['type'] == 'RETURN_TO_BASE':
            self.destination = [1, 1]
            self.state = "MOVING"
            self.move_to_destination()

    def move_to_destination(self):
        while self.position != self.destination and self.state == "MOVING":
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
            self.state = "IDLE"
            self.send_position_update()

    def send_position_update(self):
        update = {
            'taxi_id': self.taxi_id,
            'position': self.position,
            'state': self.state
        }
        self.producer.send('taxi_updates', update)

    def listen_for_sensor_data(self):
        conn, addr = self.sensor_socket.accept()
        print(f"Connected to Sensors at {addr}")
        while True:
            data = conn.recv(1024).decode()
            if data == "KO":
                self.state = "STOPPED"
                self.send_position_update()
            elif data == "OK" and self.state == "STOPPED":
                self.state = "MOVING"
                self.send_position_update()

    def run(self):
        if not self.authenticate():
            return
        
        threading.Thread(target=self.listen_for_instructions, daemon=True).start()
        threading.Thread(target=self.listen_for_sensor_data, daemon=True).start()
        
        while True:
            time.sleep(1)  # Keep the main thread alive

if __name__ == "__main__":
    # Leer los parámetros desde variables de entorno
    ec_central_ip = os.getenv('EC_CENTRAL_IP', 'localhost')
    ec_central_port = int(os.getenv('EC_CENTRAL_PORT', 8080))
    kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
    ec_s_ip = os.getenv('EC_S_IP', 'localhost')
    ec_s_port = int(os.getenv('EC_S_PORT', 5000))
    taxi_id = int(os.getenv('TAXI_ID', 1))
    
    digital_engine = DigitalEngine(ec_central_ip, ec_central_port, kafka_broker, ec_s_ip, ec_s_port, taxi_id)
    digital_engine.run()