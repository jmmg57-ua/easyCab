import socket
import sys
import threading
import time
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError  
import numpy as np

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
        self.map_size = (20, 20)
        self.map = np.full(self.map_size, ' ', dtype=str) 
        self.status = "FREE"   
        self.color = "RED"     
        self.position = [1, 1]  
        self.pickup = None
        self.destination = None
        self.customer_asigned = None
        self.picked_off = 0
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
                logger.info("Kafka producer set up successfully")

                self.consumer = KafkaConsumer(
                    'taxi_instructions','map_updates',
                    bootstrap_servers=[self.kafka_broker],
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    group_id=f'customer_{self.taxi_id}',
                    auto_offset_reset='earliest'
                )
                logger.info("Kafka consumer set up successfully")
                return
            
            except KafkaError as e:
                retry_count += 1
                logger.error(f"Failed to set up Kafka: {e}. Retrying in 5 seconds...")
                time.sleep(5)
        
        logger.critical("Failed to set up Kafka after 5 attempts. Exiting.")
        sys.exit(1)

    
    def connect_to_central(self):
        try:
            self.central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.central_socket.connect((self.ec_central_ip, self.ec_central_port))
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Digital Engine: {e}")
            return False

    def set_up_socket_sensor(self):
        self.sensor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sensor_socket.bind(('0.0.0.0', self.ec_s_port))
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
        
    def kafka_listener(self):
        while True:
            try:
                for message in self.consumer:
                    if message.topic == 'taxi_instructions':
                        data = message.value
                        logger.info(f"Received message on topic 'taxi_instructions': {data}")
                        if 'taxi_id' not in data:
                            logger.warning("Received instruction does not contain 'taxi_id'. Skipping.")
                            continue  
                        if data['taxi_id'] == self.taxi_id:
                            logger.info("Message received in topic 'taxi_instructions'.")
                            self.process_instruction(data)
                        
                    elif message.topic == 'map_updates':
                        data = message.value
                        logger.info(f"Received message on topic 'map_updates'")
                        self.process_map_update(data)

            except KafkaError as e:
                logger.error(f"Kafka listener error: {e}")
                self.setup_kafka()  
                time.sleep(5)
            except Exception as e:
                logger.error(f"General error in kafka_listener: {e} {message.topic}")
                time.sleep(5)  

    def process_instruction(self, instruction):
        if instruction['type'] == 'MOVE':
            self.pickup = instruction['pickup']
            logger.info(f'PICK UP LOCATION = {self.pickup}')
            self.destination = instruction['destination']
            logger.info(f'DESTINATION LOCATION = {self.destination}')
            self.color = "GREEN"
            self.customer_asigned = instruction['customer_id']
            logger.info(f'ASIGNED CUSTOMER = {self.customer_asigned}')
            self.move_to_destination()
        elif instruction['type'] == 'STOP':
            self.color = "RED"
        elif instruction['type'] == 'RESUME':
            self.color = "GREEN"
        elif instruction['type'] == 'RETURN_TO_BASE':
            self.destination = [1, 1]
            self.color = "GREEN"

    def move_to_destination(self):
        while self.position != self.pickup and self.color == "GREEN":
            logger.info("Taxi moving towards the pickup location")
            self.move_towards(self.pickup)

        if self.position == self.pickup:
            self.picked_off = 1
        
        while self.position != self.destination and self.color == "GREEN":
            logger.info("Taxi moving towards the destination location")
            self.move_towards(self.destination)

        if self.position == self.destination:
            self.color = "RED"
            self.status = "END"
            logger.info("Trip ENDed!!")
            self.send_position_update()
            time.sleep(4)
            self.status = "FREE"
            self.picked_off = 0
            logger.info("Taxi is now FREE!!")
            self.send_position_update()
            
    def move_towards(self, target):
        if self.position[0] < target[0]:
            self.position[0] += 1
        elif self.position[0] > target[0]:
            self.position[0] -= 1
        
        if self.position[1] < target[1]:
            self.position[1] += 1
        elif self.position[1] > target[1]:
            self.position[1] -= 1

        self.position[0] = self.position[0] % 20
        self.position[1] = self.position[1] % 20  
        
        self.send_position_update()
        time.sleep(1)  

    def send_position_update(self):
        
        update = {
            'taxi_id': self.taxi_id,
            'status': self.status,
            'color': self.color,
            'position': self.position,
            'customer_id': self.customer_asigned,
            'picked_off': self.picked_off
        }
        logger.info("Sending update through 'taxi_updates'")
        self.producer.send('taxi_updates', update)
        
    def listen_for_map_updates(self):
        
        consumer = KafkaConsumer(
            'map_updates',
            bootstrap_servers=[self.kafka_broker],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            group_id=f'digital_engine_{self.taxi_id}'
        )
        logger.info("Listening for map updates on 'map_updates'...")

        for message in consumer:
            map_data = message.value
            logger.info(f"Received map update through 'map_updates'")
            self.process_map_update(map_data) 
        
    def draw_map(self, map_data):
        """Dibuja el mapa recibido de 'Central' en los logs de Docker con delimitación de bordes."""
        
        logger.info("Current Map State with Borders:")
        map_lines = [""]  

        border_row = "#" * (self.map_size[1] + 2)
        map_lines.append(border_row)

        map_array = np.full(self.map_size, ' ', dtype=str)

        for loc_id, location in map_data['locations'].items():
            x, y = location['position']
            map_array[y, x] = loc_id  

        for taxi_id, taxi_info in map_data['taxis'].items():
            x, y = taxi_info['position']
            if taxi_id != str(self.taxi_id):  
                map_array[y, x] = str(taxi_id)  
        
        new_x, new_y = self.position
        map_array[new_y, new_x] = str(self.taxi_id)  

        for row in map_array:
            map_lines.append("#" + "".join(row) + "#")

        map_lines.append(border_row)

        logger.info("\n".join(map_lines))


    def listen_for_sensor_data(self, conn, addr):
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
       
        self.set_up_socket_sensor()
        logger.info("Waiting for sensor connection...")
        conn, addr = self.sensor_socket.accept()
        
        self.connect_to_central()
        
        if not self.authenticate():
            return

        logger.info("Digital Engine is running...")
        
        threading.Thread(target=self.kafka_listener, daemon=True).start()
        threading.Thread(target=self.listen_for_sensor_data, args=(conn, addr), daemon=True).start()
        
        map_update_thread = threading.Thread(target=self.listen_for_map_updates, daemon=True)
        map_update_thread.start()
        
        while True:
            time.sleep(1)

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
