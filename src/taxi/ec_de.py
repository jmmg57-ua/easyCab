import socket
import sys
import threading
import time
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
import numpy as np

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
        self.map_size = (20, 20)
        self.map = np.full(self.map_size, ' ', dtype=str)  # Mapa vacío inicial
        self.status = "FREE"   
        self.color = "RED"     
        self.position = [1, 1]  # Initial position
        self.pickup = None
        self.destination = None
        self.customer_asigned = None
    
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


    def connect_kafka(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_broker,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                max_block_ms=5000  # Establecer timeout de 5 segundos para enviar mensajes
            )
            self.consumer = KafkaConsumer(
                'taxi_instructions', 'updated_map',
                bootstrap_servers=self.kafka_broker,
                group_id='central_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info("Successfully connected to Kafka")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False

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
        logger.info("Listening for instructions from Kafka topic 'taxi_instructions'...")
        for message in self.consumer:
            instruction = message.value
            if instruction['taxi_id'] == self.taxi_id:
                logger.info("message recieved in topic 'taxi_instructions'.")
                self.process_instruction(instruction)

    def process_instruction(self, instruction):
        if instruction['type'] == 'MOVE':
            self.pickup = instruction['pickup']
            self.destination = instruction['destination']
            self.color = "GREEN"
            self.customer_asigned = ['customer_id']
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
            logger.info("Taxi is now FREE!!")
            self.send_position_update()
            
    def move_towards(self, target):
        # Movimiento octogonal con ajuste de límites
        if self.position[0] < target[0]:
            self.position[0] += 1
        elif self.position[0] > target[0]:
            self.position[0] -= 1
        
        if self.position[1] < target[1]:
            self.position[1] += 1
        elif self.position[1] > target[1]:
            self.position[1] -= 1

        # Manejo de límites (wrap-around)
        self.position[0] = self.position[0] % 20  # Mantener en el rango [0, 19]
        self.position[1] = self.position[1] % 20  # Mantener en el rango [0, 19]
        
        self.send_position_update()
        time.sleep(1)  # Esperar 1 segundo entre movimientos

    def send_position_update(self):
        
        update = {
            'taxi_id': self.taxi_id,
            'status': self.status,
            'color': self.color,
            'position': self.position,
            'customer_id': self.customer_asigned
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
            self.process_map_update(map_data)  # Procesar y opcionalmente modificar el mapa

    def process_map_update(self, map_data):
        """Procesa el mapa recibido, realiza cambios si es necesario y lo envía de vuelta."""
        # (Opcional) Realizar modificaciones en el mapa basado en la posición actual del taxi
        self.move_to_destination()
        #self.draw_map(map_data)
        
    def draw_map(self, map_data):
        """Dibuja el mapa recibido de 'Central' en los logs de Docker con delimitación de bordes."""
        
        logger.info("Current Map State with Borders:")
        map_lines = [""]  # Línea vacía para separar en el log

        # Crear el borde superior
        border_row = "#" * (self.map_size[1] + 2)
        map_lines.append(border_row)

        # Inicializar un mapa vacío
        map_array = np.full(self.map_size, ' ', dtype=str)

        # Colocar las ubicaciones en el mapa
        for loc_id, location in map_data['locations'].items():
            x, y = location['position']
            map_array[y, x] = loc_id  # ID de la ubicación

        # Colocar los taxis en el mapa (excluyendo el taxi actual en la posición original)
        for taxi_id, taxi_info in map_data['taxis'].items():
            x, y = taxi_info['position']
            if taxi_id != str(self.taxi_id):  # Solo dibujar otros taxis
                map_array[y, x] = str(taxi_id)  # ID del taxi
        
        # Colocar el taxi actual en su nueva posición
        new_x, new_y = self.position
        map_array[new_y, new_x] = str(self.taxi_id)  # Usar el ID del taxi actual

        # Agregar delimitadores laterales y formar cada línea
        for row in map_array:
            map_lines.append("#" + "".join(row) + "#")

        # Agregar el borde inferior
        map_lines.append(border_row)

        # Unir las líneas y registrar en el log
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
        if not self.connect_kafka():
            return
        
        #self.set_up_socket_sensor()
        #logger.info("Waiting for sensor connection...")
        #conn, addr = self.sensor_socket.accept()
        
        self.connect_to_central()
        
        if not self.authenticate():
            return

        logger.info("Digital Engine is running...")
        
        kafka_thread = threading.Thread(target=self.listen_for_instructions, daemon=True)
        kafka_thread.start()
        
        #threading.Thread(target=self.listen_for_sensor_data, args=(conn, addr), daemon=True).start()
        
        # Crear hilo para escuchar actualizaciones del mapa
        map_update_thread = threading.Thread(target=self.listen_for_map_updates, daemon=True)
        map_update_thread.start()
        
        # Mantener el hilo principal activo
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
