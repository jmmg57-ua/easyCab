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
        self.customer_asigned = "x"
        self.locations = {}
        self.taxis = {}
        self.picked_off = 0
        self.central_disconnected = False
        self.sensor_connected = False  # Estado inicial de conexión del sensor
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
        try:
            for message in self.consumer:
                if self.central_disconnected:  # Si la central está desconectada, deja de procesar mensajes
                    break
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
            logger.error(f"Lost connection to Kafka: {e}")
            self.central_disconnected = True  # Indicar que la central está desconectada
            self.continue_independently()  # Activar el modo independiente
        except Exception as e:
                logger.error(f"General error in kafka_listener: {e} {message.topic}")
                time.sleep(5)  

    def process_instruction(self, instruction):
        if instruction['type'] == 'MOVE':
            self.pickup = instruction['pickup']
            logger.info(f'PICK UP LOCATION = {self.pickup}')
            self.destination = instruction['destination']
            logger.info(f'DESTINATION LOCATION = {self.destination}')
            self.status = "BUSY"
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
        while self.position != self.pickup and self.color == "GREEN" and self.sensor_connected and self.picked_off == 0:
            logger.info("Taxi moving towards the pickup location")
            self.move_towards(self.pickup)

        if self.position == self.pickup:
            self.picked_off = 1

        while self.position != self.destination and self.color == "GREEN" and self.sensor_connected:
            logger.info("Taxi moving towards the destination location")
            self.move_towards(self.destination)

        if self.position == self.destination:
            self.color = "RED"
            self.status = "END"
            logger.info("Trip ENDED!!")
            self.send_position_update()
            
            self.customer_asigned = "x"
            time.sleep(4)
            self.picked_off = 0
            self.status = "FREE"
            logger.info("Taxi is now FREE")
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

        self.position[0] = self.position[0] % 21
        self.position[1] = self.position[1] % 21  
        
        self.send_position_update()
        time.sleep(2)  

    def send_position_update(self):
        # Verificar si el socket está abierto antes de enviar
        if self.sensor_socket and self.sensor_socket.fileno() != -1:
            update = {
                'taxi_id': self.taxi_id,
                'status': self.status,
                'color': self.color,
                'position': self.position,
                'customer_id': self.customer_asigned,
                'picked_off': self.picked_off
            }
            logger.info("Sending update through 'taxi_updates'")
            try:
                self.producer.send('taxi_updates', update)
            except KafkaError as e:
                logger.error(f"Error sending update: {e}")
        else:
            logger.warning("Socket is closed, cannot send update.")
        
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

    def process_map_update(self, map_data):
        """Actualiza el mapa local en el Digital Engine usando los datos recibidos."""
        # Limpiar el mapa local antes de cada actualización
        self.map = np.full(self.map_size, ' ', dtype=str)

        # Almacenar localizaciones en self.locations y colocarlas en el mapa
        self.locations = {loc_id: location for loc_id, location in map_data['locations'].items()}
        for loc_id, location in self.locations.items():
            x, y = location['position']
            x, y = x - 1, y - 1  # Ajustar índices a 0-19
            if 0 <= x < self.map_size[0] and 0 <= y < self.map_size[1]:
                self.map[y, x] = loc_id.ljust(2)

        # Almacenar información de los taxis en self.taxis y colocarlos en el mapa
        self.taxis = {taxi_id: taxi_info for taxi_id, taxi_info in map_data['taxis'].items()}
        for taxi_id, taxi_info in self.taxis.items():
            x, y = taxi_info['position']
            x, y = x - 1, y - 1
            if 0 <= x < self.map_size[0] and 0 <= y < self.map_size[1]:
                self.map[y, x] = str(taxi_id).ljust(2)

        # Colocar la posición del taxi actual
        new_x, new_y = self.position[0] - 1, self.position[1] - 1
        if 0 <= new_x < self.map_size[0] and 0 <= new_y < self.map_size[1]:
            self.map[new_y, new_x] = str(self.taxi_id).ljust(2)

        logger.info("Map updated in Digital Engine")
        self.draw_map()


    def draw_map(self, independent=False):
        """Dibuja el mapa en los logs del Digital Engine con estilo similar al de la central.
           Si `independent=True`, solo muestra el taxi propio y las localizaciones."""
        
        logger.info("Current Map State with Borders:")
        map_lines = [""]
        border_row = "#" * (self.map_size[1] * 2 + 2)
        map_lines.append(border_row)

        # Crear un mapa vacío con celdas de espacio formateado
        bordered_map = np.full((self.map_size[0], self.map_size[1]), '  ', dtype=str)

        # Colocar las localizaciones en el mapa
        for loc_id, location in self.locations.items():
            x, y = location['position']
            x, y = x - 1, y - 1  # Ajustar a índices de 0 a 19
            if 0 <= x < self.map_size[0] and 0 <= y < self.map_size[1]:
                bordered_map[y, x] = loc_id.ljust(2)

        # Colocar los taxis en el mapa solo si `independent` es False
        if not independent:
            for taxi_id, taxi_info in self.taxis.items():
                x, y = taxi_info['position']
                x, y = x - 1, y - 1
                if 0 <= x < self.map_size[0] and 0 <= y < self.map_size[1]:
                    bordered_map[y, x] = str(taxi_id).ljust(2)

        # Colocar la posición del propio taxi
        new_x, new_y = self.position[0] - 1, self.position[1] - 1
        if 0 <= new_x < self.map_size[0] and 0 <= new_y < self.map_size[1]:
            bordered_map[new_y, new_x] = str(self.taxi_id).ljust(2)

        # Crear filas del mapa formateadas con bordes
        for row in bordered_map:
            formatted_row = "#" + "".join([f"{cell} " for cell in row]) + "#"
            map_lines.append(formatted_row)

        # Borde inferior
        map_lines.append(border_row)

        # Mostrar el mapa en los logs
        logger.info("\n".join(map_lines))


    def continue_independently(self):
        logger.warning("Lost connection with EC_Central. Operating independently.")
        
        # Moverse hacia el destino utilizando la última instrucción recibida
        while self.position != self.destination:
            self.move_towards(self.destination)  # Mover el taxi paso a paso hacia el destino
            
            # Crear un mapa que solo contenga la posición del taxi y la localización de destino
            independent_map = np.full(self.map_size, '  ', dtype=str)
            
            # Añadir la localización de destino en el mapa
            dest_x, dest_y = self.destination[0] - 1, self.destination[1] - 1
            if 0 <= dest_x < self.map_size[0] and 0 <= dest_y < self.map_size[1]:
                independent_map[dest_y, dest_x] = 'D '.ljust(2)
            
            # Añadir la posición actual del taxi en el mapa
            taxi_x, taxi_y = self.position[0] - 1, self.position[1] - 1
            if 0 <= taxi_x < self.map_size[0] and 0 <= taxi_y < self.map_size[1]:
                independent_map[taxi_y, taxi_x] = str(self.taxi_id).ljust(2)

            # Mostrar el mapa en los logs solo con el propio taxi y la localización de destino
            map_lines = ["#" * (self.map_size[1] * 2 + 2)]
            for row in independent_map:
                formatted_row = "#" + "".join([f"{cell} " for cell in row]) + "#"
                map_lines.append(formatted_row)
            map_lines.append("#" * (self.map_size[1] * 2 + 2))
            
            logger.info("\n".join(map_lines))
            
            # Pausa antes del próximo movimiento
            time.sleep(2)

        # Una vez en el destino, mostrar mensaje final y detener actualizaciones
        logger.info("Taxi has reached its destination independently. No further instructions.")
        self.status = "END"




    def handle_sensor_disconnection(self):
        self.sensor_connected = False
        update = {
                'taxi_id': self.taxi_id,
                'status': "ERROR",
                'color': "RED",
                'position': self.position,
                'customer_id': self.customer_asigned,
                'picked_off': self.picked_off
            }
        logger.info("Sending update through 'taxi_updates'")
        try:
            self.producer.send('taxi_updates', update)
        except KafkaError as e:
            logger.error(f"Error sending update: {e}")

        logger.warning("Sensor disconnected. Taxi stopped, waiting for reconnection...")

    def handle_sensor_reconnection(self):
        self.sensor_connected = True  # Marca el sensor como reconectado
        logger.info("Sensor reconnected successfully.")
        
        # Si el taxi estaba en movimiento, reanudar su camino
        if self.color == "GREEN":
            logger.info("Resuming taxi's journey.")
            self.move_to_destination()
            
    def listen_for_sensor_data(self, conn, addr):
        logger.info(f"Connected to Sensors at {addr}")
        self.sensor_connected = True  # Marca el sensor como conectado

        while True:
            try:
                data = conn.recv(1024).decode()
                if not data:
                    self.handle_sensor_disconnection()
                    break

                if data == "KO" and self.color == "GREEN" and self.customer_asigned != "x":
                    self.color = "RED"
                    self.send_position_update()
                elif data == "OK" and self.color == "RED" and self.customer_asigned != "x":
                    self.color = "GREEN"
                    self.send_position_update()

            except (ConnectionResetError, ConnectionAbortedError) as e:
                logger.error(f"Connection error: {e}")
                self.handle_sensor_disconnection()
                break  # Salir del bucle y esperar la reconexión

        # Intentar reconectar en bucle si el sensor se ha desconectado
        while not self.sensor_connected:
            try:
                conn, addr = self.sensor_socket.accept()
                logger.info(f"Reconnecting to Sensor at {addr}")
                self.handle_sensor_reconnection()
            except Exception as e:
                logger.warning(f"Reconnection attempt failed: {e}")
                time.sleep(5)  # Espera antes de intentar reconectar


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


