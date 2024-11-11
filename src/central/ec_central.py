import time
import logging
import socket
import sys
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError  
import json
import numpy as np
from dataclasses import dataclass
from typing import Dict, Tuple
import threading


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Taxi:
    id: int
    status: str  
    color: str  
    position: Tuple[int, int]
    customer_asigned: str
    picked_off: int

@dataclass
class Location:
    id: str
    position: Tuple[int, int]
    color: str 

class ECCentral:
    def __init__(self, kafka_bootstrap_servers, listen_port):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.listen_port = listen_port
        self.producer = None
        self.consumer = None
        self.map_size = (20, 20)
        self.map = np.full(self.map_size, ' ', dtype=str)
        self.locations: Dict[str, Location] = {}
        self.taxis_file = '/data/taxis.txt'  
        self.taxis = {}  
        self.locations = {}
        self.map_changed = False  
        self.setup_kafka()

    def setup_kafka(self):
        retry_count = 0
        while retry_count < 5:
            try:
                
                self.producer = KafkaProducer(
                    bootstrap_servers=[self.kafka_bootstrap_servers],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=3
                )
                logger.info("Kafka producer set up successfully")

               
                self.consumer = KafkaConsumer(
                    'taxi_updates','taxi_requests',
                    bootstrap_servers=[self.kafka_bootstrap_servers],
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    group_id='central-group',
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


    def load_map_config(self):
        try:
            with open('/data/map_config.txt', 'r') as f:
                for line in f:
                    loc_id, x, y = line.strip().split()
                    x, y = int(x), int(y)
                    self.locations[loc_id] = Location(loc_id, (x, y),"BLUE")
                    self.map[y, x] = loc_id
                    
            logger.info("Map configuration loaded successfully")
        except Exception as e:
            logger.error(f"Error loading map configuration: {e}")

    def load_taxis(self):
        """Carga los taxis desde el fichero."""
        taxis = {}
        try:
            with open(self.taxis_file, 'r') as f:
                for line in f:
                    taxi_id, status, color, pos_x, pos_y, customer_asigned, picked_off = line.strip().split('#')
                    taxis[int(taxi_id)] = Taxi(
                        id=int(taxi_id),
                        position=(int(pos_x), int(pos_y)),
                        status=status,
                        color=color,
                        customer_asigned=customer_asigned,
                        picked_off=picked_off
                    )
            logger.info("Taxis loaded from file")
        except Exception as e:
            logger.error(f"Error loading taxis from file: {e}")
        return taxis

    def save_taxis(self, taxis):
        """Guarda los taxis en el fichero."""
        try:
            with open(self.taxis_file, 'w') as f:
                for taxi in taxis.values():
                    f.write(f"{taxi.id}#{taxi.status}#{taxi.color}#{taxi.position[0]}#{taxi.position[1]}#{taxi.customer_asigned}#{taxi.picked_off}\n")
            logger.info("Taxis saved to file")
        except Exception as e:
            logger.error(f"Error saving taxis to file: {e}")

    def handle_taxi_auth(self, conn, addr):
        """Maneja la autenticación del taxi."""
        logger.info(f"Connection from taxi at {addr}")

        try:
            data = conn.recv(1024).decode('utf-8')
            taxi_id = int(data.strip())
            logger.info(f"Authenticating taxi with ID: {taxi_id}")
            
            taxis = self.load_taxis()
            if taxi_id in taxis:
                conn.sendall(b"OK")
                logger.info(f"Taxi {taxi_id} authenticated successfully.")
            else:
                conn.sendall(b"KO")
                logger.warning(f"Taxi {taxi_id} authentication failed.")
        except Exception as e:
            logger.error(f"Error during taxi authentication: {e}")
        finally:
            conn.close()
    
    def notify_customer(self, taxi):
        customer_id = str(taxi.customer_asigned) if isinstance(taxi.customer_asigned, list) else taxi.customer_asigned
        response = {
            'customer_id': customer_id,
            'status': "END",
            'assigned_taxi': taxi.id,
            'final_position': taxi.position
        }
        try:
            self.producer.send('taxi_responses', response)
            self.producer.flush()
            logger.info(f"Trip completed sending to customer {customer_id}: {response}")
        except KafkaError as e:
            logger.error(f"Failed to send confirmation to customer {customer_id}: {e}")

    
    def update_map(self, update):
        try:
            if not all(key in update for key in ['taxi_id', 'position', 'status', 'color', 'customer_id']):
                logger.error("Update message missing required fields.")
                return

            taxi_id = update['taxi_id']
            position = update['position']
            
            if isinstance(position, (list, tuple)) and len(position) == 2:
                pos_x, pos_y = position
            else:
                logger.error(f"Invalid position format for taxi_id {taxi_id}: {position}")
                return
            
            status = update['status']
            color = update['color']
            customer_asigned = update['customer_id']
            picked_off = update['picked_off']
            taxi_updated = self.update_taxi_state(taxi_id, pos_x, pos_y, status, color, customer_asigned, picked_off)
            logger.info(f"taxi_id = {taxi_updated.id}, tiene de customer a {taxi_updated.customer_asigned}")
            self.finalize_trip_if_needed(taxi_updated)

            self.redraw_map_and_broadcast()
        
        except KeyError as e:
            logger.error(f"Key error when processing update: missing key {e}")
        except Exception as e:
            logger.error(f"Error in update_map: {e}")


    def update_taxi_state(self, taxi_id, pos_x, pos_y, status, color, customer_asigned, picked_off):
        """Actualiza la información del taxi en el sistema."""
        if taxi_id in self.taxis:
            taxi = self.taxis[taxi_id]
            taxi.position = (pos_x, pos_y)
            taxi.status = status
            taxi.color = color
            taxi.customer_asigned = customer_asigned
            taxi.picked_off = picked_off
            self.map_changed = True  
            self.save_taxis(self.taxis)
            if picked_off==1:
                self.locations[customer_asigned].position = taxi.position 
            return taxi
        else:
            logger.warning(f"No taxi found with id {taxi_id}")
            return None

    def finalize_trip_if_needed(self, taxi):
        """Notifica al cliente si el taxi ha finalizado el viaje."""
        if taxi.status == "END":
            self.save_taxis(self.taxis)
            self.notify_customer(taxi)

    def redraw_map_and_broadcast(self):
        """Redibuja el mapa y lo envía a todos los taxis."""
        self.draw_map()
        self.broadcast_map()


    def draw_map(self):
        """Dibuja el mapa en los logs con delimitación de bordes, donde (0,0) no se representa."""
        logger.info("Current Map State with Borders:")
        map_lines = [""]

        # Ajuste del borde superior e inferior
        border_row = "#" * (self.map_size[1] * 2 + 2)  # Duplicamos el ancho para una apariencia cuadrada
        map_lines.append(border_row)

        # Crear un mapa con bordes y celdas vacías
        bordered_map = np.full((self.map_size[0], self.map_size[1]), ' ', dtype=str)

        # Colocar las localizaciones en el mapa
        for location in self.locations.values():
            x, y = location.position
            bordered_map[y - 1, x - 1] = location.id

        # Colocar los taxis en el mapa
        for taxi in self.taxis.values():
            x, y = taxi.position
            bordered_map[y - 1, x - 1] = str(taxi.id)

        # Dibujar cada fila del mapa con espacio adicional para cuadrar
        for row in bordered_map:
            formatted_row = "#"
            for cell in row:
                if cell == ' ':
                    formatted_row += "  "  # Dos espacios para uniformidad
                else:
                    formatted_row += f"{cell} "  # Elemento con espacio adicional
            formatted_row += "#"
            map_lines.append(formatted_row)

        map_lines.append(border_row)  # Borde inferior

        # Imprimir el mapa completo
        logger.info("\n".join(map_lines))

    def broadcast_map(self):
        """
        Envía el estado actual del mapa a todos los taxis a través del tópico 'map_updates'.
        """
        if self.producer:
            try:
                map_data = {
                    'map': self.map.tolist(),
                    'taxis': {k: {'position': v.position, 'status': v.status, 'color': v.color} 
                                for k, v in self.taxis.items()},
                    'locations': {k: {'position': v.position, 'color': v.color}
                                    for k, v in self.locations.items()}
                }
                self.producer.send('map_updates', map_data)
                logger.info("Broadcasted map to all taxis")
            except KafkaError as e:
                logger.error(f"Error broadcasting map: {e}")

    def process_customer_request(self, request):
        customer_id = request['customer_id']
        destination = request['destination']
        customer_location = request['customer_location']

        if customer_location:
            location_key = tuple(customer_location)
            self.locations[customer_id] = Location(customer_id, location_key, 'YELLOW')
            self.map_changed = True  

        if destination not in self.locations:
            logger.error(f"Invalid destination: {destination}")
            return False

        available_taxi = self.select_available_taxi()
        if available_taxi and available_taxi.status == "FREE":
            self.assign_taxi_to_customer(available_taxi, customer_id, location_key, destination)
            self.map_changed = True  
            return True
        else:
            logger.warning("No available taxis")
            return False


    def select_available_taxi(self):
        """Selecciona el primer taxi disponible con estado 'FREE'."""
        self.taxis = self.load_taxis()

        # Log para comprobar el estado de todos los taxis antes de seleccionar
        for taxi_id, taxi in self.taxis.items():
            logger.info(f"Taxi {taxi_id} - Estado: {taxi.status}, Posición: {taxi.position}, Cliente asignado: {taxi.customer_asigned}")

        # Selección del primer taxi disponible
        available_taxi = next((taxi for taxi in self.taxis.values() if taxi.status == 'FREE' and taxi.customer_asigned == "x"), None)

        # Log adicional para saber si se encontró un taxi disponible
        if available_taxi:
            logger.info(f"Taxi disponible encontrado: {available_taxi.id}")
        else:
            logger.warning("No se encontró ningún taxi disponible.")

        return available_taxi


    def assign_taxi_to_customer(self, taxi, customer_id, customer_location, destination):
        """Asigna el taxi al cliente y envía instrucciones."""
        taxi.status = 'BUSY'
        taxi.color = 'GREEN'
        taxi.customer_asigned = customer_id

        self.save_taxis(self.taxis)
        self.notify_customer_assignment(customer_id, taxi)
        self.send_taxi_instruction(taxi, customer_id, customer_location, destination)

    def send_taxi_instruction(self, taxi, customer_id, pickup_location, destination):
        """Envía instrucciones al taxi para recoger al cliente y llevarlo al destino."""
        pickup_position = self.locations[pickup_location].position if isinstance(pickup_location, str) else pickup_location
        destination_position = self.locations[destination].position if isinstance(destination, str) else destination
        logger.info(f'Pickup_position = {pickup_position}, destination_position = {destination_position}')
    
        instruction = {
            'taxi_id': taxi.id,
            'type': 'MOVE',
            'pickup': pickup_position,
            'destination': destination_position,
            'customer_id': customer_id
        }
        self.producer.send('taxi_instructions', instruction)
        logger.info(f"Instructions sent to taxi {taxi.id} for customer {customer_id}")

    def notify_customer_assignment(self, customer_id, taxi):
        """Envía una respuesta al cliente confirmando la asignación del taxi."""
        response = {
            'customer_id': customer_id,
            'status': "OK",
            'assigned_taxi': taxi.id
        }
        try:
            self.producer.send('taxi_responses', response)
            self.producer.flush()
            logger.info(f"Confirmation sent to customer {customer_id}: {response}")
        except KafkaError as e:
            logger.error(f"Failed to send confirmation to customer {customer_id}: {e}")

    def process_update(self, data):
        if data['status'] == "ERROR":
            customer_id = data['customer_id']
            notification = {
            'customer_id': customer_id,
            'status': "ERROR",
            'assigned_taxi': data['taxi_id']
            }
            try:
                self.producer.send('taxi_responses', notification)
                self.producer.flush()
                logger.info(f"The taxi sensor for the customer '{customer_id}' stopped working, notifying the customer: {notification}")
            except KafkaError as e:
                logger.error(f"Failed to send confirmation to customer {customer_id}: {e}")
        else:
            self.update_map(data)


    def kafka_listener(self):
        while True:
            try:
                for message in self.consumer:
                    if message.topic == 'taxi_requests':
                        data = message.value
                        logger.info(f"Received message on topic 'taxi_requests': {data}")
                        self.process_customer_request(data)
                        
                    elif message.topic == 'taxi_updates':
                        data = message.value
                        logger.info(f"Received message on topic 'taxi_updates': {data}")
                        self.process_update(data)

            except KafkaError as e:
                logger.error(f"Kafka listener error: {e}")
                self.setup_kafka() 
                time.sleep(5)
            except Exception as e:
                logger.error(f"General error in kafka_listener: {e} {message.topic}")
                time.sleep(5) 

    def auto_broadcast_map(self):
        """Envía el estado del mapa solo cuando ha habido cambios."""
        while True:
            if self.map_changed:

                self.broadcast_map()
                self.map_changed = False  
            time.sleep(1)  


    def start_server_socket(self):
        """Configura el servidor de sockets y maneja la autenticación de taxis en un hilo separado."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('0.0.0.0', self.listen_port))
        self.server_socket.listen(5)  
        logger.info(f"Listening for taxi connections on port {self.listen_port}...")

        try:
            while True:
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self.handle_taxi_auth, args=(conn, addr), daemon=True).start()
        except Exception as e:
            logger.error(f"Error in start_server_socket: {e}")
        finally:
            if self.server_socket:
                self.server_socket.close()
                
    def close_producer(self):
        """Cierra el productor de Kafka con timeout."""
        if self.producer:
            try:
                self.producer.close(timeout=5.0)  
                logger.info("Kafka producer closed successfully.")
            except KafkaError as e:
                logger.error(f"Error closing Kafka producer: {e}")
            except Exception as e:
                logger.error(f"General error while closing Kafka producer: {e}")


    def run(self):
        self.load_map_config()
        self.load_taxis()
        logger.info("EC_Central is running...")
        
        self.draw_map()  


        auth_thread = threading.Thread(target=self.start_server_socket, daemon=True)
        auth_thread.start()
        
        threading.Thread(target=self.kafka_listener, daemon=True).start()
        
        map_broadcast_thread = threading.Thread(target=self.auto_broadcast_map, daemon=True)
        map_broadcast_thread.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.close_producer()
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed.")


            
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python ec_central.py <kafka_bootstrap_servers> <listen_port>")
        sys.exit(1)

    kafka_bootstrap_servers = sys.argv[1]
    listen_port = int(sys.argv[2])
    central = ECCentral(kafka_bootstrap_servers, listen_port)
    central.run()

