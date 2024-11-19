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
    auth_status: int

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
        self.customer_destinations = {}
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
                    
        except Exception as e:
            logger.error(f"Error loading map configuration: {e}")

    def load_taxis(self):
        """Carga los taxis desde el fichero."""
        taxis = {}
        try:
            with open(self.taxis_file, 'r') as f:
                for line in f:
                    taxi_id, status, color, pos_x, pos_y, customer_asigned, picked_off, auth_status = line.strip().split('#')
                    taxis[int(taxi_id)] = Taxi(
                        id=int(taxi_id),
                        position=(int(pos_x), int(pos_y)),
                        status=status,
                        color=color,
                        customer_asigned=customer_asigned,
                        picked_off=picked_off,
                        auth_status=int(auth_status)
                    )

        except Exception as e:
            logger.error(f"Error loading taxis from file: {e}")
        return taxis

    def save_taxis(self, taxis):
        """Guarda los taxis en el fichero."""
        try:
            with open(self.taxis_file, 'w') as f:
                for taxi in taxis.values():
                    f.write(f"{taxi.id}#{taxi.status}#{taxi.color}#{taxi.position[0]}#{taxi.position[1]}#{taxi.customer_asigned}#{taxi.picked_off}#{taxi.auth_status}\n")
        except Exception as e:
            logger.error(f"Error saving taxis to file: {e}")

    def handle_taxi_auth(self, conn, addr):
        """Maneja la autenticación del taxi."""
        logger.info(f"Connection from taxi at {addr}")

        try:
            data = conn.recv(1024).decode('utf-8')
            taxi_id = int(data.strip())

            # Asegurar que el taxi existe en self.taxis
            if taxi_id not in self.taxis:
                self.taxis[taxi_id] = Taxi(
                    id=taxi_id,
                    status="KO",
                    color="RED",
                    position=(1, 1),
                    customer_asigned="x",
                    picked_off=0,
                    auth_status=0  # No autenticado inicialmente
                )

            # Actualizar el estado del taxi autenticado
            taxi = self.taxis[taxi_id]
            taxi.status = "FREE"
            taxi.color = "RED"
            taxi.position = (1, 1)
            taxi.customer_asigned = "x"
            taxi.picked_off = 0
            taxi.auth_status = 1
            self.save_taxis(self.taxis)

            conn.sendall(b"OK")
            logger.info(f"Taxi {taxi_id} authenticated successfully.")
            self.listen_to_taxi(taxi_id, conn)

        except Exception as e:
            logger.error(f"Error during taxi authentication: {e}")
        finally:
            conn.close()

    def listen_to_taxi(self, taxi_id, conn):
        """Escucha mensajes de un taxi autenticado."""
        while True:
            try:
                data = conn.recv(1024).decode()
                if data == "":
                    logger.info(f"Taxi {taxi_id} has disconnected. Marking as KO.")
                    if taxi_id in self.taxis:
                        taxi = self.taxis[taxi_id]
                        taxi.status = "KO"
                        taxi.auth_status = 1  # Mantener autenticación
                        self.save_taxis(self.taxis)
                        
                        # Notificar al cliente si hay uno asignado
                        if taxi.customer_asigned != "x":
                            self.notify_customer(taxi)

                    # Esperar 10 segundos antes de considerarlo una incidencia permanente
                    time.sleep(10)
                    if self.taxis[taxi_id].status == "DOWN":
                        logger.info(f"Marking taxi {taxi_id} as inactive on the map.")
                    break

            except (ConnectionResetError, ConnectionAbortedError) as e:
                logger.error(f"Connection error with taxi {taxi_id}: {e}")
                break

        conn.close()
        logger.info(f"Connection closed for taxi {taxi_id}")

    
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
            self.finalize_trip_if_needed(taxi_updated)
            self.map_changed= True
        
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
        
    def generate_table(self):
        """Genera la tabla de estado de taxis y clientes."""
        table_lines = []
        table_lines.append("               *** EASY CAB ***        ")
        table_lines.append("     TAXIS                           CLIENTES   ")
        table_lines.append(f"{'Id':<4}{'Destino':<10}{'Estado':<15}   {'Id':<4}{'Destino':<10}{'Estado':<15}")

        taxi_lines = []
        for taxi in self.taxis.values():
            if taxi.auth_status != 1:  # Solo mostrar taxis autenticados
                continue
            taxi_id = taxi.id
            # Obtener la destinación del cliente asignado si existe
            if taxi.customer_asigned != "x" and taxi.customer_asigned in self.customer_destinations:
                destination = self.customer_destinations[taxi.customer_asigned]
            else:
                destination = "-"

            # Determinar el estado del taxi
            if taxi.status in ["KO", "SENSOR", "ERROR", "DOWN"]:
                state = "KO. Parado"
            elif taxi.customer_asigned == "x":
                state = "OK. Libre"
            else:
                state = f"OK. Servicio {taxi.customer_asigned}"

            taxi_lines.append(f"{taxi_id:<4}{destination:<10}{state:<15}")

        client_lines = []
        for customer_id, destination in self.customer_destinations.items():
            assigned_taxi = next((taxi.id for taxi in self.taxis.values() if taxi.customer_asigned == customer_id), None)
            if assigned_taxi is None or assigned_taxi not in self.taxis:
                state = "Esperando"
            elif self.taxis[assigned_taxi].status == "DOWN":
                state = f"KO. Taxi averiado {assigned_taxi}"
            elif self.taxis[assigned_taxi].status in ["KO", "SENSOR", "ERROR"]:
                state = "KO. Parado"
            elif self.taxis[assigned_taxi].picked_off == 0:
                state = f"OK. Esperando a Taxi {assigned_taxi}"
            else:
                state = f"OK. Taxi {assigned_taxi}"

            
            client_lines.append(f"{customer_id:<4}{destination:<10}{state:<15}")

        max_lines = max(len(taxi_lines), len(client_lines))
        for i in range(max_lines):
            taxi_info = taxi_lines[i] if i < len(taxi_lines) else " " * 30
            client_info = client_lines[i] if i < len(client_lines) else ""
            table_lines.append(f"{taxi_info} | {client_info}")
        
        return "\n".join(table_lines)


        
    def draw_map(self):
        """Dibuja el mapa en los logs con delimitación de bordes."""
        table = self.generate_table()
        
        map_lines = [""]  # Encabezado vacío para estilo
        border_row = "#" * (self.map_size[1] * 2 + 2)  # Bordes del mapa
        map_lines.append(border_row)

        # Crear un mapa vacío con celdas formateadas
        bordered_map = np.full((self.map_size[0], self.map_size[1]), ' ', dtype=str)

        # Colocar las localizaciones en el mapa
        for location in self.locations.values():
            x, y = location.position
            bordered_map[y - 1, x - 1] = location.id.ljust(2)

        # Colocar los taxis autenticados en el mapa
        for taxi in self.taxis.values():
            if taxi.auth_status != 1:  # Solo mostrar taxis autenticados
                continue
            x, y = taxi.position
            if 1 <= x <= self.map_size[1] and 1 <= y <= self.map_size[0]:  # Verificar límites
                if taxi.status == "DOWN":
                    bordered_map[y - 1, x - 1] = "X "  # Marcar incidencia
                elif taxi.auth_status == 1:
                    bordered_map[y - 1, x - 1] = str(taxi.id).ljust(2)

        # Construir las filas del mapa
        for row in bordered_map:
            formatted_row = "#" + "".join([f"{cell} " if cell != " " else "  " for cell in row]) + "#"
            map_lines.append(formatted_row)

        map_lines.append(border_row)

        # Integrar mapa y tabla
        half_height = len(map_lines) // 2 - 6
        table_lines = [""] * half_height + table.splitlines()

        max_lines = max(len(map_lines), len(table_lines))
        for i in range(max_lines):
            map_row = map_lines[i] if i < len(map_lines) else ""
            table_row = table_lines[i] if i < len(table_lines) else ""
            logger.info(f"{map_row:<45} |   {table_row}")



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
            except KafkaError as e:
                logger.error(f"Error broadcasting map: {e}")

    def process_customer_request(self, request):
        customer_id = request['customer_id']
        destination = request['destination']
        customer_location = request['customer_location']
        
        self.customer_destinations[customer_id] = destination

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
            'status': "SENSOR",
            'assigned_taxi': data['taxi_id']
            }

            try:
                self.producer.send('taxi_responses', notification)
                self.producer.flush()
                logger.info(f"The taxi sensor for the customer '{customer_id}' stopped working, notifying the customer: {notification}")
            except KafkaError as e:
                logger.error(f"Failed to send confirmation to customer {customer_id}: {e}")
            
            self.update_map(data)
        else:
            self.update_map(data)


    def kafka_listener(self):
        while True:
            try:
                for message in self.consumer:
                    if message.topic == 'taxi_requests':
                        data = message.value
                        self.process_customer_request(data)
                        
                    elif message.topic == 'taxi_updates':
                        data = message.value
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
                time.sleep(1)
                self.draw_map()
                self.broadcast_map()
                self.map_changed = False  
            time.sleep(0.1)  


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


# ^C
# 

