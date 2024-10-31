import time
import logging
import socket
import sys
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError  # Asegúrate de importar KafkaError
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
    status: str  # 'FREE', 'BUSY', 'END'
    color: str  # 'RED' (parado) o 'GREEN' (en movimiento)
    position: Tuple[int, int]
    customer_asigned: str
    picked_off: int

@dataclass
class Location:
    id: str
    position: Tuple[int, int]
    color: str # 'BLUE' (localización) o 'YELLOW' (cliente)

class ECCentral:
    def __init__(self, kafka_bootstrap_servers, listen_port):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.listen_port = listen_port
        self.producer = None
        self.consumer = None
        self.map_size = (20, 20)
        self.map = np.full(self.map_size, ' ', dtype=str)
        self.locations: Dict[str, Location] = {}
        self.taxis_file = '/data/taxis.txt'  # Ruta al fichero de taxis
        self.taxis = {}  # Guardar taxis en un atributo
        self.locations = {}
        self.map_changed = False  # Estado para detectar cambios en el mapa
        self.setup_kafka()

    def setup_kafka(self):
        retry_count = 0
        while retry_count < 5:
            try:
                # Set up Kafka Producer
                self.producer = KafkaProducer(
                    bootstrap_servers=[self.kafka_bootstrap_servers],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=3
                )
                logger.info("Kafka producer set up successfully")

                # Set up Kafka Consumer for responses
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
            
            # Aquí puedes implementar la lógica de autenticación
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
            # Verificar que las claves esperadas estén presentes
            if not all(key in update for key in ['taxi_id', 'position', 'status', 'color', 'customer_id']):
                logger.error("Update message missing required fields.")
                return

            # Extraer y validar la posición
            taxi_id = update['taxi_id']
            position = update['position']
            
            if isinstance(position, (list, tuple)) and len(position) == 2:
                pos_x, pos_y = position
            else:
                logger.error(f"Invalid position format for taxi_id {taxi_id}: {position}")
                return
            
            # Asignar los demás valores
            status = update['status']
            color = update['color']
            customer_asigned = update['customer_id']
            picked_off = update['picked_off']
            # Actualizar el estado del taxi
            taxi_updated = self.update_taxi_state(taxi_id, pos_x, pos_y, status, color, customer_asigned, picked_off)
            logger.info(f"taxi_id = {taxi_updated.id}, tiene de customer a {taxi_updated.customer_asigned}")
            # Finalizar viaje y notificar al cliente, si es necesario
            self.finalize_trip_if_needed(taxi_updated)

            # Redibujar y emitir el mapa actualizado
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
            self.map_changed = True  # Marcar como cambiado
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
        map_lines = [""]  # Agrega una línea vacía al inicio

        # Crear el borde superior
        border_row = "#" * (self.map_size[1] + 2)
        map_lines.append(border_row)

        # Limpiar el mapa primero
        # Crear un nuevo mapa de tamaño (map_size[0] + 1, map_size[1] + 1) para incluir el borde
        bordered_map = np.full((self.map_size[0] + 1, self.map_size[1] + 1), ' ', dtype=str)

        # Colocar las ubicaciones en el mapa
        for location in self.locations.values():
            x, y = location.position
            bordered_map[y - 1, x - 1] = location.id  # Ajustar posición para que empiece en (1,1)

        # Colocar los taxis en el mapa
        for taxi in self.taxis.values():
            x, y = taxi.position
            bordered_map[y - 1, x - 1] = str(taxi.id)  # Ajustar posición para que empiece en (1,1)

        # Crear cada fila con delimitadores laterales
        for row in bordered_map:
            map_lines.append("#" + "".join(row) + "#")

        # Agregar el borde inferior
        map_lines.append(border_row)
        
        # Unir las líneas y registrarlas
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

        # Verifica si la ubicación del cliente es válida
        if customer_location:
            # Convierte la ubicación del cliente a tupla para evitar el error de tipo 'unhashable'
            location_key = tuple(customer_location)
            self.locations[customer_id] = Location(customer_id, location_key, 'YELLOW')
            self.map_changed = True  # Marcar como cambiado

        # Validación de la ubicación de destino
        if destination not in self.locations:
            logger.error(f"Invalid destination: {destination}")
            return False

        # Selección y asignación del taxi
        available_taxi = self.select_available_taxi()
        if available_taxi:
            self.assign_taxi_to_customer(available_taxi, customer_id, location_key, destination)
            self.map_changed = True  # Marcar como cambiado
            return True
        else:
            logger.warning("No available taxis")
            return False


    def select_available_taxi(self):
        """Selecciona el primer taxi disponible con estado 'FREE'."""
        self.taxis = self.load_taxis()  # Asegurarse de cargar el último estado de los taxis
        return next((taxi for taxi in self.taxis.values() if taxi.status == 'FREE'), None)

    def assign_taxi_to_customer(self, taxi, customer_id, customer_location, destination):
        """Asigna el taxi al cliente y envía instrucciones."""
        taxi.status = 'BUSY'
        taxi.color = 'GREEN'
        taxi.customer_asigned = customer_id

        # Guarda el nuevo estado del taxi en el archivo y envía instrucciones
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
                        self.update_map(data)

            except KafkaError as e:
                logger.error(f"Kafka listener error: {e}")
                self.setup_kafka()  # Reintentar la conexión
                time.sleep(5)
            except Exception as e:
                logger.error(f"General error in kafka_listener: {e} {message.topic}")
                time.sleep(5)  # Evitar cierre inmediato

    def auto_broadcast_map(self):
        """Envía el estado del mapa solo cuando ha habido cambios."""
        while True:
            if self.map_changed:

                self.broadcast_map()
                self.map_changed = False  # Restablecer el indicador después de transmitir
            time.sleep(1)  # Espera 1 segundo antes de verificar nuevamente


    def start_server_socket(self):
        """Configura el servidor de sockets y maneja la autenticación de taxis en un hilo separado."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('0.0.0.0', self.listen_port))
        self.server_socket.listen(5)  # Permitir hasta 5 conexiones en espera
        logger.info(f"Listening for taxi connections on port {self.listen_port}...")

        try:
            while True:
                conn, addr = self.server_socket.accept()
                # Crear un hilo para manejar la autenticación del taxi
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
                # Forzar el cierre del productor con un timeout
                self.producer.close(timeout=5.0)  # 5 segundos para cerrar
                logger.info("Kafka producer closed successfully.")
            except KafkaError as e:
                logger.error(f"Error closing Kafka producer: {e}")
            except Exception as e:
                logger.error(f"General error while closing Kafka producer: {e}")


    def run(self):
        self.load_map_config()
        self.load_taxis()
        logger.info("EC_Central is running...")
        
        self.draw_map()  # Dibujar el mapa inicial


        # Iniciar el servidor de autenticación de taxis en un hilo separado
        auth_thread = threading.Thread(target=self.start_server_socket, daemon=True)
        auth_thread.start()
        
        # Iniciar el hilo para escuchar mensajes Kafka
        threading.Thread(target=self.kafka_listener, daemon=True).start()
        
        # Iniciar el hilo para la visualización del mapa
        map_broadcast_thread = threading.Thread(target=self.auto_broadcast_map, daemon=True)
        map_broadcast_thread.start()
        
        try:
            # Código de ejecución principal
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
