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
    customer_asigned: int

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
        
        self.kafka_thread = threading.Thread(target=self.kafka_listener, daemon=True)
        self.kafka_thread.start()

    def load_map_config(self):
        try:
            with open('/data/map_config.txt', 'r') as f:
                for line in f:
                    loc_id, x, y = line.strip().split()
                    x, y = int(x), int(y)
                    self.locations[loc_id] = Location(loc_id, (x, y))
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
                    taxi_id, status, color, pos_x, pos_y, customer_asigned = line.strip().split('#')
                    taxis[int(taxi_id)] = Taxi(
                        id=int(taxi_id),
                        position=(int(pos_x), int(pos_y)),
                        status=status,
                        color=color,
                        customer_asigned=customer_asigned
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
                    f.write(f"{taxi.id}#{taxi.status}#{taxi.color}#{taxi.position[0]}#{taxi.position[1]}#{taxi.customer_asigned}\n")
            logger.info("Taxis saved to file")
            self.validate_taxis_file()
        except Exception as e:
            logger.error(f"Error saving taxis to file: {e}")

    def validate_taxis_file(self):
        """Valida el archivo de taxis para asegurarse de que no está corrupto."""
        try:
            with open(self.taxis_file, 'r') as f:
                for line in f:
                    parts = line.strip().split('#')
                    if len(parts) != 5:
                        raise ValueError(f"Invalid line in taxis file: {line}")
            logger.info("Taxis file validation successful")
        except Exception as e:
            logger.error(f"Error validating taxis file: {e}")

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
        self.producer.send('taxi_response', {
                'customer_id': taxi.customer_asigned,
                'status': "OK",
                'assigned_taxi': taxi.id
            })
        logger.info(f"Completed trip from taxi {taxi.id} for customer {taxi.customer_asigned}")
         
    
    def update_map(self, update):
            """
            Actualiza el estado del mapa según las actualizaciones de posición de los taxis.
            """
            taxi_id = update['taxi_id']
            pos_x, pos_y = update['position']
            status = update['status']
            color = update['color']
            customer_asigned = update['customer_id']

            # Si el taxi existe, actualizamos sus datos; si no, lo creamos
            if taxi_id in self.taxis:
                taxi = self.taxis[taxi_id]
                taxi.position = (pos_x, pos_y)
                taxi.status = status
                taxi.customer_asigned = customer_asigned
                if taxi.status == "END":
                    self.notify_customer(taxi)
                taxi.color = color
            else:
                logger.warning(f'There is no taxi with the id = {taxi_id}')
            
            # Redibuja el mapa en la interfaz y emite una transmisión con el mapa actualizado
            self.draw_map()
            self.broadcast_map()
   

    def draw_map(self):
        """Dibuja el mapa en los logs."""
        logger.info("Current Map State:")
        map_lines = []

        # Limpiar el mapa primero
        self.map.fill(' ')

        # Colocar las ubicaciones en el mapa
        for location in self.locations.values():
            x, y = location.position
            self.map[y, x] = location.id

        # Colocar los taxis en el mapa
        for taxi in self.taxis.values():
            x, y = taxi.position
            self.map[y, x] = taxi.id  # Usar el ID del taxi

        # Crear una representación en líneas
        for row in self.map:
            map_lines.append("".join(row))
        
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

    def connect_kafka(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            self.consumer = KafkaConsumer(
                'customer_requests', 'taxi_updates',
                bootstrap_servers=self.kafka_bootstrap_servers,
                group_id='central_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info("Successfully connected to Kafka")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False

    def process_customer_request(self, request):
        customer_id = request['customer_id']
        destination = request['destination']
        customer_location = request['customer_location']
        
        if customer_location:
            self.locations[f'customer_{customer_id}'] = Location(f'customer_{customer_id}', customer_location, 'YELLOW')
        
        if destination not in self.locations:
            logger.error(f"Invalid destination: {destination}")
            return False

        # Cargar los taxis desde el fichero
        taxis = self.load_taxis()

        # Selección del taxi: elige el primer taxi que esté libre ('FREE')
        available_taxi = next((taxi for taxi in taxis.values() if taxi.status == 'FREE'), None)
        
        if available_taxi:
            available_taxi.status = 'BUSY'  # Ahora está ocupado
            available_taxi.color = 'GREEN'  # En movimiento (ya que va a recoger al cliente)
            
            # Actualizar estado del taxi en el fichero
            self.save_taxis(taxis)

            # Enviar instrucciones al taxi
            #DEFINIR TODAS LAS INSTRUCCIONES
            self.producer.send('taxi_instructions', {
                'taxi_id': available_taxi.id,
                'instruction': 'MOVE',
                'pickup': self.locations[customer_location].position,
                'destination': self.locations[destination].position     #PRIMERO TIENE QUE IR A LA UBI, DESPUES A LA LOCATION (done)
            })
            logger.info(f"Assigned taxi {available_taxi.id} to customer {customer_id}")
            
            #Send confirmation back to customer
            # Prepare confirmation response
            response = {
                'customer_id': customer_id,
                'status': "OK",
                'assigned_taxi': available_taxi.id
            }
            
            try:
                self.producer.send('taxi_responses', response)
                self.producer.flush()
                self.logger.info(f"Sent confirmation to customer {customer_id}: {response}")
            except KafkaError as e:
                self.logger.error(f"Failed to send confirmation_ {e}")
            
            return True
        else:
            response = {
                'customer_id': customer_id,
                'status': "KO",
                'assigned_taxi': 0
            }
            logger.warning("No available taxis")
            return False

    def kafka_listener(self):
        """Hilo para escuchar los mensajes de Kafka."""
        for message in self.consumer:
            topic = message.topic
            data = message.value

            if topic == 'customer_requests':
                self.process_customer_request(data)
            elif topic == 'taxi_updates':
                self.update_map(data)

    def auto_broadcast_map(self):

        while True:
            self.broadcast_map()
            time.sleep(1)  # Espera 1 segundo antes de volver a enviar

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


    def run(self):
        if not self.connect_kafka():
            return

        self.load_map_config()
        self.load_taxis()
        logger.info("EC_Central is running...")
        
        self.draw_map()  # Dibujar el mapa inicial

        # Iniciar el hilo para la visualización del mapa
        map_broadcast_thread = threading.Thread(target=self.auto_broadcast_map, daemon=True)
        map_broadcast_thread.start()

        # Iniciar el hilo para escuchar mensajes Kafka
        kafka_thread = threading.Thread(target=self.kafka_listener, daemon=True)
        kafka_thread.start()

        # Iniciar el servidor de autenticación de taxis en un hilo separado
        auth_thread = threading.Thread(target=self.start_server_socket, daemon=True)
        auth_thread.start()

            
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python ec_central.py <kafka_bootstrap_servers> <listen_port>")
        sys.exit(1)

    kafka_bootstrap_servers = sys.argv[1]
    listen_port = int(sys.argv[2])
    central = ECCentral(kafka_bootstrap_servers, listen_port)
    central.run()
