import time
import logging
import socket
import sys
import uuid
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError  
import json
import numpy as np
from dataclasses import asdict, dataclass
from typing import Dict, Tuple
import threading
import ssl
import requests
from requests.exceptions import RequestException
import queue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Taxi:
    id: int
    status: str  #FREE, BUSY, END, ERROR
    color: str  #GREEN, RED
    position: Tuple[int, int]
    customer_assigned: str
    picked_off: int
    auth_status: int
    token: str

@dataclass
class Customer:
    id: str
    status: str  #UNATTENDED, WAITING, TRANSIT, SERVICED, 
    position: Tuple[int, int]
    destination: str
    taxi_assigned: int
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
        self.taxis_file = '/data/taxis.json'
        self.customers_file = '/data/customers.json'
        self.taxis: Dict[int, Taxi] = {}  
        self.customers: Dict[int, Customer] = {}
        self.map_changed = False  
        self.assign_taxi_lock = threading.Lock()
        self.setup_kafka()
        self.requests_q = queue.Queue()
        self.updates_q = queue.Queue()
        self.ctc_url = "http://ctc:5000/api/traffic-status"
        self.running = True
        self.current_status = "OK"
        self.write_queue = queue.Queue()
        self.traffic_state = 1 #A 1 ES QUE SE PUEDE CIRCULAR

    def setup_kafka(self):
        retry_count = 0
        while retry_count < 5:
            try:
                
                self.producer = KafkaProducer(
                    bootstrap_servers=[self.kafka_bootstrap_servers],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=3,
                    security_protocol='SSL',
                    ssl_cafile='./kafka_certs/ca.crt',
                    ssl_certfile='./kafka_certs/kafka.crt',
                    ssl_keyfile='./kafka_certs/kafka.key'
                )
                logger.info("Kafka producer set up successfully")

               
                self.consumer = KafkaConsumer(
                    'taxi_updates','taxi_requests',
                    bootstrap_servers=[self.kafka_bootstrap_servers],
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    group_id='central-group',
                    auto_offset_reset='latest',
                    security_protocol='SSL',                # Protocolo SSL
                    ssl_cafile='./kafka_certs/ca.crt',  # Certificado de la autoridad certificadora
                    ssl_certfile='./kafka_certs/kafka.crt',  # Certificado del cliente
                    ssl_keyfile='./kafka_certs/kafka.key',    # Clave privada del cliente
                    session_timeout_ms=30000,  # Incrementa el timeout de sesión
                    request_timeout_ms=40000  # Incrementa el timeout para solicitudes
                )
                logger.info("Kafka consumer set up successfully")
                #self.log_audit("KAFKA", "Kafka setted up successfully")
                return
            except KafkaError as e:
                retry_count += 1
                logger.error(f"Failed to set up Kafka: {e}. Retrying in 5 seconds...")
                #self.log_audit("KAFKA", "Failed to set up Kafka")
                time.sleep(5)
        
        logger.critical("Failed to set up Kafka after 5 attempts. Exiting.")
        sys.exit(1)


    def load_map_config(self):
        try:
            with open('/data/map_config.json', 'r') as f:
                config = json.load(f)
            for loc_id, loc_data in config.items():
                x, y = loc_data['position']
                self.locations[loc_id] = Location(loc_id, (x, y), "BLUE")
                self.map[y - 1, x - 1] = loc_id
            logger.info("Map configuration loaded successfully.")
            #self.log_audit("MAP_LOAD", "Map configuration loaded successfully")

        except Exception as e:
            logger.error(f"Error loading map configuration: {e}")
            #self.log_audit("MAP_ERROR", f"Error loading map configuration: {e}")

    def load_taxis(self):
        """Carga los taxis desde el archivo JSON, soportando formato lista o diccionario."""
        self.taxis = {}  # Reiniciar el diccionario
        try:
            with open(self.taxis_file, 'r') as f:
                content = f.read().strip()
                if not content:
                    logger.warning("Taxis JSON file is empty. Starting with an empty dictionary.")
                    return

                taxi_data = json.loads(content)
                if isinstance(taxi_data, dict):
                    for taxi_id, taxi_info in taxi_data.items():
                        self.taxis[int(taxi_id)] = Taxi(
                            id=taxi_info["id"],
                            status=taxi_info["status"],
                            color=taxi_info["color"],
                            position=tuple(taxi_info["position"]),
                            customer_assigned=taxi_info["customer_assigned"],
                            picked_off=int(taxi_info["picked_off"]),
                            auth_status=int(taxi_info["authenticated"]),
                            token=taxi_info["token"]
                        )
                    #self.log_audit("Taxis", "Se han cargado los taxis desde el archivo")
                else:
                    raise ValueError("Unexpected JSON format. Must be a list or dictionary.")
        except FileNotFoundError:
            logger.warning("Taxis JSON file not found. Starting with an empty dictionary.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}. Starting with an empty dictionary.")
        except Exception as e:
            logger.error(f"Error loading taxis from JSON file: {e}")
            #self.log_audit("Taxis", "Error al cargar los taxis desde el archivo")



    def save_taxis(self):
        """Guarda los taxis desde el diccionario interno al archivo JSON en formato diccionario."""
        taxi_dict = {
            str(taxi.id): {
                "id": taxi.id,
                "status": taxi.status,
                "color": taxi.color,
                "position": list(taxi.position),
                "customer_assigned": taxi.customer_assigned,
                "picked_off": int(taxi.picked_off),
                "authenticated": int(taxi.auth_status),
                "token": str(taxi.token)
            }
            for taxi in self.taxis.values()
        }
        self.write_queue.put((self.taxis_file,taxi_dict))


    def load_customers(self):
        """Carga los taxis desde el archivo JSON, soportando formato lista o diccionario."""
        self.customers = {}  # Reiniciar el diccionario
        try:
            with open(self.customers_file, 'r') as f:
                content = f.read().strip()
                if not content:
                    logger.warning("Taxis JSON file is empty. Starting with an empty dictionary.")
                    return

                customer_data = json.loads(content)
                if isinstance(customer_data, dict):
                    for customer_id, customer_info in customer_data.items():
                        self.customers[int(customer_id)] = Customer(
                            id=customer_info["id"],
                            status=customer_info["status"],
                            position=tuple(customer_info["position"]),
                            destination=customer_info["destination"],
                            taxi_assigned=customer_info["taxi_assigned"],
                            picked_off=int(customer_info["picked_off"]),
                        )
                    #self.log_audit("Customers", "Se han cargado los customers desde el archivo")
                else:
                    raise ValueError("Unexpected JSON format. Must be a list or dictionary.")
        except FileNotFoundError:
            logger.warning("Customers JSON file not found. Starting with an empty dictionary.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}. Starting with an empty dictionary.")
        except Exception as e:
            logger.error(f"Error loading customers from JSON file: {e}")
            #self.log_audit("Customers", f"Error loading customers from JSON file: {e}")



    def save_customers(self):
        """Guarda los customers desde el diccionario interno al archivo JSON en formato diccionario."""
        customer_dict = {
            customer.id: {
                "id": customer.id,
                "status": customer.status,
                "position": list(customer.position),
                "destination": customer.destination,
                "taxi_assigned": int(customer.taxi_assigned),
                "picked_off": int(customer.picked_off)
            }
            for customer in self.customers.values()
        }
        self.write_queue.put((self.customers_file,customer_dict))

    def load_single_taxi(self, taxi_id):
        """Carga un taxi específico desde taxis.json."""
        try:
            with open(self.taxis_file, 'r') as f:
                taxi_data = json.load(f)
            if str(taxi_id) in taxi_data:
                taxi_info = taxi_data[str(taxi_id)]
                return Taxi(
                    id=taxi_info["id"],
                    status=taxi_info["status"],
                    color=taxi_info["color"],
                    position=tuple(taxi_info["position"]),
                    customer_assigned=taxi_info["customer_assigned"],
                    picked_off=int(taxi_info["picked_off"]),
                    auth_status=int(taxi_info["authenticated"]),
                    token=taxi_info["token"]
                )
            else:
                return None
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error loading taxi {taxi_id} from database: {e}")
            return None


    def handle_taxi_auth(self, conn, addr):
        """Maneja la autenticación del taxi."""
        logger.info(f"Secure connection from taxi at {addr}")
        try:
            data = conn.recv(1024).decode('utf-8')
            taxi_id = int(data.strip())

            # Verificar si el taxi existe en memoria
            if taxi_id not in self.taxis:
                taxi_info = self.load_single_taxi(taxi_id)
                if taxi_info:
                    self.taxis[taxi_id] = taxi_info
                else:
                    conn.sendall(b"NOT_FOUND")
                    return

            # Generar token y actualizar estado del taxi
            taxi = self.taxis[taxi_id]
            token = str(uuid.uuid4())
            taxi.status = "FREE"
            taxi.color = "RED"
            taxi.position = (1, 1)
            taxi.customer_assigned = "x"
            taxi.picked_off = 0
            taxi.auth_status = 1
            taxi.token = token

            self.save_taxis()  # Guardar cambios en taxis.json
            conn.sendall(f"TOKEN {token}".encode('utf-8'))
            logger.info(f"Taxi {taxi_id} authenticated successfully with token.")
            #self.log_audit("Taxis", f"Taxi {taxi_id} authenticated successfully with token.")
            self.listen_to_taxi(taxi_id, conn)

        except Exception as e:
            logger.error(f"Error during taxi authentication: {e}")
            #self.log_audit("Taxis", f"Error during taxi authentication: {e}")
        finally:
            conn.close()


    def listen_to_taxi(self, taxi_id, conn):
        """Escucha mensajes de un taxi autenticado."""
        while True:
            try:
                data = conn.recv(1024).decode()
                if data == "":
                    logger.info(f"Taxi {taxi_id} has disconnected. Marking as KO.")
                    #self.log_audit("Taxis", f"Taxi {taxi_id} has disconnected. Marking as KO.")
                    if taxi_id in self.taxis:
                        taxi = self.taxis[taxi_id]
                        self.locations[taxi_id].id = "X"
                        taxi.status = "KO"
                        taxi.auth_status = 1  # Mantener autenticación
                        self.save_taxis()
                        self.map_changed = True
                        
                        # Notificar al cliente si hay uno asignado
                        if taxi.customer_assigned != "x":
                            self.notify_customer(taxi)

                    # Esperar 10 segundos antes de considerarlo una incidencia permanente
                    time.sleep(10)
                    del self.taxis[taxi_id]
                    del self.locations[taxi_id]
                    self.save_taxis()
                    self.map_changed = True
                    logger.info(f"Marking taxi {taxi_id} as inactive on the map.")
                    break

            except (ConnectionResetError, ConnectionAbortedError) as e:
                logger.error(f"Connection error with taxi {taxi_id}: {e}")
                break

        conn.close()
        logger.info(f"Connection closed for taxi {taxi_id}")

    
    def notify_customer(self, taxi):
        try:
            response = {
                'customer_id': taxi.customer_assigned,
                'status': "END",
                'assigned_taxi': taxi.id,
                'final_position': taxi.position
            }
            self.producer.send('taxi_responses', response) 
            self.producer.flush()  # Asegura el envío inmediato

            logger.info(f"Trip completed sending to customer {taxi.customer_assigned}: {response}")
            #self.log_audit(f"Trip completed sending to customer {taxi.customer_assigned}: {response}")
        except KafkaError as e:
            logger.error(f"Failed to notify customer {taxi.customer_assigned}: {e}")
            #self.log_audit(f"Failed to notify customer {taxi.customer_assigned}: {e}")


    
    def update_map(self, update):
        try:
            if not all(key in update for key in ['taxi_id', 'position', 'status', 'color', 'customer_id', 'picked_off', 'token']):
                logger.error("Update message missing required fields.")
                return

            taxi_id = update['taxi_id']
            position = update['position']
            token = update['token']
            
            if isinstance(position, (list, tuple)) and len(position) == 2:
                pos_x, pos_y = position
            else:
                logger.error(f"Invalid position format for taxi_id {taxi_id}: {position}")
                return
            
            status = update['status']
            color = update['color']
            customer_assigned = update['customer_id']
            picked_off = update['picked_off']
            taxi_updated = self.update_taxi_state(taxi_id, pos_x, pos_y, status, color, customer_assigned, picked_off, token)
            self.update_customer(customer_assigned, taxi_id)
            self.finalize_trip_if_needed(taxi_updated)
            self.map_changed= True
        
        except KeyError as e:
            logger.error(f"Key error when processing update: missing key {e}")
        except Exception as e:
            logger.error(f"Error in update_map: {e}")


    def update_taxi_state(self, taxi_id, pos_x, pos_y, status, color, customer_assigned, picked_off, token):
        """Actualiza la información del taxi en el sistema."""
        if taxi_id in self.taxis:
            taxi = self.taxis[taxi_id]
            if self.taxis[taxi_id].token == token:
                taxi.position = (pos_x, pos_y)
                taxi.status = status
                taxi.color = color
                taxi.customer_assigned = customer_assigned
                taxi.picked_off = picked_off
                self.map_changed = True  
                self.save_taxis()
                 # Registrar evento en audit.json
                #self.log_audit(
                #    "Taxis", f"Taxi {taxi_id} está en {taxi.position} con status {status} y customer {customer_assigned}"      
                #)
                return taxi 
            else:
                logger.warning(f"Token missmatch {taxi_id}")
                #self.log_audit(
                #    "Taxis",f"Token missmatch {taxi_id}"      
                #)
                return None           

        else:
            logger.warning(f"No taxi found with id {taxi_id}")
            return None

    def finalize_trip_if_needed(self, taxi):
        """Notifica al cliente si el taxi ha finalizado el viaje."""
        if taxi.status == "END":
            self.map_changed = True
            self.update_customer(taxi.customer_assigned, taxi.id)
            self.save_taxis()
            self.notify_customer(taxi)

            

        
    

    def stop_continue(self, taxi_id):
        if taxi_id in self.taxis:
            try:
                instruction_type = 'STOP' if self.taxis[taxi_id].color == "GREEN" else 'RESUME'
                instruction = {
                    'taxi_id': taxi_id,
                    'type': instruction_type,
                }
                # Toggle color based on current state
                logger.info(f"Central ordered taxi {taxi_id} to {instruction_type}")
                self.producer.send('taxi_instructions', instruction)
                
                notification = {
                    'customer_id': self.taxis[taxi_id].customer_assigned,
                    'status': instruction_type,
                    'assigned_taxi': taxi_id,
                }
                if self.taxis[taxi_id].customer_assigned != "x":
                    print(f"Notifying customer '{self.taxis[taxi_id].customer_assigned}'")
                    self.producer.send('taxi_responses', notification)
                
            except KafkaError as e:
                logger.error(f"Error in the order communication for taxi {taxi_id} {e}")
        
    #PROBAR
    def return_to_base(self, taxi_id):
        if taxi_id in self.taxis:
            try:
                instruction = {
                'taxi_id': taxi_id,
                'type': 'RETURN_TO_BASE',
                }
                print(f"Central ordered the taxi {taxi_id} to RETURN TO BASE")
                #self.log_audit("Taxis", f"Central ordered the taxi {taxi_id} to RETURN TO BASE")
                self.producer.send('taxi_instructions', instruction)

            # Notificar al cliente solo si hay uno asignado
                customer_id = self.taxis[taxi_id].customer_assigned
                if customer_id != "x" and customer_id in self.customers:
                    self.customers[customer_id].position = self.taxis[taxi_id].position
                    self.update_customer(customer_id, "x")

                    notification = {
                        'customer_id': customer_id,
                        'status': "RETURN",
                        'assigned_taxi': taxi_id,
                        'position': self.taxis[taxi_id].position
                    }
                    print(f"Notifying customer '{customer_id}'")
                    self.producer.send('taxi_responses', notification)

            except KafkaError as e:
                logger.error(f"Error in the order communication for taxi {taxi_id} {e}")

    def change_destination(self, taxi_id, destination):
        if taxi_id in self.taxis:
            if destination in self.locations:
                destination_position = self.locations[destination].position
                try:

                    instruction = {
                    'taxi_id': taxi_id,
                    'type': 'CHANGE',
                    'destination': destination_position
                    }
                    print(f"Central ordered the taxi {taxi_id} to CHANGE ITS DESTINATION to {destination_position}")
                
                    notification = {
                        'customer_id': self.taxis[taxi_id].customer_assigned,
                        'status': "CHANGE",
                        'assigned_taxi': taxi_id,
                        'destination' : destination
                    }
                    print(f"Notifying customer '{self.taxis[taxi_id].customer_assigned}'")

                    self.producer.send('taxi_instructions', instruction)
                    self.producer.send('taxi_responses', notification)

                except KafkaError as e:
                    logger.error(f"Error in the order communication for taxi {taxi_id} {e}")
            else:
                print(f"Destino {destination} no encontrado.")

    def generate_table(self):
        """Genera la tabla de estado de taxis y clientes con el menú fijo 12 líneas debajo del título."""
        if not self.taxis:
            logger.error("Taxis not initialized or empty. Cannot generate table.")
            return "No data available."
        
        table_lines = []
        table_lines.append("               *** EASY CAB ***        ")
        table_lines.append("     TAXIS                           CLIENTES   ")
        table_lines.append(f"{'Id':<4}{'Destino':<10}{'Estado':<15}     {'Id':<4}{'Destino':<10}{'Estado':<15}")

        taxi_lines = []
        for taxi in self.taxis.values():
            if taxi.auth_status != 1:  # Solo mostrar taxis autenticados
                continue
            taxi_id = taxi.id
            # Obtener la destinación del cliente asignado si existe
            if taxi.customer_assigned != "x" and taxi.customer_assigned in self.customers:
                destination = self.customers[taxi.customer_assigned].destination
            else:
                destination = "-"

            # Determinar el estado del taxi
            if taxi.status in ["KO", "SENSOR", "ERROR", "DOWN"]:
                state = "KO. Parado"
            elif taxi.customer_assigned == "x":
                state = "OK. Libre"
            else:
                state = f"OK. Servicio {taxi.customer_assigned}"

            taxi_lines.append(f"{taxi_id:<4}{destination:<10}{state:<15}")

        client_lines = []
        for customer_id, destination in self.customers.items():
            customer = self.customers[customer_id]
            if customer.status == "SERVICED":
                state = "OK. Servicio finalizado"
            elif customer.status == "UNATTENDED":
                state = "OK. Sin taxi asignado"
            elif customer.status == "WAIT":
                state = f"OK. Esperando a Taxi {customer.taxi_assigned}"
            elif customer.picked_off == 1:
                state = f"OK. Taxi {customer.taxi_assigned}"

            client_lines.append(f"{customer_id:<4}{customer.destination:<10}{state:<15}")

        max_lines = max(len(taxi_lines), len(client_lines))
        for i in range(max_lines):
            taxi_info = taxi_lines[i] if i < len(taxi_lines) else " " * 30
            client_info = client_lines[i] if i < len(client_lines) else ""
            table_lines.append(f"{taxi_info:<30} | {client_info:<30}")

        # Calcular el número de líneas necesarias para fijar el menú a 12 líneas debajo del título
        lines_after_title = len(table_lines) - 3  # Resta las líneas del título de la tabla
        padding_lines = max(0, 12 - lines_after_title)

        # Añadir las líneas en blanco
        table_lines.extend([""] * padding_lines)

        # Añadir el menú de órdenes al final de la tabla
        menu_lines = [
            "Órdenes de Central:",
            "- Parar un taxi:  '<ID_Taxi>'",
            "- Reanudar marcha: '<ID_Taxi>'",
            "- Enviar un taxi a base: 'b <ID_Taxi>'.",
            "- Cambiar destino: '<ID_Taxi> <ID_Location>'."
        ]
        table_lines.extend(menu_lines)

        return "\n".join(table_lines)

    def draw_map(self):
        """Dibuja el mapa y la tabla de estado lado a lado en la consola."""
        table = self.generate_table()

        # Construir las líneas del mapa
        map_lines = []
        border_row = "#" * (self.map_size[1] * 2 + 2)
        map_lines.append(border_row)

        bordered_map = np.full((self.map_size[0], self.map_size[1]), ' ', dtype=str)

        for customer in self.customers.values():
            self.locations[customer.id] = Location(customer.id, customer.position, "YELLOW")
        for taxi in self.taxis.values():
            self.locations[str(taxi.id)] = Location(str(taxi.id), taxi.position, taxi.color)
        

        # Colocar las localizaciones en el mapa
        for location in self.locations.values():
            x, y = location.position
            bordered_map[y - 1, x - 1] = f"{location.id:2}"

        # Añadir las filas al mapa
        for row in bordered_map:
            formatted_row = "#" + "".join([f"{cell} " if cell != " " else "  " for cell in row]) + "#"
            map_lines.append(formatted_row)

        map_lines.append(border_row)

        # Dividir las líneas de la tabla
        table_lines = table.split("\n")

        # Calcular el número máximo de líneas entre mapa y tabla
        max_lines = max(len(map_lines), len(table_lines))

        # Ajustar mapa y tabla para que tengan el mismo número de líneas
        padded_map_lines = map_lines + [""] * (max_lines - len(map_lines))
        padded_table_lines = table_lines + [""] * (max_lines - len(table_lines))

        # Combinar mapa y tabla línea por línea
        combined_lines = [
            f"{map_line:<45}   {table_line}"
            for map_line, table_line in zip(padded_map_lines, padded_table_lines)
        ]

        # Mostrar el resultado en la consola
        print("\n".join(combined_lines))



    def broadcast_map(self):
        """
        Convierte el diccionario self.locations a un formato JSON y lo envía a través de Kafka.
        """
        try:
            # Convertir self.locations a un formato serializable en JSON
            serialized_dict = {key: asdict(location) for key, location in self.locations.items()}
            map_data = serialized_dict

            # Generar la tabla como una lista de líneas
            table_data = self.generate_table().split('\n')
            # Crear el mensaje a enviar
            message = {
                "type": "MAP_UPDATE",
                "timestamp": time.time(),
                "map": map_data,
                "table": table_data
            }
            
            # Enviar el mensaje al tópico correspondiente en Kafka
            self.producer.send('map_updates', message)
            self.producer.flush()  # Asegurarse de que el mensaje se envíe inmediatamente
            
            # Registrar evento de mapa en audit.json
            #self.log_audit("Map Update", "Enviando el mapa a todos los taxis")

        except Exception as e:
            logger.error(f"Error al enviar el mapa a los taxis: {e}")
            #self.log_audit("MAP_ERROR", f"Error broadcasting map: {e}")



    def register_customer(self, customer_id, position, destination):
        if not isinstance(customer_id, str) or not isinstance(position, (list, tuple)) or not isinstance(destination, str):
            logger.error(f"Invalid customer data: id={customer_id}, position={position}, destination={destination}")
            return False
        if customer_id not in self.customers:
            self.customers[customer_id] = Customer(
                id=customer_id,
                status="UNATTENDED",
                position=tuple(position),
                destination=destination,
                taxi_assigned=0,
                picked_off=0,
            )
            self.save_customers()
        else:
            customer = self.customers[customer_id]
            customer.status ="UNATTENDED"
            customer.position=tuple(position)
            customer.destination=destination
            customer.taxi_assigned=0
            customer.picked_off=0
            self.save_customers()
        #self.log_audit("Customers", "Customer registrado con éxito")
        self.map_changed = True


    def process_customer_request(self, request):
        print(f"RECIBIDA REQUEST: {request}")
        #self.log_audit("Customers", f"RECIBIDA REQUEST: {request}")
        if request['status'] == "REQUEST":
            customer_id = request['customer_id']
            destination = request['destination']
            customer_location = request['customer_location']

            self.assign_taxi(customer_id, customer_location, destination)
        else:
            del self.customers[request['customer_id']]
            del self.locations[request['customer_id']]
            self.map_changed = True

    def assign_taxi(self, customer_id, customer_location, destination):
        #guarda la posicion del customer en location

        if destination not in self.locations:
            logger.error(f"Invalid destination: {destination}")
            return False

        self.register_customer(customer_id, customer_location, destination)
        self.select_available_taxi(customer_id)


    def select_available_taxi(self, customer_id):
        """Selecciona el primer taxi disponible con estado 'FREE'."""
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            available_taxi = next((taxi for taxi in self.taxis.values() if taxi.status == 'FREE' and taxi.customer_assigned == "x" and taxi.auth_status == 1), None)
            
            if available_taxi:
                logger.info(f"Taxi disponible encontrado: {available_taxi.id}")
                available_taxi.status = "BUSY"
                available_taxi.customer_assigned = customer_id
                self.save_taxis()
                self.update_customer(customer_id, available_taxi.id)
                self.map_changed = True
                self.notify_customer_assignment(customer_id, available_taxi)
                #self.log_audit("Customers", f"Taxi {available_taxi.id} asignado a cliente {customer_id}")
                self.send_taxi_instruction(available_taxi, customer_id, self.customers[customer_id].position, self.customers[customer_id].destination)
                return
            else:
                logger.info(f"No se encontró taxi para el cliente '{customer_id}', reintentando en 3 segundos...")
                #self.log_audit("Customers", f"No se ha encontrado taxi para el cliente {customer_id}")
                time.sleep(3)
                retry_count += 1

        logger.warning(f"No se pudo encontrar un taxi disponible para el cliente '{customer_id}' después de {max_retries} intentos.")
        return None

    # manejar taxis vacios, taxi no tiene clientes...
    def update_customer(self, customer_id, taxi_id):
        if taxi_id in self.taxis:
            taxi = self.taxis[taxi_id]
            if customer_id in self.customers:
                customer = self.customers[customer_id]
                if not isinstance(customer_id, str) or not isinstance(customer.position, (list, tuple)) or not isinstance(customer.destination, str):
                    logger.error(f"Invalid customer data: id={customer_id}, position={customer.position}, destination={customer.destination} en update")
                    return False
                if taxi.status == "END":
                    customer.status="SERVICED"
                    customer.position=taxi.position
                    customer.taxi_assigned=0
                    customer.picked_off=0
                    self.save_customers()
                else:
                    customer.taxi_assigned = taxi.id
                    customer.picked_off = taxi.picked_off
                    if taxi.picked_off == 1:
                        customer.status = "TRANSIT"
                        customer.position = taxi.position
                    else:
                        customer.status = "WAIT"
                    self.save_customers()
        else:
            if customer_id in self.customers:
                customer = self.customers[customer_id]
                customer.status="UNATTENDED"
                customer.taxi_assigned=0
                customer.picked_off=0
                self.save_customers()
                self.assign_taxi(customer_id, customer.position, customer.destination)
        self.map_changed = True
        #self.log_audit("Customers", f"Customer {customer_id} actualizado")


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
        self.producer.flush()  # Asegura el envío inmediato
        logger.info(f"Instructions sent to taxi {taxi.id} for customer '{customer_id}'")
        #self.log_audit("Taxis", f"Instructions sent to taxi {taxi.id} for customer '{customer_id}'")

    def notify_customer_assignment(self, customer_id, taxi):
        """Envía una respuesta al cliente confirmando la asignación del taxi."""
        
        if taxi.id == 0:
            response = {
            'customer_id': customer_id,
            'status': "KO",
            'assigned_taxi': 'x'
            }

        else:
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


    def request_checker(self):
        while True:
            try:
                # Obtener la próxima instrucción de la cola
                request = self.requests_q.get()
                self.process_customer_request(request)
            except queue.Empty:
                # Si no hay instrucciones, continuar
                continue
            except Exception as e:
                logger.error(f"Error while processing customer request: {e}")

    def update_checker(self):
        while True:
            try:
                # Obtener la próxima instrucción de la cola
                update = self.updates_q.get()
                self.process_update(update)
            except queue.Empty:
                # Si no hay instrucciones, continuar
                continue
            except Exception as e:
                logger.error(f"Error while processing taxi update: {e}")

    def kafka_listener(self):
        while True:
            try:
                for message in self.consumer:
                    if message.topic == 'taxi_requests':
                        #self.log_audit("Kafka", f"Mensaje recibido en el topico 'taxi_requests': {message.value}")
                        data = message.value
                        self.requests_q.put(data)
                        
                    elif message.topic == 'taxi_updates':
                        #self.log_audit("Kafka", f"Mensaje recibido en el topico 'taxi_updates': {message.value}")
                        data = message.value
                        self.updates_q.put(data)

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


    def start_server_socket(self):
        """Configura el servidor de sockets y maneja la autenticación de taxis en un hilo separado."""
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile='/app/centralCert.pem', keyfile='/app/centralKey.pem')
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('0.0.0.0', self.listen_port))
        self.server_socket.listen(5)  
        #self.log_audit("Sockets", f"Listening for taxi connections on port {self.listen_port}...")
        logger.info(f"Listening for taxi connections on port {self.listen_port}...")

        try:
            while True:
                conn, addr = self.server_socket.accept()
                #Envolvemos la conexión con SSL
                conn_ssl = context.wrap_socket(conn, server_side=True)
                threading.Thread(target=self.handle_taxi_auth, args=(conn_ssl, addr), daemon=True).start()
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
        
    def input_listener(self):
        while True:
            try:
                user_input = input().strip()  # Capturar input del usuario
                if user_input:

                    # Procesar input
                    command_parts = user_input.split()
                    if len(command_parts) == 2:  # Comando con dos parámetros
                        action = command_parts[0]
                        try:
                            taxi_id = int(command_parts[1])
                            if action.lower() == "b":  # Return to base
                                self.return_to_base(taxi_id)
                            else:  # Cambiar destino
                                destination = command_parts[1]
                                self.change_destination(taxi_id, destination)
                        except ValueError:
                            print("Formato incorrecto. Consulte el menú para las opciones.")
                        except KeyError:
                            print(f"Destino no válido: {command_parts[1]}.")

                    elif len(command_parts) == 1:  # Parar o continuar
                        try:
                            taxi_id = int(command_parts[0])
                            self.stop_continue(taxi_id)
                        except ValueError:
                            print("Formato incorrecto. Consulte el menú para las opciones.")

                    else:
                        print("Comando no reconocido. Consulte el menú para las opciones.")
            except Exception as e:
                print(f"Error al procesar input: {e}")

    def clear_taxis(self):
        """Limpia el archivo JSON de taxis, eliminando todos los registros."""
        try:
            with open(self.taxis_file, 'w') as f:
                json.dump({}, f, indent=4)
            logger.info("Taxis JSON file cleared successfully.")
        except Exception as e:
            logger.error(f"Error clearing taxis JSON file: {e}")

    def _monitor_traffic(self):
        """Monitorea el estado del tráfico cada 10 segundos."""
        while True:
            try:
                response = requests.get(self.ctc_url)
                if response.status_code == 200:
                    data = response.json()
                    if data['status'] == "KO" and self.traffic_state == 1:
                        self.all_to_base()
                
            except RequestException as e:
                logger.error(f"Error al consultar CTC: {e}")
            
            time.sleep(10)

    def all_to_base(self):
        """Manda todos los taxis a la base."""
        logger.info("TRÁFICO CORTADO, TODOS LOS TAXIS A BASE")
        #self.log_audit("Clima", "TRÁFICO CORTADO, TODOS LOS TAXIS A BASE")
        for taxi in self.taxis.values():

            if taxi.auth_status == 1:
                self.return_to_base(taxi.id)

    def process_write_queue(self):
        while True:
            try:
                file, data = self.write_queue.get()
                with open(file, 'w') as f:
                    json.dump(data, f, indent=4)
                self.write_queue.task_done()
                #self.log_audit("Escritura", f"Escrito en el archivo {file}, los datos: {data}")
            except Exception as e:
                logger.error(f"Error writing to {file}: {e}")
    
    def log_audit(event_type, description):
        """
        Registra un evento en el archivo audit.json.
        :param event_type: Tipo de evento (e.g., "Taxi Status Update", "Error").
        :param details: Detalles del evento (diccionario).
        """
        audit_entry = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": event_type,
            "description": description,
        }
        try:
            with open("/data/audit.json", "a") as f:
                f.write(json.dumps(audit_entry) + "\n")
        except Exception as e:
            logger.error(f"Error writing to audit.json: {e}")


    def run(self):
        self.load_map_config()
        self.clear_taxis()
        self.load_taxis()
        logger.info("EC_Central is running...")

        # Mostrar el mapa inicial
        self.draw_map()
        self.broadcast_map()
        self.map_changed = False

        # Iniciar hilos para las funcionalidades existentes
        threading.Thread(target=self.process_write_queue, daemon=True).start()

        auth_thread = threading.Thread(target=self.start_server_socket, daemon=True)
        auth_thread.start()

        for partition in self.consumer.assignment():
            self.consumer.seek_to_end(partition)

        kafka_thread = threading.Thread(target=self.kafka_listener, daemon=True)
        kafka_thread.start()

        threading.Thread(target=self.request_checker, daemon=True).start()
        threading.Thread(target=self.update_checker, daemon=True).start()

        map_thread = threading.Thread(target=self.auto_broadcast_map, daemon=True)
        map_thread.start()

        # Hilo para manejar inputs sin bloquear
        input_thread = threading.Thread(target=self.input_listener, daemon=True)
        input_thread.start()

        monitor_thread = threading.Thread(target=self._monitor_traffic, daemon=True)
        monitor_thread.start()

        try:
            while True:
                time.sleep(1)  # Mantener el programa en ejecución
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.clear_taxis()
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
