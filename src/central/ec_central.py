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
    customer_assigned: str
    picked_off: int
    auth_status: int
    stopped: bool = False

@dataclass
class Customer:
    id: str
    status: str  
    position: Tuple[int, int]
    destination: Tuple[int, int]
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
        self.taxis_file = '/data/taxis.txt'  
        self.taxis: Dict[int, Taxi] = {}  
        self.customers: Dict[int, Customer] = {}
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
                    auto_offset_reset='latest'
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
                    self.locations[loc_id] = Location(loc_id, (x, y), "BLUE")
                    self.map[y - 1, x - 1] = loc_id  # Ajuste del índice para la matriz
            logger.info("Map configuration loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading map configuration: {e}")


    def load_taxis(self):
        """Carga los taxis desde el fichero."""
        self.taxis = {}  # Asegurar que sea un diccionario vacío antes de cargar
        try:
            with open(self.taxis_file, 'r') as f:
                for line in f:
                    taxi_id, status, color, pos_x, pos_y, customer_assigned, picked_off, auth_status = line.strip().split('#')
                    self.taxis[int(taxi_id)] = Taxi(
                        id=int(taxi_id),
                        status=status,
                        color=color,
                        position=(int(pos_x), int(pos_y)),
                        customer_assigned=customer_assigned,
                        picked_off=int(picked_off),
                        auth_status=int(auth_status)
                    )
            logger.info("Taxis loaded successfully.")
        except FileNotFoundError:
            logger.warning("Taxis file not found. Starting with an empty list.")
        except Exception as e:
            logger.error(f"Error loading taxis from file: {e}")

    def save_taxis(self):
        """Guarda los taxis en el fichero."""
        try:
            with open(self.taxis_file, 'w') as f:
                for taxi in self.taxis.values():
                    f.write(f"{taxi.id}#{taxi.status}#{taxi.color}#{taxi.position[0]}#{taxi.position[1]}#{taxi.customer_assigned}#{taxi.picked_off}#{taxi.auth_status}\n")
        except Exception as e:
            logger.error(f"Error saving taxis to file: {e}")

    def handle_taxi_auth(self, conn, addr):
        """Maneja la autenticación del taxi."""
        logger.info(f"Connection from taxi at {addr}")
        try:
            data = conn.recv(1024).decode('utf-8')
            taxi_id = int(data.strip())

            if taxi_id in self.taxis:
                # Actualizar estado y autenticación del taxi
                taxi = self.taxis[taxi_id]
                taxi.status = "FREE"
                taxi.color = "RED"
                taxi.position = (1, 1)
                taxi.customer_assigned = "x"
                taxi.picked_off = 0
                taxi.auth_status = 1
                self.save_taxis()
                logger.info(f"Taxi {taxi_id} authenticated successfully.")
                conn.sendall(b"OK")
                self.listen_to_taxi(taxi_id, conn)
            else:
                logger.warning(f"Taxi {taxi_id} is not in the database.")
                conn.sendall(b"NOT_FOUND")

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
                        self.save_taxis()
                        
                        # Notificar al cliente si hay uno asignado
                        if taxi.customer_assigned != "x":
                            self.notify_customer(taxi)

                    # Esperar 10 segundos antes de considerarlo una incidencia permanente
                    time.sleep(10)
                    taxi.status = "DOWN"
                    taxi.auth_status = 0 
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
            self.producer.send('taxi_responses', response).get(timeout=3)  # Bloquea hasta que el mensaje se envíe
            logger.info(f"Trip completed sending to customer {taxi.customer_assigned}: {response}")
        except KafkaError as e:
            logger.error(f"Failed to notify customer {taxi.customer_assigned}: {e}")


    
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
            customer_assigned = update['customer_id']
            picked_off = update['picked_off']
            taxi_updated = self.update_taxi_state(taxi_id, pos_x, pos_y, status, color, customer_assigned, picked_off)
            self.finalize_trip_if_needed(taxi_updated)
            self.map_changed= True
        
        except KeyError as e:
            logger.error(f"Key error when processing update: missing key {e}")
        except Exception as e:
            logger.error(f"Error in update_map: {e}")


    def update_taxi_state(self, taxi_id, pos_x, pos_y, status, color, customer_assigned, picked_off):
        """Actualiza la información del taxi en el sistema."""
        if taxi_id in self.taxis:
            taxi = self.taxis[taxi_id]
            taxi.position = (pos_x, pos_y)
            taxi.status = status
            taxi.color = color
            taxi.customer_assigned = customer_assigned
            taxi.picked_off = picked_off
            self.map_changed = True  
            self.save_taxis()
            if picked_off==1:
                self.locations[customer_assigned].position = taxi.position 
                self.customers[customer_assigned].picked_off = 1
                self.customers[customer_assigned].status = "OK"
            return taxi
        else:
            logger.warning(f"No taxi found with id {taxi_id}")
            return None

    def finalize_trip_if_needed(self, taxi):
        """Notifica al cliente si el taxi ha finalizado el viaje."""
        if taxi.status == "END":
            self.map_changed = True
            self.customers[taxi.customer_assigned].status = "SERVICED"
            self.save_taxis()
            self.notify_customer(taxi)
        
    def generate_table(self):
        """Genera la tabla de estado de taxis y clientes con el menú fijo 12 líneas debajo del título."""
        if not self.taxis:
            logger.error("Taxis not initialized or empty. Cannot generate table.")
            return "No data available."
        
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
            if taxi.customer_assigned != "x" and taxi.customer_assigned in self.customer_destinations:
                destination = self.customer_destinations[taxi.customer_assigned]
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
            table_lines.append(f"{taxi_info} | {client_info}")

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

    def stop_continue(self, taxi_id):
        if taxi_id in self.taxis:
            try:

                if self.taxis[taxi_id].stopped ==  False:
                    instruction = {
                    'taxi_id': taxi_id,
                    'type': 'STOP',
                    }
                    self.taxis[taxi_id].stopped = True
                    print(f"Central ordered the taxi {taxi_id} to STOP")
                    
                    notification = {
                        'customer_id': self.taxis[taxi_id].customer_assigned,
                        'status': "STOP",
                        'assigned_taxi': taxi_id,
                    }
                
                    print(f"Notifying customer '{self.taxis[taxi_id].customer_assigned}'")

                    
                else:
                    instruction = {
                    'taxi_id': taxi_id,
                    'type': 'RESUME',
                    }
                    self.taxis[taxi_id].stopped = False
                    print(f"Central ordered the taxi {taxi_id} to CONTINUE")

                    notification = {
                        'customer_id': self.taxis[taxi_id].customer_assigned,
                        'status': "RESUME",
                        'assigned_taxi': taxi_id,
                    }
                
                    print(f"Notifying customer '{self.taxis[taxi_id].customer_assigned}'")


                self.producer.send('taxi_instructions', instruction)
                self.producer.send('taxi_responses', notification)
            except KafkaError as e:
                logger.error(f"Error in the order communication for taxi {taxi_id} {e}")
        

    def return_to_base(self, taxi_id):
        if taxi_id in self.taxis:
            try:
                instruction = {
                'taxi_id': taxi_id,
                'type': 'RETURN_TO_BASE',
                }
                print(f"Central ordered the taxi {taxi_id} to RETURN TO BASE")
            
                self.producer.send('taxi_instructions', instruction)

                notification = {
                        'customer_id': self.taxis[taxi_id].customer_assigned,
                        'status': "RETURN",
                        'assigned_taxi': taxi_id,
                    }
                
                print(f"Notifying customer '{self.taxis[taxi_id].customer_assigned}'")

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

    def draw_map(self):
        """Dibuja el mapa y la tabla de estado lado a lado en la consola."""
        table = self.generate_table()

        # Construir las líneas del mapa
        map_lines = []
        border_row = "#" * (self.map_size[1] * 2 + 2)
        map_lines.append(border_row)

        bordered_map = np.full((self.map_size[0], self.map_size[1]), ' ', dtype=str)

        # Colocar las localizaciones en el mapa
        for location in self.locations.values():
            x, y = location.position
            bordered_map[y - 1, x - 1] = location.id.ljust(2)

        # Colocar los taxis autenticados en el mapa
        for taxi in self.taxis.values():
            
            x, y = taxi.position
            if 1 <= x <= self.map_size[1] and 1 <= y <= self.map_size[0]:
                if taxi.status == "DOWN":
                    bordered_map[y - 1, x - 1] = "X "
                elif taxi.auth_status == 1:
                    bordered_map[y - 1, x - 1] = str(taxi.id).ljust(2)

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
            return True
        else:
            customer = self.customers[customer_id]
            customer.status ="UNATTENDED"
            customer.position=tuple(position)
            customer.destination=destination
            customer.taxi_assigned=0
            customer.picked_off=0


    def process_customer_request(self, request):
        customer_id = request['customer_id']
        destination = request['destination']
        customer_location = request['customer_location']
        destination = destination
        self.customer_destinations[customer_id] = destination

        if customer_location:
            location_key = tuple(customer_location)
            self.locations[customer_id] = Location(customer_id, location_key, 'YELLOW')
            self.map_changed = True  

        if destination not in self.locations:
            logger.error(f"Invalid destination: {destination}")
            return False

        self.register_customer(customer_id, customer_location, destination) 

        available_taxi = self.select_available_taxi(customer_id)
        if available_taxi and available_taxi.status == "FREE":
            self.assign_taxi_to_customer(available_taxi, customer_id, location_key, destination)
            self.map_changed = True  
            return True
        else:
            logger.warning("No available taxis")
            return False


    def select_available_taxi(self,customer):
        """Selecciona el primer taxi disponible con estado 'FREE'."""
        while True:
            self.load_taxis()

            available_taxi = next((taxi for taxi in self.taxis.values() if taxi.status == 'FREE' and taxi.customer_assigned == "x" and taxi.auth_status==1), None)

            # Log adicional para saber si se encontró un taxi disponible
            if available_taxi:
                logger.info(f"Taxi disponible encontrado: {available_taxi.id}")
                return available_taxi
            else:
                print(f"Ningun taxi encontrado para el cliente'{customer}', probando de nuevo en 3 segundos")
                time.sleep(3)

        
    def update_customer(self, customer_id, taxi):
        if customer_id in self.customers:
            customer = self.customers[customer_id]
            customer.taxi_assigned = taxi
            customer.status = "WAIT"
            

    def assign_taxi_to_customer(self, taxi, customer_id, customer_location, destination):
        """Asigna el taxi al cliente y envía instrucciones."""
        taxi.status = 'BUSY'
        taxi.color = 'GREEN'
        taxi.customer_assigned = customer_id
        self.save_taxis()
        self.update_customer(customer_id, taxi.id)
        self.map_changed = True
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
        logger.info(f"Instructions sent to taxi {taxi.id} for customer '{customer_id}'")

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

    #def cetral_input(self):
        
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




    def run(self):
        self.load_map_config()
        self.load_taxis()
        logger.info("EC_Central is running...")

        # Mostrar el mapa inicial
        self.draw_map()
        self.broadcast_map()
        self.map_changed = False

        # Iniciar hilos para las funcionalidades existentes
        auth_thread = threading.Thread(target=self.start_server_socket, daemon=True)
        auth_thread.start()

        kafka_thread = threading.Thread(target=self.kafka_listener, daemon=True)
        kafka_thread.start()

        map_thread = threading.Thread(target=self.auto_broadcast_map, daemon=True)
        map_thread.start()

        # Hilo para manejar inputs sin bloquear
        input_thread = threading.Thread(target=self.input_listener, daemon=True)
        input_thread.start()

        try:
            while True:
                time.sleep(1)  # Mantener el programa en ejecución
        except KeyboardInterrupt:
            logger.info("Shutting down...")
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


