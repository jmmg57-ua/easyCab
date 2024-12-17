from dataclasses import dataclass
import os
import signal
import socket
import sys
import threading
import time
import json
import logging
from typing import Tuple
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError  
import numpy as np
import queue
import requests
import ssl

@dataclass
class Location:
    id: str
    position: Tuple[int, int]
    color: str 

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

class DigitalEngine:
    def __init__(self, ec_central_ip, ec_central_port, kafka_broker, ec_s_ip, ec_s_port, taxi_id, ec_registry_port):
        self.ec_central_ip = ec_central_ip
        self.ec_central_port = ec_central_port
        self.kafka_broker = kafka_broker
        self.ec_s_ip = ec_s_ip
        self.ec_s_port = ec_s_port
        self.ec_registry_port = ec_registry_port
        self.taxi_id = taxi_id
        self.map_size = (20, 20)
        self.map = np.full(self.map_size, ' ', dtype=str) 
        self.status = "FREE"   
        self.color = "RED"     
        self.position = [1, 1]  
        self.destination = [0, 0]
        self.customer_asigned = "x"
        self.picked_off = 0
        self.pickup = [0, 0]
        self.locations = {}
        self.table_lines = []
        self.central_disconnected = False
        self.sensor_connected = False  # Estado inicial de conexión del sensor
        self.instruction_queue = queue.Queue()  # Cola para las instrucciones
        self.map_queue = queue.Queue()
        self.ko = 0
        self.central_socket = None
        self.map_changed = False

        self.setup_kafka()

    def setup_kafka(self):
        retry_count = 0
        while retry_count < 5:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[self.kafka_broker],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=3,
                    security_protocol='SSL',                # Protocolo SSL
                    ssl_cafile='./kafka_certs/ca.crt',  # Certificado de la autoridad certificadora
                    ssl_certfile='./kafka_certs/kafka.crt',  # Certificado del cliente
                    ssl_keyfile='./kafka_certs/kafka.key'    # Clave privada del cliente
                )
                logger.info("Kafka producer set up successfully")

                self.consumer = KafkaConsumer(
                    'taxi_instructions','map_updates',
                    bootstrap_servers=[self.kafka_broker],
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    group_id=f'customer_{self.taxi_id}',
                    auto_offset_reset='latest',
                    security_protocol='SSL',                # Protocolo SSL
                    ssl_cafile='./kafka_certs/ca.crt',  # Certificado de la autoridad certificadora
                    ssl_certfile='./kafka_certs/kafka.crt',  # Certificado del cliente
                    ssl_keyfile='./kafka_certs/kafka.key',    # Clave privada del cliente
                    session_timeout_ms=30000,  # Incrementa el timeout de sesión
                    request_timeout_ms=40000  # Incrementa el timeout para solicitudes
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
            # Crear contexto SSL
            context = ssl.create_default_context()
            context.load_verify_locations(cafile='./centralCert.pem')

            # Crear y envolver el socket
            raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.central_socket = context.wrap_socket(raw_socket, server_hostname=self.ec_central_ip)

            # Conectar
            self.central_socket.connect((self.ec_central_ip, self.ec_central_port))
            logger.info("Secure connection established with EC_Central.")
            return True
        
        except Exception as e:
            logger.error(f"Failed to connect to Central: {e}")
            return False

    def set_up_socket_sensor(self):
        self.sensor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sensor_socket.bind(('0.0.0.0', self.ec_s_port))
        self.sensor_socket.listen(1)
        logger.info(f"Digital Engine for Taxi {self.taxi_id} initialized")

    def authenticate(self):
        try:

            auth_message = f"{self.taxi_id}"
            self.central_socket.send(auth_message.encode())
            response = self.central_socket.recv(1024).decode()
            if response.startswith("TOKEN"):
                self.token = response.split()[1]
                logger.info(f"Authenticated successfully. Token recieved")
                return True
            else:
                logger.warning(f"Authentication failed for Taxi {self.taxi_id}")
                return False
            
        except Exception as e:
            logger.error(f"Error during authentication: {e}")
            return False
        
    def kafka_listener(self):
        data = None
        try:
            for message in self.consumer:
                if self.central_disconnected:
                    break
                data = message.value
                if message.topic == 'taxi_instructions':
                    if 'taxi_id' not in data:
                        logger.warning("Received instruction does not contain 'taxi_id'. Skipping.")
                        continue
                    if data['taxi_id'] == self.taxi_id:
                        logger.info(f"Received message on topic 'taxi_instructions'")
                        self.instruction_queue.put(data)
                elif message.topic == 'map_updates':
                    logger.info("Received message on topic 'map_updates'")
                    self.map_queue.put(data)

        except KafkaError as e:
            logger.error(f"Lost connection to Kafka: {e}")
            # self.central_disconnected = True
            # self.continue_independently()
        except Exception as e:
            logger.error(f"General error in kafka_listener: {e}")
            time.sleep(5)

    def instruction_checker(self):
        while True:
            try:
                # Obtener la próxima instrucción de la cola
                instruction = self.instruction_queue.get()
                logger.info(f"Processing instruction: {instruction}")
                self.process_instruction(instruction)
            except queue.Empty:
                # Si no hay instrucciones, continuar
                continue
            except Exception as e:
                logger.error(f"Error while processing instruction: {e}")

    def map_checker(self):
        while True:
            try:
                # Obtener la próxima instrucción de la cola
                update = self.map_queue.get()
                logger.info(f"Processing map update")
                self.process_map_updates(update)
            except queue.Empty:
                # Si no hay instrucciones, continuar
                continue
            except Exception as e:
                logger.error(f"Error while processing update: {e}")


    def process_instruction(self, instruction):
        if instruction['type'] == 'STOP':
            logger.info("INSTRUCCION STOP")
            self.status = "KO"
            self.color = "RED"
            return 

        elif instruction['type'] == 'RESUME':
            logger.info("INSTRUCCION RESUME")
            self.color = "GREEN"
            self.status = "OK"
            return            

        elif instruction['type'] == 'MOVE':
            self.pickup = instruction['pickup']
            logger.info("INSTRUCCION MOVE")
            self.status = "BUSY"
            self.color = "GREEN"
            self.customer_asigned = instruction['customer_id']
            if isinstance(instruction['destination'], (list, tuple)):
                self.destination = instruction['destination']
            else:
                logger.error(f"Invalid destination format: {instruction['destination']}")
            return

        elif instruction['type'] == 'RETURN_TO_BASE':
            logger.info("INSTRUCCION RETURN")
            self.picked_off = 1
            self.destination = [1, 1]
            self.status = "BUSY"
            self.color = "GREEN"
            self.customer_asigned = "x"
            self.picked_off = 0
            self.send_position_update()
            return

        elif instruction['type'] == 'CHANGE':
            logger.info("INSTRUCCION CHANGE")
            if isinstance(instruction['destination'], (list, tuple)):
                self.destination = instruction['destination']
                self.color = "GREEN"
                self.send_position_update()
            else:
                logger.error(f"Invalid destination format: {instruction['destination']}")
            return

    def movement_thread(self):
        while True:
            try:

                if self.destination == [0,0]:
                    time.sleep(1)
                    continue
                
                # Finalizar el viaje si llega al destino
                elif self.position == self.destination:
                    self.finalize_trip()
                    time.sleep(4)
                    self.send_position_update()
                    continue
            
                elif self.status == "KO" or self.color == "RED":
                    time.sleep(1)
                    continue
                
                elif not isinstance(self.destination, (list, tuple)):
                    logger.error(f"Invalid destination: destination={self.destination}")
                    self.destination = [0, 0]
                    time.sleep(1)
                    continue

                elif self.destination == [1,1]:
                    self.move_towards(self.destination)
                # Mover al punto de recogida
                elif self.position != self.pickup and self.picked_off == 0:
                    self.move_towards(self.pickup)
                    
                # Marcar al cliente como recogido
                elif self.position == self.pickup:
                    self.picked_off = 1
                    logger.info(f"Customer picked up at position {self.position}. Moving to destination.")
                    self.move_towards(self.destination)

                # Mover al destino
                elif self.position != self.destination and self.picked_off == 1:
                    self.move_towards(self.destination)

                

                self.send_position_update()
                time.sleep(1)

            except Exception as e:
                logger.error(f"Error in movement_thread: {e}")
                time.sleep(1)  # Evita bucles rápidos en caso de error
            


    def finalize_trip(self):
        """Handle trip completion and reset taxi state."""
        try:
            self.color = "RED"  # Stop the taxi after completing the trip
            self.status = "END"  # Mark trip as ended
            logger.info("TRIP ENDED!!")
            
            if self.position == [1,1]:
                self.disconnect()
            self.send_position_update()
            logger.info(f"Trip completed sending to customer {self.customer_asigned}")
            # Reset taxi state
            self.status = "FREE"   
            self.color = "RED"       
            self.destination = [0, 0]
            self.customer_asigned = "x"
            self.picked_off = 0
            self.pickup = [0, 0]

        except KeyError as e:
            logger.error(f"Key error when processing update: missing key {e}")
        except Exception as e:
            logger.error(f"Error finalizing trip: {e}")
            
    def move_towards(self, target):
        if self.status == "KO":
            logger.info("No se puede mover: Tráfico detenido o estado KO")
            return
    
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
        

    def send_position_update(self):
        # Verificar si el socket está abierto antes de enviar
        logger.debug(f"Sending update: Position {self.position}, Status {self.status}, Color {self.color}")
        if self.sensor_socket and self.sensor_socket.fileno() != -1:
            update = {
                'taxi_id': self.taxi_id,
                'status': self.status,
                'color': self.color,
                'position': self.position,
                'customer_id': self.customer_asigned,
                'picked_off': self.picked_off,
                'token': self.token
            }
            logger.info("Sending update through 'taxi_updates'")
            try:
                self.producer.send('taxi_updates', update)
            except KafkaError as e:
                logger.error(f"Error sending update: {e}")
        else:
            logger.warning("Socket is closed, cannot send update.")

    def process_map_updates(self, message):
        """
        Almacena la información del mapa y la tabla en el formato usado en Central.
        """
        # Inicializar diccionario de ubicaciones y lista de líneas de la tabla
        try:
            self.locations = {}
            self.table_lines = []

            # Validar que 'locations' existe y es un diccionario
            map_data = message.get('map', {})

            # Procesa cada ubicación y almacénala como una instancia de Location
            self.locations = {
                str(key): Location(id=str(key), position=value['position'], color=value['color'])
                for key, value in map_data.items()
            }

            # Procesar líneas de la tabla
            table_data = message.get('table', [])
            self.table_lines = table_data

            # Marcar que el mapa ha cambiado
            self.map_changed = True
            logger.info("Mapa y tabla procesados correctamente.")
        except Exception as e:
            logger.error(f"Error procesando la actualización del mapa: {e}")

    def map_thread(self):
        while True:
            if self.map_changed == True:
                self.draw_map()
                self.map_changed=False
                time.sleep(1)

    def draw_map(self):

        # Construir las líneas del mapa
        map_lines = []
        border_row = "#" * (self.map_size[1] * 2 + 2)
        map_lines.append(border_row)

        bordered_map = np.full((self.map_size[0], self.map_size[1]), ' ', dtype=str)

        # Colocar las localizaciones en el mapa
        
        for location in self.locations.values():
            x, y = location.position
            if 0 <= x - 1 < self.map_size[1] and 0 <= y - 1 < self.map_size[0]:  # Asegura que está en rango
                bordered_map[y - 1, x - 1] = f"{location.id:2}"

        # Añadir las filas al mapa
        for row in bordered_map:
            formatted_row = "#" + "".join([f"{cell} " if cell.strip() else "  " for cell in row]) + "#"
            map_lines.append(formatted_row)

        map_lines.append(border_row)

        # Calcular el número máximo de líneas entre mapa y tabla
        max_lines = max(len(map_lines), len(self.table_lines))

        # Ajustar mapa y tabla para que tengan el mismo número de líneas
        padded_map_lines = map_lines + [""] * (max_lines - len(map_lines))
        padded_table_lines = self.table_lines + [""] * (max_lines - len(self.table_lines))

        # Combinar mapa y tabla línea por línea
        combined_lines = [
            f"{map_line:<45}   {table_line}"
            for map_line, table_line in zip(padded_map_lines, padded_table_lines)
        ]

        # Mostrar el resultado en la consola
        print("\n".join(combined_lines))



    def handle_sensor_disconnection(self):
        self.sensor_connected = False
        update = {
                'taxi_id': self.taxi_id,
                'status': "ERROR",
                'color': "RED",
                'position': self.position,
                'customer_id': self.customer_asigned,
                'picked_off': self.picked_off,
                'token': self.token
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

    def listen_for_sensor_data(self, conn, addr):
        logger.info(f"Connected to Sensors at {addr}")
        self.sensor_connected = True  # Marca el sensor como conectado

        while True:
            try:
                data = conn.recv(1024).decode()
                if not data:
                    self.handle_sensor_disconnection()
                    break

                if data == "KO" and self.ko == 0:
                    self.ko = 1
                    self.color = "RED"
                    self.status = "KO"
                    logger.info("Taxi set to STOP. Awaiting RESUME command.")
                    self.send_position_update()

                elif data == "OK" and self.ko == 1:
                    self.ko = 0
                    self.color = "GREEN"
                    self.status = "OK"
                    logger.info("Taxi set to RESUME.")
                        
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

    # incluir opcion de autenticar
    def interact_with_registry(self):
        """Menú para interactuar con el módulo Registry."""
        registry_url = f"https://registry:{self.ec_registry_port}"  # Cambia por la IP y puerto reales de Registry

        while True:
            print("\nBienvenido al Digital Engine")
            print("1. Registrarse")
            print("2. Darse de baja")
            print("3. Salir")
            choice = input("Seleccione una opción: ")

            if choice == "1":  # Registrarse
                data = {
                    "taxi_id": self.taxi_id
                }
                try:
                    response = requests.post(f"{registry_url}/register", json=data, verify="./certServ.pem")
                    if response.status_code == 201:  # Registrado con éxito
                        print("Registro exitoso.")
                        return
                    elif response.status_code == 409:  # Ya registrado
                        print("El taxi ya está registrado.")
                        return
                    else:
                        print(f"Error al registrarse: {response.json().get('error', 'Desconocido')}")
                except requests.exceptions.RequestException as e:
                    print(f"Error al conectar con Registry: {e}")

            elif choice == "2":  # Darse de baja
                try:
                    response = requests.delete(f"{registry_url}/deregister/{self.taxi_id}", verify="./certServ.pem")
                    if response.status_code == 200:  # Baja exitosa
                        print("Se dio de baja exitosamente.")
                    elif response.status_code == 404:  # No registrado
                        print("El taxi no está registrado.")
                    else:
                        print(f"Error al darse de baja: {response.json().get('error', 'Desconocido')}")
                except requests.exceptions.RequestException as e:
                    print(f"Error al conectar con Registry: {e}")

            elif choice == "3":  # Salir
                print("Saliendo del programa...")
                sys.exit(0)

            else:
                print("Opción inválida. Intente de nuevo.")

    def disconnect(self):
        """Desconecta el taxi de la central y termina el programa."""
        logger.info(f"Taxi {self.taxi_id} is disconnecting.")
        try:
            # Cerrar conexión con la central
            if self.central_socket:
                self.central_socket.close()
                logger.info("Connection to central closed.")

            # Cerrar conexión con el sensor
            if self.sensor_socket:
                self.sensor_socket.close()
                logger.info("Connection to sensor closed.")

            # Cerrar Kafka producer y consumer
            if self.producer:
                self.producer.close()
                logger.info("Kafka producer closed.")
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed.")

        except Exception as e:
            logger.error(f"Error while disconnecting: {e}")

        finally:
            logger.info(f"Taxi {self.taxi_id} disconnected successfully.")
            os.kill(os.getpid(), signal.SIGTERM)


    def run(self):
       
        self.set_up_socket_sensor()
        logger.info("Waiting for sensor connection...")
        conn, addr = self.sensor_socket.accept()
        
        self.interact_with_registry()

        self.connect_to_central()
        
        if not self.authenticate():
            return

        logger.info("Digital Engine is running...")
        
        threading.Thread(target=self.kafka_listener, daemon=True).start()
        threading.Thread(target=self.listen_for_sensor_data, args=(conn, addr), daemon=True).start()
        threading.Thread(target=self.instruction_checker, daemon=True).start()
        threading.Thread(target=self.movement_thread, daemon=True).start()
        threading.Thread(target=self.map_checker, daemon=True).start()
        threading.Thread(target=self.map_thread, daemon=True).start()

        # threading.Thread(target=self.listen_to_central, args=(conn,), daemon=True).start()

        while True:
            time.sleep(1)

if __name__ == "__main__":

    if len(sys.argv) != 8:
        logger.error(f"Arguments received: {sys.argv}")
        logger.error("Usage: python EC_DE.py <EC_Central_IP> <EC_Central_Port> <Kafka_Broker> <EC_S_IP> <EC_S_Port> <Taxi_ID>")
        sys.exit(1)
    
    ec_central_ip = sys.argv[1]
    ec_central_port = int(sys.argv[2])
    kafka_broker = sys.argv[3]
    ec_s_ip = sys.argv[4]
    ec_s_port = int(sys.argv[5])
    taxi_id = int(sys.argv[6])
    ec_registry_port = int(sys.argv[7])
    
    digital_engine = DigitalEngine(ec_central_ip, ec_central_port, kafka_broker, ec_s_ip, ec_s_port, taxi_id, ec_registry_port)
    digital_engine.run()



