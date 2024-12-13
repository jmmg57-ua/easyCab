import os
import signal
import socket
import sys
import threading
import time
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError  
import numpy as np
import queue
import requests
import ssl



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
        self.destination = None
        self.customer_asigned = "x"
        self.locations = {}
        self.taxis = {}
        self.picked_off = 0
        self.central_disconnected = False
        self.sensor_connected = False  # Estado inicial de conexión del sensor
        self.processing_instruction = False
        self.trip_ended_sent = False  # Flag para evitar duplicados
        self.ko = 0
        self.central_socket = None
        self.traffic_stopped = False
        self.previous_status = None

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
                        self.process_instruction(data)
                elif message.topic == 'map_updates':
                    logger.info("Received message on topic 'map_updates'")
                    self.handle_map_updates(message.value)
                # self.draw_map()

        except KafkaError as e:
            logger.error(f"Lost connection to Kafka: {e}")
            # self.central_disconnected = True
            # self.continue_independently()
        except Exception as e:
            logger.error(f"General error in kafka_listener: {e}")
            time.sleep(5)

    def comprobador_cola(self):
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

    def process_instruction(self, instruction):
        if instruction['type'] == 'STOP':
            logger.info("INSTRUCCION STOP")
            self.traffic_stopped = True
            self.status = "KO"
            self.color = "RED"
            self.customer_asigned = "x"
            self.destination = None
            self.picked_off = 0
            self.send_position_update()  # Notify central about the stop
            self.save_state()
            return  # No further actions until RESUME is received

        elif instruction['type'] == 'RESUME':
            logger.info("INSTRUCCION RESUME")
            self.color = "GREEN"
            self.send_position_update()
            self.move_to_destination()

        elif instruction['type'] == 'MOVE':
            self.trip_ended_sent = False
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
            self.move_to_destination()

        elif instruction['type'] == 'RETURN_TO_BASE':
            logger.info("INSTRUCCION RETURN")
            self.picked_off = 1
            self.destination = [1, 1]
            self.status = "BUSY"
            self.color = "GREEN"
            self.customer_asigned = "x"
            self.destination = None
            self.picked_off = 0
            self.send_position_update()
            self.move_to_destination()
            # Verificar si el taxi llegó a la base
            if self.position == [1, 1]:
                logger.info(f"Taxi {self.taxi_id} has returned to base.")
                self.disconnect()
            

        elif not self.traffic_stopped:
            if instruction['type'] == 'MOVE':
                self.trip_ended_sent = False
                self.pickup = instruction['pickup']
                logger.info("INSTRUCCION MOVE")
                self.status = "BUSY"
                self.color = "GREEN"
                self.customer_asigned = instruction['customer_id']
                if isinstance(instruction['destination'], (list, tuple)):
                    self.destination = instruction['destination']
                else:
                    logger.error(f"Instrucción {instruction['type']} ignorada - Tráfico detenido (Estado: KO)")
                    return
                self.move_to_destination()

            elif instruction['type'] == 'RETURN_TO_BASE':
                logger.info("INSTRUCCION RETURN")
                self.pickup = 0
                self.destination = [1, 1]
                self.status = "BUSY"
                self.color = "GREEN"
                self.customer_asigned = "x"
                logger.info("Leaving the customer and returning to base")
                self.send_position_update()
                self.move_to_destination()

    def check_for_interrupt(self):
        if not self.instruction_queue.empty():
            instruction = self.instruction_queue.get()  # Extraer nueva instrucción
            logger.info(f"Interrupting current operation for new instruction: {instruction}")
            self.process_instruction(instruction)
            return True
        return False


    def save_state(self):
        """Guarda el estado actual del taxi en el archivo."""
        try:
            with open('taxis.txt', 'r') as file:
                lines = file.readlines()
            
            # Encontrar y actualizar la línea correspondiente a este taxi
            for i, line in enumerate(lines):
                taxi_data = line.strip().split('#')
                if int(taxi_data[0]) == self.taxi_id:
                    lines[i] = f"{self.taxi_id}#{self.status}#{self.color}#{self.position[0]}#{self.position[1]}#{self.customer_asigned}#{self.picked_off}#{1}\n"
                    break
            
            with open('taxis.txt', 'w') as file:
                file.writelines(lines)
            
            logger.info(f"Estado del taxi {self.taxi_id} actualizado en archivo")
        except Exception as e:
            logger.error(f"Error al guardar estado: {e}")
    def move_to_destination(self):
        try:
            if self.traffic_stopped or self.status == "KO":
                logger.info("No se puede mover: Tráfico detenido o estado KO")
                return
            
            if not isinstance(self.destination, (list, tuple)):
                logger.error(f"Invalid destination: destination={self.destination}")
                return

            # Mover al punto de recogida
            while self.position != self.pickup and self.picked_off == 0:
                if self.check_for_interrupt():
                    break  # Interrumpir el movimiento si hay una nueva instrucción
                self.move_towards(self.pickup)

            # Marcar al cliente como recogido
            if self.position == self.pickup and not self.traffic_stopped:
                self.picked_off = 1
                logger.info(f"Customer picked up at position {self.position}. Moving to destination.")

            # Mover al destino
            while self.position != self.destination and self.picked_off == 1:
                if self.check_for_interrupt():
                    break  # Interrumpir el movimiento si hay una nueva instrucción
                self.move_towards(self.destination)

            # Finalizar el viaje si llega al destino
            if self.position == self.destination and not self.traffic_stopped:
                self.finalize_trip()

        except Exception as e:
            logger.error(f"Error in move_to_destination: {e}")


    def finalize_trip(self):
        """Handle trip completion and reset taxi state."""
        try:
            if not self.trip_ended_sent:

                self.color = "RED"  # Stop the taxi after completing the trip
                self.status = "END"  # Mark trip as ended
                logger.info("TRIP ENDED!!")
                
                # Notify central of trip completion
                update = {
                    "customer_id": self.customer_asigned,
                    "status": "END",
                    "taxi_id": self.taxi_id,
                    "position": self.position,
                    "color" : self.color,
                    "picked_off" : self.picked_off,
                    "token": self.token
                }
                self.producer.send('taxi_updates', value=update)
                logger.info(f"Trip completed sending to customer {self.customer_asigned}: {update}")

                time.sleep(4)

                # Reset taxi state
                self.customer_asigned = "x"  # Reset to no assigned customer
                self.picked_off = 0
                self.status = "FREE"
                self.destination = None
                self.color = "RED"  # Ready for the next trip

                # Notify central of the taxi being free
                self.send_position_update()

        except KeyError as e:
            logger.error(f"Key error when processing update: missing key {e}")
        except Exception as e:
            logger.error(f"Error finalizing trip: {e}")





            
    def move_towards(self, target):
        if self.traffic_stopped or self.status == "KO":
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
        
        self.send_position_update()
        # self.draw_map()
        time.sleep(1)  

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
                self.save_state()
            except KafkaError as e:
                logger.error(f"Error sending update: {e}")
        else:
            logger.warning("Socket is closed, cannot send update.")

    def handle_map_updates(self, message):
        """
        Procesa las actualizaciones de mapa recibidas a través de Kafka y
        almacena las ubicaciones y taxis en los atributos de la clase.
        """
        # Almacenar la última actualización del mapa y procesar ubicaciones y taxis
        self.locations = {loc_id: location for loc_id, location in message['locations'].items()}
        self.taxis = {taxi_id: taxi_info for taxi_id, taxi_info in message['taxis'].items()}

        logger.info("Map updated in Digital Engine")



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

                if data == "KO":
                    self.color = "RED"
                    self.processing_instruction = True
                    logger.info("Taxi set to STOP. Awaiting RESUME command.")
                    self.send_position_update()

                elif data == "OK":
                    self.color = "GREEN"
                    if not self.processing_instruction:
                        logger.info("Taxi set to RESUME.")
                        self.processing_instruction = True
                        self.move_to_destination()

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
        threading.Thread(target=self.comprobador_cola, daemon=True).start()

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



