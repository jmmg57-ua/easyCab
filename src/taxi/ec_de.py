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
        self.instruction_queue = queue.Queue()  # Cola para las instrucciones
        self.picked_off = 0
        self.central_disconnected = False
        self.sensor_connected = False  # Estado inicial de conexión del sensor
        self.processing_instruction = False
        self.trip_ended_sent = False  # Flag para evitar duplicados
        self.ko = 0

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
            self.color = "RED"
            self.send_position_update()  # Notify central about the stop
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
            logger.info("Leaving the customer and returning to base")
            self.send_position_update()
            self.move_to_destination()

        elif instruction['type'] == 'CHANGE':
            logger.info("INSTRUCCION CHANGE")
            if isinstance(instruction['destination'], (list, tuple)):
                self.destination = instruction['destination']
                self.color = "GREEN"
                self.send_position_update()
                self.move_to_destination()
            else:
                logger.error(f"Invalid destination format: {instruction['destination']}")

    def check_for_interrupt(self):
        if not self.instruction_queue.empty():
            instruction = self.instruction_queue.get()  # Extraer nueva instrucción
            logger.info(f"Interrupting current operation for new instruction: {instruction}")
            self.process_instruction(instruction)
            return True
        return False


    def move_to_destination(self):
        try:
            if not isinstance(self.destination, (list, tuple)):
                logger.error(f"Invalid destination: destination={self.destination}")
                return

            # Mover al punto de recogida
            while self.position != self.pickup and self.picked_off == 0:
                if self.check_for_interrupt():
                    break  # Interrumpir el movimiento si hay una nueva instrucción
                self.move_towards(self.pickup)

            # Marcar al cliente como recogido
            if self.position == self.pickup:
                self.picked_off = 1
                logger.info(f"Customer picked up at position {self.position}. Moving to destination.")

            # Mover al destino
            while self.position != self.destination and self.picked_off == 1:
                if self.check_for_interrupt():
                    break  # Interrumpir el movimiento si hay una nueva instrucción
                self.move_towards(self.destination)

            # Finalizar el viaje si llega al destino
            if self.position == self.destination:
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
                    "picked_off" : self.picked_off
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
        if self.ko == 0:
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
            tempstatus = self.status
            if self.ko == 1:
                tempstatus = "KO"
            update = {
                'taxi_id': self.taxi_id,
                'status': tempstatus,
                'color': self.color,
                'position': self.position,
                'customer_id': self.customer_asigned,
                'picked_off': self.picked_off,
            }
            logger.info("Sending update through 'taxi_updates'")
            try:
                self.producer.send('taxi_updates', update)
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

                if data == "KO" and self.ko == 0:
                    self.ko = 1
                    self.color = "RED"
                    self.status = "KO"
                    self.processing_instruction = True
                    logger.info("Taxi set to STOP. Awaiting RESUME command.")
                    self.send_position_update()

                elif data == "OK" and self.ko == 1:
                    self.ko = 0
                    self.color = "GREEN"
                    self.status = "OK"
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
                    else:
                        print(f"Error al registrarse: {response.json().get('error', 'Desconocido')}")
                except requests.exceptions.RequestException as e:
                    print(f"Error al conectar con Registry: {e}")

            elif choice == "2":  # Darse de baja
                try:
                    response = requests.delete(f"{registry_url}/deregister/{self.taxi_id}", verify="./certServ.pem")
                    if response.status_code == 200:  # Baja exitosa
                        print("Se dio de baja exitosamente. Cerrando el programa.")
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


