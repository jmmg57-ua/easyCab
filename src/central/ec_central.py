import os
import logging
from kafka import KafkaConsumer, KafkaProducer
import json
import numpy as np
from dataclasses import dataclass
from typing import Dict, Tuple, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Taxi:
    id: int
    position: Tuple[int, int]
    status: str  # 'FREE', 'BUSY'
    color: str # 'RED', 'GREEN'

@dataclass
class Location:
    id: str
    position: Tuple[int, int]

class ECCentral:
    def __init__(self):
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.producer = None
        self.consumer = None
        self.map_size = (20, 20)
        self.map = np.full(self.map_size, ' ', dtype=str)
        self.locations: Dict[str, Location] = {}
        self.taxis_file = '/data/taxis.txt'  # Ruta al fichero de taxis
        
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
                    taxi_id, status, color, pos_x, pos_y = line.strip().split('#')
                    taxis[int(taxi_id)] = Taxi(
                        id=int(taxi_id),
                        position=(int(pos_x), int(pos_y)),
                        status=status,
                        color=color
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
                    f.write(f"{taxi.id}#{taxi.status}#{taxi.color}#{taxi.position[0]}#{taxi.position[1]}\n")
            logger.info("Taxis saved to file")
        except Exception as e:
            logger.error(f"Error saving taxis to file: {e}")

    def update_map(self, taxis):
        # Reset map except for locations
        self.map = np.full(self.map_size, ' ', dtype=str)
        
        # Add locations
        for loc in self.locations.values():
            x, y = loc.position
            self.map[y, x] = loc.id
            
        # Add taxis
        for taxi in taxis.values():
            x, y = taxi.position
            self.map[y, x] = str(taxi.id)
        
        self.broadcast_map(taxis)

    def broadcast_map(self, taxis):
        if self.producer:
            try:
                map_data = {
                    'map': self.map.tolist(),
                    'taxis': {k: {'position': v.position, 'status': v.status} 
                             for k, v in taxis.items()},
                    'locations': {k: {'position': v.position} 
                                 for k, v in self.locations.items()}
                }
                self.producer.send('map_updates', map_data)
            except Exception as e:
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
        
        if destination not in self.locations:
            logger.error(f"Invalid destination: {destination}")
            return False

        # Cargar los taxis desde el fichero
        taxis = self.load_taxis()

        # Simple taxi selection: choose the first idle taxi
        available_taxi = next((taxi for taxi in taxis.values() if taxi.status == 'IDLE'), None)
        
        if available_taxi:
            available_taxi.status = 'MOVING'
            available_taxi.destination = self.locations[destination].position
            
            # Actualizar estado del taxi en el fichero
            self.save_taxis(taxis)

            # Enviar instrucciones al taxi
            self.producer.send('taxi_instructions', {
                'taxi_id': available_taxi.id,
                'instruction': 'PICKUP',
                'customer_id': customer_id,
                'destination': self.locations[destination].position
            })
            logger.info(f"Assigned taxi {available_taxi.id} to customer {customer_id}")
            return True
        else:
            logger.warning("No available taxis")
            return False

    def process_taxi_update(self, update):
        taxi_id = update['taxi_id']

        # Cargar taxis desde el fichero
        taxis = self.load_taxis()

        if taxi_id not in taxis:
            taxis[taxi_id] = Taxi(taxi_id, (1, 1), 'IDLE')
        
        taxi = taxis[taxi_id]
        if 'position' in update:
            taxi.position = tuple(update['position'])
        if 'status' in update:
            taxi.status = update['status']
        
        # Guardar los cambios en el fichero
        self.save_taxis(taxis)

        # Actualizar el mapa
        self.update_map(taxis)

    def run(self):
        if not self.connect_kafka():
            return

        self.load_map_config()
        logger.info("EC_Central is running...")
        
        try:
            for message in self.consumer:
                topic = message.topic
                data = message.value
                
                if topic == 'customer_requests':
                    self.process_customer_request(data)
                elif topic == 'taxi_updates':
                    self.process_taxi_update(data)
                
        except KeyboardInterrupt:
            logger.info("Shutting down EC_Central...")
        finally:
            if self.producer:
                self.producer.close()
            if self.consumer:
                self.consumer.close()

if __name__ == "__main__":
    central = ECCentral()
    central.run()
