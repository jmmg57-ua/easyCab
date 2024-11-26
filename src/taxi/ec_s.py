import socket
import sys
import time
import threading
import random
import logging



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

class Sensors:
    def __init__(self, ec_de_ip, ec_de_port):
        self.ec_de_ip = ec_de_ip
        self.ec_de_port = ec_de_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.status = "OK"
        self.running = True

    def connect_to_digital_engine(self):
        try:
            self.socket.connect((self.ec_de_ip, self.ec_de_port))
            logger.info(f"Connected to Digital Engine at {self.ec_de_ip}:{self.ec_de_port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Digital Engine: {e}")
            return False

    def send_status(self):
        while self.running:
            try:
                logger.info(f"Sending status: {self.status}")
                self.socket.send(self.status.encode())
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error sending status: {e}")
                break


    def handle_input(self):
        while self.running:
            try:
                print("Press any key to toggle status between 'KO' and 'OK'...")
                input_recieved = input()
                if input_recieved:
                    if self.status == "OK":
                        self.status = "KO"
                        logger.info("Sensor status changed to KO.")
                    else:
                        self.status = "OK"
                        logger.info("Sensor status changed to OK.")
                    
            except Exception as e:
                logger.error(f"Error handling input: {e}")
                self.running = False

    def run(self):
        if not self.connect_to_digital_engine():
            return

        threading.Thread(target=self.send_status, daemon=True).start()
        threading.Thread(target=self.handle_input, daemon=True).start()
        
        try:
            while self.running:
                time.sleep(1)  
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.running = False  

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logger.error("Usage: python EC_S.py <EC_DE_IP> <EC_DE_Port>")
        sys.exit(1)

    ec_de_ip = sys.argv[1]
    ec_de_port = int(sys.argv[2])

    sensors = Sensors(ec_de_ip, ec_de_port)
    sensors.run()

