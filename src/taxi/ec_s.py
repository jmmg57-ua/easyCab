import socket
import sys
import time
import threading
import logging

# Configurar el logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

class Sensors:
    def __init__(self, ec_de_ip, ec_de_port):
        self.ec_de_ip = ec_de_ip
        self.ec_de_port = ec_de_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.status = "OK"

    def connect_to_digital_engine(self):
        try:
            self.socket.connect((self.ec_de_ip, self.ec_de_port))
            logger.info(f"Connected to Digital Engine at {self.ec_de_ip}:{self.ec_de_port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Digital Engine: {e}")
            return False

    def send_status(self):
        while True:
            try:
                self.socket.send(self.status.encode())
                time.sleep(1)  # Send status every second
            except Exception as e:
                logger.error(f"Error sending status: {e}")
                break
#COMO MANEJAR LOS IMPUTS
    def listen_for_user_input(self):
        logger.info("Press 'i' to simulate an incident, 'r' to resolve the incident")
        while True:
            user_input = input().lower()
            if user_input == 'i':
                self.status = "KO"
                logger.info("Incident simulated. Status set to KO")
            elif user_input == 'r':
                self.status = "OK"
                logger.info("Incident resolved. Status set to OK")

    def run(self):
        if not self.connect_to_digital_engine():
            return
#POR QUE THREAD AQUI?
        threading.Thread(target=self.send_status, daemon=True).start()
        ##TIENE QUE MANDAR UN MENSAJE CADA SEGUNDO
        self.listen_for_user_input()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logger.error("Usage: python EC_S.py <EC_DE_IP> <EC_DE_Port>")
        sys.exit(1)

    ec_de_ip = sys.argv[1]
    ec_de_port = int(sys.argv[2])

    sensors = Sensors(ec_de_ip, ec_de_port)
    sensors.run()
