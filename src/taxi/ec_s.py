import socket
import sys
import time
import threading
import logging
import tkinter as tk


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
        self.root = tk.Tk()
        self.root.title("Sensor Control")
        self.label = tk.Label(self.root, text="Sensor Status")
        self.label.pack()
        self.status_label = tk.Label(self.root, text="Status: OK", fg="green")
        self.status_label.pack()
        self.button_ko = tk.Button(self.root, text="Simulate Incident", command=self.set_ko)
        self.button_ko.pack()
        self.button_ok = tk.Button(self.root, text="Resolve Incident", command=self.set_ok)
        self.button_ok.pack()
        
    def set_ko(self):
        self.status = "KO"
        self.status_label.config(text="Status: KO", fg="red")
        logger.info("Incident simulated. Status set to KO")

    def set_ok(self):
        self.status = "OK"
        self.status_label.config(text="Status: OK", fg="green")
        logger.info("Incident resolved. Status set to OK")

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
                logger.info(f"Sending status: {self.status}")
                self.socket.send(self.status.encode())
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error sending status: {e}")
                break
            
    def run(self):
        if not self.connect_to_digital_engine():
            return
        threading.Thread(target=self.send_status, daemon=True).start()
        self.root.mainloop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logger.error("Usage: python EC_S.py <EC_DE_IP> <EC_DE_Port>")
        sys.exit(1)

    ec_de_ip = sys.argv[1]
    ec_de_port = int(sys.argv[2])

    sensors = Sensors(ec_de_ip, ec_de_port)
    sensors.run()
