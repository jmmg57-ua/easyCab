import socket
import sys
import time
import threading

class Sensors:
    def __init__(self, ec_de_ip, ec_de_port):
        self.ec_de_ip = ec_de_ip
        self.ec_de_port = ec_de_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.status = "OK"

    def connect_to_digital_engine(self):
        try:
            self.socket.connect((self.ec_de_ip, self.ec_de_port))
            print(f"Connected to Digital Engine at {self.ec_de_ip}:{self.ec_de_port}")
            return True
        except Exception as e:
            print(f"Failed to connect to Digital Engine: {e}")
            return False

    def send_status(self):
        while True:
            try:
                self.socket.send(self.status.encode())
                time.sleep(1)  # Send status every second
            except Exception as e:
                print(f"Error sending status: {e}")
                break

    def listen_for_user_input(self):
        print("Press 'i' to simulate an incident, 'r' to resolve the incident")
        while True:
            user_input = input().lower()
            if user_input == 'i':
                self.status = "KO"
                print("Incident simulated. Status set to KO")
            elif user_input == 'r':
                self.status = "OK"
                print("Incident resolved. Status set to OK")

    def run(self):
        if not self.connect_to_digital_engine():
            return

        threading.Thread(target=self.send_status, daemon=True).start()
        self.listen_for_user_input()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python EC_S.py <EC_DE_IP> <EC_DE_Port>")
        sys.exit(1)

    ec_de_ip = sys.argv[1]
    ec_de_port = int(sys.argv[2])

    sensors = Sensors(ec_de_ip, ec_de_port)
    sensors.run()