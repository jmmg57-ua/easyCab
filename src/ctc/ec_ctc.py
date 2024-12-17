import requests
import json
import time

NODEJS_URL = "http://localhost:5000"
current_city = "Alicante"
current_api_key = "70656a4c102bee1ed903fcb7e3938267"

def update_nodejs_city(city):
    try:
        response = requests.post(f"{NODEJS_URL}/api/city/{city}")
        if response.status_code == 200:
            print(f"Ciudad actualizada: {city}")
            return True
        else:
            print("Error al actualizar la ciudad")
            return False
    except Exception as e:
        print(f"Error al comunicarse con el servidor: {e}")
        return False

def get_traffic_status():
    try:
        response = requests.get(f"{NODEJS_URL}/api/traffic-status")
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print("Error al obtener el estado del tráfico")
            return None
    except Exception as e:
        print(f"Error al comunicarse con el servidor: {e}")
        return None

def update_api_key(new_key):
    try:
        response = requests.post(f"{NODEJS_URL}/api/apikey/{new_key}")
        if response.status_code == 200:
            global current_api_key
            current_api_key = new_key
            print(f"API Key actualizada correctamente")
            return True
        else:
            print("Error al actualizar la API Key")
            return False
    except Exception as e:
        print(f"Error al comunicarse con el servidor: {e}")
        return False

def main_menu():
    global current_city
    print("\n=== EASY CAB - City Traffic Control ===")
    
    # Esperar a que el servidor Node.js esté listo
    print("Esperando a que el servidor esté listo...")
    time.sleep(2)
    
    # Obtener estado inicial
    status_data = get_traffic_status()
    if status_data:
        print(f"\nCiudad actual: {status_data['city']}")
        print(f"Temperatura: {status_data['temperature']}°C")
        print(f"Estado del tráfico: {status_data['status']}")

    while True:
        print("\nOpciones:")
        print("1. Cambiar ciudad")
        print("2. Ver estado actual")
        print("3. Cambiar API Key")
        print("4. Ver API Key actual")
        print("q. Salir")
        
        option = input("\nSeleccione una opción: ")
        
        if option == "1":
            new_city = input("Ingrese el nombre de la nueva ciudad: ")
            if update_nodejs_city(new_city):
                current_city = new_city
                status_data = get_traffic_status()
                if status_data:
                    print(f"\nCiudad actual: {status_data['city']}")
                    print(f"Temperatura: {status_data['temperature']}°C")
                    print(f"Estado del tráfico: {status_data['status']}")
        
        elif option == "2":
            status_data = get_traffic_status()
            if status_data:
                print(f"\nCiudad actual: {status_data['city']}")
                print(f"Temperatura: {status_data['temperature']}°C")
                print(f"Estado del tráfico: {status_data['status']}")
        
        elif option == "3":
            new_key = input("Ingrese la nueva API Key: ")
            update_api_key(new_key)

        elif option == "4":
            print(f"API Key actual: {current_api_key}")

        elif option.lower() == "q":
            print("Saliendo...")
            break
        
        else:
            print("Opción no válida. Por favor, intente de nuevo.")

if __name__ == "__main__":
    main_menu()
