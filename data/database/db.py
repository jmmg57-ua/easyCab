import mysql.connector

# Configuración de la conexión
config = {
    'user': 'root',
    'password': 'josefire29',
    'host': 'localhost',  # o la dirección IP de tu servidor MySQL
    'database': 'easyCab'  # El nombre de tu base de datos
}

# Crear una conexión
try:
    conn = mysql.connector.connect(**config)
    print("Conexión exitosa")
except mysql.connector.Error as err:
    print(f"Error: {err}")
