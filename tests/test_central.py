import pytest
import socket
from unittest.mock import MagicMock, patch
from ec_central import ECCentral

# Test para la autenticación de taxis en la central
def test_taxi_authentication_success():
    central = ECCentral("localhost:9092", 5000)
    taxi_id = 1

    # Mock de la conexión de socket
    with patch('socket.socket') as mock_socket:
        conn = MagicMock()
        conn.recv.return_value = str(taxi_id).encode('utf-8')
        mock_socket.return_value.accept.return_value = (conn, ('127.0.0.1', 12345))
        
        # Mock de carga de taxis para que siempre devuelva el taxi con ID 1
        with patch.object(central, 'load_taxis', return_value={1: MagicMock()}):
            central.handle_taxi_auth(conn, ('127.0.0.1', 12345))
            
            # Verifica que se envió el mensaje de éxito
            conn.sendall.assert_called_with(b"AUTH_SUCCESS")

def test_taxi_authentication_failure():
    central = ECCentral("localhost:9092", 5000)
    taxi_id = 99

    # Mock de la conexión de socket
    with patch('socket.socket') as mock_socket:
        conn = MagicMock()
        conn.recv.return_value = str(taxi_id).encode('utf-8')
        mock_socket.return_value.accept.return_value = (conn, ('127.0.0.1', 12345))
        
        # Mock de carga de taxis para que no contenga el ID 99
        with patch.object(central, 'load_taxis', return_value={}):
            central.handle_taxi_auth(conn, ('127.0.0.1', 12345))
            
            # Verifica que se envió el mensaje de fallo
            conn.sendall.assert_called_with(b"AUTH_FAILURE")

def test_process_customer_request_success():
    central = ECCentral("localhost:9092", 5000)
    
    # Mock de taxis disponibles
    with patch.object(central, 'load_taxis', return_value={1: MagicMock(status='FREE')}):
        with patch.object(central, 'save_taxis') as mock_save:
            # Mock de productor Kafka
            with patch.object(central, 'producer') as mock_producer:
                request = {'customer_id': 'customer_1', 'destination': 'A'}
                central.process_customer_request(request)
                
                # Verifica que se llamó a save_taxis
                mock_save.assert_called()
                # Verifica que se envió la instrucción al taxi
                mock_producer.send.assert_called()

def test_process_customer_request_failure():
    central = ECCentral("localhost:9092", 5000)
    
    # Mock sin taxis disponibles
    with patch.object(central, 'load_taxis', return_value={}):
        with patch.object(central, 'producer') as mock_producer:
            request = {'customer_id': 'customer_2', 'destination': 'A'}
            result = central.process_customer_request(request)
            
            # Verifica que no se envió ninguna instrucción al taxi y se devolvió False
            assert not result
            mock_producer.send.assert_not_called()
