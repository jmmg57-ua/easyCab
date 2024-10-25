import pytest
from unittest.mock import MagicMock, patch
from ec_de import DigitalEngine

def test_authenticate_success():
    with patch('socket.socket') as mock_socket:
        conn = MagicMock()
        conn.recv.return_value = b"OK"
        mock_socket.return_value = conn
        digital_engine = DigitalEngine("127.0.0.1", 5000, "localhost:9092", "127.0.0.1", 6000, 1)
        assert digital_engine.authenticate() is True

def test_authenticate_failure():
    with patch('socket.socket') as mock_socket:
        conn = MagicMock()
        conn.recv.return_value = b"FAIL"
        mock_socket.return_value = conn
        digital_engine = DigitalEngine("127.0.0.1", 5000, "localhost:9092", "127.0.0.1", 6000, 1)
        assert not digital_engine.authenticate()

def test_process_instruction_move():
    digital_engine = DigitalEngine("127.0.0.1", 5000, "localhost:9092", "127.0.0.1", 6000, 1)
    
    instruction = {'taxi_id': 1, 'type': 'MOVE', 'destination': [5, 5]}
    with patch.object(digital_engine, 'move_to_destination') as mock_move:
        digital_engine.process_instruction(instruction)
        mock_move.assert_called()

def test_process_instruction_stop():
    digital_engine = DigitalEngine("127.0.0.1", 5000, "localhost:9092", "127.0.0.1", 6000, 1)
    
    instruction = {'taxi_id': 1, 'type': 'STOP'}
    digital_engine.process_instruction(instruction)
    assert digital_engine.state == "STOPPED"
