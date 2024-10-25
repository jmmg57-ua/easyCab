import pytest
from unittest.mock import MagicMock, patch
from ec_s import Sensor

def test_send_ok_state():
    sensor = Sensor("127.0.0.1", 6000)
    with patch.object(sensor, 'send_state') as mock_send:
        sensor.send_state("OK")
        mock_send.assert_called_with("OK")

def test_send_ko_state():
    sensor = Sensor("127.0.0.1", 6000)
    with patch.object(sensor, 'send_state') as mock_send:
        sensor.send_state("KO")
        mock_send.assert_called_with("KO")

def test_manual_error_trigger():
    sensor = Sensor("127.0.0.1", 6000)
    with patch.object(sensor, 'send_state') as mock_send:
        sensor.trigger_error()
        mock_send.assert_called_with("KO")
