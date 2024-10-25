import pytest
from unittest.mock import MagicMock, patch
from ec_customer import Customer

def test_setup_kafka_success():
    with patch('kafka.KafkaProducer') as mock_producer, \
         patch('kafka.KafkaConsumer') as mock_consumer:
        customer = Customer("localhost:9092", "customer_1", "services.txt")
        assert customer.producer is not None
        assert customer.consumer is not None

def test_request_service_success():
    customer = Customer("localhost:9092", "customer_1", "services.txt")

    with patch.object(customer.producer, 'send') as mock_send:
        customer.request_service('A')
        mock_send.assert_called()

def test_request_service_failure():
    customer = Customer("localhost:9092", "customer_1", "services.txt")

    with patch.object(customer.producer, 'send', side_effect=Exception("Kafka error")) as mock_send:
        customer.request_service('B')
        mock_send.assert_called()
        # Se debe capturar el error y no lanzar excepción

def test_wait_for_confirmation_success():
    customer = Customer("localhost:9092", "customer_1", "services.txt")

    # Mock del consumidor Kafka
    with patch.object(customer.consumer, '_iter_', return_value=[{'customer_id': 'customer_1', 'status': 'OK'}]):
        assert customer.wait_for_confirmation() is True

def test_wait_for_confirmation_failure():
    customer = Customer("localhost:9092", "customer_1", "services.txt")

    # Mock del consumidor Kafka
    with patch.object(customer.consumer, '_iter_', return_value=[{'customer_id': 'customer_1', 'status': 'KO'}]):
        assert customer.wait_for_confirmation() is False
