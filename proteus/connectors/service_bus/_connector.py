"""
    Connector for Azure Service Bus.
"""
from typing import Optional
import os
from azure.servicebus import ServiceBusSender, ServiceBusClient, TransportType, ServiceBusMessage


class AzureServiceBusConnector:
    """
    Connector for Azure Service Bus.
    """

    def __init__(self, conn_str: Optional[str] = None, queue_name: Optional[str] = None):
        self.service_bus_client: ServiceBusClient = ServiceBusClient.from_connection_string(
            conn_str=conn_str if conn_str is not None else os.environ.get('PROTEUS__SERVICE_BUS_CONNECTION_STRING'),
            transport_type=TransportType.Amqp,
        )
        self.sender: ServiceBusSender = self.service_bus_client.get_queue_sender(
            queue_name=queue_name if queue_name is not None else os.environ.get('PROTEUS__SERVICE_BUS_QUEUE')
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()

    def send_message(self, message: str) -> None:
        """
        Send string message to service bus
        """
        sb_message = ServiceBusMessage(message)
        self.sender.send_messages(sb_message)

    def dispose(self) -> None:
        """
        Gracefully dispose object.
        """
        self.sender.close()
        self.service_bus_client.close()
