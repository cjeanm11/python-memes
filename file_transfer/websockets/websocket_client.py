import asyncio
import websockets
import logging

class WebSocketClient:
    def __init__(self, uri: str, client_id: str):
        self.uri = uri
        self.client_id = client_id

    async def connect(self):
        """Establishes a WebSocket connection and performs communication."""
        try:
            async with websockets.connect(f"{self.uri}/{self.client_id}") as websocket:
                logging.info(f"Connected to WebSocket server as {self.client_id}")
                send_task = asyncio.create_task(self.send(websocket))
                receive_task = asyncio.create_task(self.receive(websocket))
                await asyncio.gather(send_task, receive_task)
        except websockets.ConnectionClosedError:
            logging.error("WebSocket connection closed unexpectedly.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    async def send(self, websocket):
        """Continuously sends messages through the WebSocket."""
        try:
            message = input("Enter message: ")
            await websocket.send(message)
            logging.info(f"Message sent: {message}")
        except Exception as e:
            logging.error(f"Failed to send message: {e}")

    async def receive(self, websocket):
        """Continuously receives messages from the WebSocket."""
        try:
            response = await websocket.recv()
            logging.info(f"Message received: {response}")
        except websockets.ConnectionClosedError:
            logging.error("WebSocket connection closed unexpectedly while receiving.")
        except Exception as e:
            logging.error(f"An error occurred while receiving: {e}")

async def main():
    uri = 'ws://localhost:8765'
    client_id = input("Enter your client ID (e.g., client1 or client2): ")
    client = WebSocketClient(uri=uri, client_id=client_id)
    await client.connect()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())