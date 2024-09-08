import asyncio
import websockets
import logging

class WebSocketServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.clients = {} 

    async def handler(self, websocket, path):
        """Handles incoming WebSocket connections."""
        client_id = path.strip("/") 
        if not client_id:
            print("Client ID missing in the path!")
            await websocket.close()
            return
        
        print(f"Client {client_id} connected with path: {path}")
        self.clients[client_id] = websocket

        try:
            async for message in websocket:
                print(f"Received message from {client_id}: {message}")
                await self.broadcast(message, client_id)
        except websockets.ConnectionClosedError:
            print(f"Client {client_id} disconnected")
        finally:
            del self.clients[client_id]

    async def broadcast(self, message: str, sender_id: str):
        """Broadcast the message to all clients except the sender."""
        for client_id, client in self.clients.items():
            if client_id != sender_id: 
                try:
                    print(f"Sending message from {sender_id} to {client_id}: {message}")
                    await client.send(f"{sender_id}: {message}")
                except websockets.ConnectionClosedError:
                    print(f"Connection closed for client {client_id}")
                except Exception as e:
                    print(f"Error sending message to {client_id}: {e}")

    async def start(self):
        """Starts the WebSocket server."""
        server = await websockets.serve(self.handler, self.host, self.port)
        print(f"WebSocket server is running on ws://{self.host}:{self.port}")
        await asyncio.Future() 

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    server = WebSocketServer(host="localhost", port=8765)
    asyncio.run(server.start())