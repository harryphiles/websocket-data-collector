import time
import threading
import json
from queue import Queue
from abc import ABC, abstractmethod
from typing import Any
import requests
import websocket


class BinanceClient(ABC):
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = ""
        self.ws_url = ""

    @abstractmethod
    def get_listen_key(self) -> str:
        pass

    @abstractmethod
    def keep_alive_listen_key(self, listen_key: str) -> None:
        pass


class BinanceTestnetClient(BinanceClient):
    def __init__(self, api_key: str):
        super().__init__(api_key)
        self.base_url = "https://testnet.binance.vision"
        self.ws_url = "wss://testnet.binance.vision"

    def get_listen_key(self) -> str:
        response = requests.post(
            f"{self.base_url}/api/v3/userDataStream",
            headers={"X-MBX-APIKEY": self.api_key},
        )
        return response.json()["listenKey"]

    def keep_alive_listen_key(self, listen_key: str) -> None:
        endpoint = f"{self.base_url}/userDataStream"
        headers = {"X-MBX-APIKEY": self.api_key}
        params = {"listenKey": listen_key}
        requests.put(endpoint, headers=headers, params=params)


class WebSocketHandler:
    def __init__(self, client: BinanceClient):
        self.client = client
        self.ws: websocket.WebSocketApp = None
        self.message_queue = Queue()

    def on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        data = json.loads(message)
        self.message_queue.put(data)
        print(
            f"Message received {data["E"]} -> queued | Queue size: {self.message_queue.qsize()}"
        )

    def on_error(self, ws: websocket.WebSocketApp, error: Any) -> None:
        print(f"Error: {error}")

    def on_close(
        self, ws: websocket.WebSocketApp, close_status_code: int, close_msg: str
    ) -> None:
        print("WebSocket connection closed")

    def on_open(self, ws: websocket.WebSocketApp) -> None:
        print("WebSocket connection opened")

    def process_messages(self):
        while True:
            message = self.message_queue.get()
            self.store_in_db(message)
            print(f"Remaining queue size: {self.message_queue.qsize()}")
            self.message_queue.task_done()

    def store_in_db(self, message: dict[str, Any]):
        """Store data in DB"""
        print(f"Storing in DB: {message["e"] = }")
        time.sleep(0.1)

    def run(self) -> None:
        listen_key = self.client.get_listen_key()
        print(f"Listen Key: {listen_key}")
        socket_url = f"{self.client.ws_url}/ws/{listen_key}"

        self.ws = websocket.WebSocketApp(
            socket_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )

        # Start the message processing thread
        processing_thread = threading.Thread(target=self.process_messages, daemon=True)
        processing_thread.start()

        try:
            self.ws.run_forever()
        except KeyboardInterrupt:
            self.ws.close()


def main():
    from config import RSA_KEY_REGISTERED

    client = BinanceTestnetClient(RSA_KEY_REGISTERED)
    handler = WebSocketHandler(client)
    handler.run()


if __name__ == "__main__":
    main()
