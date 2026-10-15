
import time
import websocket


class BinanceClient:
    def __init__(self, socket, on_message):
        self.socket = socket
        self.message_callback = on_message
        self.min_reconnect_delay = 5
        self.max_reconnect_delay = 60
        self.ws = None
        self.connected = False

    def on_open(self, ws):
        self.connected = True
        print("WebSocket connected!")

    def on_error(self, ws, error):
        print("WebSocket error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        self.connected = False
        print(
            "WebSocket closed:",
            close_status_code,
            close_msg,
        )

    def on_message(self, ws, message):
        self.message_callback(ws, message)

    def start(self):
        delay = self.min_reconnect_delay

        while True:
            self.connected = False

            self.ws = websocket.WebSocketApp(
                self.socket,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open,
            )

            print("Connecting to Binance:", self.socket)

            try:
                self.ws.run_forever(
                    ping_interval=20,
                    ping_timeout=10,
                )
            except Exception as error:
                print("WebSocket run error:", error)

            self.connected = False
            print(
                f"WebSocket disconnected. "
                f"Reconnecting in {delay} seconds..."
            )
            time.sleep(delay)

            delay = min(delay * 2, self.max_reconnect_delay)
