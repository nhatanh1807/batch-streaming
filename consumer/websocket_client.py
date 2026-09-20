import websocket


class BinanceClient:

    def __init__(self, socket, on_message):

        self.ws = websocket.WebSocketApp(
            socket,
            on_message=on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )

    def on_open(self, ws):
        print("WebSocket connected!")

    def on_error(self, ws, error):
        print("WebSocket error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print(
            "WebSocket closed:",
            close_status_code,
            close_msg
        )

    def start(self):
        self.ws.run_forever()