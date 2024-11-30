import json
from typing import Dict, List

from loguru import logger
from websocket import create_connection
from pydantic import BaseModel

class Trade(BaseModel):
    product_id: str
    price: float
    quantity: float
    timestamp_ms: int

class KarkenWebSocketTradeAPI:
    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_id: str):
        # establish a connection to the Kraken WebSocket Trade API
        self.product_id = product_id
        self._ws = create_connection(self.URL)

        ##subscribe to the trade channel
        self._subscribe(product_id)

    def _subscribe(self, product_id: str) -> None:
        """
        Establishes a connection to the Kraken WebSocket Trade API and subscribes to the trade channel
        """
        logger.info('Connected to Kraken WebSocket Trade API')

        logger.info(f'Subscribing to trade channel {product_id}')
        # subscribe to the trade channel
        msg = {
            'method': 'subscribe',
            'params': {'channel': 'trade', 'symbol': [product_id], 'snapshot': False},
        }
        # send the message
        self._ws.send(json.dumps(msg))
        logger.info('Subscription worked')

        # dumping the first two messages from the websocket
        _ = self._ws.recv()
        _ = self._ws.recv()

    def get_trades(self) -> List[Trade]:
        # mock_trades = [
        #     {
        #         'product_id' : 'BTC-USD',
        #         'Price' : 60000 ,
        #         'volume' : 0.01,
        #         'timestamp' : 16300000,
        #     },
        #     {
        #         'product_id' : message = self._ws.recv()'BTC-USD',
        #         'Price' : 58000 ,
        #         'volume' : 0.01,
        #         'timestamp' : 16500000,
        #     }
        # ]

        message = self._ws.recv()

        if 'heartbeat' in message:
            # when i get a heardbeat message
            return []
        # parse the message str as a dict
        message = json.loads(message)

        # extract trades from the message
        trades = []
        for trade in message['data']:
            trades.append(
                Trade(
                    product_id=trade['symbol'],
                    price=float(trade['price']),
                    quantity=float(trade['qty']),
                    timestamp_ms=self.to_ms(trade['timestamp'])
                )
            )

        # logger.info(f"Received message: {message}")
        # breakpoint()

        return trades
    
    @staticmethod
    def to_ms(timestamp: str) -> int:
        """
        A function that transfores a timestamps expressed 
        as a string like this  '2021-10-01T00:00:00.000Z'
        into a timestamp in milliseconds

        Args:
            timestamp (str): A string representing a timestamp
        
        Returns:
            int: A timestamp in milliseconds
        """
        from datetime import datetime, timezone
        timestamp = datetime.fromisoformat(timestamp[:-1]).replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp() * 1000)
