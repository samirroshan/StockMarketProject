
import unittest
from unittest.mock import MagicMock, patch
import json
import sys
import os

# Ensure we can import the producer
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.stream_producer import run_producer

class TestStreamProducer(unittest.TestCase):
    def test_run_producer_logic(self):
        # Mock Producer
        mock_producer = MagicMock()
        
        # Mock Finnhub client
        mock_finnhub = MagicMock()
        mock_finnhub.quote.return_value = {'c': 150.0, 't': 1600000000}
        
        # Run producer for 1 iteration with tiny interval
        run_producer(
            producer=mock_producer,
            topic='test_topic',
            finnhub_client=mock_finnhub,
            ticker='TEST',
            interval=0.01,
            max_iterations=1
        )
        
        # Verify Finnhub was called
        mock_finnhub.quote.assert_called_with('TEST')
        
        # Verify Producer produce() was called
        self.assertTrue(mock_producer.produce.called)
        args, kwargs = mock_producer.produce.call_args
        topic = args[0]
        value_bytes = kwargs['value']
        
        self.assertEqual(topic, 'test_topic')
        
        # Verify payload structure
        data = json.loads(value_bytes.decode('utf-8'))
        self.assertEqual(data['symbol'], 'TEST')
        self.assertEqual(data['price'], 150.0)
        self.assertEqual(data['timestamp'], 1600000000)
        
        # Verify flush called
        mock_producer.flush.assert_called()

if __name__ == '__main__':
    unittest.main()
