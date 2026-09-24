import unittest
from unittest.mock import patch
from urllib.error import HTTPError
import src.integrations.sisc_heartbeat as heartbeat


class DeliveryTests(unittest.TestCase):
    def test_timeout_retries_then_accepts(self):
        response = unittest.mock.MagicMock()
        response.__enter__.return_value.status = 200
        with patch.object(heartbeat, "urlopen", side_effect=[TimeoutError(), response]) as send, patch.object(heartbeat.time, "sleep"):
            self.assertTrue(heartbeat.send_heartbeat({"status": "CURRENT"}, oidc_token="test"))
            self.assertEqual(send.call_count, 2)

    def test_permanent_error_not_retried(self):
        error = HTTPError("https://example.test", 404, "missing", {}, None)
        with patch.object(heartbeat, "urlopen", side_effect=error) as send, patch.object(heartbeat.time, "sleep"):
            self.assertFalse(heartbeat.send_heartbeat({"status": "CURRENT"}, oidc_token="test"))
            self.assertEqual(send.call_count, 1)

    def test_retry_exhaustion(self):
        with patch.object(heartbeat, "urlopen", side_effect=TimeoutError()) as send, patch.object(heartbeat.time, "sleep"):
            self.assertFalse(heartbeat.send_heartbeat({"status": "CURRENT"}, oidc_token="test"))
            self.assertEqual(send.call_count, 3)
