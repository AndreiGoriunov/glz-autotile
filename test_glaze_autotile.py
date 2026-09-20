import asyncio
import json
import unittest
from unittest.mock import AsyncMock
from unittest.mock import patch

from websockets.protocol import State

from glaze_autotile import AutoTilerApp, DEFAULT_CONFIG, GlazeWMClient


class FakeSocket:
    def __init__(self):
        self.state = State.OPEN
        self.messages = asyncio.Queue()
        self.sent = []

    async def send(self, message):
        self.sent.append(message)

    async def close(self):
        self.state = State.CLOSED
        await self.messages.put(None)

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.messages.get()
        if message is None:
            raise StopAsyncIteration
        return message


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_retries_failed_connection(self):
        app = AutoTilerApp(DEFAULT_CONFIG, enable_stats=False)
        app.client.connect = AsyncMock(side_effect=[OSError("offline"), asyncio.CancelledError()])
        app.client.close = AsyncMock()
        with patch("glaze_autotile.asyncio.sleep", new_callable=AsyncMock):
            with self.assertRaises(asyncio.CancelledError):
                await app.run()
        self.assertEqual(app.client.connect.await_count, 2)

    async def test_sequential_replies_and_malformed_event(self):
        client = GlazeWMClient("unused")
        socket = FakeSocket()
        client.ws = socket
        client.receive_task = asyncio.create_task(client._receive_loop())
        first = asyncio.create_task(client.request("sub -e focus_changed"))
        await asyncio.sleep(0)
        second = asyncio.create_task(client.query("query workspaces"))
        await asyncio.sleep(0)
        self.assertEqual(socket.sent, ["sub -e focus_changed"])
        await socket.messages.put("invalid json")
        await socket.messages.put(json.dumps({"messageType": "client_response", "success": True}))
        self.assertTrue((await first)["success"])
        await asyncio.sleep(0)
        self.assertEqual(socket.sent, ["sub -e focus_changed", "query workspaces"])
        await socket.messages.put(json.dumps({"messageType": "client_response", "data": {"workspaces": []}}))
        self.assertEqual((await second)["data"], {"workspaces": []})
        self.assertTrue(client.event_queue.empty())
        await client.close()

    async def test_rejected_command_is_not_counted_or_cached(self):
        app = AutoTilerApp(DEFAULT_CONFIG, enable_stats=False)
        app.client.query = AsyncMock(return_value={"data": {"workspaces": [{
            "id": "ws", "name": "one", "hasFocus": True,
            "children": [{"type": "window", "state": {"type": "tiling"},
                          "id": "win", "width": 800, "height": 400,
                          "hasFocus": True, "tilingDirection": "vertical"}],
        }]}})
        app.client.send_command = AsyncMock(side_effect=[False, True])
        await app._apply_guidance("event")
        self.assertNotIn("win", app.window_directions)
        await app._apply_guidance("event")
        self.assertEqual(app.window_directions["win"], "horizontal")
        self.assertEqual(app.client.send_command.await_count, 2)


if __name__ == "__main__":
    unittest.main()
