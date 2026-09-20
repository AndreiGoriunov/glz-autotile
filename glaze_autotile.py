import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

import websockets
from websockets.protocol import State

# ==============================================================================
# Configuration and models
# ==============================================================================
DEFAULT_CONFIG = {
    "core": {
        "ws_uri": "ws://localhost:6123",
        "debounce_delay_ms": 200,
        "log_level": "INFO",
    }
}


@dataclass
class Window:
    id: str
    width: int
    height: int
    hasFocus: bool
    tilingDirection: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "Window":
        return cls(
            id=data.get("id", ""),
            width=data.get("width", 0),
            height=data.get("height", 0),
            hasFocus=data.get("hasFocus", False),
            tilingDirection=data.get("tilingDirection"),
        )


@dataclass
class Workspace:
    id: str
    name: str
    children_raw: List[Dict[str, Any]]

    def get_tiling_windows(self) -> List[Window]:
        """Recursively collect all tiled child windows."""
        wins = []

        def _traverse(node):
            if "children" in node:
                for child in node["children"]:
                    if child.get("type") == "window":
                        state = child.get("state", {})
                        state_type = (
                            state.get("type") if isinstance(state, dict) else state
                        )
                        if state_type == "tiling":
                            wins.append(Window.from_dict(child))
                    else:
                        _traverse(child)

        _traverse({"children": self.children_raw})
        return wins


# ==============================================================================
# Core engine
# ==============================================================================
class GlazeWMClient:
    def __init__(self, uri: str):
        self.uri = uri
        self.ws: Any = None
        self.event_queue: asyncio.Queue[str] = asyncio.Queue()
        self.response_queue: asyncio.Queue[str] = asyncio.Queue()
        self.request_lock = asyncio.Lock()
        self.receive_task: asyncio.Task | None = None

    @property
    def is_connected(self) -> bool:
        return self.ws is not None and self.ws.state == State.OPEN

    async def connect(self):
        self.ws = await websockets.connect(self.uri)
        self.receive_task = asyncio.create_task(self._receive_loop())

    async def _receive_loop(self):
        if not self.ws:
            return
        try:
            async for msg in self.ws:
                try:
                    data = json.loads(msg)
                    if (
                        isinstance(data, dict)
                        and data.get("messageType") == "client_response"
                    ):
                        await self.response_queue.put(msg)
                    else:
                        await self.event_queue.put(msg)
                except (TypeError, ValueError):
                    continue
        except Exception:
            pass

    async def close(self):
        if self.ws:
            await self.ws.close()
            self.ws = None
        if self.receive_task:
            await self.receive_task
            self.receive_task = None
        self.event_queue = asyncio.Queue()
        self.response_queue = asyncio.Queue()

    async def request(self, message: str) -> Dict[str, Any]:
        if not self.is_connected:
            raise ConnectionError("GlazeWM is disconnected")
        async with self.request_lock:
            await self.ws.send(message)
            try:
                reply = await asyncio.wait_for(self.response_queue.get(), timeout=2.0)
            except asyncio.TimeoutError:
                await self.close()
                raise
            result = json.loads(reply)
            return result if isinstance(result, dict) else {}

    async def send_command(self, cmd: str) -> bool:
        return (await self.request(f"command {cmd}")).get("success") is True

    async def query(self, query_str: str) -> Dict[str, Any]:
        return await self.request(query_str)


class AutoTilerApp:
    def __init__(self, config: dict, enable_stats: bool = True):
        self.config = config
        self.enable_stats = enable_stats
        self.client = GlazeWMClient(config["core"]["ws_uri"])
        self.workspace_states: Dict[str, set] = {}
        self.window_directions: Dict[str, str] = {}

        # Initialize statistics.
        self.stats: Dict[str, Any] = {"total_guidance": 0}
        if self.enable_stats and os.path.exists("auto_tiler_stats.json"):
            try:
                with open("auto_tiler_stats.json", "r") as f:
                    self.stats.update(json.load(f))
            except Exception:
                pass

    def save_stats(self):
        """Persist statistics to a file."""
        if not self.enable_stats:
            return
        try:
            with open("auto_tiler_stats.json", "w") as f:
                json.dump(self.stats, f)
        except Exception:
            pass

    async def run(self):
        try:
            debounce = self.config["core"]["debounce_delay_ms"] / 1000.0
            while True:
                try:
                    await self.client.connect()
                    self.window_directions.clear()
                    for ev in (
                        "window_managed",
                        "focus_changed",
                        "workspace_activated",
                        "focused_container_moved",
                    ):
                        reply = await self.client.request(f"sub -e {ev}")
                        if reply.get("success") is not True:
                            raise ConnectionError(f"GlazeWM rejected subscription: {ev}")
                    await self._apply_guidance("startup")
                    while self.client.is_connected:
                        try:
                            msg = await asyncio.wait_for(
                                self.client.event_queue.get(), timeout=1.0
                            )
                        except asyncio.TimeoutError:
                            continue
                        try:
                            event_data = json.loads(msg)
                        except (TypeError, ValueError):
                            continue
                        if not isinstance(event_data, dict):
                            continue
                        if event_data.get("messageType") in (
                            "event_subscription",
                            "event_subscription_message",
                        ):
                            await asyncio.sleep(debounce)
                            # Discard queued messages.
                            while not self.client.event_queue.empty():
                                self.client.event_queue.get_nowait()
                            await self._apply_guidance("event")
                except Exception:
                    pass
                finally:
                    await self.client.close()
                await asyncio.sleep(1.0)
        finally:
            self.save_stats()  # Save once more before exiting.
            await self.client.close()

    async def _apply_guidance(self, event_type: str):
        res = await self.client.query("query workspaces")
        if not isinstance(res, dict):
            return
        data = res.get("data")
        if not isinstance(data, dict):
            return
        workspaces = data.get("workspaces", [])
        if not isinstance(workspaces, list):
            return
        active_ws_data = next(
            (w for w in workspaces if isinstance(w, dict) and w.get("hasFocus")), None
        )
        if not active_ws_data:
            return

        ws = Workspace(
            active_ws_data["id"],
            active_ws_data["name"],
            active_ws_data.get("children", []),
        )
        wins = ws.get_tiling_windows()
        current_ids = {w.id for w in wins}

        # Process the focused window on every triggering event.
        focused_win = next((w for w in wins if w.hasFocus), None)
        if focused_win:
            ratio = (
                focused_win.width / focused_win.height
                if focused_win.height > 0
                else 1.0
            )
            direction = "horizontal" if ratio > 1.0 else "vertical"

            current_direction = focused_win.tilingDirection or self.window_directions.get(focused_win.id)
            if current_direction != direction and await self.client.send_command(
                f"set-tiling-direction {direction}"
            ):
                self.window_directions[focused_win.id] = direction
                if self.enable_stats:
                    # Update statistics while preserving legacy field names.
                    today = datetime.now().strftime("%Y-%m-%d")

                    # 1. Update the totals.
                    self.stats["TotalSwitches"] = (
                        int(self.stats.get("TotalSwitches", 0)) + 1
                    )
                    self.stats["total_guidance"] = (
                        int(self.stats.get("total_guidance", 0)) + 1
                    )

                    # 2. Update the daily count.
                    daily = self.stats.get("DailySwitches", {})
                    if not isinstance(daily, dict):
                        daily = {}
                    daily[today] = int(daily.get(today, 0)) + 1
                    self.stats["DailySwitches"] = daily

                    # Save every 10 updates to reduce disk I/O.
                    if self.stats["total_guidance"] % 10 == 0:
                        self.save_stats()

        self.workspace_states[ws.id] = current_ids


def main():
    enable_stats = "--no-stats" not in sys.argv
    app = AutoTilerApp(DEFAULT_CONFIG, enable_stats=enable_stats)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
