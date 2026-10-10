import ast
import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

from src.application.services.owned_games import OwnedGamesService
from src.infrastructure.clients.steam import SteamClientError, SteamClientMixin
from src.presentation.renderers.owned_game import render_owned_game_image


class FakePlugin:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self._bind_data = {"qq-1": {"sid": "steam-1", "nickname": "玩家甲"}}
        self.group_steam_ids = {"group-1": ["steam-1"]}
        self.group_monitor_enabled = {"group-1": True}
        self.running_groups = {"group-1"}
        self.group_last_states = {"group-1": {"steam-1": {"name": "Steam 玩家"}}}
        self.games = []
        self.notifications = []

    async def fetch_owned_games(self, sid):
        return list(self.games)

    def _resolve_bind_name(self, sid, fallback):
        return "玩家甲"

    async def _send_merged_notification(self, group_id, notifications):
        self.notifications.extend((group_id, item) for item in notifications)
        return []


class OwnedGamesServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.plugin = FakePlugin(self.temp_dir.name)
        self.service = OwnedGamesService(self.plugin)

    async def asyncTearDown(self):
        self.temp_dir.cleanup()

    async def test_first_sync_is_silent_and_second_reports_only_added_games(self):
        self.plugin.games = [{"appid": 10, "name": "旧游戏"}]
        await self.service.sync_all()
        self.assertEqual(self.plugin.notifications, [])

        self.plugin.games = [
            {"appid": 10, "name": "旧游戏"},
            {"appid": 20, "name": "新游戏"},
        ]
        await self.service.sync_all()
        self.assertEqual(len(self.plugin.notifications), 1)
        group_id, notification = self.plugin.notifications[0]
        self.assertEqual(group_id, "group-1")
        self.assertEqual(notification["type"], "owned")
        self.assertEqual(notification["gameid"], "20")
        self.assertEqual(notification["name"], "玩家甲")

        saved = json.loads(Path(self.temp_dir.name, "owned_games_snapshots.json").read_text(encoding="utf-8"))
        self.assertEqual(len(saved["steam-1"]), 2)
        self.assertEqual(len(saved["steam-1"][0]["games"]), 1)
        self.assertEqual(len(saved["steam-1"][1]["games"]), 2)

    async def test_restart_keeps_two_snapshots_and_does_not_repeat_notification(self):
        self.plugin.games = [{"appid": 10, "name": "旧游戏"}]
        await self.service.sync_all()
        self.plugin.games.append({"appid": 20, "name": "新游戏"})
        await self.service.sync_all()

        reloaded = OwnedGamesService(self.plugin)
        await reloaded.sync_all()
        self.assertEqual(len(self.plugin.notifications), 1)
        self.assertEqual(len(reloaded.snapshots["steam-1"]), 2)

    async def test_failed_fetch_does_not_replace_snapshot_or_notify(self):
        self.plugin.games = [{"appid": 10, "name": "旧游戏"}]
        await self.service.sync_all()
        before = Path(self.temp_dir.name, "owned_games_snapshots.json").read_bytes()
        self.plugin.fetch_owned_games = AsyncMock(side_effect=SteamClientError("private"))
        await self.service.sync_all()
        self.assertEqual(before, Path(self.temp_dir.name, "owned_games_snapshots.json").read_bytes())
        self.assertEqual(self.plugin.notifications, [])

    async def test_failed_snapshot_write_keeps_previous_comparison_baseline(self):
        self.plugin.games = [{"appid": 10, "name": "旧游戏"}]
        await self.service.sync_all()
        self.plugin.games.append({"appid": 20, "name": "新游戏"})
        with patch.object(self.service, "_save", side_effect=OSError("disk full")):
            await self.service.sync_all()
        self.assertEqual(len(self.service.snapshots["steam-1"]), 1)
        await self.service.sync_all()
        self.assertEqual(len(self.plugin.notifications), 1)

    async def test_ai_query_uses_only_senders_binding_and_filters_games(self):
        self.plugin.games = [
            {"appid": 10, "name": "Portal", "playtime_forever": 120},
            {"appid": 20, "name": "Half-Life"},
        ]
        await self.service.sync_all()
        self.assertIn("error", await self.service.list_for_sender("qq-2"))
        result = await self.service.list_for_sender("qq-1", query="portal")
        self.assertEqual(result["total_owned"], 2)
        self.assertEqual(result["matched"], 1)
        self.assertEqual(result["games"][0]["appid"], 10)
        self.assertEqual(result["games"][0]["playtime_forever"], 120)


class FakeSteam(SteamClientMixin):
    API_KEY = "secret"
    STEAM_API_BASE = "https://api.steampowered.com"
    proxy = None


class OwnedGamesClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_uses_params_and_requires_complete_visible_response(self):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(200, json={"response": {"game_count": 1, "games": [{"appid": 10, "name": "Portal"}]}})

        transport = httpx.MockTransport(handler)
        real_client = httpx.AsyncClient

        def client_factory(*args, **kwargs):
            return real_client(transport=transport, **kwargs)

        with patch("src.infrastructure.clients.steam.httpx.AsyncClient", client_factory):
            games = await FakeSteam().fetch_owned_games("76561198000000001")
        self.assertEqual(games[0]["name"], "Portal")
        self.assertEqual(requests[0].url.params["key"], "secret")
        self.assertEqual(requests[0].url.params["include_appinfo"], "1")
        self.assertEqual(requests[0].url.params["include_played_free_games"], "1")

    async def test_private_or_incomplete_response_is_rejected(self):
        for body in ({"response": {}}, {"response": {"game_count": 2, "games": [{"appid": 10}]}}):
            with self.subTest(body=body):
                transport = httpx.MockTransport(lambda request: httpx.Response(200, json=body))
                real_client = httpx.AsyncClient
                with patch("src.infrastructure.clients.steam.httpx.AsyncClient", lambda *args, **kwargs: real_client(transport=transport, **kwargs)):
                    with self.assertRaises(SteamClientError):
                        await FakeSteam().fetch_owned_games("76561198000000001")


class OwnedGameImageTests(unittest.TestCase):
    def test_card_renders_even_when_cover_is_missing(self):
        image = render_owned_game_image("玩家甲", "新游戏", cover_path="missing.png")
        self.assertEqual(image.size, (680, 240))
        self.assertEqual(image.mode, "RGB")


class OwnedGameNotificationTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_game_uses_existing_image_notification_path(self):
        path = Path(__file__).resolve().parents[2] / "src/application/services/notification_tracking.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        source_class = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "NotificationTrackingMixin")
        method_names = {"_notification_event_key", "_should_send_notification", "_finish_notification", "_send_merged_notification"}
        methods = [node for node in source_class.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in method_names]
        test_class = ast.ClassDef(name="NotificationUnderTest", bases=[], keywords=[], body=methods, decorator_list=[])
        namespace = {
            "asyncio": asyncio,
            "time": time,
            "Image": SimpleNamespace(fromFileSystem=lambda path: ("image", path)),
            "Plain": lambda text: ("text", text),
            "MessageChain": list,
        }
        exec(compile(ast.fix_missing_locations(ast.Module(body=[test_class], type_ignores=[])), str(path), "exec"), namespace)
        plugin = namespace["NotificationUnderTest"]()
        plugin.config = {"notify_send_image": True, "notify_send_text": False}
        plugin.context = SimpleNamespace(send_message=AsyncMock())
        plugin._get_notify_sessions = lambda group, sid: ["qq-group-session"]
        plugin._render_notification_image = AsyncMock(return_value="card.png")
        notification = {"type": "owned", "sid": "steam-1", "gameid": "20", "name": "玩家甲", "game": "新游戏", "start_time": 123}

        failed = await plugin._send_merged_notification("group-1", [notification])

        self.assertEqual(failed, [])
        self.assertEqual(plugin.context.send_message.await_args.args[1], [("image", "card.png")])
