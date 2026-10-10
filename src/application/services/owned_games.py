"""Steam 游戏库同步、双快照差异检测和绑定用户的 AI 查询。"""

import asyncio
import json
import os
import tempfile
import time

from ...infrastructure.clients.steam import SteamClientError
from ...shared.logging import logger


class OwnedGamesService:
    FILE_NAME = "owned_games_snapshots.json"

    def __init__(self, plugin):
        self.plugin = plugin
        self.path = os.path.join(plugin.data_dir, self.FILE_NAME)
        self.snapshots = self._load()

    def _load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as stream:
                data = json.load(stream)
            if not isinstance(data, dict):
                raise ValueError("快照根节点不是对象")
            return {
                str(sid): history[-2:]
                for sid, history in data.items()
                if isinstance(history, list)
                and all(isinstance(item, dict) and isinstance(item.get("games"), list) for item in history[-2:])
            }
        except FileNotFoundError:
            return {}
        except (OSError, ValueError, TypeError) as exc:
            logger.warning("加载游戏库快照失败，将重新建立基线: %s", exc)
            return {}

    def _save(self):
        os.makedirs(self.plugin.data_dir, exist_ok=True)
        path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.plugin.data_dir,
                prefix="owned_games_", suffix=".tmp", delete=False,
            ) as stream:
                path = stream.name
                json.dump(self.snapshots, stream, ensure_ascii=False, separators=(",", ":"))
            os.replace(path, self.path)
        finally:
            if path and os.path.exists(path):
                os.unlink(path)

    def _active_groups(self):
        plugin = self.plugin
        active = {str(gid) for gid in getattr(plugin, "running_groups", ())}
        groups = {}
        for group_id, ids in getattr(plugin, "group_steam_ids", {}).items():
            gid = str(group_id)
            if gid not in active or not plugin.group_monitor_enabled.get(gid, True):
                continue
            for sid in ids:
                groups.setdefault(str(sid), []).append(gid)
        return groups

    async def sync_all(self):
        """所有启用监控的玩家去重拉取一次，按现有群路由推送。"""
        groups_by_sid = self._active_groups()
        semaphore = asyncio.Semaphore(4)

        async def sync_one(sid, groups):
            async with semaphore:
                try:
                    await self.sync_player(sid, groups)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("同步 Steam 游戏库失败 (SteamID=%s): %s", sid, exc)

        await asyncio.gather(*(sync_one(sid, groups) for sid, groups in groups_by_sid.items()))

    async def sync_player(self, sid, groups):
        games = await self.plugin.fetch_owned_games(sid)
        old_history = self.snapshots.get(str(sid), [])
        old_games = old_history[-1]["games"] if old_history else None
        new_snapshot = {"fetched_at": int(time.time()), "games": games}
        self.snapshots[str(sid)] = (old_history + [new_snapshot])[-2:]
        try:
            self._save()
        except Exception:
            if old_history:
                self.snapshots[str(sid)] = old_history
            else:
                self.snapshots.pop(str(sid), None)
            raise
        if old_games is None:
            return  # 首轮只建立基线，绝不将整库误报为新增。

        old_ids = {str(game["appid"]) for game in old_games}
        additions = [game for game in games if str(game["appid"]) not in old_ids]
        if not additions:
            return
        for group_id in groups:
            status = self.plugin.group_last_states.get(group_id, {}).get(str(sid), {})
            name = self.plugin._resolve_bind_name(str(sid), status.get("name") or str(sid))
            notifications = [
                {
                    "type": "owned",
                    "sid": str(sid),
                    "name": name,
                    "gameid": str(game["appid"]),
                    "game": game.get("name") or f"AppID {game['appid']}",
                    "status": status,
                    "start_time": new_snapshot["fetched_at"],
                }
                for game in additions
            ]
            try:
                failed = await self.plugin._send_merged_notification(group_id, notifications)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("新入库游戏通知发送失败 (group_id=%s, SteamID=%s)", group_id, sid)
                failed = notifications
            if failed:
                self.plugin._pending_end_notifications.setdefault(group_id, []).extend(failed)

    async def list_for_sender(self, sender_id, query="", offset=0, limit=50):
        """只根据消息发送者查绑定，不接受 LLM 指定的其他 QQ 或 Steam ID。"""
        binding = getattr(self.plugin, "_bind_data", {}).get(str(sender_id))
        if not isinstance(binding, dict) or not binding.get("sid"):
            return {"error": "当前 QQ 用户尚未绑定 Steam 账号；请在群内使用 /steam addid SteamID @用户 绑定。"}
        sid = str(binding["sid"])
        history = self.snapshots.get(sid)
        if history:
            snapshot = history[-1]
            games = snapshot["games"]
            source = "snapshot"
            fetched_at = snapshot["fetched_at"]
        else:
            try:
                games = await self.plugin.fetch_owned_games(sid)
            except SteamClientError:
                return {"error": "无法读取 Steam 游戏库；请检查 Steam API Key 和该账号的游戏详情隐私设置。"}
            source = "steam"
            fetched_at = int(time.time())
        needle = str(query or "").casefold().strip()
        matched = [game for game in games if needle in str(game.get("name", "")).casefold()]
        try:
            start = max(0, int(offset))
            page_size = min(100, max(1, int(limit)))
        except (TypeError, ValueError):
            start, page_size = 0, 50
        page = matched[start:start + page_size]
        return {
            "steam_id": sid,
            "total_owned": len(games),
            "matched": len(matched),
            "offset": start,
            "next_offset": start + len(page) if start + len(page) < len(matched) else None,
            "source": source,
            "fetched_at": fetched_at,
            "games": [
                {
                    "appid": game["appid"],
                    "name": game.get("name") or f"AppID {game['appid']}",
                    "playtime_forever": game.get("playtime_forever", 0),
                }
                for game in page
            ],
        }

    async def run_loop(self):
        await asyncio.sleep(10)
        while True:
            await self.sync_all()
            interval = self.plugin.config.get("owned_games_poll_interval", 1800)
            try:
                seconds = max(60, int(interval))
            except (TypeError, ValueError):
                seconds = 1800
            await asyncio.sleep(seconds)
