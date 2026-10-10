"""新增入库游戏的图片通知。"""

import asyncio
import io
import os

from PIL import Image, ImageDraw

from ...shared.fonts import load_truetype
from .game_start import get_avatar_path, get_cover_path, render_gradient_bg, text_wrap


def render_owned_game_image(player_name, game_name, avatar_path=None, cover_path=None):
    width, height = 680, 240
    image = render_gradient_bg(width, height, (49, 80, 66), (28, 35, 44)).convert("RGBA")
    draw = ImageDraw.Draw(image)
    text_x = 210
    if cover_path and os.path.exists(cover_path):
        try:
            with Image.open(cover_path) as cover:
                cover = cover.convert("RGBA")
                scaled_width = max(1, round(cover.width * height / cover.height))
                cover = cover.resize((scaled_width, height), Image.Resampling.LANCZOS)
                image.alpha_composite(cover, (0, 0))
                text_x = min(max(scaled_width + 24, 170), 250)
        except (OSError, ValueError):
            pass
    if avatar_path and os.path.exists(avatar_path):
        with Image.open(avatar_path) as avatar:
            avatar = avatar.convert("RGBA").resize((48, 48), Image.Resampling.LANCZOS)
            image.alpha_composite(avatar, (text_x, 22))
        player_x = text_x + 60
    else:
        player_x = text_x
    small_font = load_truetype("NotoSansHans-Regular.otf", 19)
    title_font = load_truetype("NotoSansHans-Medium.otf", 24)
    game_font = load_truetype("NotoSansHans-Medium.otf", 27)
    available = width - text_x - 24
    player_lines = text_wrap(str(player_name), small_font, width - player_x - 24)[:1]
    draw.text((player_x, 32), player_lines[0] if player_lines else "Steam 玩家", font=small_font, fill="#E9F3ED")
    draw.text((text_x, 85), "新入库游戏", font=title_font, fill="#B9E58B")
    for index, line in enumerate(text_wrap(str(game_name), game_font, available)[:2]):
        draw.text((text_x, 130 + index * 39), line, font=game_font, fill="#FFFFFF")
    return image.convert("RGB")


async def render_owned_game(data_dir, sid, player_name, avatar_url, game_id, game_name,
                            *, sgdb_api_key=None, proxy=None, api_key=None,
                            sgdb_api_base=None, steam_api_base=None):
    avatar_path, cover_path = await asyncio.gather(
        get_avatar_path(data_dir, sid, avatar_url, proxy=proxy),
        get_cover_path(
            data_dir, game_id, game_name, appid=game_id,
            sgdb_api_key=sgdb_api_key, proxy=proxy, api_key=api_key,
            sgdb_api_base=sgdb_api_base, steam_api_base=steam_api_base,
        ),
        return_exceptions=True,
    )
    if isinstance(avatar_path, Exception):
        avatar_path = None
    if isinstance(cover_path, Exception):
        cover_path = None
    image = render_owned_game_image(player_name, game_name, avatar_path, cover_path)
    output = io.BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()
