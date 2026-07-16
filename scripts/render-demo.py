#!/usr/bin/env python3
"""Render the README terminal demo as a deterministic animated GIF."""

from pathlib import Path
import os

from PIL import Image, ImageDraw, ImageFont


WIDTH, HEIGHT = 1120, 650
BG = "#0d1117"
BAR = "#161b22"
TEXT = "#c9d1d9"
MUTED = "#8b949e"
GREEN = "#3fb950"
CYAN = "#58a6ff"
YELLOW = "#d29922"
RED = "#f85149"
PROMPT = "#7ee787"


def font(size=22, bold=False):
    candidates = [
        "C:/Windows/Fonts/consolab.ttf" if bold else "C:/Windows/Fonts/consola.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/System/Library/Fonts/Menlo.ttc",
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return ImageFont.truetype(candidate, size)
    return ImageFont.load_default(size=size)


FONT = font()
BOLD = font(bold=True)
SMALL = font(18)
LINE = 31


def frame(lines, typed="", cursor=False):
    image = Image.new("RGB", (WIDTH, HEIGHT), BG)
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, WIDTH, 54), fill=BAR)
    for x, color in ((25, RED), (53, YELLOW), (81, GREEN)):
        draw.ellipse((x, 18, x + 16, 34), fill=color)
    draw.text((WIDTH // 2, 27), "kx — multi-cluster Kubernetes", font=SMALL, fill=MUTED, anchor="mm")

    y = 79
    for text, color, style in lines:
        draw.text((34, y), text, font=BOLD if style == "bold" else FONT, fill=color)
        y += LINE
    if typed or cursor:
        draw.text((34, y), "$ ", font=BOLD, fill=PROMPT)
        prefix = draw.textlength("$ ", font=BOLD)
        draw.text((34 + prefix, y), typed, font=FONT, fill=TEXT)
        if cursor:
            x = 34 + prefix + draw.textlength(typed, font=FONT) + 2
            draw.rectangle((x, y + 3, x + 11, y + 25), fill=TEXT)
    return image


def add_typing(frames, durations, lines, command):
    for count in range(0, len(command) + 1, 2):
        frames.append(frame(lines, command[:count], cursor=True))
        durations.append(45)
    frames.append(frame(lines, command, cursor=True))
    durations.append(280)


def add_pause(frames, durations, lines, milliseconds):
    frames.append(frame(lines, cursor=True))
    durations.append(milliseconds)


def main():
    frames, durations = [], []
    lines = []

    add_typing(frames, durations, lines, "kx ctx where @prod")
    lines += [
        ("$ kx ctx where @prod", PROMPT, "bold"),
        ("prod-eu", TEXT, "normal"),
        ("prod-us", TEXT, "normal"),
        ("prod-apac", TEXT, "normal"),
        ("", TEXT, "normal"),
    ]
    add_pause(frames, durations, lines, 900)

    add_typing(frames, durations, lines, "kx @prod matrix deploy/api -n payments")
    lines += [
        ("$ kx @prod matrix deploy/api -n payments", PROMPT, "bold"),
        ("CONTEXT    NAME  STATUS     READY  IMAGE                 ROLLOUT", CYAN, "bold"),
        ("prod-apac  api   Ready      3/3    ghcr.io/acme/api:42   complete", GREEN, "normal"),
        ("prod-eu    api   Ready      3/3    ghcr.io/acme/api:42   complete", GREEN, "normal"),
        ("prod-us    api   Degraded   1/3    ghcr.io/acme/api:41   progressing", RED, "bold"),
        ("", TEXT, "normal"),
    ]
    add_pause(frames, durations, lines, 1500)

    # Keep the diagnosis screen compact so it stays readable in embeds.
    lines = lines[-7:]
    add_typing(frames, durations, lines, "kx @prod.us why deploy/api -n payments")
    lines += [
        ("$ kx @prod.us why deploy/api -n payments", PROMPT, "bold"),
        ("[prod-us]", CYAN, "bold"),
        ("target: deployment/api  namespace=payments", TEXT, "normal"),
        ("status: Degraded       ready: 1/3       image: ghcr.io/acme/api:41", RED, "bold"),
        ("condition: Available=False  reason=MinimumReplicasUnavailable", YELLOW, "normal"),
        ("warning: FailedPull — image ghcr.io/acme/api:41 not found", RED, "normal"),
    ]
    add_pause(frames, durations, lines, 2400)

    output = Path(__file__).resolve().parents[1] / "docs" / "assets" / "kx-demo.gif"
    output.parent.mkdir(parents=True, exist_ok=True)
    frames[0].save(
        output,
        save_all=True,
        append_images=frames[1:],
        duration=durations,
        loop=0,
        optimize=True,
        disposal=2,
    )
    print(f"rendered {output} ({output.stat().st_size / 1024:.0f} KiB)")


if __name__ == "__main__":
    main()
