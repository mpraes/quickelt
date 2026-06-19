#!/usr/bin/env python3
"""
Quickelt Terminal Styling
=========================

Centralized ANSI colour palette, icons, and formatting helpers used
across all setup modules.  Every function degrades gracefully when
the terminal does not support colour (NO_COLOR / non-TTY).
"""

import os
import re
import sys

_NO_COLOR = not sys.stdout.isatty() or bool(os.getenv("NO_COLOR"))

R = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
ITALIC = "\033[3m"
UNDERLINE = "\033[4m"

BLACK = "\033[30m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"
WHITE = "\033[37m"

BRIGHT_RED = "\033[91m"
BRIGHT_GREEN = "\033[92m"
BRIGHT_YELLOW = "\033[93m"
BRIGHT_BLUE = "\033[94m"
BRIGHT_MAGENTA = "\033[95m"
BRIGHT_CYAN = "\033[96m"
BRIGHT_WHITE = "\033[97m"

BG_DARK = "\033[48;5;236"
BG_BLUE = "\033[44m"
BG_MAGENTA = "\033[45m"

ANSI_RE = re.compile(r"\033\[[0-9;]*m")


def _vis_len(text: str) -> int:
    return len(ANSI_RE.sub("", text))


def _vis_center(text: str, width: int, fillchar: str = " ") -> str:
    vis = _vis_len(text)
    if vis >= width:
        return text
    left = (width - vis) // 2
    right = width - vis - left
    return fillchar * left + text + fillchar * right


ICON_CLOUD = "\u2601"
ICON_FOLDER = "\u25C9"
ICON_SERVER = "\u2338"
ICON_DB = "\u29C9"
ICON_CHECK = "\u2714"
ICON_CROSS = "\u2716"
ICON_ARROW = "\u276F"
ICON_GEAR = "\u2699"
ICON_LOCK = "\U0001F512"
ICON_ROCKET = "\U0001F680"
ICON_SPARKLE = "\u2728"
ICON_WARN = "\u26A0"
ICON_INFO = "\u2139"
ICON_BOLT = "\u26A1"
ICON_DIAMOND = "\u25C6"

ACCENT = CYAN
ACCENT2 = BRIGHT_MAGENTA
BRAND = BRIGHT_CYAN + BOLD
SUCCESS = BRIGHT_GREEN
FAILURE = BRIGHT_RED + BOLD
WARN_COLOR = BRIGHT_YELLOW
MUTED = DIM
HIGHLIGHT = BOLD + WHITE

PANEL_WIDTH = 46
INDENT = "  "


def s(text: str, *codes: str) -> str:
    if _NO_COLOR:
        return text
    prefix = "".join(c for c in codes if c)
    return f"{prefix}{text}{R}"


def brand(text: str) -> str:
    return s(text, BRAND)


def success(text: str) -> str:
    return s(text, SUCCESS)


def failure(text: str) -> str:
    return s(text, FAILURE)


def warn(text: str) -> str:
    return s(text, WARN_COLOR)


def muted(text: str) -> str:
    return s(text, MUTED)


def accent(text: str) -> str:
    return s(text, ACCENT)


def accent2(text: str) -> str:
    return s(text, ACCENT2)


def highlight(text: str) -> str:
    return s(text, HIGHLIGHT)


def icon(icon_char: str, color: str = ACCENT) -> str:
    if _NO_COLOR:
        return ""
    return s(icon_char, color) + " "


def kv_line(key: str, value: str, key_color: str = ACCENT, val_color: str = "") -> str:
    k = s(key, key_color, BOLD)
    v = s(value, val_color) if val_color else value
    sep = s("...", MUTED)
    dots_needed = 14 - _vis_len(key)
    dots = s("." * max(dots_needed, 2), MUTED)
    return f"{INDENT}{k} {dots} {v}"


def separator(char: str = "\u2500", color: str = MUTED, width: int = PANEL_WIDTH) -> str:
    return f"{INDENT}{s(char * width, color)}"


def panel(title: str, subtitle: str = "", border_color: str = ACCENT, title_color: str = BRAND) -> str:
    inner = PANEL_WIDTH - 2

    if _NO_COLOR:
        lines = [
            f"{INDENT}╔{'═' * PANEL_WIDTH}╗",
            f"{INDENT}| {title:^{inner}} |",
        ]
        if subtitle:
            lines.append(f"{INDENT}| {subtitle:^{inner}} |")
        lines.append(f"{INDENT}╚{'═' * PANEL_WIDTH}╝")
        return "\n".join(lines)

    tl = s("\u2554", border_color, BOLD)
    tr = s("\u2557", border_color, BOLD)
    bl = s("\u255A", border_color, BOLD)
    br = s("\u255D", border_color, BOLD)
    h = s("\u2550" * PANEL_WIDTH, border_color)
    v = s("\u2551", border_color)

    styled_title = s(title, title_color)
    centered_title = _vis_center(styled_title, inner)

    lines = [
        f"{INDENT}{tl}{h}{tr}",
        f"{INDENT}{v} {centered_title} {v}",
    ]
    if subtitle:
        styled_sub = s(subtitle, MUTED)
        centered_sub = _vis_center(styled_sub, inner)
        lines.append(f"{INDENT}{v} {centered_sub} {v}")
    lines.append(f"{INDENT}{bl}{h}{br}")
    return "\n".join(lines)


def step_header(step_num: int, total: int, title: str, icon_char: str = ICON_GEAR) -> str:
    counter = s(f"Step {step_num}/{total}", ACCENT, BOLD)
    ic = icon(icon_char, ACCENT2) if not _NO_COLOR else ""
    divider = s("\u2500" * 3, MUTED)
    return f"\n{INDENT}{counter} {divider}{ic}{s(title, BOLD, ACCENT)}"


def choice_line(num: int, label: str, color: str = "") -> str:
    bracket = s(f"({num})", ACCENT, BOLD)
    lbl = s(label, color) if color else label
    return f"{INDENT}  {bracket} {lbl}"


def prompt_label(text: str) -> str:
    return f"{INDENT}{s(ICON_ARROW, ACCENT)} {s(text, BOLD)}"


def input_hint(text: str) -> str:
    return s(text, MUTED)


def goodbye() -> str:
    if _NO_COLOR:
        return f"{INDENT}Setup cancelled. Goodbye!"
    return f"{INDENT}{s(ICON_CROSS, FAILURE)} Setup cancelled. Goodbye!"


def completion() -> str:
    if _NO_COLOR:
        return f"{INDENT}Setup complete. Happy building!"
    sparkle = s(ICON_SPARKLE, BRIGHT_YELLOW)
    rocket = s(ICON_ROCKET, ACCENT2)
    return f"{INDENT}{sparkle} Setup complete. {rocket} Happy building!"


def preflight_pass(cloud: str) -> str:
    return f"{icon(ICON_CHECK, SUCCESS)}Pre-flight check passed for {brand(cloud)}"


def preflight_fail(cloud: str) -> str:
    return f"{icon(ICON_CROSS, FAILURE)}Pre-flight check FAILED for {failure(cloud)}"
