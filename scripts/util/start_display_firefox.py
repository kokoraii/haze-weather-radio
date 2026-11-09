#!/usr/bin/env python3
"""Start Firefox in kiosk mode on the GPU with a connected display."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time

DEFAULT_URL = "http://localhost:3000?videoType=vga"
DEFAULT_MODE = "800x600"
DEFAULT_DISPLAY = ":0"
DRM_ROOT = Path("/sys/class/drm")
PCI_ADDRESS = re.compile(r"^[0-9a-fA-F]{4}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-7]$")
PCI_IN_TEXT = re.compile(
    r"(?:pci[-_])?([0-9a-fA-F]{4})[:_]?([0-9a-fA-F]{2})[:_]?([0-9a-fA-F]{2})\.([0-7])"
)


@dataclass(frozen=True)
class DisplayTarget:
    card: str
    connector: str
    gpu_bdf: str
    modes: frozenset[str]

    @property
    def xorg_bus_id(self) -> str:
        domain, bus, device_function = self.gpu_bdf.split(":")
        device, function = device_function.split(".")
        return f"PCI:{int(bus, 16)}@{int(domain, 16)}:{int(device, 16)}:{int(function, 16)}"

    @property
    def pci_slot(self) -> tuple[str, str, str]:
        domain, bus, device_function = self.gpu_bdf.split(":")
        device, _function = device_function.split(".")
        return domain.lower(), bus.lower(), device.lower()


def detect_displays() -> list[DisplayTarget]:
    targets: list[DisplayTarget] = []
    for status_file in sorted(DRM_ROOT.glob("card*-*/status")):
        if status_file.read_text(encoding="utf-8").strip() != "connected":
            continue

        card, connector = status_file.parent.name.split("-", maxsplit=1)
        device_path = status_file.parent.joinpath("device").resolve()
        gpu_bdf = next(
            (part for part in reversed(device_path.parts) if PCI_ADDRESS.fullmatch(part)),
            None,
        )
        if gpu_bdf is None:
            continue

        modes_file = status_file.parent / "modes"
        modes = frozenset(modes_file.read_text(encoding="utf-8").splitlines())
        targets.append(DisplayTarget(card, connector, gpu_bdf, modes))

    return targets


def choose_display(targets: list[DisplayTarget], mode: str) -> DisplayTarget:
    if not targets:
        raise RuntimeError("No connected DRM display was detected.")

    usable = [target for target in targets if mode in target.modes]
    if not usable:
        connectors = ", ".join(target.connector for target in targets)
        raise RuntimeError(f"No connected display supports {mode}. Detected: {connectors}")

    return next(
        (target for target in usable if "HDMI" in target.connector.upper()), usable[0]
    )


def write_xorg_config(target: DisplayTarget, mode: str) -> str:
    contents = f'''Section "ServerLayout"
    Identifier "HazeDisplay"
    Screen 0 "HazeScreen"
EndSection

Section "Device"
    Identifier "HazeGPU"
    Driver "modesetting"
    BusID "{target.xorg_bus_id}"
EndSection

Section "Screen"
    Identifier "HazeScreen"
    Device "HazeGPU"
    DefaultDepth 24
    SubSection "Display"
        Depth 24
        Modes "{mode}"
    EndSubSection
EndSection
'''
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", prefix="haze-xorg-", suffix=".conf", delete=False
    ) as config:
        config.write(contents)
        return config.name


def connected_xrandr_outputs() -> list[str]:
    result = subprocess.run(
        ["xrandr", "--query"], check=True, capture_output=True, text=True
    )
    return [
        match.group(1)
        for line in result.stdout.splitlines()
        if (match := re.match(r"^(\S+) connected(?:\s|$)", line))
    ]


def choose_xrandr_output(target: DisplayTarget, outputs: list[str]) -> str:
    candidates = [name for name in outputs if "HDMI" in name.upper()]
    connector_number = target.connector.rsplit("-", maxsplit=1)[-1]
    matching = [name for name in candidates if name.rsplit("-", maxsplit=1)[-1] == connector_number]
    if len(matching) == 1:
        return matching[0]
    if len(candidates) == 1:
        return candidates[0]
    if len(outputs) == 1:
        return outputs[0]
    raise RuntimeError(f"Could not map DRM connector {target.connector} to an X11 output: {outputs}")


def pipewire_sinks() -> list[dict[str, object]]:
    result = subprocess.run(
        ["pactl", "-f", "json", "list", "sinks"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def sink_pci_slots(sink: dict[str, object]) -> set[tuple[str, str, str]]:
    properties = sink.get("properties", {})
    values = [str(sink.get("name", "")), str(sink.get("description", ""))]
    if isinstance(properties, dict):
        values.extend(str(value) for value in properties.values())

    slots: set[tuple[str, str, str]] = set()
    for value in values:
        for match in PCI_IN_TEXT.finditer(value):
            slots.add(tuple(part.lower() for part in match.groups()[:3]))
    return slots


def sink_has_available_port(sink: dict[str, object]) -> bool:
    active_port = sink.get("active_port")
    if isinstance(active_port, dict) and active_port.get("availability") == "available":
        return True

    ports = sink.get("ports", [])
    return isinstance(ports, list) and any(
        isinstance(port, dict) and port.get("availability") == "available"
        for port in ports
    )


def route_audio_to_hdmi(target: DisplayTarget) -> str | None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        sinks = pipewire_sinks()
        hdmi_sinks = [
            sink
            for sink in sinks
            if "hdmi" in f"{sink.get('name', '')} {sink.get('description', '')}".lower()
        ]
        matching = [sink for sink in hdmi_sinks if target.pci_slot in sink_pci_slots(sink)]
        available = [sink for sink in matching if sink_has_available_port(sink)]
        if len(available) == 1:
            sink_name = str(available[0]["name"])
            subprocess.run(["pactl", "set-default-sink", sink_name], check=True)
            return sink_name

        if len(matching) == 1:
            sink_name = str(matching[0]["name"])
            subprocess.run(["pactl", "set-default-sink", sink_name], check=True)
            return sink_name

        if not matching and len(hdmi_sinks) == 1 and not sink_pci_slots(hdmi_sinks[0]):
            sink_name = str(hdmi_sinks[0]["name"])
            subprocess.run(["pactl", "set-default-sink", sink_name], check=True)
            return sink_name

        time.sleep(1)
    return None


def run_browser_session(target: DisplayTarget, mode: str, url: str) -> int:
    output = choose_xrandr_output(target, connected_xrandr_outputs())
    subprocess.run(["xrandr", "--output", output, "--mode", mode], check=True)
    print(f"Using X11 output {output} at {mode}.", flush=True)

    sink = route_audio_to_hdmi(target)
    if sink is None:
        raise RuntimeError(
            f"No PipeWire HDMI sink is available for GPU {target.gpu_bdf}; Firefox was not started."
        )

    browser_env = os.environ.copy()
    browser_env["PULSE_SINK"] = sink
    print(f"PipeWire default sink set to {sink}.", flush=True)

    firefox = shutil.which("firefox")
    if firefox is None:
        raise RuntimeError("Firefox was not found in PATH.")
    process = subprocess.Popen([firefox, "--kiosk", url], env=browser_env)
    try:
        return process.wait()
    except KeyboardInterrupt:
        process.terminate()
        return process.wait()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--mode", default=DEFAULT_MODE)
    parser.add_argument("--display", default=DEFAULT_DISPLAY)
    parser.add_argument("--session", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--connector", help=argparse.SUPPRESS)
    parser.add_argument("--gpu-bdf", help=argparse.SUPPRESS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.session:
        target = DisplayTarget(
            card="",
            connector=args.connector,
            gpu_bdf=args.gpu_bdf,
            modes=frozenset({args.mode}),
        )
        return run_browser_session(target, args.mode, args.url)

    for command in ("xinit", "xrandr", "pactl"):
        if shutil.which(command) is None:
            raise RuntimeError(f"Required command was not found in PATH: {command}")

    target = choose_display(detect_displays(), args.mode)
    xorg_config = write_xorg_config(target, args.mode)
    script = str(Path(__file__).resolve())
    command = [
        "xinit",
        sys.executable,
        script,
        "--session",
        "--connector",
        target.connector,
        "--gpu-bdf",
        target.gpu_bdf,
        "--mode",
        args.mode,
        "--url",
        args.url,
        "--",
        args.display,
        "-config",
        xorg_config,
        "-nolisten",
        "tcp",
    ]

    print(
        f"Starting {target.gpu_bdf} on {target.connector} at {args.mode}; "
        f"Firefox will open {args.url}.",
        flush=True,
    )
    try:
        return subprocess.run(command, check=False).returncode
    finally:
        Path(xorg_config).unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"start_display_firefox: {error}", file=sys.stderr)
        raise SystemExit(1)
