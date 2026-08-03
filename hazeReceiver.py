from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import os
import pathlib
import re
import secrets
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse, urlunparse

import aiohttp
try:
    import av
    from aiortc import RTCPeerConnection, RTCSessionDescription
    try:
        from aiortc import RTCRtpSender
    except Exception:
        RTCRtpSender = None
except Exception as exc:
    av = None
    RTCPeerConnection = None
    RTCSessionDescription = None
    RTCRtpSender = None
    _WEBRTC_IMPORT_ERROR = exc
else:
    _WEBRTC_IMPORT_ERROR = None

pifm_bin = "/home/rai/PiFmAdv/src/pi_fm_adv"
DEFAULT_DEVIATION_HZ = 5000
DEFAULT_AUDIO_FILTERS = (
    'highpass=f=40,'
    'lowpass=f=5000,'
    'volume=30dB,'
    'alimiter=limit=0.90:level=disabled'
)
DEFAULT_PIFM_EXTRA_ARGS = ('--power', '1')

log = logging.getLogger(__name__)


class ReceiverHttpError(RuntimeError):
    def __init__(self, url: str, status: int, detail: str) -> None:
        super().__init__(f'{url} failed with HTTP {status}: {detail}')
        self.url = url
        self.status = status
        self.detail = detail


def _ensure_webrtc_dependencies() -> None:
    if _WEBRTC_IMPORT_ERROR is None:
        return
    raise RuntimeError(
        'hazeReceiver.py requires aiortc for secure WebRTC. '
        'With av>=17.1.0, install the current aiortc wheel without dependencies '
        'until upstream metadata supports PyAV 17: python -m pip install --no-deps aiortc==1.14.0'
    ) from _WEBRTC_IMPORT_ERROR


def _running_on_raspberry_pi() -> bool:
    for model_path in (
        pathlib.Path('/proc/device-tree/model'),
        pathlib.Path('/sys/firmware/devicetree/base/model'),
    ):
        try:
            model = model_path.read_text(encoding='utf-8', errors='ignore')
        except OSError:
            continue
        if 'raspberry pi' in model.replace('\x00', ' ').lower():
            return True
    return False


class _GStreamerPeerState:
    def __init__(self) -> None:
        self.connectionState = 'new'
        self.iceConnectionState = 'new'
        self.iceGatheringState = 'new'
        self.signalingState = 'stable'
        self.mediaBackend = 'gstreamer'
        self.remoteAnswerSet = False
        self.remoteAnswerSetTs = 0.0
        self.audioPadLinked = False
        self.audioPadCaps = ''


_GST_IMPORTS: tuple[Any, Any, Any, Any] | None = None
_GST_MAIN_LOOP: Any | None = None
_GST_MAIN_THREAD: threading.Thread | None = None


def _ensure_debian_dist_packages() -> None:
    if os.name == 'nt':
        return
    candidates = (
        pathlib.Path('/usr/lib/python3/dist-packages'),
        pathlib.Path(f'/usr/local/lib/python{sys.version_info.major}.{sys.version_info.minor}/dist-packages'),
    )
    for candidate in candidates:
        text = str(candidate)
        if candidate.exists() and text not in sys.path:
            sys.path.append(text)


def _ensure_gstreamer_dependencies() -> tuple[Any, Any, Any, Any]:
    global _GST_IMPORTS
    if _GST_IMPORTS is not None:
        return _GST_IMPORTS
    _ensure_debian_dist_packages()
    try:
        import gi

        gi.require_version('Gst', '1.0')
        gi.require_version('GstWebRTC', '1.0')
        gi.require_version('GstSdp', '1.0')
        from gi.repository import GLib, Gst, GstSdp, GstWebRTC
    except Exception as exc:
        raise RuntimeError(
            'GStreamer WebRTC backend needs python3-gi, python3-gst-1.0, '
            'gir1.2-gst-plugins-bad-1.0, gstreamer1.0-nice, and GStreamer Opus/RTP plugins'
        ) from exc
    Gst.init(None)
    missing = [
        name
        for name in ('webrtcbin', 'rtpopusdepay', 'opusdec', 'audioconvert', 'audioresample', 'queue', 'filesink')
        if Gst.ElementFactory.find(name) is None
    ]
    if missing:
        raise RuntimeError(f'GStreamer backend is missing required element(s): {", ".join(missing)}')
    _GST_IMPORTS = (Gst, GstWebRTC, GstSdp, GLib)
    return _GST_IMPORTS


def _ensure_gstreamer_main_loop(GLib: Any) -> None:
    global _GST_MAIN_LOOP, _GST_MAIN_THREAD
    if _GST_MAIN_LOOP is not None and _GST_MAIN_THREAD is not None and _GST_MAIN_THREAD.is_alive():
        return
    _GST_MAIN_LOOP = GLib.MainLoop()
    _GST_MAIN_THREAD = threading.Thread(
        target=_GST_MAIN_LOOP.run,
        name='haze-gstreamer-main-loop',
        daemon=True,
    )
    _GST_MAIN_THREAD.start()


def _gst_enum_nick(value: Any) -> str:
    nick = getattr(value, 'value_nick', None)
    if nick:
        return str(nick).replace('-', '_')
    name = getattr(value, 'name', None)
    if name:
        return str(name).rsplit('_', 1)[-1].lower()
    text = str(value or '').lower()
    return text.rsplit('.', 1)[-1].replace('-', '_') or 'unknown'


@dataclass(frozen=True)
class ReceiverConfig:
    server_url: str
    feed_id: str
    receiver_api_base: str
    pair_token: str
    state_file: pathlib.Path
    allow_insecure_dev: bool
    output_sample_rate: int
    channels: int
    webrtc_input_sample_rate: int
    ffmpeg_bin: str
    ffmpeg_log_level: str
    audio_filters: str
    pifmadv_bin: str
    pifm_extra_args: tuple[str, ...]
    reconnect_initial_delay_s: float
    reconnect_max_delay_s: float
    reconnect_backoff: float
    stream_stall_timeout_s: float
    pipe_drain_timeout_s: float
    status_interval_s: float
    write_chunk_size: int
    audio_frame_ms: int
    jitter_buffer_ms: int
    max_jitter_buffer_ms: int
    max_pacing_lag_ms: int
    max_active_underrun_ms: int
    preferred_codecs: tuple[str, ...]
    webrtc_backend: str
    http_codec: str
    http_reconnect_delay_max_s: int
    http_read_timeout_s: float
    metrics_interval_s: float
    diagnose_audio: bool
    diagnose_duration_s: float
    diagnose_output: pathlib.Path | None


def _safe_feed_name(feed_id: str) -> str:
    return re.sub(r'[^A-Za-z0-9_.-]+', '_', feed_id).strip('._') or 'default'


def _default_state_file(feed_id: str) -> pathlib.Path:
    base = pathlib.Path.home() / '.haze_receiver'
    return base / f'{_safe_feed_name(feed_id)}.json'


def _streaming_wav_header(sample_rate: int, channels: int) -> bytes:
    bits_per_sample = 16
    byte_rate = int(sample_rate) * int(channels) * bits_per_sample // 8
    block_align = int(channels) * bits_per_sample // 8
    return b''.join((
        b'RIFF',
        struct.pack('<I', 0xFFFFFFFF),
        b'WAVE',
        b'fmt ',
        struct.pack('<IHHIIHH', 16, 1, int(channels), int(sample_rate), byte_rate, block_align, bits_per_sample),
        b'data',
        struct.pack('<I', 0xFFFFFFFF),
    ))


def _pcm16_peak(data: bytes) -> int:
    usable = len(data) - (len(data) % 2)
    if usable <= 0:
        return 0
    samples = struct.unpack('<' + 'h' * (usable // 2), data[:usable])
    return max((abs(sample) for sample in samples), default=0)


def _pcm16_metrics(data: bytes, frame_samples: int = 960) -> dict[str, Any]:
    sample_count = len(data) // 2
    if sample_count <= 0:
        return {
            'samples': 0,
            'frames': 0,
            'peak': 0,
            'peak_dbfs': -120.0,
            'rms_avg_dbfs': -120.0,
            'rms_min_dbfs': -120.0,
            'rms_max_dbfs': -120.0,
            'near_silent_frames': 0,
            'repeated_frames': 0,
            'repeated_non_silent_frames': 0,
            'clipped_samples': 0,
            'zero_ratio': 0.0,
            'max_sample_jump': 0,
        }
    usable = sample_count * 2
    samples = struct.unpack('<' + 'h' * sample_count, data[:usable])
    frame_count = sample_count // frame_samples
    peak = 0
    clipped = 0
    zeros = 0
    max_jump = 0
    near_silent_frames = 0
    repeated_frames = 0
    repeated_non_silent_frames = 0
    rms_values: list[float] = []
    previous_frame: tuple[int, ...] | None = None
    previous_sample: int | None = None
    for sample in samples:
        abs_sample = abs(sample)
        peak = max(peak, abs_sample)
        if abs_sample >= 32760:
            clipped += 1
        if sample == 0:
            zeros += 1
        if previous_sample is not None:
            max_jump = max(max_jump, abs(sample - previous_sample))
        previous_sample = sample
    for start in range(0, frame_count * frame_samples, frame_samples):
        frame = tuple(samples[start:start + frame_samples])
        frame_peak = max((abs(sample) for sample in frame), default=0)
        if frame_peak <= 20:
            near_silent_frames += 1
        if previous_frame is not None and frame == previous_frame:
            repeated_frames += 1
            if frame_peak > 20:
                repeated_non_silent_frames += 1
        previous_frame = frame
        if frame:
            rms_values.append((sum(float(sample) * float(sample) for sample in frame) / len(frame)) ** 0.5)

    def dbfs(value: float) -> float:
        if value <= 0:
            return -120.0
        return 20.0 * math.log10(value / 32768.0)

    import math
    rms_avg = sum(rms_values) / len(rms_values) if rms_values else 0.0
    return {
        'samples': sample_count,
        'frames': frame_count,
        'peak': peak,
        'peak_dbfs': round(dbfs(float(peak)), 2),
        'rms_avg_dbfs': round(dbfs(rms_avg), 2),
        'rms_min_dbfs': round(dbfs(min(rms_values) if rms_values else 0.0), 2),
        'rms_max_dbfs': round(dbfs(max(rms_values) if rms_values else 0.0), 2),
        'near_silent_frames': near_silent_frames,
        'repeated_frames': repeated_frames,
        'repeated_non_silent_frames': repeated_non_silent_frames,
        'clipped_samples': clipped,
        'zero_ratio': round(zeros / sample_count, 6),
        'max_sample_jump': max_jump,
    }


def _normalize_server_url(raw: str, allow_insecure_dev: bool) -> str:
    value = str(raw or '').strip()
    if not value:
        raise ValueError('--server is required')
    parsed = urlparse(value)
    if not parsed.scheme:
        value = f'http://{value}'
        parsed = urlparse(value)
    if parsed.scheme in {'ws', 'wss'}:
        parsed = parsed._replace(scheme='https' if parsed.scheme == 'wss' else 'http')
    if parsed.scheme not in {'http', 'https'}:
        raise ValueError('--server must use http:// or https://')
    if not parsed.netloc:
        raise ValueError('--server must include a hostname or IP address')
    return urlunparse((parsed.scheme, parsed.netloc, '', '', '', '')).rstrip('/')


def _api_url(config: ReceiverConfig, path: str) -> str:
    base = '/' + config.receiver_api_base.strip('/') + '/'
    return urljoin(config.server_url + base, path.lstrip('/'))


def _receiver_proof_message(kind: str, values: dict[str, Any]) -> bytes:
    payload = {'kind': kind, **{str(k): str(v) for k, v in values.items()}}
    return json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')


def _receiver_hmac(secret: str, message: bytes) -> str:
    return hmac.new(secret.encode('utf-8'), message, hashlib.sha256).hexdigest()


def _sdp_audio_codecs(sdp: str) -> list[str]:
    codecs: list[str] = []
    for line in str(sdp or '').splitlines():
        line = line.strip()
        if not line.lower().startswith('a=rtpmap:'):
            continue
        _, _, payload = line.partition(' ')
        codec = payload.split('/', 1)[0].strip()
        if codec and codec not in codecs:
            codecs.append(codec)
    return codecs


def _sdp_candidate_summary(sdp: str) -> list[str]:
    candidates: list[str] = []
    for line in str(sdp or '').splitlines():
        line = line.strip()
        if not line.startswith('a=candidate:'):
            continue
        parts = line[len('a=candidate:'):].split()
        if len(parts) < 8:
            candidates.append(line)
            continue
        candidates.append(
            f'{parts[2].lower()} {parts[4]}:{parts[5]} typ {parts[7].lower()}'
        )
    return candidates


def _sdp_negotiation_summary(sdp: str) -> list[str]:
    prefixes = (
        'a=group:',
        'a=msid',
        'm=audio',
        'c=',
        'a=mid:',
        'a=rtpmap:',
        'a=fmtp:',
        'a=rtcp-fb:',
        'a=rtcp:',
        'a=rtcp-mux',
        'a=rtcp-rsize',
        'a=sendonly',
        'a=recvonly',
        'a=inactive',
        'a=setup:',
        'a=ice-ufrag:',
        'a=ice-pwd:',
        'a=ice-options:',
        'a=fingerprint:',
        'a=candidate:',
        'a=end-of-candidates',
    )
    lines: list[str] = []
    for line in str(sdp or '').splitlines():
        line = line.strip()
        if any(line.startswith(prefix) for prefix in prefixes):
            if line.startswith('a=ice-pwd:'):
                lines.append('a=ice-pwd:<redacted>')
            else:
                lines.append(line)
    return lines


def _transmitter_label(transmitter: dict[str, Any]) -> str:
    relationship = str(transmitter.get('relationship') or 'unknown').strip() or 'unknown'
    frequency = str(transmitter.get('frequency_mhz') or 'unknown').strip() or 'unknown'
    return f'{relationship}@{frequency}MHz'


def _load_state(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open('r', encoding='utf-8') as handle:
            data = json.load(handle)
    except Exception:
        log.warning('Receiver state file is unreadable; starting unpaired')
        return {}
    return data if isinstance(data, dict) else {}


def _save_state(path: pathlib.Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w', encoding='utf-8') as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
        handle.write('\n')
    os.replace(tmp, path)
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass


async def _post_json(session: aiohttp.ClientSession, url: str, payload: dict[str, Any]) -> dict[str, Any]:
    async with session.post(url, json=payload) as response:
        body = await response.text()
        if response.status >= 400:
            detail = body
            with contextlib.suppress(Exception):
                parsed = json.loads(body)
                detail = str(parsed.get('detail') or body)
            raise ReceiverHttpError(url, response.status, detail)
        try:
            parsed_body = json.loads(body or '{}')
        except json.JSONDecodeError as exc:
            raise RuntimeError(f'{url} returned invalid JSON') from exc
        if not isinstance(parsed_body, dict):
            raise RuntimeError(f'{url} returned a non-object JSON payload')
        return parsed_body


class ReceiverSupervisor:
    def __init__(self, config: ReceiverConfig) -> None:
        self.config = config
        self.stop_event = asyncio.Event()
        self.last_audio_ts = 0.0
        self.session_started_ts = 0.0
        self.last_input_audio_ts = 0.0
        self.last_ffmpeg_stdin_ts = 0.0
        self.last_processor_output_ts = 0.0
        self.last_pifm_output_ts = 0.0
        self.ffmpeg_stdin_drain_timeouts = 0
        self.pifm_stdin_drain_timeouts = 0
        self.ffmpeg_stdin_slow_drains = 0
        self.pifm_stdin_slow_drains = 0
        self.max_ffmpeg_stdin_drain_ms = 0.0
        self.max_pifm_stdin_drain_ms = 0.0
        self.receiver_hostname = socket.gethostname()
        self.state = _load_state(config.state_file)
        self.receiver_id = str(self.state.get('receiver_id') or uuid.uuid4())
        self.state['receiver_id'] = self.receiver_id
        self.state['feed_id'] = config.feed_id
        self.state['server_url'] = config.server_url
        last_transmitter = self.state.get('last_transmitter')
        self.last_transmitter = last_transmitter if isinstance(last_transmitter, dict) else None
        _save_state(config.state_file, self.state)

    async def run_forever(self) -> None:
        self._install_signal_handlers()
        delay = self.config.reconnect_initial_delay_s
        try:
            while not self.stop_event.is_set():
                attempt_started = time.monotonic()
                try:
                    reason = await self._run_once()
                    if (
                        self.last_input_audio_ts >= attempt_started
                        or self.last_processor_output_ts >= attempt_started
                    ):
                        delay = self.config.reconnect_initial_delay_s
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    reason = f'receiver failure: {exc}'
                if self.stop_event.is_set():
                    break
                log.warning('Receiver reconnect requested: %s', reason)
                await asyncio.sleep(delay)
                delay = min(self.config.reconnect_max_delay_s, delay * self.config.reconnect_backoff)
        finally:
            log.info('Receiver stopped')

    async def _run_once(self) -> str:
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=4, sock_read=None)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            receiver_session = await self._start_receiver_session(session)
            ws_url = str(receiver_session.get('ws_url') or '').strip()
            if not ws_url:
                raise RuntimeError('receiver session did not include ws_url')
            ws_headers = None
            receiver_cookie = str(receiver_session.get('cookie') or '').strip()
            if receiver_cookie:
                ws_headers = {'Authorization': f'HazeReceiverCookie {receiver_cookie}'}
            async with session.ws_connect(
                ws_url,
                headers=ws_headers,
                heartbeat=15,
                receive_timeout=None,
            ) as ws:
                transmitter = await self._wait_for_receiver_ready(ws)
                if transmitter.get('frequency_mhz') is not None:
                    self.last_transmitter = dict(transmitter)
                    self.state['last_transmitter'] = self.last_transmitter
                    _save_state(self.config.state_file, self.state)
                return await self._run_audio_session(session, ws, transmitter)

    async def _start_receiver_session(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        credential_id = str(self.state.get('credential_id') or '').strip()
        credential_secret = str(self.state.get('credential_secret') or '').strip()
        if credential_id and credential_secret:
            try:
                return await self._request_session_cookie(session, credential_id, credential_secret)
            except ReceiverHttpError as exc:
                if exc.status not in {401, 403}:
                    raise
                log.warning('Stored receiver credential was rejected; falling back to unpaired session')
        if self.config.pair_token:
            try:
                await self._pair_receiver(session)
                credential_id = str(self.state.get('credential_id') or '').strip()
                credential_secret = str(self.state.get('credential_secret') or '').strip()
                if credential_id and credential_secret:
                    return await self._request_session_cookie(session, credential_id, credential_secret)
            except ReceiverHttpError as exc:
                log.warning('Receiver pairing failed: %s', exc)
        return await self._receiver_auth(session)

    async def _wait_for_receiver_ready(self, ws: aiohttp.ClientWebSocketResponse) -> dict[str, Any]:
        deadline = time.monotonic() + 12.0
        while time.monotonic() < deadline:
            msg = await ws.receive(timeout=max(0.1, deadline - time.monotonic()))
            if msg.type == aiohttp.WSMsgType.ERROR:
                raise RuntimeError(f'receiver websocket error: {ws.exception()}')
            if msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE}:
                raise RuntimeError('receiver websocket closed before transmitter metadata arrived')
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            try:
                payload = json.loads(msg.data)
            except json.JSONDecodeError:
                continue
            if payload.get('type') == 'receiver_error':
                raise RuntimeError(f"receiver websocket error: {payload.get('detail')}")
            if payload.get('type') != 'receiver_ready':
                continue
            transmitter = payload.get('transmitter') if isinstance(payload.get('transmitter'), dict) else {}
            if not transmitter:
                raise RuntimeError('receiver_ready did not include transmitter metadata')
            log.info(
                'Receiver ready for feed %s transmitter=%s',
                payload.get('feed_id') or self.config.feed_id,
                _transmitter_label(transmitter),
            )
            return transmitter
        raise RuntimeError(f'feed {self.config.feed_id} receiver metadata timed out')

    async def _run_audio_session(
        self,
        session: aiohttp.ClientSession,
        ws: aiohttp.ClientWebSocketResponse,
        transmitter: dict[str, Any],
    ) -> str:
        _ = session
        log.info('Receiver selected WebRTC media transport for feed %s', self.config.feed_id)
        return await self._run_webrtc_session(ws, transmitter)

    def _http_audio_url(self) -> str:
        query = urlencode({
            'feed': self.config.feed_id,
            'codec': self.config.http_codec,
        })
        return f'{self.config.server_url}/api/public/v1/feed/audio?{query}'

    async def _http_audio_available(self, session: aiohttp.ClientSession) -> bool:
        url = self._http_audio_url()
        try:
            timeout = aiohttp.ClientTimeout(total=5, sock_connect=3, sock_read=3)
            async with session.head(url, timeout=timeout) as response:
                if response.status < 200 or response.status >= 300:
                    log.warning('HTTP receiver stream returned HTTP %s', response.status)
                    return False
                return True
        except Exception as exc:
            log.warning('HTTP receiver stream probe failed: %s', exc)
            return False

    async def _receiver_auth(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        return await _post_json(session, _api_url(self.config, 'session'), {
            'feed_id': self.config.feed_id,
            'receiver_id': self.receiver_id,
            'receiver_hostname': self.receiver_hostname,
        })

    async def _pair_receiver(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        nonce = secrets.token_urlsafe(24)
        challenge = await _post_json(session, _api_url(self.config, 'pair/challenge'), {
            'feed_id': self.config.feed_id,
            'receiver_id': self.receiver_id,
            'receiver_hostname': self.receiver_hostname,
            'nonce': nonce,
        })
        challenge_id = str(challenge.get('challenge_id') or '')
        server_nonce = str(challenge.get('server_nonce') or '')
        if not challenge_id or not server_nonce:
            raise RuntimeError('pairing challenge was missing required fields')
        proof = _receiver_hmac(
            self.config.pair_token,
            _receiver_proof_message('pair-v1', {
                'challenge_id': challenge_id,
                'feed_id': self.config.feed_id,
                'receiver_id': self.receiver_id,
                'receiver_hostname': self.receiver_hostname,
                'receiver_nonce': nonce,
                'server_nonce': server_nonce,
            }),
        )
        completed = await _post_json(session, _api_url(self.config, 'pair/complete'), {
            'challenge_id': challenge_id,
            'feed_id': self.config.feed_id,
            'receiver_id': self.receiver_id,
            'receiver_hostname': self.receiver_hostname,
            'nonce': nonce,
            'proof': proof,
        })
        credential_id = str(completed.get('credential_id') or '')
        credential_secret = str(completed.get('credential_secret') or '')
        if not credential_id or not credential_secret:
            raise RuntimeError('pairing response did not include receiver credentials')
        self.state.update({
            'credential_id': credential_id,
            'credential_secret': credential_secret,
            'paired_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        })
        _save_state(self.config.state_file, self.state)
        log.info('Receiver paired with %s for feed %s', self.config.server_url, self.config.feed_id)
        return completed

    async def _request_session_cookie(
        self,
        session: aiohttp.ClientSession,
        credential_id: str,
        credential_secret: str,
    ) -> dict[str, Any]:
        nonce = secrets.token_urlsafe(24)
        proof = _receiver_hmac(
            credential_secret,
            _receiver_proof_message('session-v1', {
                'credential_id': credential_id,
                'feed_id': self.config.feed_id,
                'receiver_id': self.receiver_id,
                'receiver_hostname': self.receiver_hostname,
                'nonce': nonce,
            }),
        )
        return await _post_json(session, _api_url(self.config, 'session'), {
            'feed_id': self.config.feed_id,
            'receiver_id': self.receiver_id,
            'receiver_hostname': self.receiver_hostname,
            'credential_id': credential_id,
            'nonce': nonce,
            'proof': proof,
        })

    async def _run_webrtc_session(self, ws: aiohttp.ClientWebSocketResponse, transmitter: dict[str, Any]) -> str:
        backend = self._selected_webrtc_backend()
        log.info('Receiver selected %s WebRTC backend for feed %s', backend, self.config.feed_id)
        if backend == 'gstreamer':
            return await self._run_gstreamer_webrtc_session(ws, transmitter)
        if backend == 'aiortc':
            return await self._run_aiortc_webrtc_session(ws, transmitter)
        return f'unsupported receiver WebRTC backend {backend}'

    def _selected_webrtc_backend(self) -> str:
        if self.config.webrtc_backend != 'auto':
            return self.config.webrtc_backend
        return 'aiortc'

    async def _run_gstreamer_webrtc_session(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        transmitter: dict[str, Any],
    ) -> str:
        if transmitter.get('frequency_mhz') is None:
            return 'receiver transmitter parameters did not include frequency_mhz'
        if os.name == 'nt' or not hasattr(os, 'mkfifo'):
            return 'GStreamer receiver backend requires a POSIX FIFO; run with --webrtc-backend=aiortc on this host'
        Gst, GstWebRTC, GstSdp, GLib = _ensure_gstreamer_dependencies()
        _ensure_gstreamer_main_loop(GLib)

        pifm_proc: asyncio.subprocess.Process | None = None
        ffmpeg_proc: asyncio.subprocess.Process | None = None
        fifo_path = pathlib.Path('/tmp') / f'haze-receiver-{_safe_feed_name(self.config.feed_id)}-{os.getpid()}-{uuid.uuid4().hex}.pcm'
        gst_context: dict[str, Any] | None = None
        pc_state = _GStreamerPeerState()
        try:
            os.mkfifo(fifo_path, 0o600)
            ffmpeg_proc = await self._start_fifo_audio_processor(transmitter, fifo_path)
            assert ffmpeg_proc.stdout is not None
            assert ffmpeg_proc.stderr is not None

            gst_context = self._build_gstreamer_receiver_pipeline(
                Gst,
                GstWebRTC,
                fifo_path,
                pc_state,
            )
            pipeline = gst_context['pipeline']
            webrtc = gst_context['webrtc']
            gst_done = gst_context['done']

            self._reset_session_health()
            if pipeline.set_state(Gst.State.PLAYING) == Gst.StateChangeReturn.FAILURE:
                return 'GStreamer receiver pipeline failed to start'
            offer_sdp = await self._create_gstreamer_offer(Gst, webrtc)
            log.info(
                'Sending GStreamer receiver WebRTC offer with audio codecs: %s',
                ', '.join(_sdp_audio_codecs(offer_sdp)) or 'unknown',
            )
            log.info(
                'GStreamer receiver local ICE candidates: %s',
                '; '.join(_sdp_candidate_summary(offer_sdp)) or 'none',
            )
            log.info(
                'GStreamer receiver local SDP summary: %s',
                ' | '.join(_sdp_negotiation_summary(offer_sdp)) or 'none',
            )
            await ws.send_json({
                'type': 'webrtc_offer',
                'feed_id': self.config.feed_id,
                'sdp': offer_sdp,
                'sdp_type': 'offer',
                'preferred_codec': 'opus',
                'require_opus': True,
                'disable_g722': True,
            })

            negotiation_deadline = time.monotonic() + max(8.0, self.config.stream_stall_timeout_s)
            while True:
                receive_timeout = negotiation_deadline - time.monotonic()
                if receive_timeout <= 0:
                    return 'webrtc negotiation timed out before answer'
                try:
                    msg = await asyncio.wait_for(ws.receive_json(), timeout=receive_timeout)
                except asyncio.TimeoutError:
                    return 'webrtc negotiation timed out before answer'
                msg_type = str(msg.get('type') or '')
                if msg_type == 'webrtc_answer':
                    answer_sdp = str(msg.get('sdp') or '')
                    answer_codecs = _sdp_audio_codecs(answer_sdp)
                    log.info(
                        'Received GStreamer receiver WebRTC answer with audio codecs: %s',
                        ', '.join(answer_codecs) or 'unknown',
                    )
                    log.info(
                        'GStreamer receiver remote ICE candidates: %s',
                        '; '.join(_sdp_candidate_summary(answer_sdp)) or 'none',
                    )
                    log.info(
                        'GStreamer receiver remote SDP summary: %s',
                        ' | '.join(_sdp_negotiation_summary(answer_sdp)) or 'none',
                    )
                    if 'opus' not in {codec.lower() for codec in answer_codecs}:
                        return 'webrtc answer did not include required Opus codec'
                    await self._set_gstreamer_remote_answer(Gst, GstWebRTC, GstSdp, webrtc, answer_sdp)
                    pc_state.remoteAnswerSet = True
                    pc_state.remoteAnswerSetTs = time.monotonic()
                    break
                if msg_type == 'webrtc_error':
                    return f'webrtc negotiation failed: {msg.get("detail")}'

            pifm_proc = await self._start_pifmadv(transmitter)
            assert pifm_proc.stdin is not None
            assert pifm_proc.stderr is not None
            pipe_task = asyncio.create_task(
                self._pump_processor_to_pifm(ffmpeg_proc.stdout, pifm_proc.stdin),
                name='gst_ffmpeg_to_pifm',
            )
            ffmpeg_wait_task = asyncio.create_task(self._wait_process(ffmpeg_proc, 'ffmpeg'), name='ffmpeg_wait')
            pifm_wait_task = asyncio.create_task(self._wait_process(pifm_proc, 'piFmAdv'), name='pifm_wait')
            ffmpeg_err_task = asyncio.create_task(self._log_stream(ffmpeg_proc.stderr, 'ffmpeg'), name='ffmpeg_stderr')
            pifm_err_task = asyncio.create_task(self._log_stream(pifm_proc.stderr, 'piFmAdv'), name='pifm_stderr')
            monitor_task = asyncio.create_task(self._monitor_health(ffmpeg_proc, pifm_proc, pc_state), name='monitor_health')
            ws_task = asyncio.create_task(self._watch_control_ws(ws), name='receiver_ws')
            status_task = asyncio.create_task(
                self._send_receiver_status(ws, ffmpeg_proc, pifm_proc, pc_state),
                name='receiver_status',
            )

            done, pending = await asyncio.wait(
                {gst_done, pipe_task, ffmpeg_wait_task, pifm_wait_task, monitor_task, ws_task, status_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            await self._cancel_tasks((*pending, ffmpeg_err_task, pifm_err_task), timeout_s=2.0)
            reason = 'GStreamer receiver session stopped'
            for task in done:
                with contextlib.suppress(asyncio.CancelledError):
                    result = await task
                    if isinstance(result, str) and result:
                        reason = result
            with contextlib.suppress(Exception):
                await self._send_receiver_status_once(ws, ffmpeg_proc, pifm_proc, pc_state)
            return reason
        finally:
            if gst_context is not None:
                await self._stop_gstreamer_pipeline(Gst, gst_context)
            await self._terminate_process(ffmpeg_proc, 'ffmpeg')
            await self._terminate_process(pifm_proc, 'piFmAdv')
            with contextlib.suppress(FileNotFoundError):
                fifo_path.unlink()

    def _build_gstreamer_receiver_pipeline(
        self,
        Gst: Any,
        GstWebRTC: Any,
        fifo_path: pathlib.Path,
        pc_state: _GStreamerPeerState,
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        pipeline = Gst.Pipeline.new('haze-receiver-webrtc')

        def make_element(factory: str, name: str) -> Any:
            element = Gst.ElementFactory.make(factory, name)
            if element is None:
                raise RuntimeError(f'GStreamer element {factory} is unavailable')
            pipeline.add(element)
            return element

        webrtc = make_element('webrtcbin', 'rtc')
        queue = make_element('queue', 'audio_queue')
        depay = make_element('rtpopusdepay', 'opus_depay')
        decoder = make_element('opusdec', 'opus_decoder')
        convert = make_element('audioconvert', 'audio_convert')
        resample = make_element('audioresample', 'audio_resample')
        capsfilter = make_element('capsfilter', 'audio_caps')
        progress = make_element('identity', 'audio_progress')
        filesink = make_element('filesink', 'pcm_fifo')

        with contextlib.suppress(Exception):
            webrtc.set_property('latency', int(self.config.jitter_buffer_ms))
        with contextlib.suppress(Exception):
            webrtc.set_property('bundle-policy', GstWebRTC.WebRTCBundlePolicy.MAX_BUNDLE)
        with contextlib.suppress(Exception):
            queue.set_property('leaky', 2)
        queue.set_property('max-size-buffers', 0)
        queue.set_property('max-size-bytes', 0)
        queue.set_property('max-size-time', int(max(self.config.max_jitter_buffer_ms, self.config.jitter_buffer_ms) * 1_000_000))
        with contextlib.suppress(Exception):
            decoder.set_property('use-inband-fec', True)
        capsfilter.set_property(
            'caps',
            Gst.Caps.from_string(
                'audio/x-raw,format=S16LE,layout=interleaved,'
                f'rate={self.config.webrtc_input_sample_rate},channels={self.config.channels}'
            ),
        )
        progress.set_property('signal-handoffs', True)
        filesink.set_property('location', str(fifo_path))
        filesink.set_property('sync', True)
        filesink.set_property('async', False)

        chain = (queue, depay, decoder, convert, resample, capsfilter, progress, filesink)
        for left, right in zip(chain, chain[1:]):
            if not left.link(right):
                raise RuntimeError(
                    'failed to link GStreamer receiver audio chain '
                    f'{left.get_name()} -> {right.get_name()}'
                )

        done: asyncio.Future[str] = loop.create_future()
        last_audio_mark = {'ts': 0.0}

        def complete_once(reason: str) -> None:
            if not done.done():
                done.set_result(reason)

        def thread_complete(reason: str) -> None:
            loop.call_soon_threadsafe(complete_once, reason)

        def on_pad_added(_element: Any, pad: Any) -> None:
            sink_pad = queue.get_static_pad('sink')
            if sink_pad is None or sink_pad.is_linked():
                return
            caps = pad.get_current_caps() or pad.query_caps(None)
            caps_text = caps.to_string() if caps is not None else ''
            if 'application/x-rtp' not in caps_text or 'audio' not in caps_text.lower():
                return
            result = pad.link(sink_pad)
            if result != Gst.PadLinkReturn.OK:
                thread_complete(f'GStreamer receiver failed to link WebRTC audio pad: {result}')
                return
            pc_state.audioPadLinked = True
            pc_state.audioPadCaps = caps_text or ''
            log.info('GStreamer receiver linked WebRTC audio pad: %s', caps_text or 'unknown caps')

        def on_handoff(_identity: Any, _buffer: Any, *_args: Any) -> None:
            now = time.monotonic()
            if now - last_audio_mark['ts'] < 0.1:
                return
            last_audio_mark['ts'] = now
            loop.call_soon_threadsafe(self._mark_input_audio_progress)

        def update_connection_state() -> None:
            with contextlib.suppress(Exception):
                pc_state.connectionState = _gst_enum_nick(webrtc.get_property('connection-state'))
            log.info('GStreamer WebRTC connection state: %s', pc_state.connectionState)

        def update_ice_state() -> None:
            with contextlib.suppress(Exception):
                pc_state.iceConnectionState = _gst_enum_nick(webrtc.get_property('ice-connection-state'))
            log.info('GStreamer WebRTC ICE state: %s', pc_state.iceConnectionState)

        def update_ice_gathering_state() -> None:
            with contextlib.suppress(Exception):
                pc_state.iceGatheringState = _gst_enum_nick(webrtc.get_property('ice-gathering-state'))
            log.info('GStreamer WebRTC ICE gathering state: %s', pc_state.iceGatheringState)

        def update_signaling_state() -> None:
            with contextlib.suppress(Exception):
                pc_state.signalingState = _gst_enum_nick(webrtc.get_property('signaling-state'))
            log.info('GStreamer WebRTC signaling state: %s', pc_state.signalingState)

        def on_bus_message(_bus: Any, message: Any) -> None:
            msg_type = message.type
            if msg_type == Gst.MessageType.ERROR:
                err, debug = message.parse_error()
                reason = f'GStreamer receiver error: {err}'
                if debug:
                    reason = f'{reason} ({debug})'
                thread_complete(reason)
            elif msg_type == Gst.MessageType.EOS:
                thread_complete('GStreamer receiver pipeline ended')

        webrtc.connect('pad-added', on_pad_added)
        webrtc.connect('notify::connection-state', lambda *_args: update_connection_state())
        webrtc.connect('notify::ice-connection-state', lambda *_args: update_ice_state())
        webrtc.connect('notify::ice-gathering-state', lambda *_args: update_ice_gathering_state())
        webrtc.connect('notify::signaling-state', lambda *_args: update_signaling_state())
        progress.connect('handoff', on_handoff)
        update_connection_state()
        update_ice_state()
        update_ice_gathering_state()
        update_signaling_state()
        bus = pipeline.get_bus()
        bus.add_signal_watch()
        bus_handler = bus.connect('message', on_bus_message)
        opus_caps = Gst.Caps.from_string('application/x-rtp,media=audio,encoding-name=OPUS,clock-rate=48000,payload=111')
        webrtc.emit('add-transceiver', GstWebRTC.WebRTCRTPTransceiverDirection.RECVONLY, opus_caps)
        return {
            'pipeline': pipeline,
            'webrtc': webrtc,
            'bus': bus,
            'bus_handler': bus_handler,
            'done': done,
        }

    async def _create_gstreamer_offer(self, Gst: Any, webrtc: Any) -> str:
        loop = asyncio.get_running_loop()
        offer_future: asyncio.Future[Any] = loop.create_future()

        def on_offer_created(promise: Any, _user_data: Any = None) -> None:
            try:
                reply = promise.get_reply()
                offer = reply.get_value('offer')
                webrtc.emit('set-local-description', offer, Gst.Promise.new())
            except Exception as exc:
                loop.call_soon_threadsafe(offer_future.set_exception, exc)
                return
            loop.call_soon_threadsafe(offer_future.set_result, offer)

        promise = Gst.Promise.new_with_change_func(on_offer_created, None)
        webrtc.emit('create-offer', None, promise)
        await asyncio.wait_for(offer_future, timeout=max(5.0, self.config.stream_stall_timeout_s))
        await self._wait_for_gstreamer_ice_complete(webrtc)
        local_description = webrtc.get_property('local-description')
        if local_description is None or getattr(local_description, 'sdp', None) is None:
            raise RuntimeError('GStreamer did not produce a local SDP offer')
        return local_description.sdp.as_text()

    async def _wait_for_gstreamer_ice_complete(self, webrtc: Any) -> None:
        loop = asyncio.get_running_loop()
        completed: asyncio.Future[None] = loop.create_future()

        def state_is_complete() -> bool:
            return _gst_enum_nick(webrtc.get_property('ice-gathering-state')) == 'complete'

        def complete_once() -> None:
            if not completed.done():
                completed.set_result(None)

        if state_is_complete():
            return

        def on_state_change(_element: Any, _param: Any) -> None:
            with contextlib.suppress(Exception):
                if state_is_complete():
                    loop.call_soon_threadsafe(complete_once)

        handler = webrtc.connect('notify::ice-gathering-state', on_state_change)
        try:
            await asyncio.wait_for(completed, timeout=max(5.0, self.config.stream_stall_timeout_s))
        finally:
            with contextlib.suppress(Exception):
                webrtc.disconnect(handler)

    async def _set_gstreamer_remote_answer(
        self,
        Gst: Any,
        GstWebRTC: Any,
        GstSdp: Any,
        webrtc: Any,
        answer_sdp: str,
    ) -> None:
        parse_result, sdp_message = GstSdp.SDPMessage.new()
        if parse_result != GstSdp.SDPResult.OK:
            raise RuntimeError(f'could not allocate GStreamer SDP message: {parse_result}')
        parse_result = GstSdp.sdp_message_parse_buffer(answer_sdp.encode('utf-8'), sdp_message)
        if parse_result != GstSdp.SDPResult.OK:
            raise RuntimeError(f'could not parse WebRTC answer SDP: {parse_result}')
        answer = GstWebRTC.WebRTCSessionDescription.new(GstWebRTC.WebRTCSDPType.ANSWER, sdp_message)
        promise = Gst.Promise.new()
        webrtc.emit('set-remote-description', answer, promise)
        promise_result = 'unknown'
        promise_reply = ''
        with contextlib.suppress(Exception):
            promise_result = str(promise.wait())
        with contextlib.suppress(Exception):
            reply = promise.get_reply()
            if reply is not None:
                promise_reply = reply.to_string()
        await asyncio.sleep(0)
        signaling_state = 'unknown'
        ice_state = 'unknown'
        connection_state = 'unknown'
        with contextlib.suppress(Exception):
            signaling_state = _gst_enum_nick(webrtc.get_property('signaling-state'))
        with contextlib.suppress(Exception):
            ice_state = _gst_enum_nick(webrtc.get_property('ice-connection-state'))
        with contextlib.suppress(Exception):
            connection_state = _gst_enum_nick(webrtc.get_property('connection-state'))
        log.info(
            'GStreamer set remote answer result=%s signaling=%s ice=%s connection=%s reply=%s',
            promise_result,
            signaling_state,
            ice_state,
            connection_state,
            promise_reply or 'none',
        )

    async def _stop_gstreamer_pipeline(self, Gst: Any, context: dict[str, Any]) -> None:
        pipeline = context.get('pipeline')
        bus = context.get('bus')
        handler = context.get('bus_handler')
        if bus is not None and handler is not None:
            with contextlib.suppress(Exception):
                bus.disconnect(handler)
            with contextlib.suppress(Exception):
                bus.remove_signal_watch()
        if pipeline is not None:
            with contextlib.suppress(Exception):
                pipeline.set_state(Gst.State.NULL)
            await asyncio.sleep(0)

    async def _run_aiortc_webrtc_session(self, ws: aiohttp.ClientWebSocketResponse, transmitter: dict[str, Any]) -> str:
        _ensure_webrtc_dependencies()
        if transmitter.get('frequency_mhz') is None:
            return 'receiver transmitter parameters did not include frequency_mhz'
        pifm_proc: asyncio.subprocess.Process | None = None
        ffmpeg_proc: asyncio.subprocess.Process | None = None
        pc: RTCPeerConnection | None = None
        try:
            pc = RTCPeerConnection()
            pc.mediaBackend = 'aiortc'
            track_future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()

            @pc.on('track')
            def _on_track(track: Any) -> None:
                if getattr(track, 'kind', '') == 'audio' and not track_future.done():
                    log.info('Receiver WebRTC audio track started: id=%s', getattr(track, 'id', 'unknown'))
                    track_future.set_result(track)

            @pc.on('iceconnectionstatechange')
            def _on_ice_state_change() -> None:
                log.info('Receiver WebRTC ICE state: %s', getattr(pc, 'iceConnectionState', 'unknown'))

            @pc.on('connectionstatechange')
            def _on_connection_state_change() -> None:
                log.info('Receiver WebRTC connection state: %s', getattr(pc, 'connectionState', 'unknown'))

            transceiver = pc.addTransceiver('audio', direction='recvonly')
            self._prefer_receiver_codecs(transceiver)
            offer = await pc.createOffer()
            await pc.setLocalDescription(offer)
            log.info(
                'Sending receiver WebRTC offer with audio codecs: %s',
                ', '.join(_sdp_audio_codecs(pc.localDescription.sdp)) or 'unknown',
            )
            await ws.send_json({
                'type': 'webrtc_offer',
                'feed_id': self.config.feed_id,
                'sdp': pc.localDescription.sdp,
                'sdp_type': pc.localDescription.type,
                'preferred_codec': 'opus',
                'require_opus': True,
                'disable_g722': True,
            })

            negotiation_deadline = time.monotonic() + max(8.0, self.config.stream_stall_timeout_s)
            while True:
                receive_timeout = negotiation_deadline - time.monotonic()
                if receive_timeout <= 0:
                    return 'webrtc negotiation timed out before answer'
                try:
                    msg = await asyncio.wait_for(ws.receive_json(), timeout=receive_timeout)
                except asyncio.TimeoutError:
                    return 'webrtc negotiation timed out before answer'
                msg_type = str(msg.get('type') or '')
                if msg_type == 'webrtc_answer':
                    answer_sdp = str(msg.get('sdp') or '')
                    answer_codecs = _sdp_audio_codecs(answer_sdp)
                    log.info(
                        'Received receiver WebRTC answer with audio codecs: %s',
                        ', '.join(answer_codecs) or 'unknown',
                    )
                    if 'opus' not in {codec.lower() for codec in answer_codecs}:
                        return 'webrtc answer did not include required Opus codec'
                    await pc.setRemoteDescription(RTCSessionDescription(sdp=answer_sdp, type=str(msg.get('sdp_type') or 'answer')))
                    break
                if msg_type == 'webrtc_error':
                    return f'webrtc negotiation failed: {msg.get("detail")}'

            try:
                track = await asyncio.wait_for(track_future, timeout=10.0)
            except asyncio.TimeoutError:
                return 'webrtc audio track timed out after answer'
            pifm_proc = await self._start_pifmadv(transmitter)
            ffmpeg_proc = await self._start_audio_processor(transmitter)
            assert ffmpeg_proc.stdin is not None
            assert ffmpeg_proc.stdout is not None
            assert ffmpeg_proc.stderr is not None
            assert pifm_proc.stdin is not None
            assert pifm_proc.stderr is not None
            self._reset_session_health()
            audio_task = asyncio.create_task(
                self._pump_track_to_processor(track, ffmpeg_proc.stdin),
                name='aiortc_track_to_ffmpeg',
            )
            pipe_task = asyncio.create_task(
                self._pump_processor_to_pifm(ffmpeg_proc.stdout, pifm_proc.stdin),
                name='ffmpeg_to_pifm',
            )
            ffmpeg_wait_task = asyncio.create_task(self._wait_process(ffmpeg_proc, 'ffmpeg'), name='ffmpeg_wait')
            pifm_wait_task = asyncio.create_task(self._wait_process(pifm_proc, 'piFmAdv'), name='pifm_wait')
            ffmpeg_err_task = asyncio.create_task(self._log_stream(ffmpeg_proc.stderr, 'ffmpeg'), name='ffmpeg_stderr')
            pifm_err_task = asyncio.create_task(self._log_stream(pifm_proc.stderr, 'piFmAdv'), name='pifm_stderr')
            monitor_task = asyncio.create_task(self._monitor_health(ffmpeg_proc, pifm_proc, pc), name='monitor_health')
            ws_task = asyncio.create_task(self._watch_control_ws(ws), name='receiver_ws')
            status_task = asyncio.create_task(
                self._send_receiver_status(ws, ffmpeg_proc, pifm_proc, pc),
                name='receiver_status',
            )

            done, pending = await asyncio.wait(
                {
                    audio_task,
                    pipe_task,
                    ffmpeg_wait_task,
                    pifm_wait_task,
                    monitor_task,
                    ws_task,
                    status_task,
                },
                return_when=asyncio.FIRST_COMPLETED,
            )
            await self._cancel_tasks((*pending, ffmpeg_err_task, pifm_err_task), timeout_s=2.0)
            reason = 'receiver session stopped'
            for task in done:
                with contextlib.suppress(asyncio.CancelledError):
                    result = await task
                    if isinstance(result, str) and result:
                        reason = result
            with contextlib.suppress(Exception):
                await self._send_receiver_status_once(ws, ffmpeg_proc, pifm_proc, pc)
            return reason
        finally:
            if pc is not None:
                await self._close_peer_connection(pc)
            await self._terminate_process(ffmpeg_proc, 'ffmpeg')
            await self._terminate_process(pifm_proc, 'piFmAdv')

    async def _run_http_audio_session(
        self,
        session: aiohttp.ClientSession,
        ws: aiohttp.ClientWebSocketResponse,
        transmitter: dict[str, Any],
    ) -> str:
        _ = session, ws, transmitter
        return 'HTTP receiver audio is disabled; receiver uses WebRTC only'

    def _can_direct_http_raw_to_pifm(self) -> bool:
        codec = self.config.http_codec.strip().lower().replace('-', '_')
        filters = self.config.audio_filters.strip().lower()
        return (
            codec in {'raw', 'raw_pcm16', 's16le'}
            and self.config.output_sample_rate == 48000
            and self.config.channels == 1
            and filters in {'', 'anull'}
        )

    async def _pump_http_raw_to_pifm(
        self,
        session: aiohttp.ClientSession,
        pifm_stdin: asyncio.StreamWriter,
    ) -> str:
        queue_frames = max(
            32,
            int(self.config.max_jitter_buffer_ms / max(1, self.config.audio_frame_ms)) * 4,
        )
        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=queue_frames)
        reader = asyncio.create_task(self._read_http_raw_audio(session, queue), name='http_raw_reader')
        writer = asyncio.create_task(self._write_paced_raw_audio(queue, pifm_stdin), name='http_raw_writer')
        try:
            done, pending = await asyncio.wait({reader, writer}, return_when=asyncio.FIRST_COMPLETED)
            await self._cancel_tasks(tuple(pending), timeout_s=1.0)
            for task in done:
                with contextlib.suppress(asyncio.CancelledError):
                    result = await task
                    if isinstance(result, str) and result:
                        return result
            return 'HTTP raw receiver session stopped'
        finally:
            await self._cancel_tasks((reader, writer), timeout_s=1.0)

    async def _read_http_raw_audio(
        self,
        session: aiohttp.ClientSession,
        queue: asyncio.Queue[bytes],
    ) -> str:
        url = self._http_audio_url()
        timeout = aiohttp.ClientTimeout(
            total=None,
            sock_connect=5,
            sock_read=max(5.0, self.config.http_read_timeout_s),
        )
        try:
            async with session.get(url, timeout=timeout) as response:
                if response.status < 200 or response.status >= 300:
                    detail = (await response.text())[:200]
                    return f'HTTP raw receiver stream returned HTTP {response.status}: {detail}'
                log.info(
                    'Reading HTTP raw PCM for feed %s through paced receiver clock',
                    self.config.feed_id,
                )
                last_chunk_at: float | None = None
                async for chunk in response.content.iter_chunked(self.config.write_chunk_size):
                    if not chunk:
                        continue
                    now = time.monotonic()
                    if last_chunk_at is not None:
                        gap_ms = (now - last_chunk_at) * 1000
                        if gap_ms > self.config.max_pacing_lag_ms:
                            log.warning('HTTP raw receiver chunk gap %.1f ms', gap_ms)
                    last_chunk_at = now
                    self._mark_input_audio_progress()
                    self._push_audio_chunk(queue, bytes(chunk))
        except asyncio.TimeoutError:
            return 'HTTP raw receiver stream timed out'
        except Exception as exc:
            return f'HTTP raw receiver stream failed: {exc}'
        return 'HTTP raw receiver stream ended'

    async def _write_paced_raw_audio(
        self,
        queue: asyncio.Queue[bytes],
        pifm_stdin: asyncio.StreamWriter,
        label: str = 'HTTP raw receiver',
    ) -> str:
        frame_ms = max(10, min(100, self.config.audio_frame_ms))
        sample_bytes = self.config.channels * 2
        frame_bytes = max(
            sample_bytes,
            int(self.config.output_sample_rate * sample_bytes * frame_ms / 1000),
        )
        frame_bytes -= frame_bytes % sample_bytes
        byte_rate = self.config.output_sample_rate * sample_bytes
        target_buffer_bytes = max(
            frame_bytes,
            int(byte_rate * max(0, self.config.jitter_buffer_ms) / 1000),
        )
        max_buffer_bytes = max(
            target_buffer_bytes + frame_bytes,
            int(byte_rate * max(self.config.max_jitter_buffer_ms, self.config.jitter_buffer_ms) / 1000),
        )
        target_buffer_bytes -= target_buffer_bytes % sample_bytes
        max_buffer_bytes -= max_buffer_bytes % sample_bytes
        frame_interval_s = frame_ms / 1000
        max_pacing_lag_s = max(frame_ms * 2, self.config.max_pacing_lag_ms) / 1000
        buffered = bytearray()
        silence = b'\x00' * frame_bytes
        primed = target_buffer_bytes <= frame_bytes
        recent_audio_active = False
        active_conceal_ms = 0
        next_tick = time.monotonic()
        underflows = 0
        dropped_bytes = 0
        last_underflow_log = 0.0
        last_drop_log = 0.0
        last_pacing_log = 0.0
        metrics_started_at = time.monotonic()
        next_metrics_at = metrics_started_at + self.config.metrics_interval_s
        bytes_written = 0
        frames_written = 0
        slow_drains = 0
        max_drain_ms = 0.0
        transport = getattr(pifm_stdin, 'transport', None)
        if transport is not None:
            with contextlib.suppress(Exception):
                transport.set_write_buffer_limits(
                    high=frame_bytes * 12,
                    low=frame_bytes * 4,
                )
        log.info(
            '%s pacing target=%dms max=%dms frame=%dms frame_bytes=%d',
            label,
            int(self.config.jitter_buffer_ms),
            int(self.config.max_jitter_buffer_ms),
            frame_ms,
            frame_bytes,
        )
        try:
            pifm_stdin.write(_streaming_wav_header(self.config.output_sample_rate, self.config.channels))
            ok, _, reason = await self._drain_stdin(pifm_stdin, 'piFmAdv')
            if not ok:
                return reason
        except (BrokenPipeError, ConnectionResetError):
            return 'piFmAdv stdin closed'
        except Exception as exc:
            return f'piFmAdv write failed: {exc}'

        while not self.stop_event.is_set():
            while True:
                try:
                    buffered.extend(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if len(buffered) < frame_bytes:
                try:
                    buffered.extend(await asyncio.wait_for(queue.get(), timeout=frame_ms / 1000))
                except asyncio.TimeoutError:
                    pass
                while True:
                    try:
                        buffered.extend(queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

            if not primed and len(buffered) > target_buffer_bytes:
                drop_bytes = len(buffered) - target_buffer_bytes
                drop_bytes -= drop_bytes % sample_bytes
                if drop_bytes > 0:
                    del buffered[:drop_bytes]
                    dropped_bytes += drop_bytes

            if len(buffered) > max_buffer_bytes:
                drop_bytes = len(buffered) - target_buffer_bytes
                drop_bytes -= drop_bytes % sample_bytes
                if drop_bytes > 0:
                    del buffered[:drop_bytes]
                    dropped_bytes += drop_bytes
                    now = time.monotonic()
                    if now - last_drop_log >= 5.0:
                        log.warning(
                            '%s buffer overflow; dropped %.0fms total buffered audio',
                            label,
                            dropped_bytes / byte_rate * 1000 if byte_rate else 0,
                    )
                        last_drop_log = now
                    primed = len(buffered) >= target_buffer_bytes

            if not primed and len(buffered) >= target_buffer_bytes:
                primed = True
                next_tick = time.monotonic()

            if primed and len(buffered) >= frame_bytes:
                chunk = bytes(buffered[:frame_bytes])
                del buffered[:frame_bytes]
                recent_audio_active = _pcm16_peak(chunk) > 96
            else:
                if primed:
                    underflows += 1
                    primed = False
                    now = time.monotonic()
                    if recent_audio_active:
                        return (
                            f'{label} underrun during active audio; reconnecting instead of inserting silence'
                        )
                    if now - last_underflow_log >= 5.0:
                        log.warning(
                            '%s buffer underrun; outputting silence while rebuffering with %.0fms held audio after %d underrun(s)',
                            label,
                            len(buffered) / byte_rate * 1000 if byte_rate else 0,
                            underflows,
                        )
                        last_underflow_log = now
                chunk = silence
            try:
                pifm_stdin.write(chunk)
                ok, drain_ms, reason = await self._drain_stdin(pifm_stdin, 'piFmAdv')
                if not ok:
                    return reason
                max_drain_ms = max(max_drain_ms, drain_ms)
                if drain_ms > self.config.max_pacing_lag_ms:
                    slow_drains += 1
                    log.warning('piFmAdv raw stdin drain took %.1f ms', drain_ms)
                bytes_written += len(chunk)
                frames_written += 1
            except (BrokenPipeError, ConnectionResetError):
                return 'piFmAdv stdin closed'
            except Exception as exc:
                return f'piFmAdv write failed: {exc}'

            next_tick += frame_interval_s
            now = time.monotonic()
            sleep_for = next_tick - now
            if sleep_for < 0:
                lag_s = -sleep_for
                if lag_s >= max_pacing_lag_s and now - last_pacing_log >= 5.0:
                    log.warning(
                        '%s writer lagged %.0fms; reanchored pacing clock without catch-up',
                        label,
                        lag_s * 1000,
                    )
                    last_pacing_log = now
                next_tick = now + frame_interval_s
                sleep_for = frame_interval_s
            if self.config.metrics_interval_s > 0 and now >= next_metrics_at:
                elapsed = max(0.001, now - metrics_started_at)
                log.info(
                    '%s metrics feed=%s bytes=%d frames=%d elapsed=%.1fs '
                    'buffered_ms=%.0f dropped_ms=%.0f max_drain_ms=%.1f slow_drains=%d',
                    label,
                    self.config.feed_id,
                    bytes_written,
                    frames_written,
                    elapsed,
                    len(buffered) / byte_rate * 1000 if byte_rate else 0,
                    dropped_bytes / byte_rate * 1000 if byte_rate else 0,
                    max_drain_ms,
                    slow_drains,
                )
                next_metrics_at = time.monotonic() + self.config.metrics_interval_s
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
        return 'shutdown requested'

    def _prefer_receiver_codecs(self, transceiver: Any) -> None:
        if RTCRtpSender is None or not hasattr(transceiver, 'setCodecPreferences'):
            return
        with contextlib.suppress(Exception):
            capabilities = RTCRtpSender.getCapabilities('audio')
            available = list(getattr(capabilities, 'codecs', []) or [])
            ordered_mimes = ['audio/opus']
            preferred = []
            for mime in ordered_mimes:
                preferred.extend(
                    codec
                    for codec in available
                    if str(getattr(codec, 'mimeType', '')).lower() == mime
                    and codec not in preferred
                )
            if preferred:
                transceiver.setCodecPreferences(preferred)
                names = [str(getattr(codec, 'mimeType', '')).split('/', 1)[-1] for codec in preferred]
                log.info('Receiver preferred WebRTC codecs: %s', ', '.join(names))

    async def _start_audio_processor(self, transmitter: dict[str, Any]) -> asyncio.subprocess.Process:
        cmd = [
            self.config.ffmpeg_bin,
            '-hide_banner',
            '-nostats',
            '-loglevel',
            self.config.ffmpeg_log_level,
            '-filter_threads',
            '1',
            '-nostdin',
            '-f',
            's16le',
            '-ar',
            str(self.config.webrtc_input_sample_rate),
            '-ac',
            str(self.config.channels),
            '-i',
            'pipe:0',
            '-af',
            self.config.audio_filters,
            '-ar',
            str(self.config.output_sample_rate),
            '-ac',
            str(self.config.channels),
            '-acodec',
            'pcm_s16le',
            '-f',
            'wav',
            'pipe:1',
        ]
        log.info(
            'Starting WebRTC audio processor for %s (%d Hz -> %d Hz)',
            _transmitter_label(transmitter),
            self.config.webrtc_input_sample_rate,
            self.config.output_sample_rate,
        )
        return await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **self._process_kwargs(),
        )

    async def _start_fifo_audio_processor(
        self,
        transmitter: dict[str, Any],
        fifo_path: pathlib.Path,
    ) -> asyncio.subprocess.Process:
        cmd = [
            self.config.ffmpeg_bin,
            '-hide_banner',
            '-nostats',
            '-loglevel',
            self.config.ffmpeg_log_level,
            '-filter_threads',
            '1',
            '-nostdin',
            '-f',
            's16le',
            '-ar',
            str(self.config.webrtc_input_sample_rate),
            '-ac',
            str(self.config.channels),
            '-i',
            str(fifo_path),
            '-af',
            self.config.audio_filters,
            '-ar',
            str(self.config.output_sample_rate),
            '-ac',
            str(self.config.channels),
            '-acodec',
            'pcm_s16le',
            '-f',
            'wav',
            'pipe:1',
        ]
        log.info(
            'Starting GStreamer WebRTC audio processor for %s through %s (%d Hz -> %d Hz)',
            _transmitter_label(transmitter),
            fifo_path,
            self.config.webrtc_input_sample_rate,
            self.config.output_sample_rate,
        )
        return await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **self._process_kwargs(),
        )

    async def _start_http_audio_processor(self, transmitter: dict[str, Any]) -> asyncio.subprocess.Process:
        url = self._http_audio_url()
        input_args = []
        if self.config.http_codec.strip().lower().replace('-', '_') in {'raw', 'raw_pcm16', 's16le'}:
            input_args = [
                '-f',
                's16le',
                '-ar',
                '48000',
                '-ac',
                '1',
            ]
        cmd = [
            self.config.ffmpeg_bin,
            '-hide_banner',
            '-nostats',
            '-loglevel',
            self.config.ffmpeg_log_level,
            '-filter_threads',
            '1',
            '-nostdin',
            '-reconnect',
            '1',
            '-reconnect_streamed',
            '1',
            '-reconnect_at_eof',
            '1',
            '-reconnect_delay_max',
            str(self.config.http_reconnect_delay_max_s),
            '-rw_timeout',
            str(int(self.config.http_read_timeout_s * 1_000_000)),
            *input_args,
            '-i',
            url,
            '-af',
            self.config.audio_filters,
            '-ar',
            str(self.config.output_sample_rate),
            '-ac',
            str(self.config.channels),
            '-acodec',
            'pcm_s16le',
            '-f',
            'wav',
            'pipe:1',
        ]
        log.info(
            'Starting HTTP receiver audio processor for %s from %s (%d Hz -> %d Hz)',
            _transmitter_label(transmitter),
            url,
            48000,
            self.config.output_sample_rate,
        )
        return await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **self._process_kwargs(),
        )

    async def _start_pifmadv(self, transmitter: dict[str, Any]) -> asyncio.subprocess.Process:
        frequency_mhz = float(transmitter['frequency_mhz'])
        deviation_hz = int(transmitter.get('deviation_hz') or DEFAULT_DEVIATION_HZ)
        preemphasis = str(transmitter.get('preemphasis') or 'none').strip().lower()
        cmd = [
            self.config.pifmadv_bin,
            '--audio',
            '-',
            '--freq',
            str(frequency_mhz),
            '--dev',
            str(deviation_hz),
            '--rds',
            '0',
            *self.config.pifm_extra_args,
        ]
        if preemphasis in {'50', '50us', 'eu'}:
            cmd.extend(['--preemph', 'eu'])
        elif preemphasis in {'75', '75us', 'us'}:
            cmd.extend(['--preemph', 'us'])
        log.info(
            'Starting piFmAdv transmitter %.3f MHz relationship=%s deviation=%dHz preemphasis=%s (%s)',
            frequency_mhz,
            transmitter.get('relationship') or 'unknown',
            deviation_hz,
            preemphasis,
            self.config.pifmadv_bin,
        )
        return await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            **self._process_kwargs(),
        )

    async def _pump_track_to_processor(self, track: Any, ffmpeg_stdin: asyncio.StreamWriter) -> str:
        layout = 'mono' if self.config.channels == 1 else 'stereo'
        resampler = av.AudioResampler(format='s16', layout=layout, rate=self.config.webrtc_input_sample_rate)
        sample_bytes = self.config.channels * 2
        frame_ms = max(10, min(100, self.config.audio_frame_ms))
        frame_bytes = max(
            sample_bytes,
            int(self.config.webrtc_input_sample_rate * sample_bytes * frame_ms / 1000),
        )
        frame_bytes -= frame_bytes % sample_bytes
        stall_timeout_s = max(2.0, self.config.stream_stall_timeout_s)
        transport = getattr(ffmpeg_stdin, 'transport', None)
        if transport is not None:
            with contextlib.suppress(Exception):
                transport.set_write_buffer_limits(high=frame_bytes * 12, low=frame_bytes * 4)
        log.info(
            'Receiver WebRTC using aiortc track-paced audio handoff frame=%dms stall_timeout=%.1fs',
            frame_ms,
            stall_timeout_s,
        )
        logged_frame = False
        last_drain_log = 0.0
        try:
            while not self.stop_event.is_set():
                try:
                    frame = await asyncio.wait_for(track.recv(), timeout=stall_timeout_s)
                except asyncio.TimeoutError:
                    return f'webrtc audio track stalled for {stall_timeout_s:.1f}s'
                except Exception as exc:
                    return f'webrtc audio track ended: {exc}'
                if not logged_frame:
                    log.info(
                        'Receiver WebRTC audio frame: rate=%s layout=%s samples=%s format=%s',
                        getattr(frame, 'sample_rate', 'unknown'),
                        getattr(getattr(frame, 'layout', None), 'name', 'unknown'),
                        getattr(frame, 'samples', 'unknown'),
                        getattr(getattr(frame, 'format', None), 'name', 'unknown'),
                    )
                    logged_frame = True
                try:
                    out_frames = tuple(resampler.resample(frame))
                except Exception as exc:
                    return f'webrtc audio resample failed: {exc}'
                for out_frame in out_frames:
                    needed = int(out_frame.samples) * sample_bytes
                    if needed <= 0:
                        continue
                    raw = bytes(out_frame.planes[0])
                    if len(raw) < needed:
                        raw += b'\x00' * (needed - len(raw))
                    chunk = raw[:needed]
                    if not chunk:
                        continue
                    self._mark_input_audio_progress()
                    try:
                        ffmpeg_stdin.write(chunk)
                        ok, drain_ms, reason = await self._drain_stdin(ffmpeg_stdin, 'ffmpeg')
                        if not ok:
                            return reason
                        if drain_ms > self.config.max_pacing_lag_ms:
                            now = time.monotonic()
                            if now - last_drain_log >= 5.0:
                                log.warning('ffmpeg stdin drain took %.1f ms', drain_ms)
                                last_drain_log = now
                            return f'ffmpeg stdin drain lagged {drain_ms:.1f}ms; reconnecting instead of building audio backlog'
                    except (BrokenPipeError, ConnectionResetError):
                        return 'ffmpeg stdin closed'
                    except Exception as exc:
                        return f'ffmpeg write failed: {exc}'
            return 'shutdown requested'
        finally:
            with contextlib.suppress(Exception):
                track.stop()

    async def _read_track_audio(self, track: Any, queue: asyncio.Queue[bytes]) -> str:
        layout = 'mono' if self.config.channels == 1 else 'stereo'
        resampler = av.AudioResampler(format='s16', layout=layout, rate=self.config.webrtc_input_sample_rate)
        logged_frame = False
        while not self.stop_event.is_set():
            try:
                frame = await track.recv()
            except Exception as exc:
                return f'webrtc audio track ended: {exc}'
            if not logged_frame:
                log.info(
                    'Receiver WebRTC audio frame: rate=%s layout=%s samples=%s format=%s',
                    getattr(frame, 'sample_rate', 'unknown'),
                    getattr(getattr(frame, 'layout', None), 'name', 'unknown'),
                    getattr(frame, 'samples', 'unknown'),
                    getattr(getattr(frame, 'format', None), 'name', 'unknown'),
                )
                logged_frame = True
            chunks: list[bytes] = []
            try:
                for out_frame in resampler.resample(frame):
                    needed = int(out_frame.samples) * self.config.channels * 2
                    raw = bytes(out_frame.planes[0])
                    if len(raw) < needed:
                        raw += b'\x00' * (needed - len(raw))
                    chunks.append(raw[:needed])
            except Exception as exc:
                return f'webrtc audio resample failed: {exc}'
            chunk = b''.join(chunks)
            if not chunk:
                continue
            self._mark_input_audio_progress()
            if not self._push_audio_chunk(queue, chunk, drop_oldest=False):
                return 'webrtc audio queue overflow; reconnecting instead of dropping live audio'
        return 'shutdown requested'

    async def _write_paced_audio(self, queue: asyncio.Queue[bytes], ffmpeg_stdin: asyncio.StreamWriter) -> str:
        frame_ms = max(10, min(100, self.config.audio_frame_ms))
        frame_samples = max(1, int(self.config.webrtc_input_sample_rate * frame_ms / 1000))
        sample_bytes = self.config.channels * 2
        frame_bytes = frame_samples * sample_bytes
        byte_rate = self.config.webrtc_input_sample_rate * sample_bytes
        target_buffer_bytes = max(
            frame_bytes,
            int(byte_rate * max(0, self.config.jitter_buffer_ms) / 1000),
        )
        max_buffer_bytes = max(
            target_buffer_bytes + frame_bytes,
            int(byte_rate * max(self.config.max_jitter_buffer_ms, self.config.jitter_buffer_ms) / 1000),
        )
        target_buffer_bytes -= target_buffer_bytes % sample_bytes
        max_buffer_bytes -= max_buffer_bytes % sample_bytes
        frame_interval_s = frame_ms / 1000
        max_pacing_lag_s = max(frame_ms * 2, self.config.max_pacing_lag_ms) / 1000
        active_rebuffer_bytes = max(frame_bytes, min(target_buffer_bytes, frame_bytes * 4))
        rebuffer_target_bytes = target_buffer_bytes
        buffered = bytearray()
        primed = target_buffer_bytes <= frame_bytes
        recent_audio_active = False
        active_conceal_ms = 0
        next_tick = time.monotonic()
        underflows = 0
        partial_frames_padded = 0
        last_underflow_log = 0.0
        last_pacing_log = 0.0
        last_drain_log = 0.0
        last_partial_log = 0.0
        silence = b'\x00' * frame_bytes
        min_partial_frame_bytes = max(sample_bytes, int(frame_bytes * 3 / 4))
        min_partial_frame_bytes -= min_partial_frame_bytes % sample_bytes
        transport = getattr(ffmpeg_stdin, 'transport', None)
        if transport is not None:
            with contextlib.suppress(Exception):
                transport.set_write_buffer_limits(high=frame_bytes * 12, low=frame_bytes * 4)
        log.info(
            'Receiver WebRTC handoff buffer target=%dms max=%dms frame=%dms pacing_reset=%dms',
            int(self.config.jitter_buffer_ms),
            int(self.config.max_jitter_buffer_ms),
            frame_ms,
            self.config.max_pacing_lag_ms,
        )
        while not self.stop_event.is_set():
            while True:
                try:
                    buffered.extend(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if len(buffered) < frame_bytes:
                try:
                    buffered.extend(await asyncio.wait_for(queue.get(), timeout=frame_ms / 1000))
                except asyncio.TimeoutError:
                    pass
                while True:
                    try:
                        buffered.extend(queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

            if len(buffered) > max_buffer_bytes:
                return (
                    'Receiver WebRTC handoff backlog exceeded '
                    f'{len(buffered) / byte_rate * 1000:.0f}ms; reconnecting instead of trimming live audio'
                )

            if not primed and len(buffered) >= rebuffer_target_bytes:
                primed = True
                rebuffer_target_bytes = target_buffer_bytes
                next_tick = time.monotonic()

            if primed and len(buffered) >= frame_bytes:
                chunk = bytes(buffered[:frame_bytes])
                del buffered[:frame_bytes]
                recent_audio_active = _pcm16_peak(chunk) > 96
                active_conceal_ms = 0
            elif primed and len(buffered) >= min_partial_frame_bytes:
                pad_bytes = frame_bytes - len(buffered)
                chunk = bytes(buffered) + (b'\x00' * pad_bytes)
                buffered.clear()
                recent_audio_active = _pcm16_peak(chunk) > 96
                partial_frames_padded += 1
                if recent_audio_active:
                    active_conceal_ms += max(1, int(round(pad_bytes / byte_rate * 1000))) if byte_rate else frame_ms
                    if active_conceal_ms >= self.config.max_active_underrun_ms:
                        return (
                            'Receiver WebRTC active audio had repeated short frames for '
                            f'{active_conceal_ms}ms; reconnecting without catch-up'
                        )
                else:
                    active_conceal_ms = 0
                now = time.monotonic()
                if now - last_partial_log >= 10.0:
                    log.warning(
                        'Receiver WebRTC short audio frame padded %.0fms of silence after %d padded frame(s)',
                        pad_bytes / byte_rate * 1000 if byte_rate else 0,
                        partial_frames_padded,
                    )
                    last_partial_log = now
            else:
                if primed:
                    underflows += 1
                    primed = False
                    now = time.monotonic()
                    rebuffer_target_bytes = active_rebuffer_bytes if recent_audio_active else target_buffer_bytes
                    if now - last_underflow_log >= 5.0:
                        if recent_audio_active:
                            log.warning(
                                'Receiver WebRTC active audio underrun; rebuffering to %.0fms with %.0fms held audio after %d underrun(s)',
                                rebuffer_target_bytes / byte_rate * 1000 if byte_rate else 0,
                                len(buffered) / byte_rate * 1000,
                                underflows,
                            )
                        else:
                            log.warning(
                                'Receiver WebRTC handoff buffer underrun; rebuffering to %.0fms with %.0fms held audio after %d underrun(s)',
                                rebuffer_target_bytes / byte_rate * 1000 if byte_rate else 0,
                                len(buffered) / byte_rate * 1000,
                                underflows,
                            )
                        last_underflow_log = now
                chunk = silence
                if recent_audio_active:
                    active_conceal_ms += frame_ms
                    if active_conceal_ms >= self.config.max_active_underrun_ms:
                        return (
                            'Receiver WebRTC active audio missing for '
                            f'{active_conceal_ms}ms; reconnecting without catch-up'
                        )
            try:
                ffmpeg_stdin.write(chunk)
                ok, drain_ms, reason = await self._drain_stdin(ffmpeg_stdin, 'ffmpeg')
                if not ok:
                    return reason
                drain_now = time.monotonic()
                if drain_ms > self.config.max_pacing_lag_ms:
                    if drain_now - last_drain_log >= 5.0:
                        log.warning('ffmpeg stdin drain took %.1f ms', drain_ms)
                        last_drain_log = drain_now
                    return f'ffmpeg stdin drain lagged {drain_ms:.1f}ms; reconnecting instead of building audio backlog'
            except (BrokenPipeError, ConnectionResetError):
                return 'ffmpeg stdin closed'
            except Exception as exc:
                return f'ffmpeg write failed: {exc}'
            next_tick += frame_interval_s
            now = time.monotonic()
            sleep_for = next_tick - now
            if sleep_for < 0:
                lag_s = -sleep_for
                if lag_s >= max_pacing_lag_s:
                    if now - last_pacing_log >= 5.0:
                        log.warning(
                            'Receiver WebRTC audio writer lagged %.0fms; reconnecting instead of catch-up',
                            lag_s * 1000,
                        )
                        last_pacing_log = now
                    return f'Receiver WebRTC audio writer lagged {lag_s * 1000:.0f}ms; reconnecting instead of catch-up'
                next_tick = now + frame_interval_s
                sleep_for = frame_interval_s
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
        return 'shutdown requested'

    def _push_audio_chunk(self, queue: asyncio.Queue[bytes], chunk: bytes, *, drop_oldest: bool = True) -> bool:
        while queue.full():
            if not drop_oldest:
                return False
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()
        try:
            queue.put_nowait(chunk)
        except asyncio.QueueFull:
            return False
        return True

    async def _pump_processor_to_pifm(
        self,
        ffmpeg_stdout: asyncio.StreamReader,
        pifm_stdin: asyncio.StreamWriter,
    ) -> str:
        emit_post_filter_metrics = os.environ.get('HAZE_RECEIVER_POST_FILTER_METRICS', '').strip().lower() in {'1', 'true', 'yes', 'on'}
        metrics_buffer = bytearray()
        next_metrics_at = time.monotonic() + max(1.0, self.config.metrics_interval_s)
        frame_samples = max(1, int(self.config.output_sample_rate * self.config.audio_frame_ms / 1000))
        sample_bytes = self.config.channels * 2
        byte_rate = max(1, self.config.output_sample_rate * sample_bytes)
        pace_chunk_bytes = max(sample_bytes, frame_samples * sample_bytes)
        max_pacing_lag_s = max(self.config.audio_frame_ms * 2, self.config.max_pacing_lag_ms) / 1000
        metrics_window_bytes = max(
            pace_chunk_bytes,
            int(byte_rate * 2),
        )
        transport = getattr(pifm_stdin, 'transport', None)
        if transport is not None:
            with contextlib.suppress(Exception):
                transport.set_write_buffer_limits(high=pace_chunk_bytes * 4, low=pace_chunk_bytes)
        header_buffer = bytearray()
        wav_header_done = False
        next_tick = time.monotonic()
        last_pacing_log = 0.0
        last_drain_log = 0.0
        log.info(
            'Receiver pacing ffmpeg output to piFmAdv frame=%dms chunk=%dB',
            self.config.audio_frame_ms,
            pace_chunk_bytes,
        )

        async def write_pifm(data: bytes) -> tuple[bool, str]:
            nonlocal last_drain_log
            if not data:
                return True, ''
            try:
                pifm_stdin.write(data)
                ok, drain_ms, reason = await self._drain_stdin(pifm_stdin, 'piFmAdv')
                if not ok:
                    return False, reason
                if drain_ms > self.config.max_pacing_lag_ms:
                    now = time.monotonic()
                    if now - last_drain_log >= 5.0:
                        log.warning('piFmAdv stdin drain took %.1f ms', drain_ms)
                        last_drain_log = now
                    return False, f'piFmAdv stdin drain lagged {drain_ms:.1f}ms; reconnecting instead of building RF backlog'
                return True, ''
            except (BrokenPipeError, ConnectionResetError):
                return False, 'piFmAdv stdin closed'
            except Exception as exc:
                return False, f'piFmAdv write failed: {exc}'

        async def write_audio_payload(data: bytes) -> tuple[bool, str]:
            nonlocal next_tick, last_pacing_log
            pending = memoryview(data)
            while pending:
                chunk_len = min(len(pending), pace_chunk_bytes)
                chunk = pending[:chunk_len].tobytes()
                pending = pending[chunk_len:]
                ok, reason = await write_pifm(chunk)
                if not ok:
                    return False, reason
                next_tick += len(chunk) / byte_rate
                now = time.monotonic()
                sleep_for = next_tick - now
                if sleep_for < 0:
                    lag_s = -sleep_for
                    if lag_s >= max_pacing_lag_s:
                        if now - last_pacing_log >= 5.0:
                            log.warning(
                                'piFmAdv output pacing lagged %.0fms; reconnecting instead of catch-up',
                                lag_s * 1000,
                            )
                            last_pacing_log = now
                        return False, f'piFmAdv output pacing lagged {lag_s * 1000:.0f}ms; reconnecting instead of catch-up'
                    next_tick = now
                    sleep_for = 0.0
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
            return True, ''

        while not self.stop_event.is_set():
            chunk = await ffmpeg_stdout.read(self.config.write_chunk_size)
            if not chunk:
                return 'ffmpeg output ended'
            self._mark_processor_output_progress()
            if emit_post_filter_metrics and self.config.metrics_interval_s > 0:
                metrics_buffer.extend(chunk)
                if len(metrics_buffer) > metrics_window_bytes:
                    del metrics_buffer[:-metrics_window_bytes]
                now = time.monotonic()
                if now >= next_metrics_at and metrics_buffer:
                    metrics = _pcm16_metrics(bytes(metrics_buffer), frame_samples)
                    log.info(
                        'Receiver post-filter PCM metrics feed=%s samples=%d peak_dbfs=%.2f rms_avg_dbfs=%.2f rms_max_dbfs=%.2f clipped_samples=%d zero_ratio=%.4f max_jump=%d',
                        self.config.feed_id,
                        metrics['samples'],
                        metrics['peak_dbfs'],
                        metrics['rms_avg_dbfs'],
                        metrics['rms_max_dbfs'],
                        metrics['clipped_samples'],
                        metrics['zero_ratio'],
                        metrics['max_sample_jump'],
                    )
                    metrics_buffer.clear()
                    next_metrics_at = now + max(1.0, self.config.metrics_interval_s)

            if not wav_header_done:
                header_buffer.extend(chunk)
                data_index = header_buffer.find(b'data')
                if data_index >= 0 and len(header_buffer) >= data_index + 8:
                    payload_start = data_index + 8
                    ok, reason = await write_pifm(bytes(header_buffer[:payload_start]))
                    if not ok:
                        return reason
                    wav_header_done = True
                    next_tick = time.monotonic()
                    payload = bytes(header_buffer[payload_start:])
                    header_buffer.clear()
                    ok, reason = await write_audio_payload(payload)
                    if not ok:
                        return reason
                    continue
                if len(header_buffer) <= 4096:
                    continue
                log.warning('ffmpeg WAV header was not detected before audio payload; pacing raw output')
                wav_header_done = True
                next_tick = time.monotonic()
                payload = bytes(header_buffer)
                header_buffer.clear()
                ok, reason = await write_audio_payload(payload)
                if not ok:
                    return reason
                continue

            ok, reason = await write_audio_payload(chunk)
            if not ok:
                return reason
        return 'shutdown requested'

    async def _read_processor_pcm(
        self,
        ffmpeg_stdout: asyncio.StreamReader,
        queue: asyncio.Queue[bytes],
    ) -> str:
        while not self.stop_event.is_set():
            chunk = await ffmpeg_stdout.read(self.config.write_chunk_size)
            if not chunk:
                return 'ffmpeg output ended'
            self._mark_processor_output_progress()
            self._push_audio_chunk(queue, chunk)
        return 'shutdown requested'

    def _reset_session_health(self) -> None:
        now = time.monotonic()
        self.session_started_ts = now
        self.last_audio_ts = now
        self.last_input_audio_ts = 0.0
        self.last_ffmpeg_stdin_ts = 0.0
        self.last_processor_output_ts = 0.0
        self.last_pifm_output_ts = 0.0
        self.ffmpeg_stdin_drain_timeouts = 0
        self.pifm_stdin_drain_timeouts = 0
        self.ffmpeg_stdin_slow_drains = 0
        self.pifm_stdin_slow_drains = 0
        self.max_ffmpeg_stdin_drain_ms = 0.0
        self.max_pifm_stdin_drain_ms = 0.0

    def _mark_input_audio_progress(self) -> None:
        now = time.monotonic()
        self.last_input_audio_ts = now
        self.last_audio_ts = now

    def _mark_processor_output_progress(self) -> None:
        now = time.monotonic()
        self.last_processor_output_ts = now
        self.last_audio_ts = now

    def _idle_since_session_s(self, ts: float, now: float) -> float:
        base = ts if ts > 0 else self.session_started_ts
        if base <= 0:
            return 0.0
        return max(0.0, now - base)

    def _idle_since_session_ms(self, ts: float, now: float) -> int | None:
        if self.session_started_ts <= 0:
            return None
        return int(round(self._idle_since_session_s(ts, now) * 1000))

    def _age_ms(self, ts: float, now: float) -> int | None:
        if ts <= 0:
            return None
        return int(round(max(0.0, now - ts) * 1000))

    def _record_stdin_drain(self, target: str, drain_ms: float, timed_out: bool) -> None:
        is_ffmpeg = target == 'ffmpeg'
        if is_ffmpeg:
            self.max_ffmpeg_stdin_drain_ms = max(self.max_ffmpeg_stdin_drain_ms, drain_ms)
            if timed_out:
                self.ffmpeg_stdin_drain_timeouts += 1
            elif drain_ms > self.config.max_pacing_lag_ms:
                self.ffmpeg_stdin_slow_drains += 1
                self.last_ffmpeg_stdin_ts = time.monotonic()
            else:
                self.last_ffmpeg_stdin_ts = time.monotonic()
            return
        self.max_pifm_stdin_drain_ms = max(self.max_pifm_stdin_drain_ms, drain_ms)
        if timed_out:
            self.pifm_stdin_drain_timeouts += 1
        elif drain_ms > self.config.max_pacing_lag_ms:
            self.pifm_stdin_slow_drains += 1
            now = time.monotonic()
            self.last_pifm_output_ts = now
            self.last_audio_ts = now
        else:
            now = time.monotonic()
            self.last_pifm_output_ts = now
            self.last_audio_ts = now

    async def _drain_stdin(
        self,
        writer: asyncio.StreamWriter,
        target: str,
    ) -> tuple[bool, float, str]:
        started = time.monotonic()
        try:
            await asyncio.wait_for(writer.drain(), timeout=self.config.pipe_drain_timeout_s)
        except asyncio.TimeoutError:
            drain_ms = (time.monotonic() - started) * 1000
            self._record_stdin_drain(target, drain_ms, timed_out=True)
            return False, drain_ms, f'{target} stdin drain timed out after {self.config.pipe_drain_timeout_s:.1f}s'
        except (BrokenPipeError, ConnectionResetError):
            drain_ms = (time.monotonic() - started) * 1000
            return False, drain_ms, f'{target} stdin closed'
        except Exception as exc:
            drain_ms = (time.monotonic() - started) * 1000
            return False, drain_ms, f'{target} stdin drain failed: {exc}'
        drain_ms = (time.monotonic() - started) * 1000
        self._record_stdin_drain(target, drain_ms, timed_out=False)
        return True, drain_ms, ''

    async def _wait_process(self, proc: asyncio.subprocess.Process, name: str) -> str:
        code = await proc.wait()
        return f'{name} exited with code {code}'

    async def _watch_control_ws(self, ws: aiohttp.ClientWebSocketResponse) -> str:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.ERROR:
                return f'receiver websocket error: {ws.exception()}'
            if msg.type == aiohttp.WSMsgType.CLOSED:
                return 'receiver websocket closed'
            if msg.type == aiohttp.WSMsgType.TEXT:
                with contextlib.suppress(Exception):
                    payload = json.loads(msg.data)
                    if payload.get('type') == 'webrtc_error':
                        return f"receiver websocket reported error: {payload.get('detail')}"
        return 'receiver websocket ended'

    def _log_http_control_ws_done(self, task: asyncio.Task[str]) -> None:
        if task.cancelled():
            return
        try:
            reason = task.result()
        except Exception as exc:
            log.warning('HTTP receiver control websocket failed while audio continues: %s', exc)
            return
        if reason:
            log.warning('HTTP receiver control websocket ended while audio continues: %s', reason)

    def _receiver_stall_reason(
        self,
        ffmpeg_proc: asyncio.subprocess.Process,
        pifm_proc: asyncio.subprocess.Process,
        pc: RTCPeerConnection,
    ) -> str | None:
        if ffmpeg_proc.returncode is not None:
            return f'ffmpeg exited with code {ffmpeg_proc.returncode}'
        if pifm_proc.returncode is not None:
            return f'piFmAdv exited with code {pifm_proc.returncode}'
        connection_state = str(getattr(pc, 'connectionState', '') or '')
        if connection_state in {'failed', 'closed'}:
            return f'webrtc peer connection {connection_state}'

        now = time.monotonic()
        timeout_s = self.config.stream_stall_timeout_s
        if getattr(pc, 'mediaBackend', '') == 'gstreamer' and self.last_input_audio_ts <= 0:
            ice_state = str(getattr(pc, 'iceConnectionState', '') or 'unknown')
            if ice_state in {'failed', 'closed'}:
                return f'webrtc ICE connection {ice_state}'
            audio_pad_linked = bool(getattr(pc, 'audioPadLinked', False))
            waiting_for_ice = (
                ice_state in {'new', 'checking', 'disconnected'}
                or connection_state in {'new', 'connecting', 'disconnected'}
            )
            if bool(getattr(pc, 'remoteAnswerSet', False)) and waiting_for_ice and not audio_pad_linked:
                answer_ts = float(getattr(pc, 'remoteAnswerSetTs', 0.0) or self.session_started_ts)
                ice_wait_s = max(0.0, now - answer_ts)
                ice_timeout_s = max(timeout_s * 3.0, 30.0)
                if ice_wait_s >= ice_timeout_s:
                    return (
                        f'webrtc ICE did not connect within {ice_wait_s:.1f}s after answer '
                        f'(connection={connection_state or "unknown"}, ice={ice_state})'
                    )
                return None

        input_idle_s = self._idle_since_session_s(self.last_input_audio_ts, now)
        processor_idle_s = self._idle_since_session_s(self.last_processor_output_ts, now)
        pifm_idle_s = self._idle_since_session_s(self.last_pifm_output_ts, now)
        input_seen = self.last_input_audio_ts > 0
        processor_seen = self.last_processor_output_ts > 0
        pifm_seen = self.last_pifm_output_ts > 0
        input_recent = input_seen and input_idle_s < timeout_s
        processor_recent = processor_seen and processor_idle_s < timeout_s

        if input_idle_s >= timeout_s:
            if input_seen:
                return f'webrtc input audio stalled for {input_idle_s:.1f}s'
            return f'webrtc input audio did not start within {input_idle_s:.1f}s'
        if input_recent and processor_idle_s >= timeout_s:
            if processor_seen:
                return f'ffmpeg output stalled for {processor_idle_s:.1f}s while WebRTC input continued'
            return f'ffmpeg output did not start while WebRTC input was active for {processor_idle_s:.1f}s'
        if (input_recent or processor_recent) and pifm_idle_s >= timeout_s:
            if pifm_seen:
                return f'transmitter output to piFmAdv stalled for {pifm_idle_s:.1f}s while upstream audio continued'
            return f'transmitter output to piFmAdv did not start while upstream audio was active for {pifm_idle_s:.1f}s'
        return None

    def _receiver_reason_code(self, reason: str | None) -> str:
        if not reason:
            return ''
        if reason.startswith('ffmpeg exited'):
            return 'ffmpeg_exited'
        if reason.startswith('piFmAdv exited'):
            return 'pifm_exited'
        if reason == 'webrtc peer connection failed':
            return 'webrtc_failed'
        if reason == 'webrtc peer connection closed':
            return 'webrtc_closed'
        if reason.startswith('webrtc ICE did not connect'):
            return 'ice_not_connected'
        if reason == 'webrtc ICE connection failed':
            return 'ice_failed'
        if reason == 'webrtc ICE connection closed':
            return 'ice_closed'
        if reason.startswith('webrtc input audio stalled'):
            return 'input_audio_stalled'
        if reason.startswith('webrtc input audio did not start'):
            return 'input_audio_not_started'
        if reason.startswith('ffmpeg output stalled'):
            return 'ffmpeg_output_stalled'
        if reason.startswith('ffmpeg output did not start'):
            return 'ffmpeg_output_not_started'
        if reason.startswith('transmitter output to piFmAdv stalled'):
            return 'pifm_output_stalled'
        if reason.startswith('transmitter output to piFmAdv did not start'):
            return 'pifm_output_not_started'
        return 'unknown'

    def _receiver_status_payload(
        self,
        ffmpeg_proc: asyncio.subprocess.Process,
        pifm_proc: asyncio.subprocess.Process,
        pc: RTCPeerConnection,
    ) -> dict[str, Any]:
        now = time.monotonic()
        reason = self._receiver_stall_reason(ffmpeg_proc, pifm_proc, pc)
        state = 'ok'
        if reason:
            state = 'stalled'
            if (
                ffmpeg_proc.returncode is not None
                or pifm_proc.returncode is not None
                or str(getattr(pc, 'connectionState', '') or '') in {'failed', 'closed'}
                or str(getattr(pc, 'iceConnectionState', '') or '') in {'failed', 'closed'}
            ):
                state = 'failed'
        elif (
            getattr(pc, 'mediaBackend', '') == 'gstreamer'
            and bool(getattr(pc, 'remoteAnswerSet', False))
            and self.last_input_audio_ts <= 0
        ):
            state = 'connecting'
        return {
            'type': 'receiver_status',
            'version': 1,
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'feed_id': self.config.feed_id,
            'receiver_id': self.receiver_id,
            'transport': 'webrtc',
            'webrtc_backend': str(getattr(pc, 'mediaBackend', 'aiortc') or 'aiortc'),
            'state': state,
            'reason': reason or '',
            'reason_code': self._receiver_reason_code(reason),
            'session_uptime_ms': self._idle_since_session_ms(self.session_started_ts, now),
            'webrtc_connection_state': str(getattr(pc, 'connectionState', '') or 'unknown'),
            'webrtc_ice_state': str(getattr(pc, 'iceConnectionState', '') or 'unknown'),
            'webrtc_ice_gathering_state': str(getattr(pc, 'iceGatheringState', '') or 'unknown'),
            'webrtc_signaling_state': str(getattr(pc, 'signalingState', '') or 'unknown'),
            'webrtc_remote_answer_set': bool(getattr(pc, 'remoteAnswerSet', False)),
            'gstreamer_audio_pad_linked': bool(getattr(pc, 'audioPadLinked', False)),
            'gstreamer_audio_pad_caps': str(getattr(pc, 'audioPadCaps', '') or ''),
            'input_audio_seen': self.last_input_audio_ts > 0,
            'input_audio_idle_ms': self._idle_since_session_ms(self.last_input_audio_ts, now),
            'input_audio_age_ms': self._age_ms(self.last_input_audio_ts, now),
            'ffmpeg_stdin_seen': self.last_ffmpeg_stdin_ts > 0,
            'ffmpeg_stdin_idle_ms': self._idle_since_session_ms(self.last_ffmpeg_stdin_ts, now),
            'ffmpeg_output_seen': self.last_processor_output_ts > 0,
            'ffmpeg_output_idle_ms': self._idle_since_session_ms(self.last_processor_output_ts, now),
            'pifm_output_seen': self.last_pifm_output_ts > 0,
            'pifm_output_idle_ms': self._idle_since_session_ms(self.last_pifm_output_ts, now),
            'ffmpeg_running': ffmpeg_proc.returncode is None,
            'ffmpeg_returncode': ffmpeg_proc.returncode,
            'pifm_running': pifm_proc.returncode is None,
            'pifm_returncode': pifm_proc.returncode,
            'ffmpeg_stdin_drain_timeouts': self.ffmpeg_stdin_drain_timeouts,
            'pifm_stdin_drain_timeouts': self.pifm_stdin_drain_timeouts,
            'ffmpeg_stdin_slow_drains': self.ffmpeg_stdin_slow_drains,
            'pifm_stdin_slow_drains': self.pifm_stdin_slow_drains,
            'max_ffmpeg_stdin_drain_ms': round(self.max_ffmpeg_stdin_drain_ms, 1),
            'max_pifm_stdin_drain_ms': round(self.max_pifm_stdin_drain_ms, 1),
        }

    async def _send_receiver_status(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        ffmpeg_proc: asyncio.subprocess.Process,
        pifm_proc: asyncio.subprocess.Process,
        pc: RTCPeerConnection,
    ) -> str:
        interval_s = max(1.0, self.config.status_interval_s)
        send_timeout_s = max(1.0, min(5.0, self.config.pipe_drain_timeout_s))
        while not self.stop_event.is_set():
            try:
                await self._send_receiver_status_once(ws, ffmpeg_proc, pifm_proc, pc, timeout_s=send_timeout_s)
            except asyncio.TimeoutError:
                return f'receiver status websocket send timed out after {send_timeout_s:.1f}s'
            except Exception as exc:
                return f'receiver status websocket send failed: {exc}'
            await asyncio.sleep(interval_s)
        return 'shutdown requested'

    async def _send_receiver_status_once(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        ffmpeg_proc: asyncio.subprocess.Process,
        pifm_proc: asyncio.subprocess.Process,
        pc: RTCPeerConnection,
        timeout_s: float | None = None,
    ) -> None:
        send_timeout_s = max(1.0, min(5.0, timeout_s if timeout_s is not None else self.config.pipe_drain_timeout_s))
        payload = self._receiver_status_payload(ffmpeg_proc, pifm_proc, pc)
        await asyncio.wait_for(ws.send_json(payload), timeout=send_timeout_s)

    async def _monitor_health(
        self,
        ffmpeg_proc: asyncio.subprocess.Process,
        pifm_proc: asyncio.subprocess.Process,
        pc: RTCPeerConnection,
    ) -> str:
        while not self.stop_event.is_set():
            reason = self._receiver_stall_reason(ffmpeg_proc, pifm_proc, pc)
            if reason:
                return reason
            await asyncio.sleep(1.0)
        return 'shutdown requested'

    async def _monitor_http_health(
        self,
        ffmpeg_proc: asyncio.subprocess.Process,
        pifm_proc: asyncio.subprocess.Process,
    ) -> str:
        while not self.stop_event.is_set():
            if ffmpeg_proc.returncode is not None:
                return f'ffmpeg exited with code {ffmpeg_proc.returncode}'
            if pifm_proc.returncode is not None:
                return f'piFmAdv exited with code {pifm_proc.returncode}'
            idle_for = time.monotonic() - self.last_audio_ts
            if idle_for >= self.config.stream_stall_timeout_s:
                return f'HTTP receiver audio stalled for {idle_for:.1f}s'
            await asyncio.sleep(1.0)
        return 'shutdown requested'

    async def _monitor_direct_http_health(
        self,
        pifm_proc: asyncio.subprocess.Process,
    ) -> str:
        while not self.stop_event.is_set():
            if pifm_proc.returncode is not None:
                return f'piFmAdv exited with code {pifm_proc.returncode}'
            idle_for = time.monotonic() - self.last_audio_ts
            if idle_for >= self.config.stream_stall_timeout_s:
                return f'HTTP raw receiver audio stalled for {idle_for:.1f}s'
            await asyncio.sleep(1.0)
        return 'shutdown requested'

    async def _log_stream(self, stream: asyncio.StreamReader, name: str) -> None:
        while not self.stop_event.is_set():
            line = await stream.readline()
            if not line:
                return
            text = line.decode(errors='replace').rstrip()
            if text:
                logging.info('[%s] %s', name, text)

    async def _close_peer_connection(self, pc: RTCPeerConnection) -> None:
        with contextlib.suppress(Exception):
            for receiver in pc.getReceivers():
                track = getattr(receiver, 'track', None)
                if track is not None:
                    track.stop()
        try:
            await asyncio.wait_for(pc.close(), timeout=3.0)
        except asyncio.TimeoutError:
            log.warning('WebRTC peer close timed out')
        except Exception as exc:
            log.warning('WebRTC peer close failed: %s', exc)

    async def _cancel_tasks(self, tasks: tuple[asyncio.Task[Any], ...], timeout_s: float) -> None:
        live = [task for task in tasks if not task.done()]
        for task in live:
            task.cancel()
        if not live:
            return
        done, pending = await asyncio.wait(live, timeout=timeout_s)
        for task in done:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        for task in pending:
            log.warning('Task %s did not stop cleanly', task.get_name())

    def _process_kwargs(self) -> dict[str, Any]:
        if os.name == 'nt':
            return {}
        return {'start_new_session': True}

    async def _terminate_process(self, proc: asyncio.subprocess.Process | None, name: str) -> None:
        if proc is None:
            return
        stdin = getattr(proc, 'stdin', None)
        if stdin is not None:
            with contextlib.suppress(Exception):
                stdin.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(stdin.wait_closed(), timeout=1.0)
        if proc.returncode is None:
            self._signal_process(proc, signal.SIGTERM)
            try:
                await asyncio.wait_for(proc.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                self._signal_process(proc, signal.SIGKILL)
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
        log.info('%s stopped with code %s', name, proc.returncode)

    def _signal_process(self, proc: asyncio.subprocess.Process, sig: signal.Signals) -> None:
        if proc.returncode is not None:
            return
        try:
            if os.name != 'nt':
                os.killpg(proc.pid, sig)
            elif sig == signal.SIGKILL:
                proc.kill()
            else:
                proc.terminate()
        except ProcessLookupError:
            return
        except Exception as exc:
            log.warning('Could not signal process %s with %s: %s', proc.pid, sig, exc)


    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()

        def _trigger_stop() -> None:
            if not self.stop_event.is_set():
                log.info('Shutdown signal received')
                self.stop_event.set()

        for sig_name in ('SIGINT', 'SIGTERM'):
            sig = getattr(signal, sig_name, None)
            if sig is None:
                continue
            try:
                loop.add_signal_handler(sig, _trigger_stop)
            except NotImplementedError:
                signal.signal(sig, lambda _s, _f: _trigger_stop())


def _parse_args() -> ReceiverConfig:
    parser = argparse.ArgumentParser(
        prog='hazeReceiver.py',
        description='Haze feed receiver using WebSocket signaling, WebRTC audio, and piFmAdv output.',
    )
    parser.add_argument('--server', required=True, help='Haze server URL, for example http://haze-host:6444')
    parser.add_argument('--feed-id', required=True, help='Feed ID to receive, for example sk-0001')
    parser.add_argument('--receiver-api-base', default='/api/receiver/v1')
    parser.add_argument('--pair-token', default='', help=argparse.SUPPRESS)
    parser.add_argument('--pair-token-env', default='', help=argparse.SUPPRESS)
    parser.add_argument('--state-file', default='')
    parser.add_argument('--allow-insecure-dev', action='store_true')

    parser.add_argument('--output-sample-rate', type=int, default=48000)
    parser.add_argument('--channels', type=int, choices=[1, 2], default=1)
    parser.add_argument('--webrtc-input-sample-rate', type=int, default=48000)
    parser.add_argument('--ffmpeg-bin', default='ffmpeg')
    parser.add_argument('--ffmpeg-log-level', default='warning')
    parser.add_argument(
        '--audio-filters',
        default=DEFAULT_AUDIO_FILTERS,
    )
    parser.add_argument('--pifmadv-bin', default=pifm_bin)
    parser.add_argument('--pi-extra-arg', action='append', default=[])

    parser.add_argument('--stall-timeout', type=float, default=12.0)
    parser.add_argument('--reconnect-initial-delay', type=float, default=1.0)
    parser.add_argument('--reconnect-max-delay', type=float, default=8.0)
    parser.add_argument('--reconnect-backoff', type=float, default=1.5)
    parser.add_argument('--chunk-size', type=int, default=1920)
    parser.add_argument('--audio-frame-ms', type=int, default=20)
    parser.add_argument('--jitter-buffer-ms', type=int, default=120)
    parser.add_argument('--max-jitter-buffer-ms', type=int, default=500)
    parser.add_argument('--max-pacing-lag-ms', type=int, default=120)
    parser.add_argument('--max-active-underrun-ms', type=int, default=1500)
    parser.add_argument('--preferred-codecs', default='opus')
    parser.add_argument(
        '--webrtc-backend',
        choices=['auto', 'gstreamer', 'aiortc'],
        default='auto',
        help='auto uses the aiortc WebRTC listener library; gstreamer is explicit diagnostics only.',
    )
    parser.add_argument('--http-codec', default='raw_pcm16')
    parser.add_argument('--http-reconnect-delay-max', type=int, default=2)
    parser.add_argument('--http-read-timeout', type=float, default=8.0)
    parser.add_argument('--metrics-interval', type=float, default=10.0, help='Seconds between receiver pipe metric logs; set 0 to disable.')
    parser.add_argument('--pipe-drain-timeout', type=float, default=2.0, help='Seconds to wait for ffmpeg or piFmAdv stdin drain before reconnecting.')
    parser.add_argument('--status-interval', type=float, default=5.0, help='Seconds between receiver_status control websocket messages.')
    parser.add_argument('--diagnose-audio', action='store_true', help='Fetch the HTTP raw PCM stream and print audio integrity metrics without starting piFmAdv.')
    parser.add_argument('--diagnose-duration', type=float, default=60.0, help='Seconds of audio to sample in --diagnose-audio mode.')
    parser.add_argument('--diagnose-output', default='', help='Optional WAV file path to write during --diagnose-audio mode.')
    args = parser.parse_args()

    pair_token = args.pair_token
    if not pair_token and args.pair_token_env:
        pair_token = os.environ.get(args.pair_token_env, '')

    try:
        server_url = _normalize_server_url(args.server, args.allow_insecure_dev)
    except ValueError as exc:
        parser.error(str(exc))

    state_file = pathlib.Path(args.state_file).expanduser() if args.state_file else _default_state_file(args.feed_id)
    audio_frame_ms = max(10, min(100, int(args.audio_frame_ms)))
    jitter_buffer_ms = max(0, int(args.jitter_buffer_ms))
    max_jitter_buffer_ms = max(jitter_buffer_ms + audio_frame_ms, int(args.max_jitter_buffer_ms))
    max_pacing_lag_ms = max(audio_frame_ms * 2, int(args.max_pacing_lag_ms))
    max_active_underrun_ms = max(audio_frame_ms * 2, int(args.max_active_underrun_ms))
    preferred_codecs = tuple(
        codec
        for codec in (
            re.sub(r'[^a-z0-9]+', '', part.strip().lower())
            for part in str(args.preferred_codecs or '').split(',')
        )
        if codec == 'opus'
    )
    return ReceiverConfig(
        server_url=server_url,
        feed_id=str(args.feed_id).strip(),
        receiver_api_base=str(args.receiver_api_base or '/api/receiver/v1'),
        pair_token=str(pair_token or ''),
        state_file=state_file,
        allow_insecure_dev=bool(args.allow_insecure_dev),
        output_sample_rate=max(8000, int(args.output_sample_rate)),
        channels=int(args.channels),
        webrtc_input_sample_rate=max(8000, int(args.webrtc_input_sample_rate)),
        ffmpeg_bin=str(args.ffmpeg_bin or 'ffmpeg'),
        ffmpeg_log_level=str(args.ffmpeg_log_level or 'warning'),
        audio_filters=str(args.audio_filters or 'anull'),
        pifmadv_bin=str(args.pifmadv_bin or pifm_bin),
        pifm_extra_args=(*DEFAULT_PIFM_EXTRA_ARGS, *tuple(args.pi_extra_arg)),
        reconnect_initial_delay_s=max(0.1, float(args.reconnect_initial_delay)),
        reconnect_max_delay_s=max(0.1, float(args.reconnect_max_delay)),
        reconnect_backoff=max(1.0, float(args.reconnect_backoff)),
        stream_stall_timeout_s=max(2.0, float(args.stall_timeout)),
        pipe_drain_timeout_s=max(0.2, float(args.pipe_drain_timeout)),
        status_interval_s=max(1.0, float(args.status_interval)),
        write_chunk_size=max(512, int(args.chunk_size)),
        audio_frame_ms=audio_frame_ms,
        jitter_buffer_ms=jitter_buffer_ms,
        max_jitter_buffer_ms=max_jitter_buffer_ms,
        max_pacing_lag_ms=max_pacing_lag_ms,
        max_active_underrun_ms=max_active_underrun_ms,
        preferred_codecs=preferred_codecs or ('opus',),
        webrtc_backend=str(args.webrtc_backend or 'auto'),
        http_codec=str(args.http_codec or 'raw_pcm16'),
        http_reconnect_delay_max_s=max(1, int(args.http_reconnect_delay_max)),
        http_read_timeout_s=max(2.0, float(args.http_read_timeout)),
        metrics_interval_s=max(0.0, float(args.metrics_interval)),
        diagnose_audio=bool(args.diagnose_audio),
        diagnose_duration_s=max(1.0, float(args.diagnose_duration)),
        diagnose_output=pathlib.Path(args.diagnose_output).expanduser() if args.diagnose_output else None,
    )


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    for name in ('aioice', 'aiortc'):
        logging.getLogger(name).setLevel(logging.ERROR)


async def _diagnose_audio(config: ReceiverConfig) -> None:
    codec = config.http_codec.strip().lower().replace('-', '_')
    if codec not in {'raw', 'raw_pcm16', 's16le'}:
        log.warning('--diagnose-audio uses raw PCM metrics; overriding HTTP codec %s with raw_pcm16', config.http_codec)
        codec = 'raw_pcm16'
    query = urlencode({'feed': config.feed_id, 'codec': codec})
    url = f'{config.server_url}/api/public/v1/feed/audio?{query}'
    timeout = aiohttp.ClientTimeout(
        total=None,
        sock_connect=5,
        sock_read=max(5.0, config.http_read_timeout_s),
    )
    data = bytearray()
    chunk_count = 0
    max_chunk_gap_ms = 0.0
    first_chunk_at: float | None = None
    last_chunk_at: float | None = None
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=timeout) as response:
            if response.status < 200 or response.status >= 300:
                detail = (await response.text())[:300]
                raise RuntimeError(f'{url} returned HTTP {response.status}: {detail}')
            log.info('Diagnosing %s for %.1fs from %s', config.feed_id, config.diagnose_duration_s, url)
            deadline = time.monotonic() + config.diagnose_duration_s
            async for chunk in response.content.iter_chunked(config.write_chunk_size):
                now = time.monotonic()
                if first_chunk_at is None:
                    first_chunk_at = now
                if last_chunk_at is not None:
                    max_chunk_gap_ms = max(max_chunk_gap_ms, (now - last_chunk_at) * 1000)
                last_chunk_at = now
                if chunk:
                    data.extend(chunk)
                    chunk_count += 1
                if now >= deadline:
                    break
    if config.diagnose_output:
        config.diagnose_output.parent.mkdir(parents=True, exist_ok=True)
        config.diagnose_output.write_bytes(_streaming_wav_header(48000, 1) + bytes(data))
        log.info('Wrote diagnostic WAV capture to %s', config.diagnose_output)
    metrics = _pcm16_metrics(bytes(data))
    metrics.update({
        'feed_id': config.feed_id,
        'codec': codec,
        'bytes': len(data),
        'chunks': chunk_count,
        'wall_duration_s': round((last_chunk_at - first_chunk_at), 3) if first_chunk_at and last_chunk_at else 0.0,
        'max_chunk_gap_ms': round(max_chunk_gap_ms, 2),
        'expected_duration_s': config.diagnose_duration_s,
    })
    print(json.dumps(metrics, indent=2, sort_keys=True))


async def _main() -> None:
    _configure_logging()
    config = _parse_args()
    if config.diagnose_audio:
        await _diagnose_audio(config)
        return
    supervisor = ReceiverSupervisor(config)
    await supervisor.run_forever()


if __name__ == '__main__':
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
