"""Bounded image decoding in a disposable process; never emits image contents.

Linux additionally enforces a 512 MiB address-space limit. Darwin rejects that
limit on some Python builds: there an RSS watchdog kills at 512 MiB (25 ms
sampling, not an allocator hard ceiling). Geometry/work budgets apply on both.
"""
import hashlib
import json
import os
import subprocess
import sys
import threading
import time

MAX_INPUT_BYTES = 100 << 20
MAX_PIXELS = 16_000_000
MAX_FRAMES = 32
MAX_TOTAL_PIXELS = 32_000_000
MAX_DECODE_BYTES = 256 << 20  # Conservative 16 bytes/pixel planned frame budget.
MAX_PROCESS_BYTES = 512 << 20
WALL_SECONDS = 3.0
MAX_CHECKS = 16
_SLOT = threading.BoundedSemaphore(1)

# This script is self-contained: -I prevents importing a caller's working tree,
# and binary stdin avoids private image bytes in argv, files or environment.
_WORKER = r'''
import io,json,resource,sys,warnings
def finish(valid,reason):
    print(json.dumps({"valid":valid,"reason":reason},separators=(",",":")))
    raise SystemExit(0)
try:
    resource.setrlimit(resource.RLIMIT_CPU,(2,3))
    if sys.platform.startswith("linux"):
        resource.setrlimit(resource.RLIMIT_AS,(536870912,536870912))
    from PIL import Image,ImageFile
    ImageFile.LOAD_TRUNCATED_IMAGES=False
    Image.MAX_IMAGE_PIXELS=16000000
    warnings.simplefilter("error",Image.DecompressionBombWarning)
    data=sys.stdin.buffer.read(104857601)
    if not data or len(data)>104857600: finish(False,"image_resource_limit")
    def dimensions(image):
        width,height=image.size
        if (width<=0 or height<=0 or width>16384 or height>16384
                or width*height>16000000 or width*height*16>268435456):
            finish(False,"image_resource_limit")
        return width*height
    with Image.open(io.BytesIO(data)) as image:
        if image.format not in ("JPEG","PNG","GIF","WEBP","BMP"):
            finish(False,"image_invalid")
        dimensions(image)
        frames=getattr(image,"n_frames",1)
        if type(frames) is not int or not 1<=frames<=32:
            finish(False,"image_resource_limit")
        image.verify()
    with Image.open(io.BytesIO(data)) as image:
        total=0
        for frame in range(frames):
            image.seek(frame)
            total+=dimensions(image)
            if total>32000000: finish(False,"image_resource_limit")
            image.load()
    finish(True,"ok")
except (MemoryError,Image.DecompressionBombError if "Image" in globals() else MemoryError,
        Image.DecompressionBombWarning if "Image" in globals() else MemoryError):
    finish(False,"image_resource_limit")
except Exception:
    finish(False,"image_invalid")
'''


def _run(data, timeout):
    if type(data) is not bytes or not data or len(data) > MAX_INPUT_BYTES:
        return False, "image_resource_limit" if data else "image_invalid"
    started = time.monotonic()
    if not _SLOT.acquire(timeout=max(0, timeout)):
        return False, "image_resource_limit"
    process, watcher = None, None
    stop = threading.Event()
    exceeded = []
    try:
        remaining = timeout - (time.monotonic() - started)
        if remaining <= 0:
            return False, "image_validation_timeout"
        environment = {"PATH": os.defpath, "PYTHONDONTWRITEBYTECODE": "1",
                       "PYTHONNOUSERSITE": "1", "LANG": "C"}
        process = subprocess.Popen([sys.executable, "-I", "-c", _WORKER],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            close_fds=True, env=environment)

        def watch_rss():
            while not stop.wait(0.025) and process.poll() is None:
                try:
                    result = subprocess.run(["/bin/ps", "-o", "rss=", "-p", str(process.pid)],
                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                        timeout=0.2, env=environment)
                    raw = result.stdout.strip()
                    if result.returncode != 0 or not (len(raw) <= 20 and raw.isdigit()):
                        if process.poll() is None:
                            exceeded.append(True)
                            process.kill()
                        return
                    if int(raw) * 1024 > MAX_PROCESS_BYTES:
                        exceeded.append(True)
                        process.kill()
                        return
                except Exception:
                    # An unavailable watchdog cannot silently lift the Darwin bound.
                    if process.poll() is None:
                        exceeded.append(True)
                        try:
                            process.kill()
                        except OSError:
                            pass
                    return

        if sys.platform == "darwin":
            watcher = threading.Thread(target=watch_rss, daemon=True)
            watcher.start()
        elif not sys.platform.startswith("linux"):
            process.kill()
            return False, "image_resource_limit"
        try:
            remaining = timeout - (time.monotonic() - started)
            output, _ = process.communicate(data, timeout=max(0.001, remaining))
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=1)
            return False, "image_validation_timeout"
        if exceeded or process.returncode in (-9, -24):
            return False, "image_resource_limit"
        if process.returncode != 0 or len(output) > 256:
            return False, "image_invalid"
        result = json.loads(output)
        if (type(result) is dict and type(result.get("valid")) is bool
                and result.get("reason") in ("ok", "image_invalid", "image_resource_limit")
                and result["valid"] == (result["reason"] == "ok")):
            return result["valid"], result["reason"]
        return False, "image_invalid"
    except Exception:
        return False, "image_resource_limit"
    finally:
        stop.set()
        if process is not None:
            if process.poll() is None:
                process.kill()
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            for stream in (process.stdin, process.stdout):
                if stream is not None:
                    stream.close()
        if watcher is not None:
            watcher.join(timeout=0.3)
        _SLOT.release()


class ImageValidationBudget:
    """A transfer-wide 3 s decode budget, <=16 unique plaintext checks."""
    def __init__(self):
        self.remaining = WALL_SECONDS
        self.reason = "ok"
        self.checked = False
        self.limited = False
        self._memo = {}

    def __call__(self, data):
        self.checked = True
        if self.limited:
            return False
        if type(data) is not bytes or not data or len(data) > MAX_INPUT_BYTES:
            self.reason = "image_resource_limit" if data else "image_invalid"
            return False
        identity = hashlib.sha256(data).digest()  # Private in-memory dedup only.
        if identity in self._memo:
            valid, self.reason = self._memo[identity]
            return valid
        if len(self._memo) >= MAX_CHECKS or self.remaining <= 0:
            self.reason = "image_resource_limit" if len(self._memo) >= MAX_CHECKS else "image_validation_timeout"
            self.limited = True
            return False
        started = time.monotonic()
        valid, self.reason = _run(data, self.remaining)
        self.remaining = max(0, self.remaining - (time.monotonic() - started))
        self.limited = self.reason == "image_validation_timeout" or self.remaining <= 0
        self._memo[identity] = valid, self.reason
        return valid


def validate_image(data):
    return ImageValidationBudget()(data)
