"""Modal app that cuts silent preview clips out of the 9s clips in R2.

Source : clips_9s/<movie_id>/<randid>.mp4
Output : clips_9s/<movie_id>/<randid>_short.mp4       (480 wide, small grid)
         clips_9s/<movie_id>/<randid>_short_960.mp4   (960 wide, medium grid)
         clips_9s/<movie_id>/<randid>_short_1920.mp4  (1920 wide, one per page)

Every size is cut from one download of the source; only missing sizes are
cut unless overwrite is set. Sizes are capped at the source width.

The scene window (start_time/end_time, seconds inside the 9s clip) comes from
frl.frl_image_scene_boundaries and is supplied by the caller.
"""

import modal

app = modal.App("clip-preview-generator")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg")
    .pip_install("boto3", "fastapi[standard]")
)

SOURCE_PREFIX = "clips_9s"
# (suffix, width, x264 profile, x264 level)
PREVIEW_TIERS = [
    ("_short", 480, "baseline", "3.1"),
    ("_short_960", 960, "high", "3.1"),
    ("_short_1920", 1920, "high", "4.1"),
]
MIN_PREVIEW_SECONDS = 0.5
MAX_PREVIEW_SECONDS = 12.0

with image.imports():
    import os
    import subprocess
    import tempfile

    import boto3
    from botocore.client import Config

    BUCKET_NAME = "clips"

    def connect_r2():
        return boto3.client(
            "s3",
            endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            config=Config(signature_version="s3v4", max_pool_connections=50),
        )

    def key_exists(r2, key):
        try:
            r2.head_object(Bucket=BUCKET_NAME, Key=key)
            return True
        except r2.exceptions.ClientError:
            return False

    def source_width(path):
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width", "-of", "csv=p=0", path],
            capture_output=True, text=True, errors="replace",
        )
        try:
            return int(result.stdout.strip().splitlines()[0])
        except (ValueError, IndexError):
            return None

    def encode_preview(source_path, output_path, start_time, duration, width, profile, level):
        cmd = [
            "ffmpeg",
            "-nostdin",
            "-ss", f"{start_time:.3f}",
            "-i", source_path,
            "-t", f"{duration:.3f}",
            "-map", "0:v:0",
            "-map_chapters", "-1",
            "-an", "-dn", "-sn",
            "-vf", f"scale={width}:-2",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "26",
            "-profile:v", profile,
            "-level", level,
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            "-y", output_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, errors="replace")
        if result.returncode != 0:
            raise RuntimeError(f"ffmpeg failed: {result.stderr[-500:]}")
        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            raise RuntimeError("ffmpeg produced an empty file")


@app.function(image=image, secrets=[modal.Secret.from_name("r2-credentials")],
              timeout=900, max_containers=1000)
def generate_preview(item: dict) -> dict:
    """Cut one preview. `item` needs movie_id, filename, start_time, end_time."""
    movie_id = item["movie_id"]
    filename = item["filename"]
    start_time = item.get("start_time")
    end_time = item.get("end_time")
    overwrite = bool(item.get("overwrite", False))

    result = {"movie_id": movie_id, "filename": filename}

    if start_time is None or end_time is None:
        return result | {"status": "skipped", "reason": "no scene boundary"}

    start_time = max(0.0, float(start_time))
    duration = min(float(end_time) - start_time, MAX_PREVIEW_SECONDS)
    if duration < MIN_PREVIEW_SECONDS:
        return result | {"status": "skipped", "reason": "scene shorter than minimum"}

    source_key = f"{SOURCE_PREFIX}/{movie_id}/{filename}.mp4"
    tier_keys = {
        suffix: f"{SOURCE_PREFIX}/{movie_id}/{filename}{suffix}.mp4"
        for suffix, _, _, _ in PREVIEW_TIERS
    }
    result["key"] = tier_keys["_short"]

    r2 = connect_r2()

    tiers = PREVIEW_TIERS if overwrite else [
        tier for tier in PREVIEW_TIERS if not key_exists(r2, tier_keys[tier[0]])
    ]
    if not tiers:
        return result | {"status": "exists"}

    created = {}
    with tempfile.TemporaryDirectory() as tmp:
        source_path = os.path.join(tmp, "source.mp4")

        try:
            r2.download_file(BUCKET_NAME, source_key, source_path)
        except Exception as exc:
            return result | {"status": "error", "reason": f"source unavailable: {exc}"}

        max_width = source_width(source_path)

        for suffix, width, profile, level in tiers:
            output_path = os.path.join(tmp, f"preview{suffix}.mp4")
            # Never upscale, and keep the width even for yuv420p.
            target_width = min(width, max_width) if max_width else width
            target_width -= target_width % 2

            try:
                encode_preview(source_path, output_path, start_time, duration,
                               target_width, profile, level)
            except Exception as exc:
                return result | {"status": "error", "reason": f"{suffix}: {exc}",
                                 "created": created}

            r2.upload_file(
                output_path,
                BUCKET_NAME,
                tier_keys[suffix],
                ExtraArgs={
                    "ContentType": "video/mp4",
                    "CacheControl": "public, max-age=31536000, immutable",
                },
            )
            created[suffix] = os.path.getsize(output_path)

    return result | {
        "status": "created",
        "created": created,
        "start_time": start_time,
        "duration": round(duration, 3),
    }


@app.function(image=image, secrets=[modal.Secret.from_name("r2-credentials")], timeout=900)
@modal.fastapi_endpoint(method="POST", label="clip-preview", docs=True)
def clip_preview(payload: dict):
    """Single preview.

    {"movie_id": 76652, "filename": "ABCD1234", "start_time": 2.1, "end_time": 5.4}
    """
    return generate_preview.local(payload)


@app.function(image=image, secrets=[modal.Secret.from_name("r2-credentials")], timeout=3600)
@modal.fastapi_endpoint(method="POST", label="clip-previews-batch", docs=True)
def clip_previews_batch(payload: dict):
    """Fan a batch out over containers.

    {"items": [{"movie_id": 1, "filename": "A", "start_time": 0.0, "end_time": 3.0}, ...],
     "overwrite": false}
    """
    items = payload.get("items") or []
    overwrite = bool(payload.get("overwrite", False))
    if not items:
        return {"error": "items is required"}

    items = [item | {"overwrite": overwrite} for item in items]
    results = list(generate_preview.map(items, return_exceptions=True))

    summary = {"created": 0, "exists": 0, "skipped": 0, "error": 0}
    cleaned = []
    for res in results:
        if isinstance(res, Exception):
            summary["error"] += 1
            cleaned.append({"status": "error", "reason": str(res)})
            continue
        summary[res.get("status", "error")] = summary.get(res.get("status", "error"), 0) + 1
        cleaned.append(res)

    return {"total": len(items), "summary": summary, "results": cleaned}
