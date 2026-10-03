"""Modal app that cuts 480p silent preview clips out of the 9s clips in R2.

Source : clips_9s/<movie_id>/<randid>.mp4
Output : clips_9s/<movie_id>/<randid>_short.mp4

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
PREVIEW_SUFFIX = "_short"
PREVIEW_WIDTH = 480
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

    def encode_preview(source_path, output_path, start_time, duration):
        cmd = [
            "ffmpeg",
            "-nostdin",
            "-ss", f"{start_time:.3f}",
            "-i", source_path,
            "-t", f"{duration:.3f}",
            "-map", "0:v:0",
            "-map_chapters", "-1",
            "-an", "-dn", "-sn",
            "-vf", f"scale={PREVIEW_WIDTH}:-2",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "26",
            "-profile:v", "baseline",
            "-level", "3.1",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            "-y", output_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
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
    preview_key = f"{SOURCE_PREFIX}/{movie_id}/{filename}{PREVIEW_SUFFIX}.mp4"
    result["key"] = preview_key

    r2 = connect_r2()

    if not overwrite and key_exists(r2, preview_key):
        return result | {"status": "exists"}

    with tempfile.TemporaryDirectory() as tmp:
        source_path = os.path.join(tmp, "source.mp4")
        output_path = os.path.join(tmp, "preview.mp4")

        try:
            r2.download_file(BUCKET_NAME, source_key, source_path)
        except Exception as exc:
            return result | {"status": "error", "reason": f"source unavailable: {exc}"}

        try:
            encode_preview(source_path, output_path, start_time, duration)
        except Exception as exc:
            return result | {"status": "error", "reason": str(exc)}

        size = os.path.getsize(output_path)
        r2.upload_file(
            output_path,
            BUCKET_NAME,
            preview_key,
            ExtraArgs={
                "ContentType": "video/mp4",
                "CacheControl": "public, max-age=31536000, immutable",
            },
        )

    return result | {
        "status": "created",
        "bytes": size,
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
