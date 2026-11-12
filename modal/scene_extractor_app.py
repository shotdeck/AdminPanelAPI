"""Modal app behind MoviePipeline:SceneDetectionBaseUrl (r2-transnet-v2).

POST movie_id, filename (randid), threshold -> the preview window (seconds
inside clips_9s/<movie_id>/<filename>.mp4) plus the TransNetV2 cut times.

The target frame is the frame that matches the image's still (searched within
1s of the clip centre), falling back to the centre frame, so a still just
before a cut gets its own shot rather than the next one.

Deploy: modal deploy scene_extractor_app.py
(TEST_LABEL=x MAX_CONTAINERS=n deploys a separate copy at r2-transnet-v2-x.)
The TransNetV2 SavedModel lives in the transnetv2-model volume at /models.
"""

import os

import modal

TEST_LABEL = os.environ.get("TEST_LABEL", "")
MAX_CONTAINERS = int(os.environ.get("MAX_CONTAINERS", "1"))

app = modal.App("r2-transnetv2-scene-extractor" + (f"-{TEST_LABEL}" if TEST_LABEL else ""))

MODEL_DIR = "/model/models"
R2_PREFIX = "clips_9s"
BUCKET_NAME = "clips"

image = (
    modal.Image.from_registry("nvidia/cuda:12.1.1-cudnn8-devel-ubuntu22.04", add_python="3.11")
    .apt_install("ffmpeg")
    .pip_install("tensorflow==2.15.0", "numpy==1.26.4", "opencv-python-headless==4.10.0.84")
    .pip_install("fastapi[standard]", "ffmpeg-python", "boto3")
    .add_local_python_source("transnet_scene")
)
model_volume = modal.Volume.from_name("transnetv2-model")

with image.imports():
    import boto3
    from botocore.client import Config
    from fastapi import Form, HTTPException


@app.cls(
    image=image,
    gpu="T4",
    timeout=300,
    max_containers=MAX_CONTAINERS,
    scaledown_window=60 * 3,
    volumes={"/model": model_volume},
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("movie-server")],
)
class TransNetV2Worker:
    @modal.enter()
    def setup(self):
        from transnet_scene import TransNetV2Interface, load_model
        self.interface = TransNetV2Interface(load_model(MODEL_DIR))
        self.r2 = boto3.client(
            "s3",
            endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            config=Config(signature_version="s3v4"),
            region_name="auto",
        )
        os.makedirs("/tmp/videos", exist_ok=True)

    @modal.fastapi_endpoint(
        method="POST",
        label="r2-transnet-v2" + (f"-{TEST_LABEL}" if TEST_LABEL else ""),
        requires_proxy_auth=False,
        docs=True,
    )
    async def fastapi_api(
        self,
        movie_id: int = Form(...),
        filename: str = Form(...),
        threshold: float = Form(0.7),
    ):
        from transnet_scene import extract_scene, fetch_still

        if not movie_id or not filename:
            raise HTTPException(status_code=400, detail="Both movie_id and filename are required")

        key = f"{R2_PREFIX}/{movie_id}/{filename}.mp4"
        try:
            self.r2.head_object(Bucket=BUCKET_NAME, Key=key)
        except self.r2.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise HTTPException(status_code=404, detail=f"File not found: {key}")
            raise

        local_video = f"/tmp/videos/{filename}.mp4"
        self.r2.download_file(BUCKET_NAME, key, local_video)
        try:
            result = extract_scene(self.interface, local_video, "", threshold, still=fetch_still(filename))
        finally:
            if os.path.exists(local_video):
                os.remove(local_video)

        return {"movie_id": movie_id, "filename": filename, "threshold": threshold} | result
