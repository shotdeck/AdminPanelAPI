import os
import subprocess
import numpy as np
import tensorflow as tf
import ffmpeg
import warnings

warnings.filterwarnings("ignore")

class TransNetV2Interface:
    def __init__(self, model):
        self._model = model
        self._input_size = (27, 48, 3)

    def _predict_raw(self, frames: np.ndarray):
        assert len(frames.shape) == 5 and frames.shape[2:] == self._input_size, \
            "Input must be [batch, frames, height, width, 3]."
        frames = tf.cast(frames, tf.float32)
        logits, _ = self._model(frames)
        return tf.sigmoid(logits)

    def predict(self, frames: np.ndarray):
        if len(frames) == 0:
            raise ValueError("Cannot predict on empty frames array")

        def iterator():
            pad_start = 25
            pad_end = 25 + 50 - (len(frames) % 50 or 50)
            start_f = np.expand_dims(frames[0], 0)
            end_f   = np.expand_dims(frames[-1], 0)
            padded = np.concatenate(
                [start_f]*pad_start + [frames] + [end_f]*pad_end, 0
            )
            ptr = 0
            while ptr + 100 <= len(padded):
                chunk = padded[ptr:ptr+100]
                ptr += 50
                yield chunk[np.newaxis]

        preds = []
        for inp in iterator():
            preds.append(self._predict_raw(inp).numpy()[0, 25:75, 0])
        all_preds = np.concatenate(preds)
        return all_preds[:len(frames)]

def load_model(path: str):
    return tf.saved_model.load(path) if os.path.isdir(path) else tf.keras.models.load_model(path)


def get_video_metadata(video_path):
    try:
        probe = ffmpeg.probe(video_path)
        video_stream = next(s for s in probe['streams'] if s['codec_type'] == 'video')
        fps = eval(video_stream['avg_frame_rate'])
        duration = float(video_stream.get('duration', probe['format']['duration']))
        #frame_count = int(video_stream.get('nb_frames', int(duration * fps)))
        frame_count = int(duration * fps)
        return {"duration": duration, "fps": fps, "frame_count": frame_count}
    except Exception as e:
        print(f"Failed to read metadata: {e}")
        return None

STILL_URL = "https://crunch.shotdeck.com/assets/images/stills/smthumb/small_{rid}.{ext}"
STILL_SEARCH_SECONDS = 1.0
STILL_MIN_SCORE = 0.9
STILL_MIN_GAIN = 0.02


def fetch_still(rid: str):
    """The still's small thumbnail as a BGR image, or None."""
    import urllib.request
    import cv2
    for ext in ("jpg", "png", "JPG", "jpeg"):
        req = urllib.request.Request(STILL_URL.format(rid=rid, ext=ext),
                                     headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
        except Exception:
            continue
        if len(data) > 500:
            img = cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_COLOR)
            if img is not None:
                return cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return None


def _content_box(gray: np.ndarray):
    rows = np.where(gray.mean(axis=1) > 16)[0]
    cols = np.where(gray.mean(axis=0) > 16)[0]
    if len(rows) < gray.shape[0] * 0.3 or len(cols) < gray.shape[1] * 0.3:
        return 0, gray.shape[0], 0, gray.shape[1]
    return rows[0], rows[-1] + 1, cols[0], cols[-1] + 1


def _feat(gray: np.ndarray, box=None):
    import cv2
    if box is not None:
        gray = gray[box[0]:box[1], box[2]:box[3]]
    g = cv2.resize(gray, (96, 40), interpolation=cv2.INTER_AREA).astype(np.float32)
    g -= g.mean()
    n = np.linalg.norm(g)
    return g / n if n else g


def find_still_frame(video_path: str, still: np.ndarray, centre_frame: int, fps: float):
    """
    The frame near the clip's centre that best matches the still, compared both
    as-is and with black borders cropped. Returns (frame, score, centre_score).
    """
    out, _ = (
        ffmpeg.input(video_path)
              .output("pipe:", format="rawvideo", pix_fmt="gray", s="384x216")
              .run(capture_stdout=True, quiet=True)
    )
    frames = np.frombuffer(out, np.uint8).reshape([-1, 216, 384])
    if len(frames) == 0:
        return None, None, None
    still_plain = _feat(still)
    still_crop = _feat(still, _content_box(still))
    clip_box = _content_box(frames[min(centre_frame, len(frames) - 1)])
    span = int(round(STILL_SEARCH_SECONDS * fps))
    lo, hi = max(0, centre_frame - span), min(len(frames) - 1, centre_frame + span)
    scores = {}
    for i in range(lo, hi + 1):
        scores[i] = max(float((_feat(frames[i]) * still_plain).sum()),
                        float((_feat(frames[i], clip_box) * still_crop).sum()))
    best = max(scores, key=scores.get)
    return best, scores[best], scores.get(min(max(centre_frame, lo), hi))


def get_start_end_frames(arr, number):
    for i in range(len(arr) - 1):
        if arr[i] <= number <= arr[i + 1]:
            return arr[i], arr[i + 1]
    return None, None


def extract_scene(
    transnet,
    video_path: str,
    output_path: str,
    threshold: float = 0.5,
    min_shot_time: float = 2.0,
    max_shot_time: float = 12.0,
    still=None,
):

    print("Threshold: ", threshold)
    metadata = get_video_metadata(video_path)
    print("metadata: ", metadata)
    if metadata is None:
        raise RuntimeError("Failed to read video metadata")

    duration = metadata['duration']
    fps = metadata['fps']
    frame_count = metadata['frame_count']
    target_frame = int(frame_count * 0.5)
    #target_frame = int((frame_count * 0.5) + fps)

    # The still can sit a frame or two off the centre. When it is just before a
    # cut, the centre frame is already the next shot, so target the still itself.
    target_source, target_score = "centre", None
    if still is not None:
        best, score, centre_score = find_still_frame(video_path, still, target_frame, fps)
        if best is not None:
            target_score = score
            if score >= STILL_MIN_SCORE and (centre_score is None or score - centre_score >= STILL_MIN_GAIN):
                target_frame, target_source = best, "still"

    offset = 1.0 #if safe_mode else 0.0
    target_time = (target_frame / fps) #+ offset

    # Step 1: Decode full video for prediction (downscaled)
    print(f"Decoding full video for inference: {video_path}")
    #out, _ = (
    #    ffmpeg.input(video_path, hwaccel='cuda')
    out, _ = (
        ffmpeg.input(video_path)
              .output("pipe:", format="rawvideo", pix_fmt="rgb24", s="48x27")
              .run(capture_stdout=True, quiet=True)
    )
    frames = np.frombuffer(out, np.uint8).reshape([-1, 27, 48, 3])
    if len(frames) == 0:
        raise RuntimeError("No frames extracted.")

    # Step 2: Shot boundary prediction
    predictions = transnet.predict(frames)
    cut_indices = np.where((predictions > threshold).astype(np.uint8) == 1)[0]

    cut_indices = np.unique(
            np.concatenate([
                [0],
                cut_indices,
                [frame_count - 1]
            ])
    )
    cut_times = (cut_indices / fps).tolist()
    start_frame, end_frame = get_start_end_frames(cut_indices, target_frame)

    # Fallback: if no shot boundaries found
    if start_frame is None or end_frame is None:
        start_time = max(0, target_time - max_shot_time / 2)
        end_time = min(duration, start_time + max_shot_time)
    else:
        # Skip TransNet's soft first frames, but never past the still.
        start_frame = min(start_frame + 3, max(start_frame, target_frame))
        start_time = start_frame / fps
        end_time = end_frame / fps

        clip_duration = end_time - start_time

        # Enforce minimum shot duration
        if clip_duration < min_shot_time:
            center_time = (start_time + end_time) / 2
            start_time = max(0, center_time - min_shot_time / 2)
            end_time = min(duration, start_time + min_shot_time)

        elif clip_duration > max_shot_time:
            half_max = max_shot_time / 2
            half_min = min_shot_time / 2

            # Ideal start and end centered on target_time
            ideal_start = target_time - half_max
            ideal_end = target_time + half_max

            # Clamp to scene boundaries
            proposed_start = max(start_time, ideal_start)
            proposed_end = min(end_time, ideal_end)

            # Adjust if result is shorter than min_shot_time
            proposed_duration = proposed_end - proposed_start
            if proposed_duration < min_shot_time:
                # Try to expand the window within the scene
                center_time = (proposed_start + proposed_end) / 2
                proposed_start = max(start_time, center_time - half_min)
                proposed_end = min(end_time, proposed_start + min_shot_time)

            start_time = proposed_start
            end_time = proposed_end

    print(f"Extracted scene: {start_time:.2f}s → {end_time:.2f}s, saved to {output_path}")
    return {"start_time": start_time , "end_time": end_time, "cut_times": cut_times, "duration": duration, "fps": fps, "frame_count": frame_count, "target_frame": int(target_frame),
            "target_source": target_source, "target_score": target_score}
