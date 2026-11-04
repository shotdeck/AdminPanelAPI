"""Modal app that finds where each still really is in its movie and recuts its
9s clip around it, for clips cut from a wrong frame (e.g. a detection batch
matched against a different cut of the film).

1. Every clip in clips_9s/<movie_id>/<randid>.mp4 is matched against its still.
   A clip is flagged when the best-matching frame is away from the 4.5s centre
   (or the clip is missing). A low score alone doesn't flag a clip: stills with
   a different grade score low even when the clip is right.
2. The whole film is pulled from the movie server (:8888 /extract-clip) in 30s
   windows and every frame is matched against every flagged still at once.
3. Each still found in the film gets a new 9s clip centred on its frame (or as
   near as the film's start allows), which must have the still there before it
   is used. With recut_all every still is searched for and recut, not only the
   flagged ones; a clip that already matched keeps its clip if its still can't
   be found or the recut fails. With apply the old
   clip and its 3 previews are copied to clips_9s_backup_<date>/<movie_id>/,
   the new clip is uploaded and the old previews are deleted.

Scene boundaries, previews and any database writes are left to the caller
(AdminPanelAPI ClipRelocateWorker).

POST /  (label clip-relocate-start) spawns a job and returns its call_id.
GET  /?call_id=&job_tag= (label clip-relocate-result) returns its progress or result.
"""

import modal

app = modal.App("clip-relocate")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg")
    .pip_install("boto3", "fastapi[standard]", "numpy", "opencv-python-headless", "requests")
)

progress = modal.Dict.from_name("clip-relocate-progress", create_if_missing=True)

MOVIE_SERVER = "http://35.89.51.60:8888"
# Whole films, basic auth from the `movie-server` secret (MOVIE_USERNAME / MOVIE_PASSWORD).
MOVIE_FILE_URL = "http://35.89.51.60:8889/movie/{movie_id}.mp4"
FILM_DECODE_PROCS = 8
STILL_URL = "https://crunch.shotdeck.com/assets/images/stills/smthumb/small_{rid}.jpg"
# crunch refuses requests without a browser user agent.
STILL_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120"}
SOURCE_PREFIX = "clips_9s"
PREVIEW_SUFFIXES = ["_short", "_short_960", "_short_1920"]
BUCKET_NAME = "clips"

CLIP_SECONDS = 9.0
CLIP_CENTRE = 4.5
WINDOW_SECONDS = 30
# A frame at least this similar to the still is the still.
MATCH_MIN = 0.9
# The best frame may be this far from the centre before a clip is flagged...
CENTRE_TOLERANCE = 0.5
# ...unless the centre frame scores within this of it (static shots).
CENTRE_SLACK = 0.02
# A weaker match counts when it lands this close to where the movie's drift line
# (fitted from its confident matches) puts the still: stills graded differently
# from the film's copy can score well under MATCH_MIN at the right frame.
DRIFT_MATCH_MIN = 0.6
DRIFT_TOLERANCE = 2.0
DRIFT_MIN_POINTS = 8
# The :8888 box re-encodes each window; at 8 at once its ffmpeg runs get OOM-killed.
MOVIE_SERVER_PARALLEL = 4

with image.imports():
    import os
    import subprocess
    import tempfile
    import time

    import boto3
    import cv2
    import numpy as np
    import requests
    from botocore.client import Config

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

    def feature(img):
        g = cv2.cvtColor(cv2.resize(img, (96, 40), interpolation=cv2.INTER_AREA), cv2.COLOR_BGR2GRAY)
        g = g.astype(np.float32).ravel()
        g -= g.mean()
        n = np.linalg.norm(g)
        return g / n if n else g

    def still_feature(rid):
        for attempt in range(3):
            try:
                r = requests.get(STILL_URL.format(rid=rid), headers=STILL_HEADERS, timeout=60)
                if r.status_code == 200:
                    img = cv2.imdecode(np.frombuffer(r.content, np.uint8), cv2.IMREAD_COLOR)
                    if img is not None:
                        return feature(img)
                elif r.status_code == 404:
                    return None
            except requests.RequestException:
                pass
            time.sleep(2 * (attempt + 1))
        return None

    def video_features(path):
        cap = cv2.VideoCapture(path)
        fps = cap.get(cv2.CAP_PROP_FPS) or 24000 / 1001
        frames = []
        while True:
            ok, f = cap.read()
            if not ok:
                break
            frames.append(feature(f))
        cap.release()
        return (np.stack(frames) if frames else None), fps

    def best_in_clip(still, path, expected=CLIP_CENTRE):
        """Best-matching time in a clip, its score, and the score at the expected time."""
        F, fps = video_features(path)
        if F is None:
            return None
        sc = F @ still
        i = int(np.argmax(sc))
        c = min(len(sc) - 1, int(round(expected * fps)))
        return dict(best_t=i / fps, best=float(sc[i]), centre=float(sc[max(0, c - 1):c + 2].max()),
                    expected_t=expected)

    def at_centre(m):
        return m is not None and (
            abs(m["best_t"] - m.get("expected_t", CLIP_CENTRE)) <= CENTRE_TOLERANCE
            or m["centre"] >= m["best"] - CENTRE_SLACK
        )

    def fetch_window(movie_id, centre, tag, path):
        url = f"{MOVIE_SERVER}/extract-clip"
        params = dict(file_id=f"RELOC_{tag}_{centre:.3f}", movie_id=movie_id, timestamp=f"{centre:.3f}")
        for attempt in range(4):
            try:
                with requests.get(url, params=params, timeout=600, stream=True) as r:
                    if r.status_code == 200:
                        with open(path, "wb") as fh:
                            for chunk in r.iter_content(1 << 20):
                                fh.write(chunk)
                        if os.path.getsize(path) > 0:
                            return True
            except requests.RequestException:
                pass
            time.sleep(10 * (attempt + 1))
        return False

    def download_film(movie_id, path):
        auth = (os.environ["MOVIE_USERNAME"], os.environ["MOVIE_PASSWORD"])
        for attempt in range(4):
            try:
                with requests.get(MOVIE_FILE_URL.format(movie_id=movie_id), auth=auth, stream=True, timeout=600) as r:
                    if r.status_code == 200:
                        with open(path, "wb") as fh:
                            for chunk in r.iter_content(8 << 20):
                                fh.write(chunk)
                        if os.path.getsize(path) > 0:
                            return True
            except requests.RequestException:
                pass
            time.sleep(30 * (attempt + 1))
        return False

    def score_film_span(path, S, first_window, last_window):
        """Best (score, time) per still in each WINDOW_SECONDS window of [first_window, last_window)."""
        cap = cv2.VideoCapture(path)
        cap.set(cv2.CAP_PROP_POS_MSEC, first_window * WINDOW_SECONDS * 1000)
        out = {}
        while True:
            ok, f = cap.read()
            if not ok:
                break
            t = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000
            w = int(t // WINDOW_SECONDS)
            if w >= last_window:
                break
            if w < first_window:
                continue
            sc = S @ feature(f)
            cur = out.get(w)
            if cur is None:
                out[w] = [sc.copy(), np.full(len(sc), t)]
            else:
                better = sc > cur[0]
                cur[0][better] = sc[better]
                cur[1][better] = t
        cap.release()
        return out

    def recut_from(movie_id, rid, still, src, found_t, apply, backup_prefix, min_score, out, src_offset=0.0):
        """Cut the 9s clip centred on found_t from src (which starts at src_offset), verify, upload."""
        start = max(0.0, found_t - CLIP_CENTRE - src_offset)
        subprocess.run([
            "ffmpeg", "-y", "-v", "error", "-ss", f"{start:.3f}", "-i", src, "-t", f"{CLIP_SECONDS:g}",
            "-map", "0:v:0", "-map", "0:a:0?", "-c:v", "libx264", "-profile:v", "main", "-pix_fmt", "yuv420p",
            "-crf", "18", "-preset", "medium", "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", out,
        ], check=True)
        # A still in the film's first 4.5s sits before the centre of its clip.
        m = best_in_clip(still, out, expected=found_t - src_offset - start)
        if m is None or m["best"] < min_score or not at_centre(m):
            return dict(rid=rid, status="failed", reason="verify_failed", **(m or {}))
        if apply:
            r2 = connect_r2()
            for suffix in [""] + PREVIEW_SUFFIXES:
                key = f"{SOURCE_PREFIX}/{movie_id}/{rid}{suffix}.mp4"
                backup = f"{backup_prefix}/{movie_id}/{rid}{suffix}.mp4"
                # A preempted run is restarted from scratch; never replace the original backup with a recut.
                if key_exists(r2, key) and not key_exists(r2, backup):
                    r2.copy_object(Bucket=BUCKET_NAME, Key=backup,
                                   CopySource={"Bucket": BUCKET_NAME, "Key": key})
            r2.upload_file(out, BUCKET_NAME, f"{SOURCE_PREFIX}/{movie_id}/{rid}.mp4",
                           ExtraArgs={"ContentType": "video/mp4"})
            for suffix in PREVIEW_SUFFIXES:
                r2.delete_object(Bucket=BUCKET_NAME, Key=f"{SOURCE_PREFIX}/{movie_id}/{rid}{suffix}.mp4")
        return dict(rid=rid, status="uploaded" if apply else "verified", **m)

    def set_progress(job_tag, **kw):
        cur = progress.get(job_tag, {})
        cur.update(kw)
        cur["updated"] = time.time()
        progress[job_tag] = cur


def fit_drift(points):
    """Fit found_time = a * history_time + b to (history_time, found_time) pairs, ignoring
    stills from other bad batches. Returns (a, b, inliers) or None."""
    if len(points) < DRIFT_MIN_POINTS:
        return None
    h, f = np.array(points, dtype=float).T
    ok = h > 0
    ratio = np.median(f[ok] / h[ok]) if ok.any() else 1.0
    keep = np.abs(f - ratio * h) < max(30.0, 0.01 * h.max())
    for _ in range(5):
        if keep.sum() < DRIFT_MIN_POINTS:
            return None
        a, b = np.polyfit(h[keep], f[keep], 1)
        keep = np.abs(f - (a * h + b)) < 2 * DRIFT_TOLERANCE
    if keep.sum() < DRIFT_MIN_POINTS:
        return None
    return float(a), float(b), int(keep.sum())


@app.function(image=image, secrets=[modal.Secret.from_name("r2-credentials")], timeout=600, max_containers=100)
def check_clip(movie_id: int, rid: str) -> dict:
    still = still_feature(rid)
    if still is None:
        return dict(rid=rid, status="no_still")
    r2 = connect_r2()
    with tempfile.TemporaryDirectory() as d:
        path = f"{d}/{rid}.mp4"
        try:
            r2.download_file(BUCKET_NAME, f"{SOURCE_PREFIX}/{movie_id}/{rid}.mp4", path)
        except Exception:
            return dict(rid=rid, status="flagged", reason="no_clip")
        m = best_in_clip(still, path)
    if m is None:
        return dict(rid=rid, status="flagged", reason="unreadable_clip")
    return dict(rid=rid, status="ok" if at_centre(m) else "flagged", reason=None if at_centre(m) else "off_centre", **m)


@app.function(image=image, timeout=1800, max_containers=MOVIE_SERVER_PARALLEL, retries=1)
def match_window(movie_id: int, centre: float, tag: str, stills: bytes, count: int) -> dict:
    S = np.frombuffer(stills, dtype=np.float16).astype(np.float32).reshape(count, -1)
    with tempfile.TemporaryDirectory() as d:
        path = f"{d}/w.mp4"
        if not fetch_window(movie_id, centre, tag, path):
            return dict(centre=centre, error="download_failed")
        F, fps = video_features(path)
    if F is None:
        return dict(centre=centre, error="unreadable_window")
    sc = S @ F.T
    idx = sc.argmax(axis=1)
    base = max(0.0, centre - WINDOW_SECONDS / 2)
    return dict(centre=centre, best=[(float(sc[j, i]), base + i / fps) for j, i in enumerate(idx)])


@app.function(image=image, secrets=[modal.Secret.from_name("r2-credentials")], timeout=1800, max_containers=MOVIE_SERVER_PARALLEL)
def recut(movie_id: int, rid: str, found_t: float, tag: str, apply: bool, backup_prefix: str,
          min_score: float = MATCH_MIN) -> dict:
    still = still_feature(rid)
    centre = max(WINDOW_SECONDS / 2, found_t)
    base = centre - WINDOW_SECONDS / 2
    with tempfile.TemporaryDirectory() as d:
        win = f"{d}/w.mp4"
        if not fetch_window(movie_id, centre, tag, win):
            return dict(rid=rid, status="failed", reason="download_failed")
        return recut_from(movie_id, rid, still, win, found_t, apply, backup_prefix, min_score,
                          f"{d}/{rid}.mp4", src_offset=base)


@app.function(image=image, secrets=[modal.Secret.from_name("movie-server")], cpu=FILM_DECODE_PROCS,
              memory=16384, timeout=4 * 3600)
def search_film(movie_id: int, stills: bytes, count: int, duration: float, job_tag: str) -> list:
    """Downloads the whole film once from :8889 and returns, per still, its best (score, time) in every window."""
    from concurrent.futures import ProcessPoolExecutor
    S = np.frombuffer(stills, dtype=np.float16).astype(np.float32).reshape(count, -1)
    n_windows = int(np.ceil(duration / WINDOW_SECONDS))
    with tempfile.TemporaryDirectory() as d:
        path = f"{d}/film.mp4"
        set_progress(job_tag, phase="downloading film")
        if not download_film(movie_id, path):
            raise RuntimeError(f"Could not download movie {movie_id} from :8889.")
        set_progress(job_tag, phase="searching film", windows=n_windows, windows_done=0)
        step = int(np.ceil(n_windows / FILM_DECODE_PROCS))
        spans = [(a, min(n_windows, a + step)) for a in range(0, n_windows, step)]
        per_window = {}
        with ProcessPoolExecutor(len(spans)) as pool:
            futures = [pool.submit(score_film_span, path, S, a, b) for a, b in spans]
            for f in futures:
                per_window.update(f.result())
                set_progress(job_tag, windows_done=len(per_window))
    return [[(float(per_window[w][0][j]), float(per_window[w][1][j])) for w in sorted(per_window)]
            for j in range(count)]


@app.function(image=image, secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("movie-server")],
              cpu=FILM_DECODE_PROCS, memory=16384, timeout=4 * 3600)
def recut_film(movie_id: int, items: list, job_tag: str, apply: bool, backup_prefix: str) -> list:
    """items: [(rid, found_t, min_score)]. Recuts every clip from one download of the whole film."""
    from concurrent.futures import ThreadPoolExecutor
    with tempfile.TemporaryDirectory() as d:
        path = f"{d}/film.mp4"
        if not download_film(movie_id, path):
            raise RuntimeError(f"Could not download movie {movie_id} from :8889.")

        def one(item):
            rid, found_t, min_score = item
            try:
                return recut_from(movie_id, rid, still_feature(rid), path, found_t, apply, backup_prefix,
                                  min_score, f"{d}/{rid}.mp4")
            except Exception as ex:
                return dict(rid=rid, status="failed", reason=f"recut_error: {ex}"[:300])

        results = []
        with ThreadPoolExecutor(FILM_DECODE_PROCS) as pool:
            for r in pool.map(one, items):
                results.append(r)
                set_progress(job_tag, recut=len(results))
    return results


# Preemption restarts the whole run from the start (an hour's film search), so keep it on one instance.
@app.function(image=image, secrets=[modal.Secret.from_name("movie-server")], timeout=24 * 3600, nonpreemptible=True)
def relocate_movie(movie_id: int, images: list, fps: float, apply: bool, job_tag: str,
                   recut_all: bool = False) -> dict:
    """images: [{"rid": str, "frame": int | None}]. Returns one entry per image."""
    set_progress(job_tag, phase="checking", images=len(images), checked=0)
    report = {}
    for n, r in enumerate(check_clip.map([movie_id] * len(images), [i["rid"] for i in images],
                                         order_outputs=False, return_exceptions=True)):
        if isinstance(r, Exception):
            continue
        report[r["rid"]] = r
        if n % 25 == 0:
            set_progress(job_tag, checked=n + 1)
    for i in images:
        e = report.setdefault(i["rid"], dict(rid=i["rid"], status="failed", reason="check_failed"))
        e["history_frame"] = i.get("frame")
    if images and all(e["status"] == "no_still" for e in report.values()):
        raise RuntimeError("No still could be downloaded for any image; is crunch reachable?")
    for e in report.values():
        e["flagged"] = e["status"] == "flagged"
    flagged = [rid for rid, e in report.items() if e["flagged"]]
    # With recut_all, clips that already match are recut too.
    targets = [rid for rid, e in report.items() if e["flagged"] or (recut_all and e["status"] == "ok")]
    report_meta = {}
    # With the :8889 login the film is downloaded whole; otherwise it's pulled through :8888 in windows.
    whole_film = bool(os.environ.get("MOVIE_USERNAME"))
    report_meta["source"] = "whole_film" if whole_film else "windows"
    set_progress(job_tag, checked=len(images), flagged=len(flagged), targets=len(targets))

    if targets:
        for attempt in range(4):
            try:
                info = requests.get(f"{MOVIE_SERVER}/video-info", params=dict(movie_id=movie_id), timeout=300).json()
                break
            except requests.RequestException:
                if attempt == 3:
                    raise
                time.sleep(60 * (attempt + 1))
        duration = float(info["duration"])
        feats = {rid: still_feature(rid) for rid in targets}
        rids = [rid for rid in targets if feats[rid] is not None]
        S = np.stack([feats[r] for r in rids]).astype(np.float16).tobytes()
        centres = [WINDOW_SECONDS / 2 + WINDOW_SECONDS * k for k in range(int(np.ceil(duration / WINDOW_SECONDS)))]
        best = {rid: (-1.0, None) for rid in rids}
        window_best = {rid: [] for rid in rids}
        failed_centres = []
        todo = [] if whole_film else centres
        if whole_film:
            for rid, wins in zip(rids, search_film.remote(movie_id, S, len(rids), duration, job_tag)):
                window_best[rid] = wins
                if wins:
                    best[rid] = max(wins)
        else:
            set_progress(job_tag, phase="searching film", windows=len(centres), windows_done=0)
        for round_no in range(3 if todo else 0):
            done_before = len(centres) - len(todo)
            failed_centres = []
            for n, (c, w) in enumerate(zip(todo, match_window.map([movie_id] * len(todo), todo, [f"{job_tag}r{round_no}"] * len(todo),
                                                                    [S] * len(todo), [len(rids)] * len(todo),
                                                                    order_outputs=True, return_exceptions=True))):
                if isinstance(w, Exception) or "error" in w:
                    failed_centres.append(c)
                else:
                    for rid, (s, t) in zip(rids, w["best"]):
                        window_best[rid].append((s, t))
                        if s > best[rid][0]:
                            best[rid] = (s, t)
                set_progress(job_tag, windows_done=done_before + n + 1 - len(failed_centres),
                             windows_failed=len(failed_centres), retry_round=round_no)
            if not failed_centres:
                break
            todo = failed_centres
        missing = len(failed_centres)
        report_meta["windows_failed"] = [round(c, 3) for c in failed_centres]

        hist_frame = {i["rid"]: i.get("frame") for i in images}
        drift = fit_drift([(hist_frame[r] / fps, best[r][1]) for r in rids
                           if hist_frame.get(r) is not None and best[r][1] is not None and best[r][0] >= MATCH_MIN])
        if drift:
            a, b, n_in = drift
            report_meta["drift_fit"] = dict(a=round(a, 6), b=round(b, 3), inliers=n_in)
        found, min_scores = [], {}
        for rid in targets:
            e = report[rid]
            s, t = best.get(rid, (-1.0, None))
            min_scores[rid] = MATCH_MIN
            if (t is None or s < MATCH_MIN) and drift and hist_frame.get(rid) is not None:
                pred = drift[0] * hist_frame[rid] / fps + drift[1]
                near = [(ws, wt) for ws, wt in window_best.get(rid, [])
                        if abs(wt - pred) <= DRIFT_TOLERANCE and ws >= DRIFT_MATCH_MIN]
                if near:
                    s, t = max(near)
                    e["found_via"] = "drift_fit"
                    min_scores[rid] = DRIFT_MATCH_MIN
            e["found_score"] = s if t is not None else None
            if t is None or s < min_scores[rid]:
                if e["flagged"]:
                    e["status"] = "not_found"
                else:
                    e["reason"] = "kept: not found in film"
                continue
            e["found_time"] = round(t, 3)
            e["found_frame"] = int(round(t * fps))
            found.append(rid)

        set_progress(job_tag, phase="recutting" if apply else "verifying", recut=0, recut_total=len(found))
        backup_prefix = f"{SOURCE_PREFIX}_backup_{time.strftime('%Y%m%d')}"
        if whole_film:
            recuts = recut_film.remote(movie_id, [(r, report[r]["found_time"], min_scores[r]) for r in found],
                                       job_tag, apply, backup_prefix) if found else []
        else:
            recuts = recut.map([movie_id] * len(found), found, [report[r]["found_time"] for r in found],
                               [job_tag] * len(found), [apply] * len(found), [backup_prefix] * len(found),
                               [min_scores[r] for r in found], order_outputs=False, return_exceptions=True)
        for n, r in enumerate(recuts):
            if isinstance(r, Exception):
                continue
            e = report[r["rid"]]
            e["recut_score"] = r.get("best")
            if r["status"] in ("uploaded", "verified"):
                e["status"] = "relocated"
            elif e["flagged"]:
                e.update(status="failed", reason=r.get("reason"))
            else:
                e["reason"] = f"kept: {r.get('reason')}"
            set_progress(job_tag, recut=n + 1)
        for rid in found:
            e = report[rid]
            if e["status"] == "flagged":
                e.update(status="failed", reason="recut_failed")
            elif e["status"] == "ok" and not e.get("reason"):
                e["reason"] = "kept: recut_failed"
        if apply:
            report_meta["backup_prefix"] = f"{backup_prefix}/{movie_id}/"

    set_progress(job_tag, phase="done")
    return dict(movie_id=movie_id, apply=apply, recut_all=recut_all, fps=fps, **report_meta,
                images=list(report.values()))


@app.function(image=image, timeout=300)
@modal.fastapi_endpoint(method="POST", label="clip-relocate-start", docs=True)
def start(body: dict):
    job_tag = str(body["job_tag"])
    progress[job_tag] = dict(phase="queued")
    call = relocate_movie.spawn(int(body["movie_id"]), body["images"], float(body["fps"]),
                                bool(body.get("apply", False)), job_tag, bool(body.get("recut_all", False)))
    return dict(call_id=call.object_id)


@app.function(image=image, timeout=300)
@modal.fastapi_endpoint(method="GET", label="clip-relocate-result", docs=True)
def result(call_id: str, job_tag: str = ""):
    call = modal.FunctionCall.from_id(call_id)
    try:
        return dict(status="done", result=call.get(timeout=0))
    except (TimeoutError, modal.exception.TimeoutError):
        return dict(status="running", progress=progress.get(job_tag, {}))
    except Exception as ex:
        return dict(status="failed", error=f"{type(ex).__name__}: {ex}"[:2000])
