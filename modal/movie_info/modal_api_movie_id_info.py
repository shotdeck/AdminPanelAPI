import modal

CONTAINER_SCALEDOWN_WINDOW = 2 * 60  # Keep container warm for 10 minutes
app = modal.App("modal-movie-metadata-api")

image = (
    modal.Image.debian_slim()
    .apt_install("libpq-dev", "openssh-client")
    .pip_install(
        "psycopg2-binary",
        "sshtunnel==0.4.0",
        "paramiko==2.12.0",
        "python-dotenv",
        "loguru",
        "fastapi[standard]"
    )
    .add_local_dir("./", remote_path="/root")
)

with image.imports():
    import os
    import re
    import time
    from loguru import logger
    from dotenv import load_dotenv
    from sshtunnel import SSHTunnelForwarder
    from config import load_config
    from database import connect_db

    load_dotenv()

    # -------------------------------------------------------------------
    # Batch version of shot_time lookup (LATEST row per imageid)
    # -------------------------------------------------------------------
    def get_timestamps_batch(cur, image_ids, fps: float = 23.976):
        if not image_ids:
            return {}

        cur.execute(
            """
            SELECT DISTINCT ON (imageid)
                   imageid,
                   notes
            FROM frl_imagehistory
            WHERE action = 'Shot Time Autodetected'
              AND imageid = ANY(%s)
            ORDER BY imageid, "timestamp" DESC;
            """,
            (image_ids,),
        )

        results = {}

        for imageid, notes in cur.fetchall():
            if not notes:
                continue

            match = re.search(r"#0*(\d+)", notes)
            if not match:
                continue

            raw_value = int(match.group(1))

            if "Conf:" in notes or "Hash:" in notes or "Crop:" in notes:
                ts = raw_value / 1000.0
            else:
                ts = raw_value / fps

            results[imageid] = ts

        return results

# -------------------------------------------------------------------
# Main Modal Class
# -------------------------------------------------------------------
@app.cls(
    image=image,
    scaledown_window=CONTAINER_SCALEDOWN_WINDOW,
    timeout=600
)
class MovieMetadataAPI:
    @modal.enter()
    def setup(self):
        from loguru import logger

        self.config = load_config()
        self.ssh_config = self.config.ssh
        self.db_config = self.config.db

        start = time.time()
        self.tunnel = SSHTunnelForwarder(
            (self.ssh_config.host, self.ssh_config.port),
            ssh_username=self.ssh_config.user,
            ssh_pkey=self.ssh_config.pem,
            remote_bind_address=(self.db_config.host, 5432),
            local_bind_address=("127.0.0.1", 5434),
        )
        self.tunnel.start()
        self.conn, self.cur = connect_db()
        logger.info(f"Connected to DB via SSH tunnel in {time.time()-start:.2f}s.")

    @modal.exit()
    def cleanup(self):
        if hasattr(self, "cur"):
            self.cur.close()
        if hasattr(self, "conn"):
            self.conn.close()
        if hasattr(self, "tunnel") and self.tunnel.is_active:
            self.tunnel.stop()
        logger.info("Closed DB + SSH tunnel.")

    # -------------------------------------------------------------------
    # Optimized API Endpoint
    # -------------------------------------------------------------------
    @modal.fastapi_endpoint(method="GET", label="movie-info", docs=True)
    def movie_info(self, movie_id: int):
        t0 = time.time()
        try:
            # --- 1. Movie metadata ---
            self.cur.execute(
                """
                SELECT movieid, fps, duration, frame_count
                FROM frl.frl_movie_info
                WHERE movieid = %s;
                """,
                (movie_id,),
            )
            movie = self.cur.fetchone()
            if not movie:
                return {"error": f"Movie ID {movie_id} not found."}

            fps = float(movie[1])

            # --- 2. Fetch all images at once ---
            t1 = time.time()
            self.cur.execute(
                """
                SELECT idnum, filename
                FROM frl.frl_images
                WHERE movieid = %s
              AND status <> 'deleted'
                ORDER BY idnum;
                """,
                (movie_id,),
            )
            images = self.cur.fetchall()
            if not images:
                return {"error": f"No images found for movie {movie_id}."}

            image_ids = [img[0] for img in images]
            logger.info(f"Fetched {len(images)} images in {time.time()-t1:.2f}s.")

            # --- 3. Batch get all timestamps in ONE query ---
            t2 = time.time()
            timestamps = get_timestamps_batch(self.cur, image_ids, fps)
            logger.info(f"Fetched all timestamps in {time.time()-t2:.2f}s.")

            # --- 4. Build response efficiently ---
            results = [
                {
                    "idnum": img[0],
                    "filename": img[1],
                    "shot_time": timestamps.get(img[0])
                }
                for img in images
            ]

            # --- 5. Final result ---
            total_time = time.time() - t0
            logger.info(f"movie-info query for movie_id={movie_id} took {total_time:.2f}s.")
            return {
                "movie_id": movie_id,
                "fps": fps,
                "duration": float(movie[2]),
                "frame_count": int(movie[3]),
                "image_count": len(results),
                "images": results,
                "query_time_sec": round(total_time, 2)
            }

        except Exception as e:
            return {"error": str(e)}

