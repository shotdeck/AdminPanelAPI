import os
from pydantic import Field, BaseModel
from dotenv import load_dotenv
load_dotenv()

class SSHConfig(BaseModel):
    host: str = Field(default=os.environ.get("SSH_HOST"))
    port: int = Field(default=int(os.environ.get("SSH_PORT")))
    user: str = Field(default=os.environ.get("SSH_USER"))
    pem: str = Field(default=os.environ.get("SSH_PEM"))

class DatabaseConfig(BaseModel):
    host: str = Field(default=os.environ.get("SHOTDECK_HOST"))
    port: int = Field(default=int(os.environ.get("SHOTDECK_PORT")))
    username: str = Field(default=os.environ.get("SHOTDECK_USR"))
    password: str = Field(default=os.environ.get("SHOTDECK_PWD"))
    database: str = Field(default=os.environ.get("SHOTDECK_DB"))
    local_host: str = "127.0.0.1"
    local_port: int = 5434

class LoginConfig(BaseModel):
    username: str = Field(default=os.environ.get("SHOTDECK_LOGIN_USERNAME"))
    password: str = Field(default=os.environ.get("SHOTDECK_LOGIN_PASSWORD"))

class EmbeddingsConfig(BaseModel):
    model_name: str = "florence"
    table_name: str = "frl.frl_caption_embeddings"
    column_name: str = "keyword_vectorized_embeddings"
    images_dir: str = "/mnt/share/shotdeck/stills/"

class TextEmbeddingsConfig(BaseModel):
    model_name: str = "llave"
    table_name: str = "frl.frl_images"
    text_column: str = "metadata"
    text_embedding_column: str = "llave_metadata_embeddings_v2"

class LlaveConfig(BaseModel):
    pretrained_model: str = "zhibinlan/LLaVE-7B"
    model_name: str = "llave_qwen"
    device_map: str = "auto"
    device: str = "cuda"
    embedding_dim: int = 3584

class APIKeys(BaseModel):
    groq_api_key: str = Field(default=os.environ.get("GROQ_API_KEY"))

class VideosConfig(BaseModel):
    videos_dir: str = "./videos"
    table_name: str = "frl.frl_videos_ml_new"
    column_name: str = "embedding_half"
    resolution: tuple = (640, 480)

class AudiosConfig(BaseModel):
    audios_dir: str = "../../audio_outputs"
    table_name: str = "frl.frl_audio_ml"
    column_name: str = "embedding"

class GroqConfig(BaseModel):
    groq_api_key: str = Field(default=os.environ.get("GROQ_API_KEY"))

class VespaConfig(BaseModel):
    endpoint: str = "https://c6bc0b12.fd166af3.z.vespa-app.cloud/"
    cert_path: str = "./data-plane-public-cert.pem"
    key_path: str = "./data-plane-private-key.pem"
    target_hits: int = 10000

class AppConfig(BaseModel):
    ssh: SSHConfig = Field(default_factory=SSHConfig)
    db: DatabaseConfig = Field(default_factory=DatabaseConfig)
    login: LoginConfig = Field(default_factory=LoginConfig)
    api_keys: APIKeys = Field(default_factory=APIKeys)
    embeddings: EmbeddingsConfig = Field(default_factory=EmbeddingsConfig)
    textembeddings: TextEmbeddingsConfig = Field(default_factory=TextEmbeddingsConfig)
    llave: LlaveConfig = Field(default_factory=LlaveConfig)
    videos: VideosConfig = Field(default_factory=VideosConfig)
    audios: AudiosConfig = Field(default_factory=AudiosConfig)
    groqapikey: GroqConfig = Field(default_factory=GroqConfig)
    vespaconfig: VespaConfig = Field(default_factory=VespaConfig)


def load_config():
    return AppConfig()
