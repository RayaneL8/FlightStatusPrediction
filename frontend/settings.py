from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    API_URL: Optional[str] = "http://127.0.0.1:8000/"

    class Config:
        env_file = ".env"
        from_attributes = True

settings = Settings()
print(settings.API_URL)