import os
from typing import List
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()

class Settings(BaseSettings):
    # 基础配置
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")
    API_VERSION: str = os.getenv("API_VERSION", "v1")
    
    # CORS 配置
    # 从环境变量读取并解析
    allowed_origins_str: str = os.getenv(
        "ALLOWED_ORIGINS", 
        "http://localhost:3000,http://127.0.0.1:3000"
    )
    ALLOWED_ORIGINS: List[str] = [origin.strip() for origin in allowed_origins_str.split(",") if origin.strip()]

    # ALLOWED_ORIGINS: List[str] = os.getenv(
    #     "ALLOWED_ORIGINS", 
    #     "http://localhost:3000,http://127.0.0.1:3000"
    # ).split(",")
    
    # 日志
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # 根据环境返回配置
    @property
    def is_development(self) -> bool:
        return self.ENVIRONMENT == "development"
    
    @property
    def is_production(self) -> bool:
        return self.ENVIRONMENT == "production"
    
    @property
    def is_testing(self) -> bool:
        return self.ENVIRONMENT == "testing"
    
    def get_cors_origins(self) -> List[str]:
        """获取CORS允许的源"""
        if self.is_development:
            # 开发环境添加更多本地地址
            origins = self.ALLOWED_ORIGINS.copy()
            origins.extend([
                "http://localhost:3000",
                "http://127.0.0.1:3000",
            ])
            return origins
        else:
            # 生产环境只使用配置的源
            return self.ALLOWED_ORIGINS

# 创建全局配置实例
settings = Settings()