"""
系统配置管理
支持环境变量和配置文件的双重配置
"""

import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """应用配置类"""

    # 应用基础配置
    APP_NAME: str = "加密货币专业交易终端"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = Field(default=False, env="DEBUG")

    # API配置
    API_HOST: str = Field(default="0.0.0.0", env="API_HOST")
    API_PORT: int = Field(default=8000, env="API_PORT")
    API_WORKERS: int = Field(default=1, env="API_WORKERS")

    # 数据库配置
    DATABASE_URL: str = Field(
        default="sqlite:///./crypto_trading.db", env="DATABASE_URL"
    )
    REDIS_URL: str = Field(default="redis://localhost:6379/0", env="REDIS_URL")

    # 交易所API配置
    BINANCE_API_KEY: Optional[str] = Field(default=None, env="BINANCE_API_KEY")
    BINANCE_SECRET_KEY: Optional[str] = Field(default=None, env="BINANCE_SECRET_KEY")
    BINANCE_TESTNET: bool = Field(default=True, env="BINANCE_TESTNET")

    OKX_API_KEY: Optional[str] = Field(default=None, env="OKX_API_KEY")
    OKX_SECRET_KEY: Optional[str] = Field(default=None, env="OKX_SECRET_KEY")
    OKX_PASSPHRASE: Optional[str] = Field(default=None, env="OKX_PASSPHRASE")
    OKX_PAPER_TRADING: bool = Field(default=True, env="OKX_PAPER_TRADING")

    # WebSocket配置
    WEBSOCKET_RECONNECT_INTERVAL: int = Field(
        default=5, env="WEBSOCKET_RECONNECT_INTERVAL"
    )
    WEBSOCKET_MAX_RECONNECT_ATTEMPTS: int = Field(
        default=10, env="WEBSOCKET_MAX_RECONNECT_ATTEMPTS"
    )

    # 交易配置
    DEFAULT_SYMBOL: str = Field(default="BTCUSDT", env="DEFAULT_SYMBOL")
    SPOT_SYMBOLS: list[str] = Field(
        default=["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOTUSDT"],
        env="SPOT_SYMBOLS",
    )
    FUTURES_SYMBOLS: list[str] = Field(
        default=["BTCUSDT-PERP", "ETHUSDT-PERP", "BNBUSDT-PERP"], env="FUTURES_SYMBOLS"
    )

    # 风险控制配置
    MAX_POSITION_RATIO: float = Field(
        default=0.1, env="MAX_POSITION_RATIO"
    )  # 最大仓位比例
    STOP_LOSS_RATIO: float = Field(default=0.05, env="STOP_LOSS_RATIO")  # 止损比例
    TAKE_PROFIT_RATIO: float = Field(default=0.1, env="TAKE_PROFIT_RATIO")  # 止盈比例

    # 通知配置
    TELEGRAM_BOT_TOKEN: Optional[str] = Field(default=None, env="TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID: Optional[str] = Field(default=None, env="TELEGRAM_CHAT_ID")
    EMAIL_SMTP_HOST: Optional[str] = Field(default=None, env="EMAIL_SMTP_HOST")
    EMAIL_SMTP_PORT: int = Field(default=587, env="EMAIL_SMTP_PORT")
    EMAIL_USERNAME: Optional[str] = Field(default=None, env="EMAIL_USERNAME")
    EMAIL_PASSWORD: Optional[str] = Field(default=None, env="EMAIL_PASSWORD")

    # AI模型配置
    AI_MODEL_PATH: str = Field(default="./models", env="AI_MODEL_PATH")
    AI_PREDICTION_INTERVAL: int = Field(
        default=300, env="AI_PREDICTION_INTERVAL"
    )  # 5分钟

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# 全局配置实例
settings = Settings()


# 环境变量验证
def validate_environment() -> bool:
    """验证必要环境变量"""
    required_vars = []

    # 如果启用实盘交易，需要API密钥
    if not settings.BINANCE_TESTNET and not settings.OKX_PAPER_TRADING:
        required_vars.extend(
            [
                "BINANCE_API_KEY",
                "BINANCE_SECRET_KEY",
                "OKX_API_KEY",
                "OKX_SECRET_KEY",
                "OKX_PASSPHRASE",
            ]
        )

    missing_vars = [var for var in required_vars if not getattr(settings, var)]
    if missing_vars:
        print(f"警告: 缺少环境变量: {', '.join(missing_vars)}")
        return False

    return True


if __name__ == "__main__":
    # 测试配置加载
    print(f"应用名称: {settings.APP_NAME}")
    print(f"调试模式: {settings.DEBUG}")
    print(f"API端口: {settings.API_PORT}")
    print(f"数据库URL: {settings.DATABASE_URL}")
    print(f"币安测试网: {settings.BINANCE_TESTNET}")
    print(f"OKX模拟交易: {settings.OKX_PAPER_TRADING}")
