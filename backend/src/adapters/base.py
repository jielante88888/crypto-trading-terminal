"""
交易所适配器抽象基类
定义现货和合约交易的标准接口
支持币安和OKX交易所的插拔式架构
"""

from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Optional, Any, AsyncGenerator, Tuple
from dataclasses import dataclass
from enum import Enum

import structlog

logger = structlog.get_logger()


class MarketType(Enum):
    """市场类型"""

    SPOT = "spot"
    FUTURES = "futures"


class OrderType(Enum):
    """订单类型"""

    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class OrderSide(Enum):
    """订单方向"""

    BUY = "buy"
    SELL = "sell"


class TimeInterval(Enum):
    """K线时间间隔"""

    MINUTE_1 = "1m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    HOUR_1 = "1h"
    HOUR_4 = "4h"
    DAY_1 = "1d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"


@dataclass
class MarketData:
    """市场数据结构"""

    symbol: str
    current_price: Decimal
    previous_close: Decimal
    high_24h: Decimal
    low_24h: Decimal
    price_change: Decimal
    price_change_percent: Decimal
    volume_24h: Decimal
    quote_volume_24h: Decimal
    timestamp: datetime

    # 合约特有字段
    funding_rate: Optional[Decimal] = None
    open_interest: Optional[Decimal] = None
    index_price: Optional[Decimal] = None
    mark_price: Optional[Decimal] = None


@dataclass
class OrderBook:
    """订单簿数据结构"""

    symbol: str
    bids: List[Tuple[Decimal, Decimal]]  # (价格, 数量)
    asks: List[Tuple[Decimal, Decimal]]  # (价格, 数量)
    timestamp: datetime


@dataclass
class Trade:
    """交易记录数据结构"""

    id: str
    symbol: str
    price: Decimal
    quantity: Decimal
    side: OrderSide
    timestamp: datetime


@dataclass
class Candle:
    """K线数据结构"""

    symbol: str
    interval: TimeInterval
    open_time: datetime
    close_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    quote_volume: Decimal
    trades_count: int


@dataclass
class ExchangeInfo:
    """交易所信息"""

    name: str
    is_testnet: bool
    spot_supported: bool
    futures_supported: bool
    rate_limits: Dict[str, Any]
    supported_intervals: List[TimeInterval]


class BaseExchangeAdapter(ABC):
    """交易所适配器抽象基类"""

    def __init__(
        self,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        passphrase: Optional[str] = None,
        is_testnet: bool = True,
    ):
        """
        初始化交易所适配器

        Args:
            api_key: API密钥
            secret_key: 密钥
            passphrase: 密码短语 (OKX需要)
            is_testnet: 是否使用测试环境
        """
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.is_testnet = is_testnet
        self.logger = structlog.get_logger(self.__class__.__name__)

        # 连接状态
        self._is_connected = False
        self._last_heartbeat = datetime.utcnow()

    @property
    @abstractmethod
    def exchange_name(self) -> str:
        """交易所名称"""
        pass

    @property
    @abstractmethod
    def exchange_info(self) -> ExchangeInfo:
        """交易所信息"""
        pass

    @abstractmethod
    async def connect(self) -> bool:
        """连接到交易所API"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """断开连接"""
        pass

    @abstractmethod
    async def is_healthy(self) -> bool:
        """检查连接健康状态"""
        pass

    # 市场数据接口
    @abstractmethod
    async def get_spot_ticker(self, symbol: str) -> MarketData:
        """获取现货价格信息"""
        pass

    @abstractmethod
    async def get_futures_ticker(self, symbol: str) -> MarketData:
        """获取期货价格信息"""
        pass

    @abstractmethod
    async def get_spot_order_book(self, symbol: str, limit: int = 100) -> OrderBook:
        """获取现货订单簿"""
        pass

    @abstractmethod
    async def get_futures_order_book(self, symbol: str, limit: int = 100) -> OrderBook:
        """获取期货订单簿"""
        pass

    @abstractmethod
    async def get_spot_trades(self, symbol: str, limit: int = 100) -> List[Trade]:
        """获取现货交易记录"""
        pass

    @abstractmethod
    async def get_futures_trades(self, symbol: str, limit: int = 100) -> List[Trade]:
        """获取期货交易记录"""
        pass

    # K线数据接口
    @abstractmethod
    async def get_spot_klines(
        self,
        symbol: str,
        interval: TimeInterval,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[Candle]:
        """获取现货K线数据"""
        pass

    @abstractmethod
    async def get_futures_klines(
        self,
        symbol: str,
        interval: TimeInterval,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[Candle]:
        """获取期货K线数据"""
        pass

    # WebSocket接口
    @abstractmethod
    async def subscribe_spot_ticker(
        self, symbol: str
    ) -> AsyncGenerator[MarketData, None]:
        """订阅现货价格流"""
        pass

    @abstractmethod
    async def subscribe_futures_ticker(
        self, symbol: str
    ) -> AsyncGenerator[MarketData, None]:
        """订阅期货价格流"""
        pass

    @abstractmethod
    async def subscribe_spot_order_book(
        self, symbol: str
    ) -> AsyncGenerator[OrderBook, None]:
        """订阅现货订单簿流"""
        pass

    @abstractmethod
    async def subscribe_futures_order_book(
        self, symbol: str
    ) -> AsyncGenerator[OrderBook, None]:
        """订阅期货订单簿流"""
        pass

    # 交易接口 (需要API密钥)
    @abstractmethod
    async def create_spot_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """创建现货订单"""
        pass

    @abstractmethod
    async def create_futures_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        client_order_id: Optional[str] = None,
        leverage: Optional[int] = None,
    ) -> Dict[str, Any]:
        """创建期货订单"""
        pass

    @abstractmethod
    async def get_spot_order_status(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """获取现货订单状态"""
        pass

    @abstractmethod
    async def get_futures_order_status(
        self, symbol: str, order_id: str
    ) -> Dict[str, Any]:
        """获取期货订单状态"""
        pass

    @abstractmethod
    async def cancel_spot_order(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """取消现货订单"""
        pass

    @abstractmethod
    async def cancel_futures_order(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """取消期货订单"""
        pass

    # 账户信息
    @abstractmethod
    async def get_spot_balance(self) -> Dict[str, Any]:
        """获取现货账户余额"""
        pass

    @abstractmethod
    async def get_futures_balance(self) -> Dict[str, Any]:
        """获取期货账户余额"""
        pass

    # 工具方法
    def validate_symbol(self, symbol: str, market_type: MarketType) -> bool:
        """验证交易对是否支持"""
        if market_type == MarketType.SPOT:
            return symbol in self.exchange_info.spot_supported_symbols
        else:
            return symbol in self.exchange_info.futures_supported_symbols

    def format_symbol(self, symbol: str, market_type: MarketType) -> str:
        """格式化交易对名称"""
        if market_type == MarketType.FUTURES:
            # 期货交易对格式转换
            if self.exchange_name == "binance":
                return symbol  # 已经是期货格式
            elif self.exchange_name == "okx":
                return symbol  # 已经是期货格式
        return symbol

    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            # 检查连接状态
            is_connected = await self.is_healthy()

            # 检查API响应时间
            start_time = datetime.utcnow()
            await self.get_spot_ticker("BTCUSDT")  # 使用默认交易对测试
            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000

            return {
                "exchange": self.exchange_name,
                "is_connected": is_connected,
                "response_time_ms": round(response_time, 2),
                "is_testnet": self.is_testnet,
                "last_heartbeat": self._last_heartbeat.isoformat(),
                "status": (
                    "healthy" if is_connected and response_time < 5000 else "unhealthy"
                ),
            }

        except Exception as e:
            self.logger.error(f"健康检查失败: {e}")
            return {
                "exchange": self.exchange_name,
                "is_connected": False,
                "status": "error",
                "error": str(e),
            }


class ExchangeAdapterFactory:
    """交易所适配器工厂"""

    _adapters = {}

    @classmethod
    def register(cls, name: str, adapter_class):
        """注册交易所适配器"""
        cls._adapters[name.lower()] = adapter_class

    @classmethod
    def create_adapter(
        cls,
        exchange_name: str,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        passphrase: Optional[str] = None,
        is_testnet: bool = True,
    ) -> BaseExchangeAdapter:
        """创建交易所适配器实例"""
        name = exchange_name.lower()

        if name not in cls._adapters:
            raise ValueError(f"不支持的交易所: {exchange_name}")

        adapter_class = cls._adapters[name]

        try:
            return adapter_class(
                api_key=api_key,
                secret_key=secret_key,
                passphrase=passphrase,
                is_testnet=is_testnet,
            )
        except Exception as e:
            raise RuntimeError(f"创建{exchange_name}适配器失败: {e}")

    @classmethod
    def get_supported_exchanges(cls) -> List[str]:
        """获取支持的交易所列表"""
        return list(cls._adapters.keys())


# 装饰器：自动注册适配器
def register_exchange(exchange_name: str):
    """注册交易所适配器的装饰器"""

    def decorator(cls):
        ExchangeAdapterFactory.register(exchange_name, cls)
        return cls

    return decorator


if __name__ == "__main__":
    # 测试基础适配器
    print("交易所适配器基类定义完成")

    # 显示支持的交易所
    exchanges = ExchangeAdapterFactory.get_supported_exchanges()
    print(f"支持的交易所: {exchanges}")

    # 验证基类接口
    adapter_methods = [
        method
        for method in dir(BaseExchangeAdapter)
        if not method.startswith("_") and callable(getattr(BaseExchangeAdapter, method))
    ]
    print(f"基类方法数量: {len(adapter_methods)}")