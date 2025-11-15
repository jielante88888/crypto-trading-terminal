"""
WebSocket客户端管理器
管理所有交易所的WebSocket连接和数据流
"""

import asyncio
import json
import uuid
import time
import websockets
import ssl
import certifi
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Callable, Any, Union
from dataclasses import dataclass, field
from enum import Enum
import structlog

from ..adapters.base import MarketData, OrderBook, Trade, Order
from ..utils.exceptions import WebSocketError, ConnectionError

logger = structlog.get_logger(__name__)


class ConnectionStatus(Enum):
    """连接状态"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


@dataclass
class SubscriptionConfig:
    """订阅配置"""
    symbols: List[str]
    channels: List[str]
    callback: Optional[Callable] = None
    interval: Optional[str] = None  # kline interval
    depth: Optional[str] = None  # orderbook depth


class WebSocketConnection:
    """WebSocket连接类"""

    def __init__(
        self,
        name: str,
        url: str,
        exchange: str,
        message_handler: Optional[Callable] = None
    ):
        self.name = name
        self.url = url
        self.exchange = exchange
        self.message_handler = message_handler
        
        # 连接状态
        self.status = ConnectionStatus.DISCONNECTED
        self.websocket: Optional[websockets.WebSocketServerProtocol] = None
        
        # 订阅管理
        self.subscriptions: Dict[str, SubscriptionConfig] = {}
        self.active_subscriptions: Set[str] = set()
        
        # 统计信息
        self.connect_time: Optional[datetime] = None
        self.last_message_time: Optional[datetime] = None
        self.message_count = 0
        self.error_count = 0
        
        # 重连参数
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5
        self.current_reconnect_attempt = 0
        
        logger.info(f"WebSocket连接创建: {name}")

    async def connect(self) -> bool:
        """建立连接"""
        if self.status == ConnectionStatus.CONNECTED:
            return True
            
        try:
            self.status = ConnectionStatus.CONNECTING
            logger.info(f"正在连接 {self.name}: {self.url}")
            
            # 创建SSL上下文
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            
            # 建立连接
            self.websocket = await websockets.connect(
                self.url,
                ssl=ssl_context,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            )
            
            self.status = ConnectionStatus.CONNECTED
            self.connect_time = datetime.utcnow()
            self.current_reconnect_attempt = 0
            
            logger.info(f"WebSocket连接成功: {self.name}")
            return True
            
        except Exception as e:
            self.status = ConnectionStatus.FAILED
            logger.error(f"WebSocket连接失败 {self.name}: {e}")
            return False

    async def disconnect(self):
        """断开连接"""
        try:
            if self.websocket:
                await self.websocket.close()
            self.status = ConnectionStatus.DISCONNECTED
            self.active_subscriptions.clear()
            
            logger.info(f"WebSocket连接已断开: {self.name}")
            
        except Exception as e:
            logger.error(f"断开WebSocket连接失败 {self.name}: {e}")

    async def send_message(self, message: Union[str, dict]):
        """发送消息"""
        if self.status != ConnectionStatus.CONNECTED:
            raise ConnectionError(f"WebSocket未连接: {self.name}")
            
        try:
            if isinstance(message, dict):
                message = json.dumps(message, separators=(',', ':'))
                
            await self.websocket.send(message)
            logger.debug(f"消息已发送 {self.name}: {message[:100]}")
            
        except Exception as e:
            logger.error(f"发送消息失败 {self.name}: {e}")
            self.error_count += 1
            raise

    async def listen(self):
        """监听消息"""
        if not self.websocket:
            return
            
        try:
            async for message in self.websocket:
                try:
                    await self._handle_message(message)
                except Exception as e:
                    logger.error(f"处理消息失败 {self.name}: {e}")
                    self.error_count += 1
                    
        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"WebSocket连接关闭 {self.name}")
            self.status = ConnectionStatus.DISCONNECTED
            
        except Exception as e:
            logger.error(f"WebSocket监听异常 {self.name}: {e}")
            self.status = ConnectionStatus.FAILED

    async def _handle_message(self, message: str):
        """处理接收到的消息"""
        try:
            self.last_message_time = datetime.utcnow()
            self.message_count += 1
            
            # 解析JSON消息
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                logger.debug(f"收到非JSON消息 {self.name}: {message[:100]}")
                return
                
            # 处理不同交易所的消息格式
            if self.exchange == "binance":
                await self._handle_binance_message(data)
            elif self.exchange == "okx":
                await self._handle_okx_message(data)
            elif self.exchange == "bybit":
                await self._handle_bybit_message(data)
            else:
                logger.debug(f"未知的交易所 {self.exchange}: {data}")
                
        except Exception as e:
            logger.error(f"处理消息异常 {self.name}: {e}")
            raise

    async def _handle_binance_message(self, data: dict):
        """处理币安消息"""
        try:
            # 行情数据
            if "e" in data:
                event_type = data["e"]
                
                if event_type == "24hrTicker":
                    # 24小时价格变动统计
                    symbol = data["s"]
                    price = float(data["c"])
                    change_percent = float(data["P"])
                    
                    market_data = MarketData(
                        symbol=symbol,
                        price=price,
                        change_24h=change_percent,
                        volume=float(data["v"]),
                        timestamp=datetime.utcnow()
                    )
                    
                    await self._notify_subscribers("ticker", symbol, market_data)
                    
                elif event_type == "trade":
                    # 实时交易数据
                    symbol = data["s"]
                    price = float(data["p"])
                    quantity = float(data["q"])
                    
                    trade = Trade(
                        symbol=symbol,
                        price=price,
                        quantity=quantity,
                        side="buy" if data["m"] else "sell",
                        timestamp=datetime.fromtimestamp(data["E"] / 1000)
                    )
                    
                    await self._notify_subscribers("trade", symbol, trade)
                    
                elif event_type == "kline":
                    # K线数据
                    kline_data = data["k"]
                    symbol = kline_data["s"]
                    
                    kline = {
                        "symbol": symbol,
                        "open_time": kline_data["t"],
                        "close_time": kline_data["T"],
                        "open": float(kline_data["o"]),
                        "high": float(kline_data["h"]),
                        "low": float(kline_data["l"]),
                        "close": float(kline_data["c"]),
                        "volume": float(kline_data["v"]),
                        "is_closed": kline_data["x"]
                    }
                    
                    await self._notify_subscribers("kline", symbol, kline)
                    
        except Exception as e:
            logger.error(f"处理币安消息失败: {e}")

    async def _handle_okx_message(self, data: dict):
        """处理OKX消息"""
        try:
            if data.get("event") == "subscribe":
                logger.info(f"订阅成功: {data}")
                return
                
            if "data" in data:
                for item in data["data"]:
                    channel = data["arg"]["channel"]
                    
                    if channel == "tickers":
                        # 价格快照
                        symbol = item["instId"]
                        price = float(item["last"])
                        change_percent = float(item["change24h"]) * 100
                        
                        market_data = MarketData(
                            symbol=symbol,
                            price=price,
                            change_24h=change_percent,
                            volume=float(item["vol24h"]),
                            timestamp=datetime.utcnow()
                        )
                        
                        await self._notify_subscribers("ticker", symbol, market_data)
                        
                    elif channel == "trades":
                        # 交易数据
                        symbol = item["instId"]
                        price = float(item["px"])
                        quantity = float(item["sz"])
                        
                        trade = Trade(
                            symbol=symbol,
                            price=price,
                            quantity=quantity,
                            side=item["side"],
                            timestamp=datetime.fromtimestamp(int(item["ts"]) / 1000)
                        )
                        
                        await self._notify_subscribers("trade", symbol, trade)
                        
        except Exception as e:
            logger.error(f"处理OKX消息失败: {e}")

    async def _handle_bybit_message(self, data: dict):
        """处理Bybit消息"""
        try:
            topic = data.get("topic", "")
            data_item = data.get("data", {})
            
            if "orderBook" in topic:
                # 订单簿数据
                symbol = data_item.get("s", "")
                bids = [[float(b[0]), float(b[1])] for b in data_item.get("b", [])]
                asks = [[float(a[0]), float(a[1])] for a in data_item.get("a", [])]
                
                orderbook = OrderBook(
                    symbol=symbol,
                    bids=bids,
                    asks=asks,
                    timestamp=datetime.utcnow()
                )
                
                await self._notify_subscribers("orderbook", symbol, orderbook)
                
            elif "tickers" in topic:
                # 价格数据
                symbol = data_item.get("symbol", "")
                price = float(data_item.get("lastPrice", 0))
                change_percent = float(data_item.get("price24hPcnt", 0)) * 100
                
                market_data = MarketData(
                    symbol=symbol,
                    price=price,
                    change_24h=change_percent,
                    volume=float(data_item.get("volume24h", 0)),
                    timestamp=datetime.utcnow()
                )
                
                await self._notify_subscribers("ticker", symbol, market_data)
                
        except Exception as e:
            logger.error(f"处理Bybit消息失败: {e}")

    async def _notify_subscribers(self, channel: str, symbol: str, data: Any):
        """通知订阅者"""
        try:
            subscription_key = f"{channel}:{symbol}"
            
            if subscription_key in self.active_subscriptions:
                if self.message_handler:
                    await self.message_handler(subscription_key, data)
                    
                # 调用配置的回调函数
                for config in self.subscriptions.values():
                    if symbol in config.symbols and channel in config.channels:
                        if config.callback:
                            try:
                                if asyncio.iscoroutinefunction(config.callback):
                                    await config.callback(data)
                                else:
                                    config.callback(data)
                            except Exception as e:
                                logger.error(f"订阅回调执行失败: {e}")
                                
        except Exception as e:
            logger.error(f"通知订阅者失败: {e}")

    def get_status(self) -> dict:
        """获取连接状态"""
        return {
            "name": self.name,
            "exchange": self.exchange,
            "status": self.status.value,
            "connected_since": self.connect_time.isoformat() if self.connect_time else None,
            "last_message": self.last_message_time.isoformat() if self.last_message_time else None,
            "message_count": self.message_count,
            "error_count": self.error_count,
            "active_subscriptions": len(self.active_subscriptions)
        }


class WebSocketClientManager:
    """WebSocket客户端管理器"""

    def __init__(self):
        # 连接管理
        self.connections: Dict[str, WebSocketConnection] = {}
        self.connection_tasks: Dict[str, asyncio.Task] = {}
        
        # 订阅管理
        self.subscription_counter = 0
        
        # 全局消息处理器
        self.global_message_handler: Optional[Callable] = None
        
        # 监控任务
        self._monitor_task: Optional[asyncio.Task] = None
        self._is_running = False
        
        # 重连管理
        self.reconnect_enabled = True
        self.reconnect_interval = 30
        
        logger.info("WebSocket客户端管理器初始化完成")

    async def start(self):
        """启动管理器"""
        if self._is_running:
            return
            
        self._is_running = True
        self._monitor_task = asyncio.create_task(self._monitor_connections())
        
        logger.info("WebSocket客户端管理器已启动")

    async def stop(self):
        """停止管理器"""
        self._is_running = False
        
        # 停止监控任务
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        # 断开所有连接
        for connection in self.connections.values():
            await connection.disconnect()
            
        # 取消所有任务
        for task in self.connection_tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self.connections.clear()
        self.connection_tasks.clear()
        
        logger.info("WebSocket客户端管理器已停止")

    async def add_connection(
        self,
        name: str,
        url: str,
        exchange: str,
        message_handler: Optional[Callable] = None
    ) -> bool:
        """添加连接"""
        try:
            if name in self.connections:
                logger.warning(f"连接已存在: {name}")
                return False
                
            connection = WebSocketConnection(
                name=name,
                url=url,
                exchange=exchange,
                message_handler=message_handler or self.global_message_handler
            )
            
            self.connections[name] = connection
            
            # 启动连接监听
            self.connection_tasks[name] = asyncio.create_task(
                self._handle_connection(name, connection)
            )
            
            logger.info(f"WebSocket连接已添加: {name}")
            return True
            
        except Exception as e:
            logger.error(f"添加WebSocket连接失败 {name}: {e}")
            return False

    async def remove_connection(self, name: str) -> bool:
        """移除连接"""
        try:
            if name not in self.connections:
                return False
                
            # 停止连接任务
            if name in self.connection_tasks:
                self.connection_tasks[name].cancel()
                try:
                    await self.connection_tasks[name]
                except asyncio.CancelledError:
                    pass
                del self.connection_tasks[name]
            
            # 断开连接
            connection = self.connections[name]
            await connection.disconnect()
            
            # 移除连接
            del self.connections[name]
            
            logger.info(f"WebSocket连接已移除: {name}")
            return True
            
        except Exception as e:
            logger.error(f"移除WebSocket连接失败 {name}: {e}")
            return False

    async def subscribe(
        self,
        connection_name: str,
        symbols: List[str],
        channels: List[str],
        callback: Optional[Callable] = None,
        **kwargs
    ) -> Optional[str]:
        """订阅数据"""
        try:
            connection = self.connections.get(connection_name)
            if not connection:
                raise WebSocketError(f"连接不存在: {connection_name}")
                
            subscription_id = f"sub_{self.subscription_counter}"
            self.subscription_counter += 1
            
            config = SubscriptionConfig(
                symbols=symbols,
                channels=channels,
                callback=callback,
                **kwargs
            )
            
            connection.subscriptions[subscription_id] = config
            
            # 发送订阅消息到交易所
            await self._send_subscription_message(connection, symbols, channels)
            
            # 标记为活跃订阅
            for symbol in symbols:
                for channel in channels:
                    subscription_key = f"{channel}:{symbol}"
                    connection.active_subscriptions.add(subscription_key)
            
            logger.info(f"订阅成功: {connection_name} - {symbols} - {channels}")
            return subscription_id
            
        except Exception as e:
            logger.error(f"订阅失败 {connection_name}: {e}")
            raise WebSocketError(f"订阅失败: {e}")

    async def unsubscribe(
        self,
        connection_name: str,
        subscription_id: str
    ) -> bool:
        """取消订阅"""
        try:
            connection = self.connections.get(connection_name)
            if not connection or subscription_id not in connection.subscriptions:
                return False
                
            config = connection.subscriptions[subscription_id]
            
            # 发送取消订阅消息到交易所
            await self._send_unsubscription_message(connection, config.symbols, config.channels)
            
            # 移除活跃订阅标记
            for symbol in config.symbols:
                for channel in config.channels:
                    subscription_key = f"{channel}:{symbol}"
                    connection.active_subscriptions.discard(subscription_key)
            
            # 移除订阅配置
            del connection.subscriptions[subscription_id]
            
            logger.info(f"取消订阅成功: {connection_name} - {subscription_id}")
            return True
            
        except Exception as e:
            logger.error(f"取消订阅失败 {connection_name}: {e}")
            return False

    async def _send_subscription_message(
        self,
        connection: WebSocketConnection,
        symbols: List[str],
        channels: List[str]
    ):
        """发送订阅消息"""
        try:
            if connection.exchange == "binance":
                for channel in channels:
                    for symbol in symbols:
                        await connection.send_message({
                            "method": "SUBSCRIBE",
                            "params": [f"{symbol.lower()}@{channel}"],
                            "id": int(time.time())
                        })
                        
            elif connection.exchange == "okx":
                params = []
                for channel in channels:
                    for symbol in symbols:
                        if channel == "ticker":
                            params.append(f"{channel}:{symbol}")
                        elif channel == "trades":
                            params.append(f"{channel}:{symbol}")
                        elif channel == "kline":
                            params.append(f"{channel}:{symbol}:1m")  # 默认1分钟
                            
                await connection.send_message({
                    "op": "subscribe",
                    "args": params
                })
                
            elif connection.exchange == "bybit":
                for channel in channels:
                    if channel == "orderbook":
                        # Bybit需要指定深度
                        for symbol in symbols:
                            await connection.send_message({
                                "op": "subscribe",
                                "args": [f"orderBook.50.{symbol}"]
                            })
                    elif channel == "tickers":
                        for symbol in symbols:
                            await connection.send_message({
                                "op": "subscribe",
                                "args": [f"tickers.{symbol}"]
                            })
                            
        except Exception as e:
            logger.error(f"发送订阅消息失败 {connection.name}: {e}")
            raise

    async def _send_unsubscription_message(
        self,
        connection: WebSocketConnection,
        symbols: List[str],
        channels: List[str]
    ):
        """发送取消订阅消息"""
        try:
            if connection.exchange == "binance":
                for channel in channels:
                    for symbol in symbols:
                        await connection.send_message({
                            "method": "UNSUBSCRIBE",
                            "params": [f"{symbol.lower()}@{channel}"],
                            "id": int(time.time())
                        })
                        
            elif connection.exchange == "okx":
                params = []
                for channel in channels:
                    for symbol in symbols:
                        if channel == "ticker":
                            params.append(f"{channel}:{symbol}")
                        elif channel == "trades":
                            params.append(f"{channel}:{symbol}")
                            
                await connection.send_message({
                    "op": "unsubscribe",
                    "args": params
                })
                
        except Exception as e:
            logger.error(f"发送取消订阅消息失败 {connection.name}: {e}")
            raise

    async def _handle_connection(self, name: str, connection: WebSocketConnection):
        """处理连接生命周期"""
        while self._is_running:
            try:
                if connection.status == ConnectionStatus.DISCONNECTED:
                    # 尝试连接
                    if await connection.connect():
                        # 重新订阅
                        await self._resubscribe_all(connection)
                    else:
                        # 连接失败，等待重连
                        if self.reconnect_enabled:
                            await asyncio.sleep(connection.reconnect_delay)
                        else:
                            break
                            
                elif connection.status == ConnectionStatus.CONNECTED:
                    # 监听消息
                    await connection.listen()
                    
                elif connection.status == ConnectionStatus.FAILED:
                    # 失败状态，尝试重连
                    if self.reconnect_enabled:
                        connection.status = ConnectionStatus.RECONNECTING
                        await asyncio.sleep(connection.reconnect_delay)
                    else:
                        break
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"连接处理异常 {name}: {e}")
                await asyncio.sleep(5)

    async def _resubscribe_all(self, connection: WebSocketConnection):
        """重新订阅所有数据"""
        try:
            for config in connection.subscriptions.values():
                await self._send_subscription_message(
                    connection, config.symbols, config.channels
                )
                
                # 标记为活跃订阅
                for symbol in config.symbols:
                    for channel in config.channels:
                        subscription_key = f"{channel}:{symbol}"
                        connection.active_subscriptions.add(subscription_key)
                        
        except Exception as e:
            logger.error(f"重新订阅失败 {connection.name}: {e}")

    async def _monitor_connections(self):
        """监控连接状态"""
        while self._is_running:
            try:
                await self._check_connection_health()
                await self._cleanup_inactive_connections()
                await asyncio.sleep(30)  # 每30秒检查一次
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"连接监控异常: {e}")
                await asyncio.sleep(10)

    async def _check_connection_health(self):
        """检查连接健康状态"""
        for name, connection in self.connections.items():
            try:
                # 检查连接是否超时
                if (connection.status == ConnectionStatus.CONNECTED and
                    connection.last_message_time):
                    
                    time_since_last_message = (
                        datetime.utcnow() - connection.last_message_time
                    ).total_seconds()
                    
                    # 如果超过5分钟没有消息，认为连接异常
                    if time_since_last_message > 300:
                        logger.warning(f"连接可能已断开: {name}")
                        connection.status = ConnectionStatus.FAILED
                        
                # 检查错误数量
                if connection.error_count > 100:
                    logger.warning(f"连接错误过多: {name} ({connection.error_count})")
                    connection.status = ConnectionStatus.FAILED
                    
            except Exception as e:
                logger.error(f"检查连接健康状态失败 {name}: {e}")

    async def _cleanup_inactive_connections(self):
        """清理非活跃连接"""
        cutoff_time = datetime.utcnow() - timedelta(minutes=10)
        
        for name, connection in list(self.connections.items()):
            try:
                if (connection.status == ConnectionStatus.DISCONNECTED and
                    connection.connect_time and
                    connection.connect_time < cutoff_time):
                    
                    logger.info(f"清理非活跃连接: {name}")
                    await self.remove_connection(name)
                    
            except Exception as e:
                logger.error(f"清理连接失败 {name}: {e}")

    def get_connection_status(self, name: str) -> Optional[dict]:
        """获取连接状态"""
        connection = self.connections.get(name)
        if connection:
            return connection.get_status()
        return None

    def get_all_connections_status(self) -> List[dict]:
        """获取所有连接状态"""
        return [conn.get_status() for conn in self.connections.values()]

    def set_global_message_handler(self, handler: Callable):
        """设置全局消息处理器"""
        self.global_message_handler = handler


# 期货WebSocket连接管理
class FuturesWebSocketManager:
    """期货WebSocket管理器"""

    def __init__(self, client_manager: WebSocketClientManager):
        self.client_manager = client_manager
        self.futures_connections: Dict[str, WebSocketConnection] = {}
        
        logger.info("期货WebSocket管理器初始化完成")

    async def connect_futures(
        self,
        exchange: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None
    ) -> bool:
        """连接期货WebSocket"""
        try:
            connection_name = f"futures_{exchange}"
            
            # 设置连接URL（这里需要根据实际交易所API文档调整）
            if exchange == "binance":
                url = "wss://fstream.binance.com/ws/"
            elif exchange == "okx":
                url = "wss://ws.okx.com:8443/ws/v5/public"
            elif exchange == "bybit":
                url = "wss://stream.bybit.com/v5/public/linear"
            else:
                raise WebSocketError(f"不支持的期货交易所: {exchange}")
            
            # 添加连接
            success = await self.client_manager.add_connection(
                name=connection_name,
                url=url,
                exchange=exchange,
                message_handler=self._handle_futures_message
            )
            
            if success:
                self.futures_connections[exchange] = self.client_manager.connections[connection_name]
                
            return success
            
        except Exception as e:
            logger.error(f"连接期货WebSocket失败: {e}")
            return False

    async def subscribe_futures_data(
        self,
        exchange: str,
        symbols: List[str],
        channels: List[str]
    ) -> Optional[str]:
        """订阅期货数据"""
        try:
            connection_name = f"futures_{exchange}"
            return await self.client_manager.subscribe(
                connection_name=connection_name,
                symbols=symbols,
                channels=channels,
                callback=self._handle_futures_data
            )
            
        except Exception as e:
            logger.error(f"订阅期货数据失败: {e}")
            return None

    async def unsubscribe_futures_data(
        self,
        exchange: str,
        subscription_id: str
    ) -> bool:
        """取消期货数据订阅"""
        try:
            connection_name = f"futures_{exchange}"
            return await self.client_manager.unsubscribe(
                connection_name=connection_name,
                subscription_id=subscription_id
            )
            
        except Exception as e:
            logger.error(f"取消期货数据订阅失败: {e}")
            return False

    async def _handle_futures_message(self, subscription_key: str, data: Any):
        """处理期货消息"""
        try:
            logger.debug(f"期货消息: {subscription_key}")
            # 这里可以添加期货特定的逻辑
            
        except Exception as e:
            logger.error(f"处理期货消息失败: {e}")

    async def _handle_futures_data(self, data: Any):
        """处理期货数据"""
        try:
            logger.debug(f"期货数据: {data}")
            # 这里可以添加期货数据处理逻辑
            
        except Exception as e:
            logger.error(f"处理期货数据失败: {e}")

    async def get_futures_funding_rate(self, exchange: str, symbol: str) -> Optional[float]:
        """获取期货资金费率"""
        try:
            connection = self.futures_connections.get(exchange)
            if not connection:
                return None
                
            # 这里需要实现具体的资金费率获取逻辑
            # 可能需要发送特定的订阅请求
            
            return None
            
        except Exception as e:
            logger.error(f"获取期货资金费率失败: {e}")
            return None

    async def subscribe_funding_rates(
        self,
        exchange: str,
        symbols: List[str]
    ) -> Optional[str]:
        """订阅资金费率"""
        try:
            return await self.subscribe_futures_data(
                exchange=exchange,
                symbols=symbols,
                channels=["funding_rate"]
            )
            
        except Exception as e:
            logger.error(f"订阅资金费率失败: {e}")
            return None

    async def subscribe_positions(
        self,
        exchange: str,
        symbols: List[str]
    ) -> Optional[str]:
        """订阅持仓数据"""
        try:
            return await self.subscribe_futures_data(
                exchange=exchange,
                symbols=symbols,
                channels=["position", "order"]
            )
            
        except Exception as e:
            logger.error(f"订阅持仓数据失败: {e}")
            return None

    async def disconnect_futures(self, exchange: str) -> bool:
        """断开期货连接"""
        try:
            connection_name = f"futures_{exchange}"
            success = await self.client_manager.remove_connection(connection_name)
            
            if success and exchange in self.futures_connections:
                del self.futures_connections[exchange]
                
            return success
            
        except Exception as e:
            logger.error(f"断开期货连接失败: {e}")
            return False

    def get_futures_status(self, exchange: str) -> Optional[dict]:
        """获取期货连接状态"""
        connection_name = f"futures_{exchange}"
        return self.client_manager.get_connection_status(connection_name)


# 全局WebSocket客户端管理器实例
_client_manager: Optional[WebSocketClientManager] = None


async def get_websocket_client_manager() -> WebSocketClientManager:
    """获取全局WebSocket客户端管理器实例"""
    global _client_manager

    if _client_manager is None:
        _client_manager = WebSocketClientManager()
        await _client_manager.start()

    return _client_manager


async def shutdown_websocket_client_manager():
    """关闭WebSocket客户端管理器"""
    global _client_manager

    if _client_manager:
        await _client_manager.stop()
        _client_manager = None


if __name__ == "__main__":
    async def test_websocket_manager():
        """测试WebSocket管理器"""
        manager = WebSocketClientManager()
        
        try:
            await manager.start()
            
            # 添加币安测试连接
            success = await manager.add_connection(
                name="binance_test",
                url="wss://stream.binance.com:9443/ws/btcusdt@ticker",
                exchange="binance"
            )
            
            if success:
                # 订阅数据
                sub_id = await manager.subscribe(
                    connection_name="binance_test",
                    symbols=["BTCUSDT"],
                    channels=["ticker"],
                    callback=lambda data: print(f"收到数据: {data}")
                )
                
                print(f"订阅ID: {sub_id}")
                
                # 运行一段时间
                await asyncio.sleep(30)
                
                # 取消订阅
                if sub_id:
                    await manager.unsubscribe("binance_test", sub_id)
                    
        finally:
            await manager.stop()

    # 运行测试
    asyncio.run(test_websocket_manager())