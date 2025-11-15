"""
合约交易策略基础类

提供合约交易策略的基础框架，包括：
- 订单管理和执行
- 风险控制
- 性能监控
- 策略状态管理
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass, field
import uuid


# ===== 枚举类型定义 =====


class OrderType(Enum):
    """订单类型"""
    MARKET = "market"  # 市价单
    LIMIT = "limit"  # 限价单
    STOP = "stop"  # 止损单
    STOP_LIMIT = "stop_limit"  # 止损限价单
    TAKE_PROFIT = "take_profit"  # 止盈单
    TAKE_PROFIT_LIMIT = "take_profit_limit"  # 止盈限价单


class OrderSide(Enum):
    """订单方向"""
    BUY = "buy"  # 买入
    SELL = "sell"  # 卖出


class PositionSide(Enum):
    """持仓方向"""
    LONG = "long"  # 多头
    SHORT = "short"  # 空头
    BOTH = "both"  # 双向


class OrderStatus(Enum):
    """订单状态"""
    CREATED = "created"  # 已创建
    SUBMITTED = "submitted"  # 已提交
    PARTIALLY_FILLED = "partially_filled"  # 部分成交
    FILLED = "filled"  # 完全成交
    CANCELLED = "cancelled"  # 已取消
    REJECTED = "rejected"  # 已拒绝
    EXPIRED = "expired"  # 已过期


class OrderTimeInForce(Enum):
    """订单时间有效性"""
    GTC = "gtc"  # 成交为止
    IOC = "ioc"  # 立即成交或取消
    FOK = "fok"  # 全部成交或取消


class FuturesStrategyType(Enum):
    """合约策略类型"""
    SCALPING = "scalping"  # 剥头皮
    DAY_TRADING = "day_trading"  # 日内交易
    SWING_TRADING = "swing_trading"  # 摆动交易
    TREND_FOLLOWING = "trend_following"  # 趋势跟踪
    MEAN_REVERSION = "mean_reversion"  # 均值回归
    ARBITRAGE = "arbitrage"  # 套利
    GRID_TRADING = "grid_trading"  # 网格交易
    MARTINGALE = "martingale"  # 马丁格尔
    DCA = "dca"  # 定投策略
    CUSTOM = "custom"  # 自定义


class FuturesStrategyStatus(Enum):
    """策略状态"""
    CREATED = "created"  # 已创建
    INITIALIZING = "initializing"  # 初始化中
    RUNNING = "running"  # 运行中
    PAUSED = "paused"  # 已暂停
    STOPPED = "stopped"  # 已停止
    ERROR = "error"  # 错误状态
    COMPLETED = "completed"  # 已完成


class FuturesRiskLevel(Enum):
    """风险等级"""
    LOW = "low"  # 低风险
    MEDIUM = "medium"  # 中等风险
    HIGH = "high"  # 高风险
    CRITICAL = "critical"  # 极高风险


# ===== 异常类定义 =====


class ValidationException(Exception):
    """验证异常"""
    pass


class RiskManagementException(Exception):
    """风险管理异常"""
    pass


# ===== 数据模型定义 =====


@dataclass
class FuturesMarketData:
    """合约市场数据"""

    symbol: str  # 交易对
    current_price: Decimal  # 当前价格
    bid_price: Decimal  # 买入价
    ask_price: Decimal  # 卖出价
    spread: Decimal  # 价差
    volume_24h: Decimal  # 24小时成交量
    price_change_24h: Decimal  # 24小时价格变化
    price_change_percent_24h: Decimal  # 24小时价格变化百分比
    high_24h: Decimal  # 24小时最高价
    low_24h: Decimal  # 24小时最低价
    open_price: Decimal  # 开盘价
    funding_rate: Decimal  # 资金费率
    next_funding_time: datetime  # 下次资金费率结算时间
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def mid_price(self) -> Decimal:
        """中间价格"""
        return (self.bid_price + self.ask_price) / 2

    @property
    def price_volatility(self) -> Decimal:
        """价格波动率"""
        if self.open_price == 0:
            return Decimal("0")
        return abs(self.price_change_24h) / self.open_price


@dataclass
class FuturesPosition:
    """合约持仓信息"""

    symbol: str  # 交易对
    quantity: Decimal  # 持仓数量
    entry_price: Decimal  # 开仓价格
    current_price: Decimal  # 当前价格
    margin_used: Decimal  # 使用保证金
    unrealized_pnl: Decimal  # 未实现盈亏
    realized_pnl: Decimal = Decimal("0")  # 已实现盈亏
    liquidation_price: Decimal = Decimal("0")  # 强平价格
    position_side: PositionSide = PositionSide.BOTH  # 持仓方向
    leverage: Decimal = Decimal("1")  # 杠杆倍数
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def position_value(self) -> Decimal:
        """持仓价值"""
        return abs(self.quantity) * self.current_price

    @property
    def margin_level(self) -> Decimal:
        """保证金水平"""
        if self.margin_used == 0:
            return Decimal("inf")
        return self.position_value / self.margin_used

    @property
    def is_long(self) -> bool:
        """是否多头"""
        return self.quantity > 0

    @property
    def is_short(self) -> bool:
        """是否空头"""
        return self.quantity < 0

    @property
    def is_flat(self) -> bool:
        """是否空仓"""
        return self.quantity == 0

    def get_position_side_name(self) -> str:
        """获取持仓方向名称"""
        return self.position_side.value


@dataclass
class FuturesAccountBalance:
    """合约账户余额"""

    total_balance: Decimal  # 总余额
    available_balance: Decimal  # 可用余额
    used_margin: Decimal  # 已用保证金
    frozen_margin: Decimal = Decimal("0")  # 冻结保证金
    unrealized_pnl: Decimal = Decimal("0")  # 未实现盈亏
    realized_pnl: Decimal = Decimal("0")  # 已实现盈亏
    maintenance_margin: Decimal = Decimal("0")  # 维持保证金
    currency: str = "USDT"  # 币种
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def free_margin(self) -> Decimal:
        """空闲保证金"""
        return self.available_balance - self.frozen_margin

    @property
    def margin_level(self) -> Decimal:
        """保证金水平"""
        if self.used_margin == 0:
            return Decimal("inf")
        return self.total_balance / self.used_margin

    @property
    def total_margin(self) -> Decimal:
        """总保证金"""
        return self.used_margin + self.frozen_margin


@dataclass
class FuturesStrategyConfig:
    """合约策略配置"""

    strategy_id: str
    strategy_type: FuturesStrategyType
    user_id: str
    account_id: str
    symbol: str
    leverage: Decimal = Decimal("1")  # 杠杆倍数
    initial_balance: Decimal = Decimal("1000")  # 初始余额
    min_order_size: Decimal = Decimal("10")  # 最小订单大小
    max_order_size: Decimal = Decimal("1000")  # 最大订单大小
    max_position_size: Decimal = Decimal("100")  # 最大仓位大小
    max_leverage: Decimal = Decimal("10")  # 最大杠杆
    max_daily_loss: Decimal = Decimal("100")  # 最大日亏损
    max_drawdown: Decimal = Decimal("0.2")  # 最大回撤
    stop_loss_percent: Decimal = Decimal("0.05")  # 止损百分比
    take_profit_percent: Decimal = Decimal("0.1")  # 止盈百分比
    max_funding_rate: Decimal = Decimal("0.01")  # 最大资金费率
    risk_per_trade: Decimal = Decimal("0.02")  # 单次交易风险
    performance_check_interval: int = 60  # 性能检查间隔(秒)
    risk_check_interval: int = 30  # 风险检查间隔(秒)
    enable_trailing_stop: bool = True  # 启用追踪止损
    enable_partial_close: bool = True  # 启用分批平仓
    max_concurrent_orders: int = 5  # 最大并发订单数
    order_callback: Optional[Callable] = None  # 订单回调
    performance_callback: Optional[Callable] = None  # 性能回调
    risk_callback: Optional[Callable] = None  # 风险回调
    error_callback: Optional[Callable] = None  # 错误回调
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FuturesStrategyState:
    """合约策略状态"""

    strategy_id: str
    status: FuturesStrategyStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    stopped_at: Optional[datetime] = None
    last_error: Optional[str] = None
    error_count: int = 0
    
    # 交易统计
    total_orders: int = 0
    filled_orders: int = 0
    cancelled_orders: int = 0
    rejected_orders: int = 0
    
    # 盈亏统计
    total_profit: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    daily_pnl: Decimal = Decimal("0")
    max_daily_loss_reached: bool = False
    
    # 当前持仓
    current_position: FuturesPosition = field(
        default_factory=lambda: FuturesPosition(
            symbol="", quantity=Decimal("0"),
            entry_price=Decimal("0"), current_price=Decimal("0"),
            margin_used=Decimal("0"), unrealized_pnl=Decimal("0")
        )
    )
    
    # 账户信息
    account_balance: FuturesAccountBalance = field(
        default_factory=lambda: FuturesAccountBalance(
            total_balance=Decimal("0"), available_balance=Decimal("0"),
            used_margin=Decimal("0")
        )
    )
    
    # 风险指标
    max_drawdown: Decimal = Decimal("0")
    current_drawdown: Decimal = Decimal("0")
    margin_level: Decimal = Decimal("inf")
    liquidation_risk_level: FuturesRiskLevel = FuturesRiskLevel.LOW
    funding_rate_exposure: Decimal = Decimal("0")
    uptime_seconds: int = 0
    consecutive_losses: int = 0
    
    # 资金费率统计
    funding_rate_paid: Decimal = Decimal("0")
    funding_rate_received: Decimal = Decimal("0")
    
    # 性能指标
    win_rate: Decimal = Decimal("0")
    sharpe_ratio: Decimal = Decimal("0")
    profit_factor: Decimal = Decimal("0")
    
    def __post_init__(self):
        """初始化后的处理"""
        # 初始化账户余额
        if self.account_balance.total_balance == 0:
            self.account_balance = FuturesAccountBalance(
                total_balance=Decimal("1000"),
                available_balance=Decimal("1000"),
                used_margin=Decimal("0")
            )

    @property
    def success_rate(self) -> Decimal:
        """成功率"""
        if self.total_orders == 0:
            return Decimal("0")
        return Decimal(self.filled_orders) / Decimal(self.total_orders)

    @property
    def average_win(self) -> Decimal:
        """平均盈利"""
        winning_trades = max(self.filled_orders - self.consecutive_losses, 0)
        if winning_trades == 0:
            return Decimal("0")
        return self.realized_pnl / Decimal(winning_trades)

    @property
    def average_loss(self) -> Decimal:
        """平均亏损"""
        if self.consecutive_losses == 0:
            return Decimal("0")
        # 这里需要更复杂的计算，暂时设为总亏损的相反数
        return abs(self.total_profit - self.realized_pnl) / Decimal(self.consecutive_losses)

    def is_trading_allowed(self) -> bool:
        """是否允许交易"""
        return (
            self.status in [FuturesStrategyStatus.RUNNING]
            and not self.max_daily_loss_reached
            and self.margin_level > Decimal("1.1")  # 保证金水平高于110%
            and self.consecutive_losses < 10  # 连续亏损不超过10次
        )

    def should_stop_loss(self) -> bool:
        """是否应该止损"""
        return (
            self.current_drawdown > self.max_drawdown
            or self.margin_level < Decimal("1.15")  # 保证金水平低于115%
            or self.liquidation_risk_level == FuturesRiskLevel.CRITICAL
        )

    def should_reduce_position(self) -> bool:
        """是否应该减仓"""
        return (
            self.liquidation_risk_level in [FuturesRiskLevel.HIGH, FuturesRiskLevel.CRITICAL]
            or self.current_drawdown > self.max_drawdown * Decimal("0.8")
        )

    def update_performance_metrics(self):
        """更新性能指标"""
        now = datetime.now()
        
        if self.started_at:
            self.uptime_seconds = int((now - self.started_at).total_seconds())

        # 更新回撤
        if self.total_profit > 0:
            peak = max(self.total_profit, self.max_drawdown)
            if peak > 0:
                self.current_drawdown = (peak - self.total_profit) / peak
                self.max_drawdown = max(self.max_drawdown, self.current_drawdown)

        # 更新胜率
        if self.filled_orders > 0:
            self.win_rate = self.realized_pnl / Decimal(self.filled_orders)

        # 更新夏普比率和盈亏比（简化计算）
        if self.uptime_seconds > 0 and self.total_profit != 0:
            daily_return = self.total_profit / Decimal(self.uptime_seconds) * Decimal("86400")
            self.sharpe_ratio = daily_return / (abs(self.total_profit) + Decimal("1"))
            self.profit_factor = abs(self.total_profit) / (abs(self.total_profit - self.realized_pnl) + Decimal("1"))

    def update_risk_metrics(self, market_data: FuturesMarketData):
        """更新风险指标"""
        # 更新保证金水平
        if self.current_position.quantity != 0:
            self.margin_level = (
                self.current_position.position_value / self.current_position.margin_used
                if self.current_position.margin_used > 0
                else Decimal("inf")
            )

        # 更新强平风险
        self._update_liquidation_risk(market_data)

        # 更新资金费率风险
        self._update_funding_rate_risk(market_data)

    def _update_liquidation_risk(self, market_data: FuturesMarketData):
        """更新强平风险"""
        if self.current_position.quantity == 0:
            return

        liquidation_price = self.current_position.liquidation_price
        if liquidation_price == 0:
            return

        current_price = market_data.current_price

        # 计算距离强平价格的距离
        if self.current_position.quantity > 0:  # 多头
            distance = (current_price - liquidation_price) / current_price
        else:  # 空头
            distance = (liquidation_price - current_price) / current_price

        # 更新风险等级
        if distance < 0.05:  # 5%以内
            self.liquidation_risk_level = FuturesRiskLevel.CRITICAL
        elif distance < 0.1:  # 10%以内
            self.liquidation_risk_level = FuturesRiskLevel.HIGH
        elif distance < 0.2:  # 20%以内
            self.liquidation_risk_level = FuturesRiskLevel.MEDIUM
        else:
            self.liquidation_risk_level = FuturesRiskLevel.LOW

    def _update_funding_rate_risk(self, market_data: FuturesMarketData):
        """更新资金费率风险"""
        # 计算资金费率敞口
        if self.current_position.quantity != 0:
            position_value = (
                abs(self.current_position.quantity) * market_data.current_price
            )
            self.funding_rate_exposure = position_value * abs(market_data.funding_rate)

    def update_daily_stats(self):
        """更新日统计"""
        # 重置日统计（实际实现中需要根据时间间隔判断是否需要重置）
        current_date = datetime.now().date()
        # 这里应该根据具体需求实现日期重置逻辑
        pass

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "strategy_id": self.strategy_id,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "stopped_at": self.stopped_at.isoformat() if self.stopped_at else None,
            "total_orders": self.total_orders,
            "filled_orders": self.filled_orders,
            "success_rate": float(self.success_rate),
            "total_profit": float(self.total_profit),
            "realized_pnl": float(self.realized_pnl),
            "unrealized_pnl": float(self.unrealized_pnl),
            "current_position": {
                "symbol": self.current_position.symbol,
                "quantity": float(self.current_position.quantity),
                "side": self.current_position.get_position_side_name(),
                "margin_used": float(self.current_position.margin_used),
            },
            "max_drawdown": float(self.max_drawdown),
            "current_drawdown": float(self.current_drawdown),
            "daily_pnl": float(self.daily_pnl),
            "margin_level": float(self.margin_level),
            "liquidation_risk": self.liquidation_risk_level.value,
            "funding_rate_exposure": float(self.funding_rate_exposure),
            "uptime_seconds": self.uptime_seconds,
            "consecutive_losses": self.consecutive_losses,
            "is_trading_allowed": self.is_trading_allowed(),
            "should_stop_loss": self.should_stop_loss(),
            "should_reduce_position": self.should_reduce_position(),
        }


@dataclass
class FuturesOrderRequest:
    """合约订单请求"""

    order_id: str
    symbol: str
    order_type: OrderType
    order_side: OrderSide
    quantity: Decimal
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None  # 止损价格
    client_order_id: Optional[str] = None
    position_side: PositionSide = PositionSide.BOTH
    time_in_force: str = "GTC"  # GTC, IOC, FOK
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.order_id:
            self.order_id = str(uuid.uuid4())

        if self.quantity <= 0:
            raise ValidationException("订单数量必须大于0")

        if self.order_type == OrderType.LIMIT and (not self.price or self.price <= 0):
            raise ValidationException("限价单必须提供有效价格")

        if self.order_type == OrderType.STOP and (
            not self.stop_price or self.stop_price <= 0
        ):
            raise ValidationException("止损单必须提供有效止损价格")


@dataclass
class FuturesOrderResult:
    """合约订单执行结果"""

    success: bool
    order_id: str
    filled_quantity: Decimal
    average_price: Decimal
    commission: Decimal
    execution_time: datetime
    latency_ms: Optional[float] = None
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    exchange_order_id: Optional[str] = None
    fill_time: Optional[datetime] = None
    funding_rate: Optional[Decimal] = None  # 执行时的资金费率

    def __post_init__(self):
        if self.filled_quantity < 0:
            raise ValidationException("成交数量不能为负数")

        if self.average_price < 0:
            raise ValidationException("成交价格不能为负数")

        if self.commission < 0:
            raise ValidationException("手续费不能为负数")


# ===== 策略接口定义 =====


class FuturesStrategyInterface(ABC):
    """合约策略接口"""

    def __init__(self, config: FuturesStrategyConfig):
        self.config = config
        self.state = FuturesStrategyState(
            strategy_id=config.strategy_id,
            status=FuturesStrategyStatus.CREATED,
            created_at=datetime.now(),
        )
        self.order_manager = None  # Will be set after strategy initialization
        self.running_tasks: List[asyncio.Task] = []
        self.performance_history: List[Dict[str, Any]] = []
        self.order_history: List[FuturesOrderRequest] = []

    @abstractmethod
    async def initialize(self) -> bool:
        """初始化策略"""
        pass

    @abstractmethod
    async def start(self) -> bool:
        """启动策略"""
        pass

    @abstractmethod
    async def pause(self) -> bool:
        """暂停策略"""
        pass

    @abstractmethod
    async def resume(self) -> bool:
        """恢复策略"""
        pass

    @abstractmethod
    async def stop(self) -> bool:
        """停止策略"""
        pass

    @abstractmethod
    async def get_next_orders(
        self, market_data: FuturesMarketData
    ) -> List[FuturesOrderRequest]:
        """获取下一批订单"""
        pass

    @abstractmethod
    async def process_order_result(self, order_result: FuturesOrderResult) -> bool:
        """处理订单执行结果"""
        pass

    def get_state(self) -> FuturesStrategyState:
        """获取策略状态"""
        self.state.update_performance_metrics()
        return self.state

    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取性能指标"""
        return {
            "total_trades": self.state.total_orders,
            "successful_trades": self.state.filled_orders,
            "success_rate": float(self.state.success_rate),
            "win_rate": float(self.state.win_rate),
            "total_pnl": float(self.state.total_profit),
            "realized_pnl": float(self.state.realized_pnl),
            "unrealized_pnl": float(self.state.unrealized_pnl),
            "funding_rate_cost": float(self.state.funding_rate_paid),
            "max_drawdown": float(self.state.max_drawdown),
            "sharpe_ratio": float(self.state.sharpe_ratio),
            "profit_factor": float(self.state.profit_factor),
            "margin_level": float(self.state.margin_level),
            "liquidation_risk": self.state.liquidation_risk_level.value,
            "consecutive_losses": self.state.consecutive_losses,
        }


class BaseFuturesStrategy(FuturesStrategyInterface):
    """合约策略基类"""

    def __init__(
        self, config: FuturesStrategyConfig, order_manager: Optional[Any] = None
    ):
        super().__init__(config)
        self.order_manager = order_manager
        self.logger = logging.getLogger(
            f"futures_strategy.{config.strategy_type}.{config.strategy_id}"
        )

        # 性能监控
        self.performance_monitor_task: Optional[asyncio.Task] = None
        self.risk_monitor_task: Optional[asyncio.Task] = None

        # 市场数据缓存
        self.last_market_data: Optional[FuturesMarketData] = None
        self.market_data_history: List[FuturesMarketData] = []

        # 技术指标缓存
        self.technical_indicators: Dict[str, List[Decimal]] = {}

    async def initialize(self) -> bool:
        """初始化策略"""
        try:
            self.logger.info(f"初始化合约策略 {self.config.strategy_id}")
            self.state.status = FuturesStrategyStatus.INITIALIZING

            # 验证配置
            self._validate_config()

            # 初始化子类特定功能
            await self._initialize_specific()

            self.state.status = FuturesStrategyStatus.CREATED
            self.logger.info(f"合约策略 {self.config.strategy_id} 初始化完成")
            return True

        except Exception as e:
            self.logger.error(f"合约策略初始化失败: {e}")
            self.state.status = FuturesStrategyStatus.ERROR
            self.state.last_error = str(e)
            return False

    async def start(self) -> bool:
        """启动策略"""
        try:
            if self.state.status != FuturesStrategyStatus.CREATED:
                self.logger.warning(f"策略状态不正确: {self.state.status}")
                return False

            self.logger.info(f"启动合约策略 {self.config.strategy_id}")

            # 启动监控任务
            self._start_monitoring_tasks()

            # 启动子类特定功能
            await self._start_specific()

            self.state.status = FuturesStrategyStatus.RUNNING
            self.state.started_at = datetime.now()

            self.logger.info(f"合约策略 {self.config.strategy_id} 启动完成")
            return True

        except Exception as e:
            self.logger.error(f"合约策略启动失败: {e}")
            self.state.status = FuturesStrategyStatus.ERROR
            self.state.last_error = str(e)
            return False

    async def pause(self) -> bool:
        """暂停策略"""
        try:
            if self.state.status != FuturesStrategyStatus.RUNNING:
                return False

            self.logger.info(f"暂停合约策略 {self.config.strategy_id}")

            # 停止监控任务
            self._stop_monitoring_tasks()

            # 暂停子类特定功能
            await self._pause_specific()

            self.state.status = FuturesStrategyStatus.PAUSED

            self.logger.info(f"合约策略 {self.config.strategy_id} 暂停完成")
            return True

        except Exception as e:
            self.logger.error(f"合约策略暂停失败: {e}")
            return False

    async def resume(self) -> bool:
        """恢复策略"""
        try:
            if self.state.status != FuturesStrategyStatus.PAUSED:
                return False

            self.logger.info(f"恢复合约策略 {self.config.strategy_id}")

            # 恢复子类特定功能
            await self._resume_specific()

            # 重新启动监控
            self._start_monitoring_tasks()

            self.state.status = FuturesStrategyStatus.RUNNING

            self.logger.info(f"合约策略 {self.config.strategy_id} 恢复完成")
            return True

        except Exception as e:
            self.logger.error(f"合约策略恢复失败: {e}")
            return False

    async def stop(self) -> bool:
        """停止策略"""
        try:
            if self.state.status not in [
                FuturesStrategyStatus.RUNNING,
                FuturesStrategyStatus.PAUSED,
            ]:
                return False

            self.logger.info(f"停止合约策略 {self.config.strategy_id}")

            # 停止所有任务
            await self._stop_all_tasks()

            # 停止子类特定功能
            await self._stop_specific()

            self.state.status = FuturesStrategyStatus.STOPPED
            self.state.stopped_at = datetime.now()
            self.state.update_performance_metrics()

            self.logger.info(f"合约策略 {self.config.strategy_id} 停止完成")
            return True

        except Exception as e:
            self.logger.error(f"合约策略停止失败: {e}")
            return False

    async def process_market_data(self, market_data: FuturesMarketData):
        """处理市场数据"""
        try:
            # 缓存市场数据
            self.last_market_data = market_data
            self.market_data_history.append(market_data)

            # 保持历史数据在合理范围内
            if len(self.market_data_history) > 200:
                self.market_data_history = self.market_data_history[-100:]

            # 更新风险指标
            self.state.update_risk_metrics(market_data)

            # 检查是否可以交易
            if not self.state.is_trading_allowed():
                return

            # 检查风险条件
            if self.state.should_stop_loss():
                self.logger.warning("达到止损条件，停止策略")
                await self.stop()
                return

            # 检查是否应该减仓
            if self.state.should_reduce_position():
                self.logger.info("风险较高，考虑减仓")

            # 获取订单建议
            orders = await self.get_next_orders(market_data)

            # 执行订单
            for order_request in orders:
                await self._execute_order(order_request)

        except Exception as e:
            self.logger.error(f"处理市场数据失败: {e}")
            self.state.error_count += 1
            self.state.last_error = str(e)

    async def _execute_order(self, order_request: FuturesOrderRequest) -> bool:
        """执行订单"""
        try:
            if not self.order_manager:
                self.logger.warning("订单管理器未设置，跳过订单执行")
                return False

            # 创建订单
            order = await self.order_manager.create_order(
                user_id=self.config.user_id,
                account_id=self.config.account_id,
                symbol=order_request.symbol,
                order_side=order_request.order_side,
                quantity=order_request.quantity,
                order_type=order_request.order_type,
                price=order_request.price,
                stop_price=order_request.stop_price,
                position_side=order_request.position_side,
                client_order_id=order_request.client_order_id,
            )

            # 执行订单
            success = await self.order_manager.execute_order(
                order_id=order.id,
                user_id=self.config.user_id,
                account_id=self.config.account_id,
                current_price=(
                    self.last_market_data.current_price
                    if self.last_market_data
                    else None
                ),
            )

            # 处理结果
            order_result = FuturesOrderResult(
                success=success,
                order_id=order_request.order_id,
                filled_quantity=order.quantity_filled,
                average_price=order.average_price or Decimal("0"),
                commission=order.commission or Decimal("0"),
                execution_time=datetime.now(),
                funding_rate=(
                    self.last_market_data.funding_rate
                    if self.last_market_data
                    else None
                ),
            )

            await self.process_order_result(order_result)

            # 触发回调
            if self.config.order_callback:
                await self._safe_callback(self.config.order_callback, order_result)

            return success

        except Exception as e:
            self.logger.error(f"订单执行失败: {e}")
            self.state.error_count += 1
            self.state.last_error = str(e)

            # 触发错误回调
            if self.config.error_callback:
                await self._safe_callback(self.config.error_callback, str(e))

            return False

    async def _safe_callback(self, callback: Callable, *args):
        """安全执行回调函数"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(*args)
            else:
                callback(*args)
        except Exception as e:
            self.logger.error(f"回调函数执行失败: {e}")

    def _start_monitoring_tasks(self):
        """启动监控任务"""
        # 性能监控
        self.performance_monitor_task = asyncio.create_task(
            self._performance_monitoring_loop()
        )

        # 风险监控
        self.risk_monitor_task = asyncio.create_task(self._risk_monitoring_loop())

        self.running_tasks.extend(
            [self.performance_monitor_task, self.risk_monitor_task]
        )

    def _stop_monitoring_tasks(self):
        """停止监控任务"""
        for task in [self.performance_monitor_task, self.risk_monitor_task]:
            if task and not task.done():
                task.cancel()
                self.running_tasks.remove(task)

    async def _stop_all_tasks(self):
        """停止所有任务"""
        # 取消所有运行中的任务
        for task in self.running_tasks:
            if not task.done():
                task.cancel()

        # 等待任务完成
        if self.running_tasks:
            await asyncio.gather(*self.running_tasks, return_exceptions=True)

        self.running_tasks.clear()
        self.performance_monitor_task = None
        self.risk_monitor_task = None

    async def _performance_monitoring_loop(self):
        """性能监控循环"""
        while self.state.status == FuturesStrategyStatus.RUNNING:
            try:
                await asyncio.sleep(self.config.performance_check_interval)

                # 更新性能统计
                self.state.update_performance_metrics()

                # 检查性能回调
                if self.config.performance_callback:
                    await self._safe_callback(
                        self.config.performance_callback, self.get_performance_metrics()
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"性能监控错误: {e}")

    async def _risk_monitoring_loop(self):
        """风险监控循环"""
        while self.state.status == FuturesStrategyStatus.RUNNING:
            try:
                await asyncio.sleep(self.config.risk_check_interval)

                # 检查保证金水平
                if self.state.margin_level < Decimal("1.15"):  # 低于115%
                    self.logger.warning(f"保证金水平过低: {self.state.margin_level}")

                    # 触发风险回调
                    if self.config.risk_callback:
                        await self._safe_callback(
                            self.config.risk_callback,
                            {
                                "margin_level": self.state.margin_level,
                                "liquidation_risk": self.state.liquidation_risk_level.value,
                                "action": "margin_low",
                            },
                        )

                # 检查资金费率风险
                if (
                    self.last_market_data
                    and abs(self.last_market_data.funding_rate)
                    > self.config.max_funding_rate
                ):
                    self.logger.warning(
                        f"资金费率过高: {self.last_market_data.funding_rate}"
                    )

                # 检查日亏损限制
                if self.state.daily_pnl <= -self.config.max_daily_loss:
                    self.state.max_daily_loss_reached = True
                    self.logger.warning(f"达到日亏损限制: {self.state.daily_pnl}")

                # 检查连续亏损
                if self.state.consecutive_losses >= 10:
                    self.logger.warning("连续亏损次数过多，考虑暂停策略")

                # 更新日统计
                self.state.update_daily_stats()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"风险监控错误: {e}")

    def _validate_config(self):
        """验证配置"""
        if not self.config.user_id or not self.config.account_id:
            raise ValidationException("用户ID和账户ID不能为空")

        if self.config.min_order_size > self.config.max_order_size:
            raise ValidationException("最小订单大小不能大于最大订单大小")

        if self.config.leverage > self.config.max_leverage:
            raise ValidationException(
                f"杠杆倍数不能超过最大杠杆: {self.config.max_leverage}"
            )

    async def _initialize_specific(self):
        """初始化子类特定功能"""
        pass

    async def _start_specific(self):
        """启动子类特定功能"""
        pass

    async def _pause_specific(self):
        """暂停子类特定功能"""
        pass

    async def _resume_specific(self):
        """恢复子类特定功能"""
        pass

    async def _stop_specific(self):
        """停止子类特定功能"""
        pass

    # 抽象方法实现
    @abstractmethod
    async def get_next_orders(
        self, market_data: FuturesMarketData
    ) -> List[FuturesOrderRequest]:
        """获取下一批订单"""
        pass

    @abstractmethod
    async def process_order_result(self, order_result: FuturesOrderResult) -> bool:
        """处理订单执行结果"""
        pass