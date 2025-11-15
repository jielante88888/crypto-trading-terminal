"""
持仓管理和追踪系统
负责管理所有交易持仓的状态、风险和操作
"""

import asyncio
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Callable, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict, deque
import decimal

import structlog
import numpy as np

from ..adapters.base import Position, Order, MarketData
from ..utils.exceptions import PositionManagerError

logger = structlog.get_logger(__name__)


class PositionStatus(Enum):
    """持仓状态"""
    OPEN = "open"
    CLOSED = "closed"
    PENDING = "pending"
    LIQUIDATED = "liquidated"
    HEDGED = "hedged"


class PositionAction(Enum):
    """持仓操作"""
    OPEN = "open"
    CLOSE = "close"
    ADD = "add"
    REDUCE = "reduce"
    HEDGE = "hedge"
    ADJUST = "adjust"


class PositionAlertType(Enum):
    """持仓预警类型"""
    PROFIT_TARGET = "profit_target"
    STOP_LOSS = "stop_loss"
    RISK_WARNING = "risk_warning"
    MARGIN_CALL = "margin_call"
    LIQUIDATION_RISK = "liquidation_risk"
    POSITION_SIZE = "position_size"


@dataclass
class PositionAlert:
    """持仓预警"""
    id: str
    type: PositionAlertType
    message: str
    severity: str  # info, warning, error, critical
    position_id: str
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    acknowledged: bool = False


@dataclass
class PositionSnapshot:
    """持仓快照"""
    position_id: str
    symbol: str
    side: str
    size: float
    entry_price: float
    current_price: float
    unrealized_pnl: float
    margin_used: float
    leverage: float
    timestamp: datetime


class PositionTrackingService:
    """持仓追踪服务"""

    def __init__(self):
        # 持仓存储
        self.positions: Dict[str, Position] = {}
        self.position_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        
        # 预警系统
        self.alerts: List[PositionAlert] = []
        self.alert_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=500))
        self.alert_callbacks: List[Callable] = []
        
        # 监控任务
        self._tracking_task: Optional[asyncio.Task] = None
        self._is_running = False
        
        # 风险参数
        self.max_position_size = 10000.0
        self.max_daily_loss = 1000.0
        self.default_stop_loss = 0.02  # 2%
        self.default_take_profit = 0.04  # 4%
        
        # 价格缓存
        self.price_cache: Dict[str, float] = {}
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        logger.info("持仓追踪服务初始化完成")

    async def start_tracking(self):
        """启动持仓追踪"""
        if self._is_running:
            return
            
        self._is_running = True
        self._tracking_task = asyncio.create_task(self._tracking_loop())
        
        logger.info("持仓追踪服务已启动")

    async def stop_tracking(self):
        """停止持仓追踪"""
        self._is_running = False
        
        if self._tracking_task:
            self._tracking_task.cancel()
            try:
                await self._tracking_task
            except asyncio.CancelledError:
                pass
                
        logger.info("持仓追踪服务已停止")

    async def _tracking_loop(self):
        """追踪循环"""
        while self._is_running:
            try:
                await self._update_positions()
                await self._check_alerts()
                await self._cleanup_old_data()
                await asyncio.sleep(1)  # 每秒更新一次
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"追踪循环异常: {e}")
                await asyncio.sleep(5)

    async def _update_positions(self):
        """更新持仓状态"""
        for position in list(self.positions.values()):
            try:
                await self._update_single_position(position)
            except Exception as e:
                logger.error(f"更新持仓失败 {position.id}: {e}")

    async def _update_single_position(self, position: Position):
        """更新单个持仓"""
        try:
            # 获取当前价格
            current_price = await self._get_current_price(position.symbol)
            if current_price:
                # 更新未实现盈亏
                await self._update_pnl(position, current_price)
                
                # 检查是否需要触发预警
                await self._check_position_alerts(position, current_price)
                
        except Exception as e:
            logger.error(f"更新持仓状态失败 {position.id}: {e}")

    async def _update_pnl(self, position: Position, current_price: float):
        """更新未实现盈亏"""
        try:
            if position.side == "LONG":
                pnl = (current_price - position.entry_price) * position.size
            else:  # SHORT
                pnl = (position.entry_price - current_price) * position.size
                
            position.unrealized_pnl = pnl
            position.mark_price = current_price
            position.updated_at = datetime.utcnow()
            
            # 记录快照
            snapshot = PositionSnapshot(
                position_id=position.id,
                symbol=position.symbol,
                side=position.side,
                size=position.size,
                entry_price=position.entry_price,
                current_price=current_price,
                unrealized_pnl=pnl,
                margin_used=position.margin_used,
                leverage=position.leverage,
                timestamp=datetime.utcnow()
            )
            
            self.position_history[position.id].append(snapshot)
            
        except Exception as e:
            logger.error(f"更新盈亏失败 {position.id}: {e}")

    async def _check_position_alerts(self, position: Position, current_price: float):
        """检查持仓预警"""
        try:
            # 检查止损
            if position.stop_loss and current_price <= position.stop_loss:
                await self._create_alert(
                    position.id,
                    PositionAlertType.STOP_LOSS,
                    f"止损被触发: {position.symbol}",
                    "warning",
                    {
                        "current_price": current_price,
                        "stop_loss": position.stop_loss,
                        "unrealized_pnl": position.unrealized_pnl
                    }
                )
            
            # 检查止盈
            if position.take_profit and current_price >= position.take_profit:
                await self._create_alert(
                    position.id,
                    PositionAlertType.PROFIT_TARGET,
                    f"止盈被触发: {position.symbol}",
                    "info",
                    {
                        "current_price": current_price,
                        "take_profit": position.take_profit,
                        "unrealized_pnl": position.unrealized_pnl
                    }
                )
            
            # 检查持仓大小
            if abs(position.size) > self.max_position_size:
                await self._create_alert(
                    position.id,
                    PositionAlertType.POSITION_SIZE,
                    f"持仓大小超过限制: {position.symbol}",
                    "warning",
                    {
                        "position_size": position.size,
                        "max_size": self.max_position_size
                    }
                )
            
            # 检查风险预警
            if position.leverage > 10:  # 高杠杆预警
                await self._create_alert(
                    position.id,
                    PositionAlertType.RISK_WARNING,
                    f"高杠杆风险: {position.symbol} ({position.leverage}x)",
                    "warning",
                    {
                        "leverage": position.leverage,
                        "margin_used": position.margin_used
                    }
                )
                
        except Exception as e:
            logger.error(f"检查持仓预警失败 {position.id}: {e}")

    async def _check_alerts(self):
        """检查所有预警"""
        try:
            # 检查未读预警
            unread_alerts = [alert for alert in self.alerts if not alert.acknowledged]
            
            if unread_alerts:
                await self._notify_alerts(unread_alerts)
                
        except Exception as e:
            logger.error(f"检查预警失败: {e}")

    async def _cleanup_old_data(self):
        """清理旧数据"""
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            
            # 清理旧的预警
            self.alerts = [alert for alert in self.alerts if alert.timestamp > cutoff_time]
            
            # 清理旧的价格历史
            for symbol in list(self.price_history.keys()):
                while (self.price_history[symbol] and 
                       self.price_history[symbol][0].timestamp < cutoff_time):
                    self.price_history[symbol].popleft()
                    
        except Exception as e:
            logger.error(f"清理旧数据失败: {e}")

    async def _get_current_price(self, symbol: str) -> Optional[float]:
        """获取当前价格"""
        return self.price_cache.get(symbol)

    async def _create_alert(
        self,
        position_id: str,
        alert_type: PositionAlertType,
        message: str,
        severity: str,
        data: Dict[str, Any]
    ):
        """创建预警"""
        try:
            alert = PositionAlert(
                id=str(uuid.uuid4()),
                type=alert_type,
                message=message,
                severity=severity,
                position_id=position_id,
                data=data
            )
            
            self.alerts.append(alert)
            self.alert_history[position_id].append(alert)
            
            logger.info(f"创建预警: {message}", extra={"alert": asdict(alert)})
            
        except Exception as e:
            logger.error(f"创建预警失败: {e}")

    async def _notify_alerts(self, alerts: List[PositionAlert]):
        """通知预警"""
        for callback in self.alert_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(alerts)
                else:
                    callback(alerts)
            except Exception as e:
                logger.error(f"预警回调执行失败: {e}")

    # 公共API方法
    
    async def get_positions(self, status: Optional[PositionStatus] = None) -> List[Position]:
        """获取持仓列表"""
        positions = list(self.positions.values())
        
        if status:
            positions = [p for p in positions if p.status == status]
            
        return positions

    async def get_position(self, position_id: str) -> Optional[Position]:
        """获取单个持仓"""
        return self.positions.get(position_id)

    async def update_position_price(self, symbol: str, price: float):
        """更新持仓价格"""
        self.price_cache[symbol] = price
        self.price_history[symbol].append({
            "price": price,
            "timestamp": datetime.utcnow()
        })

    async def close_position(
        self,
        position_id: str,
        close_price: Optional[float] = None,
        reason: str = "manual"
    ) -> bool:
        """平仓"""
        try:
            position = self.positions.get(position_id)
            if not position:
                logger.warning(f"持仓不存在: {position_id}")
                return False
                
            if close_price:
                position.exit_price = close_price
                await self._update_pnl(position, close_price)
                
            position.status = PositionStatus.CLOSED
            position.closed_at = datetime.utcnow()
            position.close_reason = reason
            
            # 创建预警
            await self._create_alert(
                position_id,
                PositionAlertType.PROFIT_TARGET,
                f"持仓已平仓: {position.symbol}",
                "info",
                {
                    "exit_price": position.exit_price,
                    "realized_pnl": position.realized_pnl,
                    "reason": reason
                }
            )
            
            logger.info(f"持仓已平仓: {position_id}")
            return True
            
        except Exception as e:
            logger.error(f"平仓失败 {position_id}: {e}")
            return False

    async def add_position(
        self,
        symbol: str,
        side: str,
        size: float,
        entry_price: float,
        leverage: float = 1.0,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        margin_used: float = 0.0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """创建新持仓"""
        try:
            position_id = str(uuid.uuid4())
            
            position = Position(
                id=position_id,
                symbol=symbol,
                side=side,
                size=size,
                entry_price=entry_price,
                leverage=leverage,
                stop_loss=stop_loss,
                take_profit=take_profit,
                margin_used=margin_used,
                metadata=metadata or {},
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            self.positions[position_id] = position
            
            # 创建预警
            await self._create_alert(
                position_id,
                PositionAlertType.RISK_WARNING,
                f"新持仓创建: {symbol}",
                "info",
                {
                    "side": side,
                    "size": size,
                    "entry_price": entry_price,
                    "leverage": leverage
                }
            )
            
            logger.info(f"新持仓创建: {position_id} - {symbol}")
            return position_id
            
        except Exception as e:
            logger.error(f"创建持仓失败: {e}")
            raise PositionManagerError(f"创建持仓失败: {e}")

    async def update_position(
        self,
        position_id: str,
        new_size: Optional[float] = None,
        new_stop_loss: Optional[float] = None,
        new_take_profit: Optional[float] = None,
        new_leverage: Optional[float] = None
    ) -> bool:
        """更新持仓"""
        try:
            position = self.positions.get(position_id)
            if not position:
                return False
                
            updated = False
            
            if new_size is not None and new_size != position.size:
                position.size = new_size
                updated = True
                
            if new_stop_loss is not None:
                position.stop_loss = new_stop_loss
                updated = True
                
            if new_take_profit is not None:
                position.take_profit = new_take_profit
                updated = True
                
            if new_leverage is not None:
                position.leverage = new_leverage
                updated = True
                
            if updated:
                position.updated_at = datetime.utcnow()
                logger.info(f"持仓已更新: {position_id}")
                
            return True
            
        except Exception as e:
            logger.error(f"更新持仓失败 {position_id}: {e}")
            return False

    async def add_alert_callback(self, callback: Callable):
        """添加预警回调"""
        self.alert_callbacks.append(callback)

    async def get_alerts(
        self,
        position_id: Optional[str] = None,
        unacknowledged_only: bool = False
    ) -> List[PositionAlert]:
        """获取预警"""
        alerts = self.alerts
        
        if position_id:
            alerts = [a for a in alerts if a.position_id == position_id]
            
        if unacknowledged_only:
            alerts = [a for a in alerts if not a.acknowledged]
            
        return alerts

    async def acknowledge_alert(self, alert_id: str) -> bool:
        """确认预警"""
        for alert in self.alerts:
            if alert.id == alert_id:
                alert.acknowledged = True
                return True
        return False

    async def get_position_summary(self) -> Dict[str, Any]:
        """获取持仓摘要"""
        try:
            positions = list(self.positions.values())
            
            total_positions = len(positions)
            open_positions = len([p for p in positions if p.status == PositionStatus.OPEN])
            total_pnl = sum(p.unrealized_pnl for p in positions)
            total_margin = sum(p.margin_used for p in positions)
            
            # 按符号统计
            symbol_stats = defaultdict(lambda: {"count": 0, "pnl": 0.0, "margin": 0.0})
            for position in positions:
                symbol = position.symbol
                symbol_stats[symbol]["count"] += 1
                symbol_stats[symbol]["pnl"] += position.unrealized_pnl
                symbol_stats[symbol]["margin"] += position.margin_used
            
            return {
                "timestamp": datetime.utcnow(),
                "total_positions": total_positions,
                "open_positions": open_positions,
                "total_unrealized_pnl": total_pnl,
                "total_margin_used": total_margin,
                "symbol_breakdown": dict(symbol_stats),
                "unacknowledged_alerts": len([a for a in self.alerts if not a.acknowledged])
            }
            
        except Exception as e:
            logger.error(f"获取持仓摘要失败: {e}")
            return {"error": str(e)}


class PositionManager:
    """持仓管理器 - 高级接口"""

    def __init__(self):
        self.tracking_service = PositionTrackingService()
        self.executed_orders: Dict[str, Order] = {}
        
        logger.info("持仓管理器初始化完成")

    async def start(self):
        """启动持仓管理器"""
        await self.tracking_service.start_tracking()
        logger.info("持仓管理器已启动")

    async def stop(self):
        """停止持仓管理器"""
        await self.tracking_service.stop_tracking()
        logger.info("持仓管理器已停止")

    # 高级持仓操作
    
    async def create_tracked_position(
        self,
        symbol: str,
        side: str,
        size: float,
        entry_price: float,
        leverage: float = 1.0,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        order_id: Optional[str] = None
    ) -> str:
        """创建并追踪持仓"""
        try:
            # 计算保证金
            margin_used = abs(size * entry_price / leverage)
            
            position_id = await self.tracking_service.add_position(
                symbol=symbol,
                side=side,
                size=size,
                entry_price=entry_price,
                leverage=leverage,
                stop_loss=stop_loss,
                take_profit=take_profit,
                margin_used=margin_used,
                metadata={"order_id": order_id} if order_id else {}
            )
            
            logger.info(f"创建追踪持仓: {position_id}")
            return position_id
            
        except Exception as e:
            logger.error(f"创建追踪持仓失败: {e}")
            raise

    async def execute_position_action(
        self,
        position_id: str,
        action: PositionAction,
        size: Optional[float] = None,
        price: Optional[float] = None,
        reason: str = "manual"
    ) -> bool:
        """执行持仓操作"""
        try:
            position = await self.tracking_service.get_position(position_id)
            if not position:
                return False
                
            if action == PositionAction.CLOSE:
                close_price = price or await self.tracking_service._get_current_price(position.symbol)
                return await self.tracking_service.close_position(
                    position_id, close_price, reason
                )
                
            elif action == PositionAction.ADD and size:
                new_size = position.size + size
                return await self.tracking_service.update_position(
                    position_id, new_size=new_size
                )
                
            elif action == PositionAction.REDUCE and size:
                new_size = position.size - size
                if abs(new_size) < 0.001:  # 小于最小单位则平仓
                    return await self.tracking_service.close_position(
                        position_id, price, reason
                    )
                else:
                    return await self.tracking_service.update_position(
                        position_id, new_size=new_size
                    )
                    
            elif action == PositionAction.ADJUST:
                return await self.tracking_service.update_position(
                    position_id,
                    new_stop_loss=price if price else position.stop_loss
                )
                
            return False
            
        except Exception as e:
            logger.error(f"执行持仓操作失败 {position_id}: {e}")
            return False

    async def get_portfolio_overview(self) -> Dict[str, Any]:
        """获取投资组合总览"""
        try:
            summary = await self.tracking_service.get_position_summary()
            
            # 添加风险指标
            positions = await self.tracking_service.get_positions(PositionStatus.OPEN)
            
            if positions:
                total_exposure = sum(abs(p.size * p.entry_price) for p in positions)
                total_leverage = total_exposure / max(sum(p.margin_used for p in positions), 1)
                largest_position = max(positions, key=lambda p: abs(p.size * p.entry_price))
                
                summary.update({
                    "total_exposure": total_exposure,
                    "average_leverage": total_leverage,
                    "largest_position": {
                        "symbol": largest_position.symbol,
                        "size": largest_position.size,
                        "value": abs(largest_position.size * largest_position.entry_price)
                    },
                    "risk_distribution": await self._calculate_risk_distribution(positions)
                })
            
            return summary
            
        except Exception as e:
            logger.error(f"获取投资组合总览失败: {e}")
            return {"error": str(e)}

    async def _calculate_risk_distribution(self, positions: List[Position]) -> Dict[str, float]:
        """计算风险分布"""
        try:
            total_exposure = sum(abs(p.size * p.entry_price) for p in positions)
            
            if total_exposure == 0:
                return {}
                
            distribution = {}
            for position in positions:
                position_exposure = abs(position.size * position.entry_price)
                percentage = (position_exposure / total_exposure) * 100
                distribution[position.symbol] = percentage
                
            return distribution
            
        except Exception as e:
            logger.error(f"计算风险分布失败: {e}")
            return {}


# 全局持仓管理器实例
_position_manager: Optional[PositionManager] = None


async def get_position_manager() -> PositionManager:
    """获取全局持仓管理器实例"""
    global _position_manager

    if _position_manager is None:
        _position_manager = PositionManager()

    return _position_manager


async def shutdown_position_manager():
    """关闭持仓管理器"""
    global _position_manager

    if _position_manager:
        await _position_manager.stop()
        _position_manager = None