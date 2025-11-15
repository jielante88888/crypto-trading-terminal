"""
持仓管理和追踪系统
提供统一的持仓管理功能，支持实时监控、历史追踪和风险管理
"""

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional, Union, Tuple
from enum import Enum
import json

from ..storage.models.account_models import (
    Position,
    PositionType,
    Account,
    PnLRecord,
    PnLType,
    account_manager,
)
from .pnl_calculator import RealTimePnLCalculator, PnLCalculationMode
from .account_sync_manager import AccountBalanceSyncManager
from ..core.exceptions import ValidationException, ManagementException


class PositionStatus(Enum):
    """持仓状态"""

    OPEN = "open"
    CLOSED = "closed"
    PARTIAL = "partial"
    PENDING = "pending"
    CANCELLED = "cancelled"


class PositionAction(Enum):
    """持仓操作类型"""

    OPEN_POSITION = "open_position"
    CLOSE_POSITION = "close_position"
    ADD_POSITION = "add_position"
    REDUCE_POSITION = "reduce_position"
    CHANGE_LEVERAGE = "change_leverage"
    UPDATE_STOP_LOSS = "update_stop_loss"
    UPDATE_TAKE_PROFIT = "update_take_profit"


class PositionAlertType(Enum):
    """持仓预警类型"""

    LIQUIDATION_RISK = "liquidation_risk"
    PROFIT_TARGET = "profit_target"
    STOP_LOSS = "stop_loss"
    MARGIN_CALL = "margin_call"
    PRICE_ALERT = "price_alert"


class PositionAlert:
    """持仓预警"""

    def __init__(
        self,
        position_id: str,
        alert_type: PositionAlertType,
        message: str,
        severity: str = "medium",  # low, medium, high, critical
        triggered_at: Optional[datetime] = None,
    ):
        self.id = f"{position_id}_{alert_type.value}_{int(datetime.now().timestamp())}"
        self.position_id = position_id
        self.alert_type = alert_type
        self.message = message
        self.severity = severity
        self.triggered_at = triggered_at or datetime.now()
        self.acknowledged = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "position_id": self.position_id,
            "alert_type": self.alert_type.value,
            "message": self.message,
            "severity": self.severity,
            "triggered_at": self.triggered_at.isoformat(),
            "acknowledged": self.acknowledged,
        }


class PositionTrackingService:
    """持仓追踪服务"""

    def __init__(self):
        self.active_positions: Dict[str, Position] = {}
        self.position_history: Dict[str, List[Dict[str, Any]]] = {}
        self.position_alerts: Dict[str, List[PositionAlert]] = {}
        self.pnl_calculator = RealTimePnLCalculator()
        self.sync_manager = AccountBalanceSyncManager()

        self.logger = logging.getLogger(__name__)

        # 追踪配置
        self.alert_thresholds = {
            "liquidation_distance_pct": Decimal("10"),  # 10%强平距离预警
            "profit_target_pct": Decimal("5"),  # 5%盈利目标
            "stop_loss_pct": Decimal("3"),  # 3%止损
            "margin_level_warning": Decimal("1.5"),  # 150%保证金水平预警
        }

    async def start_tracking_position(
        self, position: Position, alert_config: Optional[Dict[str, Any]] = None
    ) -> bool:
        """开始追踪持仓"""
        try:
            # 添加到活跃持仓
            self.active_positions[position.position_id] = position

            # 初始化历史记录
            if position.position_id not in self.position_history:
                self.position_history[position.position_id] = []

            # 添加初始记录
            await self._add_position_snapshot(position, "position_opened")

            # 设置预警
            if alert_config:
                await self._setup_position_alerts(position, alert_config)

            self.logger.info(f"开始追踪持仓: {position.position_id} {position.symbol}")
            return True

        except Exception as e:
            self.logger.error(f"开始追踪持仓失败: {e}")
            raise ManagementException(f"开始追踪持仓失败: {e}")

    async def stop_tracking_position(
        self, position_id: str, reason: str = "manual"
    ) -> bool:
        """停止追踪持仓"""
        try:
            if position_id not in self.active_positions:
                return False

            position = self.active_positions[position_id]

            # 添加最终记录
            await self._add_position_snapshot(position, f"position_closed_{reason}")

            # 从活跃持仓中移除
            del self.active_positions[position_id]

            # 清理预警
            if position_id in self.position_alerts:
                del self.position_alerts[position_id]

            self.logger.info(f"停止追踪持仓: {position_id}")
            return True

        except Exception as e:
            self.logger.error(f"停止追踪持仓失败: {e}")
            raise ManagementException(f"停止追踪持仓失败: {e}")

    async def get_position_status(self, position_id: str) -> Dict[str, Any]:
        """获取持仓状态"""
        try:
            position = self.active_positions.get(position_id)
            if not position:
                return {"error": "持仓不存在或已关闭"}

            # 计算实时盈亏
            pnl_result = await self.pnl_calculator.calculate_position_pnl(
                position, mode=PnLCalculationMode.COMPREHENSIVE
            )

            # 获取风险评估
            risk_assessment = position.get_risk_assessment()

            # 检查预警
            alerts = await self._check_position_alerts(position)

            return {
                "position_id": position_id,
                "status": PositionStatus.OPEN.value,
                "symbol": position.symbol,
                "quantity": float(position.quantity),
                "side": position.side,
                "entry_price": float(position.entry_price),
                "current_price": float(position.current_price),
                "unrealized_pnl": pnl_result["unrealized_pnl"],
                "total_pnl": pnl_result["total_pnl"],
                "pnl_percentage": pnl_result["pnl_percentage"],
                "margin_level": risk_assessment["margin_level"],
                "leverage_ratio": risk_assessment["leverage_ratio"],
                "risk_level": risk_assessment["risk_level"],
                "alerts": [alert.to_dict() for alert in alerts],
                "opened_at": position.opened_at.isoformat(),
                "last_updated": position.last_updated.isoformat(),
                "tracking_duration": (
                    datetime.now() - position.opened_at
                ).total_seconds(),
            }

        except Exception as e:
            self.logger.error(f"获取持仓状态失败: {e}")
            return {"error": str(e)}

    async def get_all_active_positions(self) -> List[Dict[str, Any]]:
        """获取所有活跃持仓"""
        try:
            active_positions = []

            for position_id, position in self.active_positions.items():
                status = await self.get_position_status(position_id)
                if "error" not in status:
                    active_positions.append(status)

            return active_positions

        except Exception as e:
            self.logger.error(f"获取活跃持仓列表失败: {e}")
            return []

    async def get_position_history(
        self,
        position_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """获取持仓历史记录"""
        try:
            history = self.position_history.get(position_id, [])

            if start_time or end_time:
                filtered_history = []
                for record in history:
                    record_time = datetime.fromisoformat(record["timestamp"])
                    if start_time and record_time < start_time:
                        continue
                    if end_time and record_time > end_time:
                        continue
                    filtered_history.append(record)
                history = filtered_history

            return history

        except Exception as e:
            self.logger.error(f"获取持仓历史失败: {e}")
            return []

    async def update_position_price(
        self, position_id: str, new_price: Decimal, source: str = "manual"
    ) -> bool:
        """更新持仓价格"""
        try:
            if position_id not in self.active_positions:
                raise ValidationException(f"持仓不存在: {position_id}")

            position = self.active_positions[position_id]
            old_price = position.current_price

            # 更新价格
            position.update_current_price(new_price)

            # 添加价格更新记录
            await self._add_position_snapshot(position, f"price_update_{source}")

            # 检查是否触发预警
            await self._check_and_trigger_alerts(position)

            self.logger.debug(
                f"更新持仓价格: {position_id} " f"{old_price} -> {new_price}"
            )
            return True

        except Exception as e:
            self.logger.error(f"更新持仓价格失败: {e}")
            return False

    async def close_position(
        self,
        position_id: str,
        close_price: Optional[Decimal] = None,
        close_quantity: Optional[Decimal] = None,
        reason: str = "manual",
    ) -> Dict[str, Any]:
        """平仓操作"""
        try:
            if position_id not in self.active_positions:
                raise ValidationException(f"持仓不存在: {position_id}")

            position = self.active_positions[position_id]

            # 获取平仓价格
            if close_price is None:
                close_price = position.current_price

            # 计算平仓数量
            quantity_to_close = close_quantity or abs(position.quantity)

            # 执行平仓
            close_result = position.close_position(close_price, quantity_to_close)

            # 添加平仓记录
            await self._add_position_snapshot(position, f"position_closed_{reason}")

            # 创建盈亏记录
            pnl_record = await account_manager.create_pnl_record(
                account_id=position.account_id,
                symbol=position.symbol,
                pnl_type=PnLType.REALIZED,
                pnl_amount=Decimal(str(close_result["realized_pnl"])),
                period_start=position.opened_at,
                period_end=datetime.now(),
                position_id=position_id,
                quantity=Decimal(str(close_result["close_quantity"])),
                entry_price=position.entry_price,
                exit_price=close_price,
                commission=position.commission_paid,
            )

            # 如果完全平仓，停止追踪
            if position.quantity == 0:
                await self.stop_tracking_position(position_id, reason)

            self.logger.info(
                f"平仓成功: {position_id} 数量: {close_result['close_quantity']} 价格: {close_price}"
            )

            return {
                "position_id": position_id,
                "close_quantity": close_result["close_quantity"],
                "close_price": float(close_price),
                "realized_pnl": close_result["realized_pnl"],
                "remaining_quantity": close_result["remaining_quantity"],
                "pnl_record_id": pnl_record.record_id,
                "closed_at": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"平仓失败: {e}")
            raise ManagementException(f"平仓失败: {e}")

    async def add_position(
        self,
        position_id: str,
        additional_quantity: Decimal,
        additional_price: Decimal,
        reason: str = "manual_addition",
    ) -> Dict[str, Any]:
        """加仓操作"""
        try:
            if position_id not in self.active_positions:
                raise ValidationException(f"持仓不存在: {position_id}")

            position = self.active_positions[position_id]

            # 计算新的平均成本
            total_quantity = abs(position.quantity) + additional_quantity
            total_cost = (
                abs(position.quantity) * position.entry_price
                + additional_quantity * additional_price
            )

            if total_quantity > 0:
                new_average_price = total_cost / total_quantity
            else:
                new_average_price = position.entry_price

            # 更新持仓
            if position.quantity > 0:
                position.quantity += additional_quantity
            else:
                position.quantity -= additional_quantity

            position.entry_price = new_average_price
            position.last_updated = datetime.now()

            # 记录加仓
            await self._add_position_snapshot(position, f"position_added_{reason}")

            self.logger.info(
                f"加仓成功: {position_id} 数量: {additional_quantity} 价格: {additional_price}"
            )

            return {
                "position_id": position_id,
                "added_quantity": float(additional_quantity),
                "added_price": float(additional_price),
                "new_total_quantity": float(position.quantity),
                "new_average_price": float(new_average_price),
                "updated_at": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"加仓失败: {e}")
            raise ManagementException(f"加仓失败: {e}")

    async def reduce_position(
        self,
        position_id: str,
        reduce_quantity: Decimal,
        reduce_price: Decimal,
        reason: str = "manual_reduction",
    ) -> Dict[str, Any]:
        """减仓操作"""
        try:
            if position_id not in self.active_positions:
                raise ValidationException(f"持仓不存在: {position_id}")

            position = self.active_positions[position_id]

            if reduce_quantity > abs(position.quantity):
                raise ValidationException("减仓数量不能超过当前持仓数量")

            # 计算减仓盈亏
            if position.side == "LONG":
                pnl_per_unit = reduce_price - position.entry_price
            else:  # SHORT
                pnl_per_unit = position.entry_price - reduce_price

            realized_pnl = pnl_per_unit * reduce_quantity

            # 更新持仓
            if position.quantity > 0:
                position.quantity -= reduce_quantity
            else:
                position.quantity += reduce_quantity

            position.last_updated = datetime.now()

            # 记录减仓
            await self._add_position_snapshot(position, f"position_reduced_{reason}")

            # 如果完全平仓，停止追踪
            if position.quantity == 0:
                await self.stop_tracking_position(position_id, reason)

            self.logger.info(
                f"减仓成功: {position_id} 数量: {reduce_quantity} 盈利: {realized_pnl}"
            )

            return {
                "position_id": position_id,
                "reduced_quantity": float(reduce_quantity),
                "reduce_price": float(reduce_price),
                "realized_pnl": float(realized_pnl),
                "remaining_quantity": float(position.quantity),
                "updated_at": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"减仓失败: {e}")
            raise ManagementException(f"减仓失败: {e}")

    async def update_position_parameters(
        self,
        position_id: str,
        stop_loss: Optional[Decimal] = None,
        take_profit: Optional[Decimal] = None,
        leverage: Optional[Decimal] = None,
    ) -> Dict[str, Any]:
        """更新持仓参数"""
        try:
            if position_id not in self.active_positions:
                raise ValidationException(f"持仓不存在: {position_id}")

            position = self.active_positions[position_id]
            updates = {}

            # 更新止损价
            if stop_loss is not None:
                position.stop_loss_price = stop_loss
                updates["stop_loss_price"] = float(stop_loss)

            # 更新止盈价
            if take_profit is not None:
                position.take_profit_price = take_profit
                updates["take_profit_price"] = float(take_profit)

            # 更新杠杆
            if leverage is not None and position.position_type == PositionType.FUTURES:
                if leverage > 0 and leverage <= position.max_leverage:
                    position.leverage = leverage
                    # 重新计算保证金
                    position_value = (
                        abs(position.quantity)
                        * position.current_price
                        * position.contract_value
                    )
                    position.margin_used = position_value / leverage
                    updates["leverage"] = float(leverage)
                    updates["margin_used"] = float(position.margin_used)
                else:
                    raise ValidationException(f"杠杆设置无效: {leverage}")

            position.last_updated = datetime.now()

            # 添加参数更新记录
            await self._add_position_snapshot(position, "parameters_updated")

            self.logger.info(f"更新持仓参数成功: {position_id} {updates}")

            return {
                "position_id": position_id,
                "updates": updates,
                "updated_at": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"更新持仓参数失败: {e}")
            raise ManagementException(f"更新持仓参数失败: {e}")

    async def get_position_alerts(
        self, position_id: Optional[str] = None, unacknowledged_only: bool = False
    ) -> List[Dict[str, Any]]:
        """获取持仓预警"""
        try:
            if position_id:
                alerts = self.position_alerts.get(position_id, [])
            else:
                # 获取所有预警
                alerts = []
                for alert_list in self.position_alerts.values():
                    alerts.extend(alert_list)

            # 过滤未确认的预警
            if unacknowledged_only:
                alerts = [alert for alert in alerts if not alert.acknowledged]

            # 按时间排序
            alerts.sort(key=lambda x: x.triggered_at, reverse=True)

            return [alert.to_dict() for alert in alerts]

        except Exception as e:
            self.logger.error(f"获取持仓预警失败: {e}")
            return []

    async def acknowledge_alert(self, alert_id: str) -> bool:
        """确认预警"""
        try:
            for position_id, alerts in self.position_alerts.items():
                for alert in alerts:
                    if alert.id == alert_id:
                        alert.acknowledged = True
                        self.logger.info(f"确认预警: {alert_id}")
                        return True
            return False

        except Exception as e:
            self.logger.error(f"确认预警失败: {e}")
            return False

    async def bulk_update_prices(
        self, price_updates: Dict[str, Decimal]
    ) -> Dict[str, Any]:
        """批量更新价格"""
        try:
            successful_updates = 0
            failed_updates = []

            for symbol, new_price in price_updates.items():
                try:
                    # 找到所有相关持仓
                    symbol_positions = [
                        (pos_id, pos)
                        for pos_id, pos in self.active_positions.items()
                        if pos.symbol == symbol
                    ]

                    for position_id, position in symbol_positions:
                        await self.update_position_price(
                            position_id, new_price, "bulk_update"
                        )

                    successful_updates += len(symbol_positions)

                except Exception as e:
                    failed_updates.append({"symbol": symbol, "error": str(e)})

            return {
                "total_symbols": len(price_updates),
                "successful_updates": successful_updates,
                "failed_updates": failed_updates,
                "processed_at": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"批量更新价格失败: {e}")
            raise ManagementException(f"批量更新价格失败: {e}")

    async def _add_position_snapshot(
        self, position: Position, action: str
    ) -> Dict[str, Any]:
        """添加持仓快照"""
        snapshot = {
            "position_id": position.position_id,
            "action": action,
            "timestamp": datetime.now().isoformat(),
            "symbol": position.symbol,
            "quantity": float(position.quantity),
            "entry_price": float(position.entry_price),
            "current_price": float(position.current_price),
            "unrealized_pnl": float(position.unrealized_pnl),
            "realized_pnl": float(position.realized_pnl),
            "total_pnl": float(position.unrealized_pnl + position.realized_pnl),
            "margin_used": float(position.margin_used),
            "side": position.side,
        }

        # 添加到历史记录
        if position.position_id not in self.position_history:
            self.position_history[position.position_id] = []

        self.position_history[position.position_id].append(snapshot)

        # 保持历史记录在合理范围内
        if len(self.position_history[position.position_id]) > 1000:
            self.position_history[position.position_id] = self.position_history[
                position.position_id
            ][-500:]

        return snapshot

    async def _setup_position_alerts(
        self, position: Position, config: Dict[str, Any]
    ) -> List[PositionAlert]:
        """设置持仓预警"""
        alerts = []

        # 强平距离预警
        if config.get("enable_liquidation_alert", True):
            alert = PositionAlert(
                position.position_id,
                PositionAlertType.LIQUIDATION_RISK,
                "持仓接近强平价格",
                "high",
            )
            alerts.append(alert)

        # 止盈止损预警
        if config.get("enable_profit_alert", True) and position.take_profit_price:
            alert = PositionAlert(
                position.position_id,
                PositionAlertType.PROFIT_TARGET,
                "持仓达到止盈目标",
                "medium",
            )
            alerts.append(alert)

        if config.get("enable_stop_loss_alert", True) and position.stop_loss_price:
            alert = PositionAlert(
                position.position_id,
                PositionAlertType.STOP_LOSS,
                "持仓触发止损",
                "high",
            )
            alerts.append(alert)

        # 价格预警
        if "price_alerts" in config:
            for price_alert in config["price_alerts"]:
                alert = PositionAlert(
                    position.position_id,
                    PositionAlertType.PRICE_ALERT,
                    f"价格达到 {price_alert['price']}",
                    price_alert.get("severity", "medium"),
                )
                alerts.append(alert)

        # 保存预警设置
        if position.position_id not in self.position_alerts:
            self.position_alerts[position.position_id] = []

        self.position_alerts[position.position_id].extend(alerts)

        return alerts

    async def _check_position_alerts(self, position: Position) -> List[PositionAlert]:
        """检查持仓预警"""
        alerts = []

        if position.position_id not in self.position_alerts:
            return alerts

        position_alerts = self.position_alerts[position.position_id]

        for alert in position_alerts:
            if alert.acknowledged:
                continue

            triggered = False
            message = ""

            if alert.alert_type == PositionAlertType.LIQUIDATION_RISK:
                risk_assessment = position.get_risk_assessment()
                if risk_assessment["margin_level"] < 1.2:  # 120%以下为高风险
                    triggered = True
                    message = f"保证金水平过低: {risk_assessment['margin_level']:.2f}"

            elif alert.alert_type == PositionAlertType.PROFIT_TARGET:
                if position.take_profit_price and (
                    (
                        position.side == "LONG"
                        and position.current_price >= position.take_profit_price
                    )
                    or (
                        position.side == "SHORT"
                        and position.current_price <= position.take_profit_price
                    )
                ):
                    triggered = True
                    message = f"达到止盈价格: {position.take_profit_price}"

            elif alert.alert_type == PositionAlertType.STOP_LOSS:
                if position.stop_loss_price and (
                    (
                        position.side == "LONG"
                        and position.current_price <= position.stop_loss_price
                    )
                    or (
                        position.side == "SHORT"
                        and position.current_price >= position.stop_loss_price
                    )
                ):
                    triggered = True
                    message = f"触发止损价格: {position.stop_loss_price}"

            if triggered:
                alert.message = message
                alert.triggered_at = datetime.now()
                alerts.append(alert)

        return alerts

    async def _check_and_trigger_alerts(self, position: Position):
        """检查并触发预警"""
        alerts = await self._check_position_alerts(position)

        for alert in alerts:
            self.logger.warning(
                f"持仓预警触发: {position.position_id} "
                f"{alert.alert_type.value} - {alert.message}"
            )