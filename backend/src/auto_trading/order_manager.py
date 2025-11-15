"""
订单管理器
负责订单的创建、管理和执行，集成风险检查和自动交易功能
"""

import asyncio
import logging
import uuid
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Callable
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_, or_, func

from .risk_checker import RiskCheckerService, RiskCheckResult
from .emergency_stop import EmergencyStopService, get_emergency_stop_service
from ..storage.models import (
    User,
    Account,
    Order,
    AutoOrder,
    OrderExecution,
    RiskAlert,
    OrderType,
    OrderSide,
    OrderStatus,
    MarketType,
    ExecutionResultStatus,
)
from ..utils.exceptions import (
    RiskManagementException,
    OrderManagementException,
    ValidationException,
    ExchangeException,
)


logger = logging.getLogger(__name__)


class OrderManager:
    """订单管理器"""

    def __init__(
        self,
        db_session: AsyncSession,
        risk_checker: RiskCheckerService,
        emergency_stop_service: Optional[EmergencyStopService] = None,
    ):
        self.db_session = db_session
        self.risk_checker = risk_checker
        self.emergency_stop_service = (
            emergency_stop_service or get_emergency_stop_service(db_session)
        )
        self.order_callbacks: Dict[str, Callable] = {}
        self.execution_callbacks: Dict[str, Callable] = {}

    async def create_order(
        self,
        user_id: int,
        account_id: int,
        symbol: str,
        order_side: OrderSide,
        quantity: Decimal,
        order_type: OrderType = OrderType.MARKET,
        price: Optional[Decimal] = None,
        client_order_id: Optional[str] = None,
        market_type: MarketType = MarketType.SPOT,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Order:
        """创建新订单（不执行，仅创建）"""
        try:
            # 生成客户端订单ID
            if not client_order_id:
                client_order_id = f"order_{int(datetime.now().timestamp() * 1000)}_{uuid.uuid4().hex[:8]}"

            # 验证输入参数
            self._validate_order_parameters(
                symbol, order_side, quantity, order_type, price
            )

            # 检查紧急停止状态
            await self._check_emergency_stop(user_id, account_id, symbol)

            # 创建订单对象
            order = Order(
                user_id=user_id,  # 来自Account关系
                account_id=account_id,
                client_order_id=client_order_id,
                symbol=symbol,
                market_type=market_type,
                order_type=order_type,
                order_side=order_side,
                price=price,
                quantity=quantity,
                quantity_filled=Decimal("0"),
                quantity_remaining=quantity,
                status=OrderStatus.NEW,
                order_time=datetime.now(),
                metadata_info=metadata or {},
            )

            self.db_session.add(order)
            await self.db_session.flush()

            logger.info(
                f"订单创建成功: {order.id} - {symbol} {order_side.value} {quantity}",
                extra={
                    "order_id": order.id,
                    "user_id": user_id,
                    "account_id": account_id,
                    "symbol": symbol,
                    "order_side": order_side.value,
                    "quantity": float(quantity),
                    "order_type": order_type.value,
                },
            )

            return order

        except Exception as e:
            logger.error(
                f"创建订单失败: {e}", extra={"user_id": user_id, "symbol": symbol}
            )
            raise OrderManagementException(f"创建订单失败: {e}")

    async def execute_order(
        self,
        order_id: int,
        user_id: int,
        account_id: int,
        current_price: Optional[Decimal] = None,
    ) -> bool:
        """执行订单"""
        try:
            # 获取订单信息
            order = await self._get_order_by_id(order_id, user_id, account_id)
            if not order:
                raise ValidationException(f"订单 {order_id} 不存在")

            # 检查订单状态
            if order.status not in [OrderStatus.NEW, OrderStatus.PENDING]:
                logger.warning(
                    f"订单状态不允许执行: {order.status}", extra={"order_id": order_id}
                )
                return False

            # 执行风险检查
            risk_result = await self.risk_checker.check_order_risk(
                user_id=user_id,
                account_id=account_id,
                symbol=order.symbol,
                order_side=order.order_side,
                quantity=order.quantity,
                order_type=order.order_type,
                price=order.price,
            )

            # 如果风险检查失败，创建风险警告并拒绝执行
            if not risk_result.is_approved:
                await self.risk_checker.create_risk_alert(
                    user_id=user_id,
                    account_id=account_id,
                    alert_result=risk_result,
                    symbol=order.symbol,
                    order_id=order.id,
                )

                # 更新订单状态为拒绝
                await self._update_order_status(order_id, OrderStatus.REJECTED)

                # 记录执行结果
                await self._create_execution_record(
                    order_id=order_id,
                    status=ExecutionResultStatus.REJECTED,
                    success=False,
                    message=f"风险检查失败: {risk_result.message}",
                )

                logger.warning(
                    f"订单因风险检查失败被拒绝: {order_id}",
                    extra={"order_id": order_id, "risk_result": risk_result.message},
                )

                return False

            # 执行订单逻辑（这里应该调用交易所API）
            execution_result = await self._execute_order_with_exchange(
                order, current_price
            )

            # 记录执行结果
            await self._create_execution_record(
                order_id=order_id,
                status=(
                    ExecutionResultStatus.SUCCESS
                    if execution_result["success"]
                    else ExecutionResultStatus.FAILED
                ),
                success=execution_result["success"],
                message=execution_result.get("message", ""),
                filled_quantity=execution_result.get("filled_quantity"),
                average_price=execution_result.get("average_price"),
                commission=execution_result.get("commission"),
                latency_ms=execution_result.get("latency_ms"),
                error_code=execution_result.get("error_code"),
            )

            # 更新订单状态
            if execution_result["success"]:
                await self._update_order_status(order_id, OrderStatus.FILLED)

                # 更新仓位信息
                await self.risk_checker.update_position_after_order_execution(
                    account_id=account_id,
                    symbol=order.symbol,
                    order_side=order.order_side,
                    executed_quantity=Decimal(
                        str(execution_result.get("filled_quantity", 0))
                    ),
                    execution_price=Decimal(
                        str(execution_result.get("average_price", 0))
                    ),
                )
            else:
                await self._update_order_status(order_id, OrderStatus.REJECTED)

            # 触发回调
            await self._trigger_order_callbacks(order_id, execution_result)

            logger.info(
                f"订单执行完成: {order_id} - {'成功' if execution_result['success'] else '失败'}",
                extra={
                    "order_id": order_id,
                    "success": execution_result["success"],
                    "message": execution_result.get("message", ""),
                },
            )

            return execution_result["success"]

        except Exception as e:
            logger.error(f"订单执行失败: {e}", extra={"order_id": order_id})

            # 记录执行失败
            await self._create_execution_record(
                order_id=order_id,
                status=ExecutionResultStatus.FAILED,
                success=False,
                message=f"执行异常: {str(e)}",
            )

            await self._update_order_status(order_id, OrderStatus.REJECTED)
            raise OrderManagementException(f"订单执行失败: {e}")

    async def create_auto_order(
        self,
        user_id: int,
        account_id: int,
        strategy_name: str,
        symbol: str,
        order_side: OrderSide,
        quantity: Decimal,
        entry_condition_id: int,
        stop_loss_price: Optional[Decimal] = None,
        take_profit_price: Optional[Decimal] = None,
        market_type: MarketType = MarketType.SPOT,
        max_slippage: Decimal = Decimal("0.01"),
        max_spread: Decimal = Decimal("0.005"),
        expires_at: Optional[datetime] = None,
    ) -> AutoOrder:
        """创建自动订单"""
        try:
            # 验证输入参数
            self._validate_order_parameters(
                symbol, order_side, quantity, OrderType.MARKET, None
            )

            # 验证策略名称
            if not strategy_name or len(strategy_name) > 100:
                raise ValidationException("策略名称无效")

            # 检查条件是否存在
            condition_query = select(RiskAlert).where(
                RiskAlert.id == entry_condition_id
            )
            condition_result = await self.db_session.execute(condition_query)
            condition = condition_result.scalar_one_or_none()

            if not condition:
                raise ValidationException(f"触发条件 {entry_condition_id} 不存在")

            # 创建自动订单
            auto_order = AutoOrder(
                user_id=user_id,
                account_id=account_id,
                auto_order_id=f"auto_{int(datetime.now().timestamp() * 1000)}_{uuid.uuid4().hex[:8]}",
                strategy_name=strategy_name,
                symbol=symbol,
                market_type=market_type,
                order_side=order_side,
                quantity=quantity,
                entry_condition_id=entry_condition_id,
                stop_loss_price=stop_loss_price,
                take_profit_price=take_profit_price,
                max_slippage=max_slippage,
                max_spread=max_spread,
                status=OrderStatus.NEW,
                is_active=True,
                is_paused=False,
                created_at=datetime.now(),
                expires_at=expires_at,
            )

            self.db_session.add(auto_order)
            await self.db_session.flush()

            logger.info(
                f"自动订单创建成功: {auto_order.id} - {strategy_name}",
                extra={
                    "auto_order_id": auto_order.id,
                    "user_id": user_id,
                    "strategy_name": strategy_name,
                    "symbol": symbol,
                },
            )

            return auto_order

        except Exception as e:
            logger.error(f"创建自动订单失败: {e}", extra={"user_id": user_id})
            raise OrderManagementException(f"创建自动订单失败: {e}")

    async def trigger_auto_order(
        self, auto_order_id: int, current_price: Decimal
    ) -> bool:
        """触发自动订单执行"""
        try:
            # 获取自动订单信息
            auto_order_query = select(AutoOrder).where(AutoOrder.id == auto_order_id)
            result = await self.db_session.execute(auto_order_query)
            auto_order = result.scalar_one_or_none()

            if not auto_order:
                logger.warning(f"自动订单不存在: {auto_order_id}")
                return False

            # 检查自动订单状态
            if not auto_order.is_active or auto_order.is_paused:
                logger.warning(f"自动订单未激活或已暂停: {auto_order_id}")
                return False

            # 检查是否过期
            if auto_order.expires_at and auto_order.expires_at < datetime.now():
                await self._update_auto_order_status(auto_order_id, OrderStatus.EXPIRED)
                logger.info(f"自动订单已过期: {auto_order_id}")
                return False

            # 更新触发统计
            auto_order.trigger_count += 1
            auto_order.last_triggered = datetime.now()

            # 创建实际订单
            order = await self.create_order(
                user_id=auto_order.user_id,
                account_id=auto_order.account_id,
                symbol=auto_order.symbol,
                order_side=auto_order.order_side,
                quantity=auto_order.quantity,
                order_type=OrderType.MARKET,  # 自动订单通常使用市价单
                market_type=auto_order.market_type,
                metadata={
                    "auto_order_id": auto_order.id,
                    "strategy_name": auto_order.strategy_name,
                    "trigger_time": datetime.now().isoformat(),
                    "trigger_price": float(current_price),
                },
            )

            # 尝试执行订单
            execution_success = await self.execute_order(
                order_id=order.id,
                user_id=auto_order.user_id,
                account_id=auto_order.account_id,
                current_price=current_price,
            )

            # 更新自动订单执行统计
            if execution_success:
                auto_order.execution_count += 1
                auto_order.last_execution_result = {
                    "order_id": order.id,
                    "success": True,
                    "execution_time": datetime.now().isoformat(),
                    "price": float(current_price),
                }
            else:
                auto_order.last_execution_result = {
                    "order_id": order.id,
                    "success": False,
                    "execution_time": datetime.now().isoformat(),
                    "price": float(current_price),
                    "failure_reason": "订单执行失败",
                }

            await self.db_session.flush()

            logger.info(
                f"自动订单触发执行完成: {auto_order_id} - {'成功' if execution_success else '失败'}",
                extra={
                    "auto_order_id": auto_order_id,
                    "order_id": order.id,
                    "execution_success": execution_success,
                },
            )

            return execution_success

        except Exception as e:
            logger.error(
                f"自动订单触发执行失败: {e}", extra={"auto_order_id": auto_order_id}
            )
            raise OrderManagementException(f"自动订单执行失败: {e}")

    async def cancel_order(self, order_id: int, user_id: int, account_id: int) -> bool:
        """取消订单"""
        try:
            order = await self._get_order_by_id(order_id, user_id, account_id)
            if not order:
                raise ValidationException(f"订单 {order_id} 不存在")

            if order.status not in [OrderStatus.NEW, OrderStatus.PENDING]:
                logger.warning(
                    f"订单状态不允许取消: {order.status}", extra={"order_id": order_id}
                )
                return False

            # 更新订单状态
            await self._update_order_status(order_id, OrderStatus.CANCELLED)

            # 记录执行记录
            await self._create_execution_record(
                order_id=order_id,
                status=ExecutionResultStatus.REJECTED,
                success=False,
                message="用户取消订单",
            )

            logger.info(f"订单已取消: {order_id}", extra={"order_id": order_id})
            return True

        except Exception as e:
            logger.error(f"取消订单失败: {e}", extra={"order_id": order_id})
            raise OrderManagementException(f"取消订单失败: {e}")

    async def get_user_orders(
        self,
        user_id: int,
        account_id: Optional[int] = None,
        symbol: Optional[str] = None,
        status: Optional[OrderStatus] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Order]:
        """获取用户订单列表"""
        try:
            query = select(Order).where(Order.account_id == Account.id)

            # 连接Account表获取user_id
            from ..storage.models import Account

            query = select(Order).join(Account).where(Account.user_id == user_id)

            # 添加过滤条件
            if account_id:
                query = query.where(Order.account_id == account_id)

            if symbol:
                query = query.where(Order.symbol == symbol)

            if status:
                query = query.where(Order.status == status)

            # 排序和分页
            query = query.order_by(Order.order_time.desc()).limit(limit).offset(offset)

            result = await self.db_session.execute(query)
            orders = result.scalars().all()

            return list(orders)

        except Exception as e:
            logger.error(f"获取用户订单失败: {e}", extra={"user_id": user_id})
            raise OrderManagementException(f"获取订单列表失败: {e}")

    async def get_user_auto_orders(
        self,
        user_id: int,
        account_id: Optional[int] = None,
        is_active: Optional[bool] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[AutoOrder]:
        """获取用户自动订单列表"""
        try:
            query = select(AutoOrder).where(AutoOrder.user_id == user_id)

            if account_id:
                query = query.where(AutoOrder.account_id == account_id)

            if is_active is not None:
                query = query.where(AutoOrder.is_active == is_active)

            query = (
                query.order_by(AutoOrder.created_at.desc()).limit(limit).offset(offset)
            )

            result = await self.db_session.execute(query)
            auto_orders = result.scalars().all()

            return list(auto_orders)

        except Exception as e:
            logger.error(f"获取用户自动订单失败: {e}", extra={"user_id": user_id})
            raise OrderManagementException(f"获取自动订单列表失败: {e}")

    async def pause_auto_order(self, auto_order_id: int, user_id: int) -> bool:
        """暂停自动订单"""
        try:
            query = select(AutoOrder).where(
                AutoOrder.id == auto_order_id,
                AutoOrder.user_id == user_id,
                AutoOrder.is_active == True,
            )

            result = await self.db_session.execute(query)
            auto_order = result.scalar_one_or_none()

            if not auto_order:
                return False

            auto_order.is_paused = True
            await self.db_session.flush()

            logger.info(
                f"自动订单已暂停: {auto_order_id}",
                extra={"auto_order_id": auto_order_id},
            )
            return True

        except Exception as e:
            logger.error(
                f"暂停自动订单失败: {e}", extra={"auto_order_id": auto_order_id}
            )
            raise OrderManagementException(f"暂停自动订单失败: {e}")

    async def resume_auto_order(self, auto_order_id: int, user_id: int) -> bool:
        """恢复自动订单"""
        try:
            query = select(AutoOrder).where(
                AutoOrder.id == auto_order_id,
                AutoOrder.user_id == user_id,
                AutoOrder.is_active == True,
            )

            result = await self.db_session.execute(query)
            auto_order = result.scalar_one_or_none()

            if not auto_order:
                return False

            auto_order.is_paused = False
            await self.db_session.flush()

            logger.info(
                f"自动订单已恢复: {auto_order_id}",
                extra={"auto_order_id": auto_order_id},
            )
            return True

        except Exception as e:
            logger.error(
                f"恢复自动订单失败: {e}", extra={"auto_order_id": auto_order_id}
            )
            raise OrderManagementException(f"恢复自动订单失败: {e}")

    def _validate_order_parameters(
        self,
        symbol: str,
        order_side: OrderSide,
        quantity: Decimal,
        order_type: OrderType,
        price: Optional[Decimal],
    ):
        """验证订单参数"""
        if not symbol or len(symbol) > 50:
            raise ValidationException("交易对符号无效")

        if quantity <= 0:
            raise ValidationException("订单数量必须大于0")

        if order_type == OrderType.LIMIT and (not price or price <= 0):
            raise ValidationException("限价单必须提供有效价格")

    async def _get_order_by_id(
        self, order_id: int, user_id: int, account_id: int
    ) -> Optional[Order]:
        """根据ID获取订单"""
        query = select(Order).where(
            and_(
                Order.id == order_id,
                Order.account_id == account_id,
                Order.account_id == Account.id,
                Account.user_id == user_id,
            )
        )

        from ..storage.models import Account

        query = (
            select(Order)
            .join(Account)
            .where(
                and_(
                    Order.id == order_id,
                    Order.account_id == account_id,
                    Account.user_id == user_id,
                )
            )
        )

        result = await self.db_session.execute(query)
        return result.scalar_one_or_none()

    async def _update_order_status(self, order_id: int, status: OrderStatus):
        """更新订单状态"""
        query = (
            update(Order)
            .where(Order.id == order_id)
            .values(status=status, update_time=datetime.now())
        )
        await self.db_session.execute(query)
        await self.db_session.flush()

    async def _update_auto_order_status(self, auto_order_id: int, status: OrderStatus):
        """更新自动订单状态"""
        query = (
            update(AutoOrder)
            .where(AutoOrder.id == auto_order_id)
            .values(status=status, updated_at=datetime.now())
        )
        await self.db_session.execute(query)
        await self.db_session.flush()

    async def _create_execution_record(
        self,
        order_id: int,
        status: ExecutionResultStatus,
        success: bool,
        message: str,
        filled_quantity: Optional[Decimal] = None,
        average_price: Optional[Decimal] = None,
        commission: Optional[Decimal] = None,
        latency_ms: Optional[float] = None,
        error_code: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
    ):
        """创建执行记录"""
        execution = OrderExecution(
            order_id=order_id,
            execution_id=f"exec_{int(datetime.now().timestamp() * 1000)}_{uuid.uuid4().hex[:8]}",
            status=status,
            success=success,
            message=message,
            filled_quantity=filled_quantity,
            average_price=average_price,
            commission=commission,
            latency_ms=latency_ms,
            error_code=error_code,
            error_details=error_details,
            execution_time=datetime.now(),
        )

        self.db_session.add(execution)
        await self.db_session.flush()

    async def _execute_order_with_exchange(
        self, order: Order, current_price: Optional[Decimal] = None
    ) -> Dict[str, Any]:
        """与交易所执行订单（模拟）"""
        try:
            start_time = datetime.now()

            # 模拟订单执行延迟
            await asyncio.sleep(0.1)  # 100ms延迟

            execution_time = (datetime.now() - start_time).total_seconds() * 1000  # ms

            # 模拟执行结果（90%成功率）
            import random

            if random.random() < 0.9:
                return {
                    "success": True,
                    "filled_quantity": float(order.quantity),
                    "average_price": float(order.price or current_price or 50000),
                    "commission": float(order.quantity * 0.001),  # 0.1%手续费
                    "latency_ms": execution_time,
                    "message": "订单执行成功",
                }
            else:
                return {
                    "success": False,
                    "message": "交易所执行失败",
                    "latency_ms": execution_time,
                    "error_code": "EXCHANGE_ERROR",
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"执行异常: {str(e)}",
                "error_code": "EXECUTION_ERROR",
            }

    async def _trigger_order_callbacks(
        self, order_id: int, execution_result: Dict[str, Any]
    ):
        """触发订单回调"""
        if order_id in self.order_callbacks:
            try:
                await self.order_callbacks[order_id](order_id, execution_result)
            except Exception as e:
                logger.error(f"订单回调执行失败: {e}", extra={"order_id": order_id})

    def register_order_callback(self, order_id: int, callback: Callable):
        """注册订单状态变化回调"""
        self.order_callbacks[order_id] = callback

    def register_execution_callback(self, order_id: int, callback: Callable):
        """注册订单执行结果回调"""
        self.execution_callbacks[order_id] = callback

    async def _check_emergency_stop(self, user_id: int, account_id: int, symbol: str):
        """检查紧急停止状态"""
        try:
            if (
                self.emergency_stop_service
                and self.emergency_stop_service.is_trading_stopped(
                    user_id=user_id, account_id=account_id, symbol=symbol
                )
            ):
                raise RiskManagementException(
                    f"交易已被紧急停止 - 用户:{user_id}, 账户:{account_id}, 交易对:{symbol}"
                )
        except Exception as e:
            if isinstance(e, RiskManagementException):
                raise
            logger.error(f"检查紧急停止状态失败: {str(e)}")