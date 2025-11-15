"""
风险控制API端点
提供实时风险监控、预警和报告功能
"""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
import asyncio

from ...storage.database import get_db
from ...storage.models import (
    User,
    Account,
    Order,
    Position,
    AutoOrder,
    RiskAlert,
    RiskManagement,
    TradingStatistics,
)
from ...auto_trading.risk_checker import RiskChecker
from ...auto_trading.position_manager import PositionManager
from ...auto_trading.order_manager import OrderManager

router = APIRouter(prefix="/api/v1/risk", tags=["risk_control"])


# 依赖注入
async def get_risk_checker(db: Session = Depends(get_db)) -> RiskChecker:
    return RiskChecker(db)


async def get_position_manager(db: Session = Depends(get_db)) -> PositionManager:
    return PositionManager(db)


async def get_order_manager(db: Session = Depends(get_db)) -> OrderManager:
    return OrderManager(db)


@router.get("/dashboard/{user_id}")
async def get_risk_dashboard(
    user_id: int, account_id: Optional[int] = None, db: Session = Depends(get_db)
):
    """
    获取风险控制仪表板数据
    包含实时风险指标、仓位信息、交易活动等
    """
    try:
        # 获取用户和账户信息
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")

        # 获取账户列表（如果未指定account_id，则获取所有账户）
        if account_id:
            accounts = (
                db.query(Account)
                .filter(
                    and_(
                        Account.user_id == user_id,
                        Account.id == account_id,
                        Account.is_active == True,
                    )
                )
                .all()
            )
        else:
            accounts = (
                db.query(Account)
                .filter(and_(Account.user_id == user_id, Account.is_active == True))
                .all()
            )

        if not accounts:
            raise HTTPException(status_code=404, detail="未找到活跃账户")

        # 聚合所有账户的风险数据
        dashboard_data = await _aggregate_risk_dashboard_data(accounts, db)

        return {
            "user_id": user_id,
            "accounts": [acc.id for acc in accounts],
            "timestamp": datetime.utcnow().isoformat(),
            "data": dashboard_data,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取风险仪表板失败: {str(e)}")


@router.get("/positions/{user_id}")
async def get_risk_positions(
    user_id: int, account_id: Optional[int] = None, db: Session = Depends(get_db)
):
    """
    获取实时仓位风险数据
    """
    try:
        query = (
            db.query(Position)
            .join(Account)
            .filter(Account.user_id == user_id, Position.is_active == True)
        )

        if account_id:
            query = query.filter(Position.account_id == account_id)

        positions = query.all()

        # 获取仓位风险指标
        position_risks = []
        for position in positions:
            risk_data = {
                "position_id": position.id,
                "account_id": position.account_id,
                "symbol": position.symbol,
                "market_type": position.market_type.value,
                "quantity": float(position.quantity),
                "quantity_available": float(position.quantity_available),
                "quantity_frozen": float(position.quantity_frozen),
                "avg_price": float(position.avg_price),
                "entry_price": float(position.entry_price),
                "unrealized_pnl": float(position.unrealized_pnl),
                "realized_pnl": float(position.realized_pnl),
                "leverage": position.leverage,
                "position_side": position.position_side,
                "status": position.status,
                "updated_at": position.updated_at.isoformat(),
                # 风险指标
                "risk_level": _calculate_position_risk_level(position),
                "exposure_percent": _calculate_exposure_percent(position),
                "margin_ratio": _calculate_margin_ratio(position),
                "liquidation_price": (
                    _calculate_liquidation_price(position)
                    if position.leverage
                    else None
                ),
            }
            position_risks.append(risk_data)

        return {
            "user_id": user_id,
            "positions": position_risks,
            "total_count": len(position_risks),
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取仓位风险失败: {str(e)}")


@router.get("/alerts/{user_id}")
async def get_risk_alerts(
    user_id: int,
    limit: int = 50,
    unacknowledged_only: bool = False,
    severity: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    获取风险警告列表
    """
    try:
        query = db.query(RiskAlert).filter(RiskAlert.user_id == user_id)

        # 过滤未确认的警告
        if unacknowledged_only:
            query = query.filter(RiskAlert.is_acknowledged == False)

        # 按严重性过滤
        if severity:
            query = query.filter(RiskAlert.severity == severity)

        # 按时间倒序排列并限制数量
        alerts = query.order_by(RiskAlert.timestamp.desc()).limit(limit).all()

        # 转换为响应格式
        alert_list = []
        for alert in alerts:
            alert_data = {
                "alert_id": alert.id,
                "account_id": alert.account_id,
                "severity": alert.severity,
                "message": alert.message,
                "alert_type": alert.alert_type,
                "symbol": alert.symbol,
                "auto_order_id": alert.auto_order_id,
                "order_id": alert.order_id,
                "details": alert.details,
                "current_value": (
                    float(alert.current_value) if alert.current_value else None
                ),
                "limit_value": float(alert.limit_value) if alert.limit_value else None,
                "is_acknowledged": alert.is_acknowledged,
                "acknowledged_at": (
                    alert.acknowledged_at.isoformat() if alert.acknowledged_at else None
                ),
                "is_resolved": alert.is_resolved,
                "resolved_at": (
                    alert.resolved_at.isoformat() if alert.resolved_at else None
                ),
                "notification_sent": alert.notification_sent,
                "timestamp": alert.timestamp.isoformat(),
            }
            alert_list.append(alert_data)

        return {
            "user_id": user_id,
            "alerts": alert_list,
            "total_count": len(alert_list),
            "unacknowledged_count": len(
                [a for a in alert_list if not a["is_acknowledged"]]
            ),
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取风险警告失败: {str(e)}")


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_risk_alert(
    alert_id: int, user_id: int, db: Session = Depends(get_db)
):
    """
    确认风险警告
    """
    try:
        alert = (
            db.query(RiskAlert)
            .filter(and_(RiskAlert.id == alert_id, RiskAlert.user_id == user_id))
            .first()
        )

        if not alert:
            raise HTTPException(status_code=404, detail="警告不存在")

        if alert.is_acknowledged:
            return {"message": "警告已确认", "alert_id": alert_id}

        alert.is_acknowledged = True
        alert.acknowledged_at = datetime.utcnow()

        db.commit()

        return {
            "message": "警告已确认",
            "alert_id": alert_id,
            "acknowledged_at": alert.acknowledged_at.isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"确认警告失败: {str(e)}")


@router.get("/metrics/{user_id}")
async def get_risk_metrics(
    user_id: int,
    period_hours: int = 24,
    account_id: Optional[int] = None,
    db: Session = Depends(get_db),
):
    """
    获取风险指标统计
    """
    try:
        # 计算时间范围
        start_time = datetime.utcnow() - timedelta(hours=period_hours)

        # 获取交易统计
        query = (
            db.query(TradingStatistics)
            .join(Account)
            .filter(
                Account.user_id == user_id, TradingStatistics.period_start >= start_time
            )
        )

        if account_id:
            query = query.filter(TradingStatistics.account_id == account_id)

        stats = query.all()

        # 聚合统计数据
        total_trades = sum(s.total_trades for s in stats)
        total_volume = sum(s.total_volume for s in stats)
        total_pnl = sum(s.total_pnl for s in stats)
        win_rate = (
            (sum(s.successful_trades for s in stats) / total_trades * 100)
            if total_trades > 0
            else 0
        )

        # 获取最近的订单活动
        recent_orders = (
            db.query(Order)
            .join(Account)
            .filter(Account.user_id == user_id, Order.order_time >= start_time)
        )

        if account_id:
            recent_orders = recent_orders.filter(Order.account_id == account_id)

        order_count = recent_orders.count()
        avg_execution_time = (
            db.query(func.avg(Order.average_price))
            .join(Account)
            .filter(
                Account.user_id == user_id,
                Order.order_time >= start_time,
                Order.average_price.isnot(None),
            )
            .scalar()
        )

        return {
            "user_id": user_id,
            "period_hours": period_hours,
            "metrics": {
                "total_trades": total_trades,
                "total_volume": float(total_volume),
                "total_pnl": float(total_pnl),
                "win_rate": float(win_rate),
                "order_count": order_count,
                "avg_execution_time": (
                    float(avg_execution_time) if avg_execution_time else None
                ),
            },
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取风险指标失败: {str(e)}")


@router.get("/config/{user_id}")
async def get_risk_config(user_id: int, db: Session = Depends(get_db)):
    """
    获取风险管理配置
    """
    try:
        configs = (
            db.query(RiskManagement)
            .filter(
                and_(
                    RiskManagement.user_id == user_id, RiskManagement.is_active == True
                )
            )
            .all()
        )

        config_list = []
        for config in configs:
            config_data = {
                "config_id": config.id,
                "account_id": config.account_id,
                "config_name": config.config_name,
                "max_order_size": float(config.max_order_size),
                "max_position_size": float(config.max_position_size),
                "max_daily_trades": config.max_daily_trades,
                "max_daily_volume": float(config.max_daily_volume),
                "max_loss_per_trade": float(config.max_loss_per_trade),
                "max_total_exposure": float(config.max_total_exposure),
                "stop_loss_percentage": float(config.stop_loss_percentage),
                "take_profit_percentage": float(config.take_profit_percentage),
                "default_risk_level": config.default_risk_level.value,
                "trading_hours_start": config.trading_hours_start,
                "trading_hours_end": config.trading_hours_end,
                "additional_rules": config.additional_rules,
                "created_at": config.created_at.isoformat(),
                "updated_at": config.updated_at.isoformat(),
            }
            config_list.append(config_data)

        return {
            "user_id": user_id,
            "configs": config_list,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取风险配置失败: {str(e)}")


@router.post("/emergency_stop/{user_id}")
async def emergency_stop_trading(
    user_id: int, account_id: Optional[int] = None, db: Session = Depends(get_db)
):
    """
    紧急停止所有交易活动
    """
    try:
        # 获取活跃的自动订单
        query = db.query(AutoOrder).filter(
            AutoOrder.user_id == user_id, AutoOrder.is_active == True
        )

        if account_id:
            query = query.filter(AutoOrder.account_id == account_id)

        auto_orders = query.all()

        # 停用所有自动订单
        stopped_count = 0
        for auto_order in auto_orders:
            auto_order.is_active = False
            auto_order.is_paused = True
            stopped_count += 1

        # 记录紧急停止事件
        emergency_alert = RiskAlert(
            user_id=user_id,
            account_id=account_id or 0,
            alert_id=f"emergency_stop_{user_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            severity="CRITICAL",
            message=f"紧急停止交易: 用户ID {user_id} 停止了 {stopped_count} 个自动订单",
            alert_type="emergency_stop",
            details={
                "stopped_orders": stopped_count,
                "stop_reason": "用户手动触发紧急停止",
                "timestamp": datetime.utcnow().isoformat(),
            },
            timestamp=datetime.utcnow(),
        )

        db.add(emergency_alert)
        db.commit()

        return {
            "message": "紧急停止执行成功",
            "stopped_orders": stopped_count,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"紧急停止失败: {str(e)}")


async def _aggregate_risk_dashboard_data(
    accounts: List[Account], db: Session
) -> Dict[str, Any]:
    """
    聚合风险仪表板数据
    """
    total_accounts = len(accounts)

    # 计算总仓位价值
    total_positions_value = 0.0
    total_unrealized_pnl = 0.0
    position_count = 0

    # 计算账户余额
    account_balances = {}

    for account in accounts:
        # 获取该账户的所有活跃仓位
        positions = (
            db.query(Position)
            .filter(and_(Position.account_id == account.id, Position.is_active == True))
            .all()
        )

        for position in positions:
            total_positions_value += abs(float(position.quantity * position.avg_price))
            total_unrealized_pnl += float(position.unrealized_pnl)
            position_count += 1

    # 获取未确认的风险警告数量
    unacknowledged_alerts = (
        db.query(func.count(RiskAlert.id))
        .filter(
            and_(
                RiskAlert.user_id == accounts[0].user_id,
                RiskAlert.is_acknowledged == False,
            )
        )
        .scalar()
        or 0
    )

    # 获取活跃的自动订单数量
    active_auto_orders = (
        db.query(func.count(AutoOrder.id))
        .filter(
            and_(AutoOrder.user_id == accounts[0].user_id, AutoOrder.is_active == True)
        )
        .scalar()
        or 0
    )

    # 计算风险等级
    overall_risk_level = _calculate_overall_risk_level(
        total_unrealized_pnl, total_positions_value, unacknowledged_alerts
    )

    return {
        "summary": {
            "total_accounts": total_accounts,
            "total_positions": position_count,
            "total_positions_value": total_positions_value,
            "total_unrealized_pnl": total_unrealized_pnl,
            "unacknowledged_alerts": unacknowledged_alerts,
            "active_auto_orders": active_auto_orders,
            "overall_risk_level": overall_risk_level,
        },
        "risk_distribution": {
            "low_risk": 0,  # 待实现具体的风险分级逻辑
            "medium_risk": 0,
            "high_risk": 0,
            "critical_risk": 0,
        },
        "performance": {
            "daily_pnl": total_unrealized_pnl,  # 简化计算
            "win_rate": 0.0,  # 需要从交易统计中计算
            "total_trades": 0,  # 需要从交易统计中计算
        },
    }


def _calculate_position_risk_level(position: Position) -> str:
    """
    计算单个仓位的风险等级
    """
    unrealized_pnl_percent = (
        abs(position.unrealized_pnl / (position.quantity * position.avg_price))
        if position.quantity > 0
        else 0
    )

    if unrealized_pnl_percent < 0.02:  # 2%
        return "LOW"
    elif unrealized_pnl_percent < 0.05:  # 5%
        return "MEDIUM"
    elif unrealized_pnl_percent < 0.10:  # 10%
        return "HIGH"
    else:
        return "CRITICAL"


def _calculate_exposure_percent(position: Position) -> float:
    """
    计算仓位暴露百分比
    """
    # 这里需要根据账户总余额计算
    # 简化实现，返回一个模拟值
    return min(100.0, abs(position.quantity * position.avg_price) * 0.001)  # 模拟计算


def _calculate_margin_ratio(position: Position) -> float:
    """
    计算保证金比例
    """
    if not position.leverage:
        return 100.0  # 现货交易保证金比例

    return 100.0 / position.leverage


def _calculate_liquidation_price(position: Position) -> Optional[float]:
    """
    计算强平价格
    """
    if not position.leverage or not position.avg_price:
        return None

    # 简化计算：基于仓位方向和杠杆计算
    maintenance_margin = 0.005  # 0.5% 维持保证金率

    if position.position_side == "LONG":
        return position.avg_price * (1 - (1 / position.leverage) + maintenance_margin)
    else:  # SHORT
        return position.avg_price * (1 + (1 / position.leverage) - maintenance_margin)


def _calculate_overall_risk_level(
    pnl: float, positions_value: float, alerts_count: int
) -> str:
    """
    计算整体风险等级
    """
    if positions_value == 0:
        return "LOW"

    pnl_ratio = abs(pnl) / positions_value

    if alerts_count == 0 and pnl_ratio < 0.02:
        return "LOW"
    elif alerts_count <= 2 and pnl_ratio < 0.05:
        return "MEDIUM"
    elif alerts_count <= 5 and pnl_ratio < 0.10:
        return "HIGH"
    else:
        return "CRITICAL"