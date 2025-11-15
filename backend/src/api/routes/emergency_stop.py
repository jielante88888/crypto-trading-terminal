"""
紧急停止API路由
提供紧急停止管理、状态查询和控制接口
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import desc, and_, or_, func
import asyncio

from ...storage.database import get_db
from ...storage.models import User, Account, RiskAlert
from ...auto_trading.emergency_stop import (
    EmergencyStopService,
    EmergencyStopConfig,
    StopLevel,
    StopReason,
    StopStatus,
    get_emergency_stop_service,
    init_emergency_stop_service,
)
from ..dependencies import get_current_user, get_current_account

router = APIRouter(prefix="/api/v1/emergency-stop", tags=["emergency-stop"])


# 全局服务实例
emergency_stop_service: Optional[EmergencyStopService] = None


@router.on_event("startup")
async def startup_event():
    """应用启动时初始化紧急停止服务"""
    global emergency_stop_service
    try:
        # 获取数据库会话
        async with get_db() as db:
            emergency_stop_service = init_emergency_stop_service(db)
            await emergency_stop_service.start_monitoring()
        print("紧急停止服务启动成功")
    except Exception as e:
        print(f"紧急停止服务启动失败: {str(e)}")


@router.on_event("shutdown")
async def shutdown_event():
    """应用关闭时清理紧急停止服务"""
    global emergency_stop_service
    if emergency_stop_service:
        await emergency_stop_service.stop_monitoring()
        print("紧急停止服务已停止")


async def get_emergency_service() -> EmergencyStopService:
    """获取紧急停止服务实例"""
    global emergency_stop_service
    if not emergency_stop_service:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="紧急停止服务未初始化",
        )
    return emergency_stop_service


@router.post("/trigger-global", status_code=status.HTTP_201_CREATED)
async def trigger_global_emergency_stop(
    *,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    service: EmergencyStopService = Depends(get_emergency_service),
    reason: str = Query(..., description="停止原因"),
    stop_all_orders: bool = Query(True, description="是否停止所有订单"),
    max_duration: int = Query(3600, ge=60, le=86400, description="最大停止时长(秒)"),
    confirmation_token: Optional[str] = Query(None, description="确认令牌"),
):
    """触发全局紧急停止"""
    try:
        # 验证用户权限
        if not current_user.is_active:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="用户账户未激活"
            )

        # 创建配置
        config = EmergencyStopConfig(
            stop_level=StopLevel.GLOBAL,
            target_id="global",
            reason=(
                StopReason(reason)
                if reason in [r.value for r in StopReason]
                else StopReason.MANUAL
            ),
            stop_all_orders=stop_all_orders,
            cancel_pending_orders=True,
            pause_new_orders=True,
            max_stop_duration=max_duration,
            require_confirmation=True,
            metadata={
                "triggered_by_user_id": current_user.id,
                "triggered_by_username": current_user.username,
            },
        )

        # 执行紧急停止
        stop_id = await service.execute_emergency_stop(
            config=config,
            triggered_by=f"user_{current_user.id}",
            confirmation_token=confirmation_token,
        )

        return {
            "success": True,
            "stop_id": stop_id,
            "message": "全局紧急停止已触发",
            "estimated_duration": max_duration,
        }

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"参数错误: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"触发全局紧急停止失败: {str(e)}",
        )


@router.post("/trigger-user/{user_id}", status_code=status.HTTP_201_CREATED)
async def trigger_user_emergency_stop(
    user_id: int,
    *,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    service: EmergencyStopService = Depends(get_emergency_service),
    reason: str = Query(..., description="停止原因"),
    max_duration: int = Query(1800, ge=60, le=86400, description="最大停止时长(秒)"),
    confirmation_token: Optional[str] = Query(None, description="确认令牌"),
):
    """触发用户紧急停止"""
    try:
        # 验证目标用户存在
        target_user = db.query(User).filter(User.id == user_id).first()
        if not target_user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="目标用户不存在"
            )

        # 权限检查：用户只能停止自己的账户，或者管理员可以停止任何用户
        if current_user.id != user_id and not current_user.is_verified:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="权限不足"
            )

        # 创建配置
        config = EmergencyStopConfig(
            stop_level=StopLevel.USER,
            target_id=user_id,
            reason=(
                StopReason(reason)
                if reason in [r.value for r in StopReason]
                else StopReason.MANUAL
            ),
            stop_all_orders=True,
            cancel_pending_orders=True,
            pause_new_orders=True,
            max_stop_duration=max_duration,
            require_confirmation=True,
            metadata={
                "target_user_id": user_id,
                "target_username": target_user.username,
                "triggered_by_user_id": current_user.id,
                "triggered_by_username": current_user.username,
            },
        )

        # 执行紧急停止
        stop_id = await service.execute_emergency_stop(
            config=config,
            triggered_by=f"user_{current_user.id}",
            confirmation_token=confirmation_token,
        )

        return {
            "success": True,
            "stop_id": stop_id,
            "target_user_id": user_id,
            "message": f"用户 {target_user.username} 的交易已停止",
            "estimated_duration": max_duration,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"触发用户紧急停止失败: {str(e)}",
        )


@router.post("/trigger-account/{account_id}", status_code=status.HTTP_201_CREATED)
async def trigger_account_emergency_stop(
    account_id: int,
    *,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    current_account: Optional[Account] = Depends(get_current_account),
    service: EmergencyStopService = Depends(get_emergency_service),
    reason: str = Query(..., description="停止原因"),
    max_duration: int = Query(900, ge=60, le=86400, description="最大停止时长(秒)"),
    confirmation_token: Optional[str] = Query(None, description="确认令牌"),
):
    """触发账户紧急停止"""
    try:
        # 验证账户存在且属于用户
        account = (
            db.query(Account)
            .filter(Account.id == account_id, Account.user_id == current_user.id)
            .first()
        )

        if not account:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="账户不存在或无权访问"
            )

        # 创建配置
        config = EmergencyStopConfig(
            stop_level=StopLevel.ACCOUNT,
            target_id=account_id,
            reason=(
                StopReason(reason)
                if reason in [r.value for r in StopReason]
                else StopReason.MANUAL
            ),
            stop_all_orders=True,
            cancel_pending_orders=True,
            pause_new_orders=True,
            max_stop_duration=max_duration,
            require_confirmation=True,
            metadata={
                "account_id": account_id,
                "exchange": account.exchange.value,
                "account_type": account.account_type,
                "triggered_by_user_id": current_user.id,
                "triggered_by_username": current_user.username,
            },
        )

        # 执行紧急停止
        stop_id = await service.execute_emergency_stop(
            config=config,
            triggered_by=f"user_{current_user.id}",
            confirmation_token=confirmation_token,
        )

        return {
            "success": True,
            "stop_id": stop_id,
            "account_id": account_id,
            "exchange": account.exchange.value,
            "message": f"账户 {account.exchange.value} 的交易已停止",
            "estimated_duration": max_duration,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"触发账户紧急停止失败: {str(e)}",
        )


@router.post("/trigger-symbol/{symbol}", status_code=status.HTTP_201_CREATED)
async def trigger_symbol_emergency_stop(
    symbol: str,
    *,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    service: EmergencyStopService = Depends(get_emergency_service),
    reason: str = Query(..., description="停止原因"),
    max_duration: int = Query(600, ge=60, le=86400, description="最大停止时长(秒)"),
    confirmation_token: Optional[str] = Query(None, description="确认令牌"),
):
    """触发交易对紧急停止"""
    try:
        # 验证交易对是否有效
        if not symbol or len(symbol) < 3:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="交易对格式无效"
            )

        # 创建配置
        config = EmergencyStopConfig(
            stop_level=StopLevel.SYMBOL,
            target_id=symbol.upper(),
            reason=(
                StopReason(reason)
                if reason in [r.value for r in StopReason]
                else StopReason.MANUAL
            ),
            stop_all_orders=True,
            cancel_pending_orders=True,
            pause_new_orders=True,
            max_stop_duration=max_duration,
            require_confirmation=True,
            metadata={
                "symbol": symbol.upper(),
                "triggered_by_user_id": current_user.id,
                "triggered_by_username": current_user.username,
            },
        )

        # 执行紧急停止
        stop_id = await service.execute_emergency_stop(
            config=config,
            triggered_by=f"user_{current_user.id}",
            confirmation_token=confirmation_token,
        )

        return {
            "success": True,
            "stop_id": stop_id,
            "symbol": symbol.upper(),
            "message": f"交易对 {symbol.upper()} 的交易已停止",
            "estimated_duration": max_duration,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"触发交易对紧急停止失败: {str(e)}",
        )


@router.get("/status", response_model=Dict[str, Any])
async def get_emergency_stop_status(
    current_user: User = Depends(get_current_user),
    service: EmergencyStopService = Depends(get_emergency_service),
    db: Session = Depends(get_db),
):
    """获取紧急停止状态"""
    try:
        # 获取用户的活跃停止
        user_stops = []
        active_stops = await service.get_active_stops()

        for stop in active_stops:
            if stop.metadata.get("triggered_by_user_id") == current_user.id:
                user_stops.append(
                    {
                        "stop_id": stop.stop_id,
                        "stop_level": stop.stop_level.value,
                        "target_id": stop.target_id,
                        "reason": stop.reason.value,
                        "status": stop.status.value,
                        "triggered_at": stop.triggered_at.isoformat(),
                        "expires_at": (
                            stop.expires_at.isoformat() if stop.expires_at else None
                        ),
                        "orders_affected": stop.orders_affected,
                        "total_amount": stop.total_amount,
                    }
                )

        # 检查用户交易是否被停止
        is_trading_stopped = service.is_trading_stopped(user_id=current_user.id)

        return {
            "is_trading_stopped": is_trading_stopped,
            "user_active_stops": user_stops,
            "global_active_stops": [
                {
                    "stop_id": stop.stop_id,
                    "stop_level": stop.stop_level.value,
                    "target_id": stop.target_id,
                    "reason": stop.reason.value,
                    "triggered_at": stop.triggered_at.isoformat(),
                }
                for stop in active_stops
                if stop.stop_level == StopLevel.GLOBAL
            ],
            "last_updated": datetime.now().isoformat(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取紧急停止状态失败: {str(e)}",
        )


@router.post("/{stop_id}/cancel")
async def cancel_emergency_stop(
    stop_id: str,
    *,
    current_user: User = Depends(get_current_user),
    service: EmergencyStopService = Depends(get_emergency_service),
    reason: Optional[str] = Query(None, description="取消原因"),
):
    """取消紧急停止"""
    try:
        # 查找停止记录
        active_stops = await service.get_active_stops()
        stop_record = None

        for stop in active_stops:
            if stop.stop_id == stop_id:
                stop_record = stop
                break

        if not stop_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="紧急停止记录不存在"
            )

        # 权限检查
        metadata = stop_record.metadata or {}
        trigger_user_id = metadata.get("triggered_by_user_id")

        if trigger_user_id != current_user.id and not current_user.is_verified:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限不足，只有触发者或管理员可以取消",
            )

        # 取消紧急停止
        success = await service.cancel_emergency_stop(
            stop_id=stop_id, cancelled_by=f"user_{current_user.id}", reason=reason
        )

        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="取消紧急停止失败",
            )

        return {"success": True, "stop_id": stop_id, "message": "紧急停止已取消"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"取消紧急停止失败: {str(e)}",
        )


@router.post("/{stop_id}/resume")
async def resume_trading(
    stop_id: str,
    *,
    current_user: User = Depends(get_current_user),
    service: EmergencyStopService = Depends(get_emergency_service),
):
    """恢复交易"""
    try:
        # 查找停止记录
        active_stops = await service.get_active_stops()
        stop_record = None

        for stop in active_stops:
            if stop.stop_id == stop_id:
                stop_record = stop
                break

        if not stop_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="紧急停止记录不存在"
            )

        # 权限检查
        metadata = stop_record.metadata or {}
        trigger_user_id = metadata.get("triggered_by_user_id")

        if trigger_user_id != current_user.id and not current_user.is_verified:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限不足，只有触发者或管理员可以恢复",
            )

        # 恢复交易
        success = await service.resume_trading(
            stop_id=stop_id, resumed_by=f"user_{current_user.id}"
        )

        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="恢复交易失败"
            )

        return {"success": True, "stop_id": stop_id, "message": "交易已恢复"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"恢复交易失败: {str(e)}",
        )


@router.get("/history", response_model=List[Dict[str, Any]])
async def get_emergency_stop_history(
    current_user: User = Depends(get_current_user),
    service: EmergencyStopService = Depends(get_emergency_service),
    limit: int = Query(50, ge=1, le=200, description="返回记录数量"),
    offset: int = Query(0, ge=0, description="偏移量"),
):
    """获取紧急停止历史"""
    try:
        history = await service.get_stop_history(
            user_id=current_user.id, limit=limit, offset=offset
        )

        return [
            {
                "stop_id": record["stop_id"],
                "stop_level": record["stop_level"],
                "target_id": record["target_id"],
                "reason": record["reason"],
                "status": record["status"],
                "triggered_at": record["triggered_at"],
                "cancelled_at": record["cancelled_at"],
                "orders_affected": record["orders_affected"],
                "total_amount": record["total_amount"],
            }
            for record in history
        ]

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取紧急停止历史失败: {str(e)}",
        )


@router.get("/statistics")
async def get_emergency_stop_statistics(
    current_user: User = Depends(get_current_user),
    service: EmergencyStopService = Depends(get_emergency_service),
):
    """获取紧急停止统计信息"""
    try:
        stats = await service.get_statistics()

        # 过滤用户相关统计
        user_stats = stats["stats"].copy()
        user_stats["user_active_stops"] = len(
            [
                stop
                for stop in stats["active_stops"]
                if stop["metadata"].get("triggered_by_user_id") == current_user.id
            ]
        )

        return {
            "user_statistics": user_stats,
            "global_statistics": {
                "total_stops": stats["stats"]["total_stops"],
                "active_stops": stats["active_stops_count"],
                "by_level": stats["stats"]["by_level"],
                "by_reason": stats["stats"]["by_reason"],
            },
            "generated_at": datetime.now().isoformat(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取统计信息失败: {str(e)}",
        )


@router.post("/test")
async def test_emergency_stop(
    *,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    service: EmergencyStopService = Depends(get_emergency_service),
    test_level: str = Query("user", description="测试级别：user/account/symbol"),
):
    """测试紧急停止功能"""
    try:
        if test_level not in ["user", "account", "symbol"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="测试级别必须是 user、account 或 symbol",
            )

        # 创建测试配置
        if test_level == "user":
            target_id = current_user.id
            level = StopLevel.USER
        elif test_level == "account":
            # 使用用户的第一个账户
            account = (
                db.query(Account).filter(Account.user_id == current_user.id).first()
            )
            if not account:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="用户没有账户"
                )
            target_id = account.id
            level = StopLevel.ACCOUNT
        else:  # symbol
            target_id = "BTCUSDT"
            level = StopLevel.SYMBOL

        config = EmergencyStopConfig(
            stop_level=level,
            target_id=target_id,
            reason=StopReason.SYSTEM_ERROR,
            stop_all_orders=False,  # 测试时不实际停止订单
            cancel_pending_orders=False,
            pause_new_orders=False,
            max_stop_duration=60,  # 1分钟
            require_confirmation=False,
            metadata={"test": True, "triggered_by_user_id": current_user.id},
        )

        # 执行测试停止
        stop_id = await service.execute_emergency_stop(
            config=config, triggered_by=f"test_user_{current_user.id}"
        )

        # 立即取消测试停止
        await asyncio.sleep(2)  # 等待2秒让测试者看到效果

        await service.cancel_emergency_stop(
            stop_id=stop_id,
            cancelled_by=f"test_user_{current_user.id}",
            reason="测试完成",
        )

        return {
            "success": True,
            "test_stop_id": stop_id,
            "test_level": test_level,
            "message": f"紧急停止测试完成 (级别: {test_level})",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"紧急停止测试失败: {str(e)}",
        )


@router.get("/types")
async def get_emergency_stop_types():
    """获取紧急停止类型和原因"""
    return {
        "stop_levels": [level.value for level in StopLevel],
        "stop_reasons": [reason.value for reason in StopReason],
        "stop_statuses": [status.value for status in StopStatus],
        "descriptions": {
            "stop_levels": {
                "global": "全局停止 - 停止所有用户的交易",
                "user": "用户停止 - 停止特定用户的所有交易",
                "account": "账户停止 - 停止特定账户的交易",
                "symbol": "交易对停止 - 停止特定交易对的交易",
                "strategy": "策略停止 - 停止特定策略的交易",
            },
            "stop_reasons": {
                "manual": "手动触发",
                "risk_threshold": "风险阈值触发",
                "exchange_issue": "交易所问题",
                "system_error": "系统错误",
                "liquidation_risk": "清算风险",
                "connection_loss": "连接丢失",
                "suspicious_activity": "可疑活动",
                "compliance_issue": "合规问题",
            },
        },
    }