from fastapi import APIRouter, Depends, HTTPException, Query, Path, Body
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import asyncio
import json

from ...core.database import get_db
from ...core.security import get_current_user
from ...models.order_history import OrderHistoryRecord, OrderExecutionStatus
from ...schemas.order_history import (
    OrderHistoryResponse,
    OrderExecutionResponse,
    OrderHistoryFilter,
    OrderExecutionStatusUpdate,
    OrderStatisticsResponse
)
from ...services.order_history_service import OrderHistoryService
from ...utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/order-history", tags=["order-history"])

@router.post("/", response_model=OrderHistoryResponse)
async def create_order_history_record(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    record_data: OrderExecutionResponse
):
    """创建订单执行记录"""
    try:
        order_service = OrderHistoryService(db)
        result = await order_service.create_order_history_record(
            user_id=current_user["id"],
            order_data=record_data
        )
        return result
    except Exception as e:
        logger.error(f"创建订单历史记录失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/", response_model=List[OrderHistoryResponse])
async def get_order_history(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    symbol: Optional[str] = Query(None, description="交易对"),
    exchange: Optional[str] = Query(None, description="交易所"),
    order_type: Optional[str] = Query(None, description="订单类型"),
    status: Optional[str] = Query(None, description="订单状态"),
    side: Optional[str] = Query(None, description="买卖方向"),
    start_time: Optional[datetime] = Query(None, description="开始时间"),
    end_time: Optional[datetime] = Query(None, description="结束时间"),
    page: int = Query(1, ge=1, description="页码"),
    limit: int = Query(50, ge=1, le=500, description="每页数量"),
    sort_by: str = Query("created_at", description="排序字段"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="排序方向")
):
    """获取订单执行历史"""
    try:
        order_service = OrderHistoryService(db)
        filters = OrderHistoryFilter(
            symbol=symbol,
            exchange=exchange,
            order_type=order_type,
            status=status,
            side=side,
            start_time=start_time,
            end_time=end_time
        )
        
        result = await order_service.get_order_history(
            user_id=current_user["id"],
            filters=filters,
            page=page,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order
        )
        
        return result
    except Exception as e:
        logger.error(f"获取订单历史失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/{order_id}", response_model=OrderHistoryResponse)
async def get_order_by_id(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    order_id: str = Path(..., description="订单ID")
):
    """根据订单ID获取订单执行信息"""
    try:
        order_service = OrderHistoryService(db)
        result = await order_service.get_order_by_id(
            user_id=current_user["id"],
            order_id=order_id
        )
        
        if not result:
            raise HTTPException(status_code=404, detail="订单未找到")
            
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取订单信息失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/{order_id}/status", response_model=OrderExecutionResponse)
async def update_order_status(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    order_id: str = Path(..., description="订单ID"),
    status_update: OrderExecutionStatusUpdate
):
    """更新订单执行状态"""
    try:
        order_service = OrderHistoryService(db)
        result = await order_service.update_order_status(
            user_id=current_user["id"],
            order_id=order_id,
            status_update=status_update
        )
        
        if not result:
            raise HTTPException(status_code=404, detail="订单未找到")
            
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新订单状态失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/statistics/summary", response_model=OrderStatisticsResponse)
async def get_order_statistics(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    start_time: Optional[datetime] = Query(None, description="统计开始时间"),
    end_time: Optional[datetime] = Query(None, description="统计结束时间"),
    symbol: Optional[str] = Query(None, description="交易对")
):
    """获取订单统计信息"""
    try:
        order_service = OrderHistoryService(db)
        result = await order_service.get_order_statistics(
            user_id=current_user["id"],
            start_time=start_time,
            end_time=end_time,
            symbol=symbol
        )
        
        return result
    except Exception as e:
        logger.error(f"获取订单统计失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/monitor/active", response_model=List[OrderHistoryResponse])
async def get_active_orders(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """获取活跃订单监控"""
    try:
        order_service = OrderHistoryService(db)
        result = await order_service.get_active_orders(
            user_id=current_user["id"]
        )
        
        return result
    except Exception as e:
        logger.error(f"获取活跃订单失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/cleanup/old-records")
async def cleanup_old_records(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    days: int = Query(90, ge=1, le=365, description="保留天数")
):
    """清理过旧订单记录"""
    try:
        # 只有管理员可以执行清理操作
        if not current_user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="权限不足")
            
        order_service = OrderHistoryService(db)
        result = await order_service.cleanup_old_records(
            days=days
        )
        
        return {"message": f"成功清理 {result} 条旧记录"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"清理旧记录失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# WebSocket端点用于实时订单状态更新
@router.get("/ws/order-status/{user_id}")
async def order_status_websocket(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    user_id: str = Path(..., description="用户ID")
):
    """实时订单状态更新WebSocket"""
    # WebSocket实现需要在main.py中配置
    if current_user["id"] != user_id and not current_user.get("is_admin", False):
        raise HTTPException(status_code=403, detail="权限不足")
    
    # 这里应该返回WebSocket端点信息
    return {
        "websocket_url": f"/ws/order-status/{user_id}",
        "message": "WebSocket连接端点已配置"
    }