from fastapi import APIRouter, Depends, HTTPException, Query, Body
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import os
import tempfile
import zipfile
import asyncio
import json
from pathlib import Path

from ...core.database import get_db
from ...core.security import get_current_user
from ...models.user import User
from ...schemas.reports import (
    ReportRequest,
    ReportResponse,
    ReportTemplate,
    ReportStatistics,
    ReportFormat
)
from ...services.report_service import ReportService
from ...utils.logger import get_logger
from ...utils.file_handler import FileHandler

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/reports", tags=["reports"])

# 报告服务实例
report_service = ReportService()

@router.post("/generate", response_model=ReportResponse)
async def generate_report(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    report_request: ReportRequest
):
    """生成报告"""
    try:
        # 验证用户权限
        if report_request.user_id != current_user["id"] and not current_user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="权限不足")
        
        result = await report_service.generate_report(
            user_id=report_request.user_id,
            report_type=report_request.report_type,
            format=report_request.format,
            parameters=report_request.parameters,
            start_date=report_request.start_date,
            end_date=report_request.end_date,
            db=db
        )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"生成报告失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/generate-bulk", response_model=List[ReportResponse])
async def generate_bulk_reports(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    report_requests: List[ReportRequest]
):
    """批量生成报告"""
    try:
        # 验证用户权限
        for req in report_requests:
            if req.user_id != current_user["id"] and not current_user.get("is_admin", False):
                raise HTTPException(status_code=403, detail="权限不足")
        
        results = await report_service.generate_bulk_reports(
            report_requests=report_requests,
            db=db
        )
        
        return results
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"批量生成报告失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/list", response_model=List[ReportResponse])
async def list_reports(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    user_id: Optional[str] = Query(None, description="用户ID（管理员可指定其他用户）"),
    report_type: Optional[str] = Query(None, description="报告类型"),
    status: Optional[str] = Query(None, description="状态"),
    limit: int = Query(50, ge=1, le=200, description="限制数量"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    """获取报告列表"""
    try:
        # 非管理员只能查看自己的报告
        if not current_user.get("is_admin", False):
            user_id = current_user["id"]
        elif user_id is None:
            user_id = current_user["id"]
        
        reports = await report_service.list_reports(
            user_id=user_id,
            report_type=report_type,
            status=status,
            limit=limit,
            offset=offset
        )
        
        return reports
    except Exception as e:
        logger.error(f"获取报告列表失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/{report_id}", response_model=ReportResponse)
async def get_report(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    report_id: str
):
    """获取报告详情"""
    try:
        report = await report_service.get_report(report_id=report_id, db=db)
        
        if not report:
            raise HTTPException(status_code=404, detail="报告未找到")
        
        # 检查权限
        if report.user_id != current_user["id"] and not current_user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="权限不足")
        
        return report
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取报告详情失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/{report_id}/download")
async def download_report(
    *,
    current_user: dict = Depends(get_current_user),
    report_id: str
):
    """下载报告文件"""
    try:
        report = await report_service.get_report(report_id=report_id)
        
        if not report:
            raise HTTPException(status_code=404, detail="报告未找到")
        
        # 检查权限
        if report["user_id"] != current_user["id"] and not current_user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="权限不足")
        
        if report["status"] != "completed":
            raise HTTPException(status_code=400, detail="报告尚未完成生成")
        
        # 获取文件路径
        file_path = report["file_path"]
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="报告文件不存在")
        
        # 返回文件下载响应
        filename = f"{report['report_type']}_{report_id}.{report['format']}"
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type='application/octet-stream'
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"下载报告失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/{report_id}")
async def delete_report(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    report_id: str
):
    """删除报告"""
    try:
        # 检查报告是否存在且有权限
        report = await report_service.get_report(report_id=report_id, db=db)
        
        if not report:
            raise HTTPException(status_code=404, detail="报告未找到")
        
        if report.user_id != current_user["id"] and not current_user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="权限不足")
        
        # 删除报告
        result = await report_service.delete_report(
            report_id=report_id,
            db=db
        )
        
        return {"message": "报告删除成功", "deleted": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除报告失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/templates/list", response_model=List[ReportTemplate])
async def list_report_templates():
    """获取可用报告模板"""
    try:
        templates = await report_service.get_available_templates()
        return templates
    except Exception as e:
        logger.error(f"获取报告模板失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/templates/custom")
async def create_custom_template(
    *,
    current_user: dict = Depends(get_current_user),
    template_data: Dict[str, Any]
):
    """创建自定义报告模板"""
    try:
        # 只有管理员可以创建模板
        if not current_user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="权限不足")
        
        template = await report_service.create_custom_template(
            template_data=template_data,
            created_by=current_user["id"]
        )
        
        return template
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建自定义模板失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/statistics", response_model=ReportStatistics)
async def get_report_statistics(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    user_id: Optional[str] = Query(None, description="用户ID"),
    start_date: Optional[datetime] = Query(None, description="开始日期"),
    end_date: Optional[datetime] = Query(None, description="结束日期")
):
    """获取报告统计信息"""
    try:
        # 非管理员只能查看自己的统计
        if not current_user.get("is_admin", False):
            user_id = current_user["id"]
        elif user_id is None:
            user_id = current_user["id"]
        
        statistics = await report_service.get_report_statistics(
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
            db=db
        )
        
        return statistics
    except Exception as e:
        logger.error(f"获取报告统计失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/cleanup/old-reports")
async def cleanup_old_reports(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    days: int = Query(30, ge=1, le=365, description="保留天数"),
    confirm: bool = Query(False, description="确认删除")
):
    """清理过旧报告"""
    try:
        # 只有管理员可以执行清理操作
        if not current_user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="权限不足")
        
        if not confirm:
            raise HTTPException(status_code=400, detail="请确认删除操作")
        
        result = await report_service.cleanup_old_reports(
            days=days,
            db=db
        )
        
        return {
            "message": f"成功清理 {result} 个过旧报告",
            "deleted_count": result
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"清理过旧报告失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/export/csv")
async def export_reports_csv(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    user_id: Optional[str] = Query(None, description="用户ID"),
    report_type: Optional[str] = Query(None, description="报告类型"),
    start_date: Optional[datetime] = Query(None, description="开始日期"),
    end_date: Optional[datetime] = Query(None, description="结束日期")
):
    """导出报告列表为CSV"""
    try:
        # 非管理员只能导出自己的报告
        if not current_user.get("is_admin", False):
            user_id = current_user["id"]
        elif user_id is None:
            user_id = current_user["id"]
        
        csv_data = await report_service.export_reports_to_csv(
            user_id=user_id,
            report_type=report_type,
            start_date=start_date,
            end_date=end_date,
            db=db
        )
        
        # 创建临时CSV文件
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8')
        temp_file.write(csv_data)
        temp_file.close()
        
        return FileResponse(
            path=temp_file.name,
            filename=f"reports_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            media_type='text/csv'
        )
    except Exception as e:
        logger.error(f"导出CSV失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/schedule")
async def schedule_report(
    *,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
    schedule_data: Dict[str, Any]
):
    """定时报告设置"""
    try:
        # 只有管理员可以设置定时报告
        if not current_user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="权限不足")
        
        result = await report_service.schedule_report(
            schedule_data=schedule_data,
            created_by=current_user["id"],
            db=db
        )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"设置定时报告失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))