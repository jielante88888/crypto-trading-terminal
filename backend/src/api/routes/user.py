"""
用户API路由
提供用户管理、认证和账户信息的REST API接口
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Depends, Body
from pydantic import BaseModel, Field, EmailStr
from sqlalchemy.orm import Session

from ...storage.database import get_db_session
from ...storage.models import User, Account
from ...utils.exceptions import ValidationError

router = APIRouter()


# Pydantic模型定义
class UserCreate(BaseModel):
    """用户创建模型"""

    email: EmailStr = Field(..., description="邮箱地址")
    username: str = Field(..., min_length=3, max_length=50, description="用户名")


class UserResponse(BaseModel):
    """用户响应模型"""

    id: int
    email: str
    username: str
    is_active: bool
    is_verified: bool
    created_at: datetime
    updated_at: datetime


class AccountCreate(BaseModel):
    """账户创建模型"""

    exchange: str = Field(..., description="交易所名称")
    account_type: str = Field(..., description="账户类型")
    api_key: Optional[str] = Field(None, description="API密钥")
    api_secret: Optional[str] = Field(None, description="密钥")
    passphrase: Optional[str] = Field(None, description="密码短语")
    is_testnet: bool = Field(True, description="是否使用测试环境")


class AccountResponse(BaseModel):
    """账户响应模型"""

    id: int
    exchange: str
    account_type: str
    is_active: bool
    is_testnet: bool
    created_at: datetime
    updated_at: datetime


class UserProfile(BaseModel):
    """用户资料模型"""

    username: Optional[str] = Field(None, min_length=3, max_length=50)
    email: Optional[EmailStr] = None


class PasswordChange(BaseModel):
    """密码修改模型"""

    old_password: str = Field(..., description="旧密码")
    new_password: str = Field(..., min_length=6, description="新密码")


# 用户管理API
@router.post("/users", response_model=UserResponse)
async def create_user(
    user_data: UserCreate = Body(...), session: Session = Depends(get_db_session)
):
    """创建新用户"""
    try:
        # 检查邮箱是否已存在
        existing_user = (
            session.query(User).filter(User.email == user_data.email).first()
        )
        if existing_user:
            raise ValidationError("邮箱已被注册")

        # 检查用户名是否已存在
        existing_user = (
            session.query(User).filter(User.username == user_data.username).first()
        )
        if existing_user:
            raise ValidationError("用户名已被使用")

        # 创建用户
        user = User(
            email=user_data.email,
            username=user_data.username,
            is_active=True,
            is_verified=False,
        )

        session.add(user)
        session.commit()
        session.refresh(user)

        # TODO: 发送验证邮件
        # await send_verification_email(user.email)

        return UserResponse(
            id=user.id,
            email=user.email,
            username=user.username,
            is_active=user.is_active,
            is_verified=user.is_verified,
            created_at=user.created_at,
            updated_at=user.updated_at,
        )

    except ValidationError:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"创建用户失败: {str(e)}")


@router.get("/users", response_model=List[UserResponse])
async def get_users(
    skip: int = Query(0, ge=0, description="跳过记录数"),
    limit: int = Query(100, ge=1, le=1000, description="返回记录数"),
    session: Session = Depends(get_db_session),
):
    """获取用户列表"""
    try:
        users = session.query(User).offset(skip).limit(limit).all()

        return [
            UserResponse(
                id=user.id,
                email=user.email,
                username=user.username,
                is_active=user.is_active,
                is_verified=user.is_verified,
                created_at=user.created_at,
                updated_at=user.updated_at,
            )
            for user in users
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取用户列表失败: {str(e)}")


@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(user_id: int, session: Session = Depends(get_db_session)):
    """获取用户详情"""
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")

        return UserResponse(
            id=user.id,
            email=user.email,
            username=user.username,
            is_active=user.is_active,
            is_verified=user.is_verified,
            created_at=user.created_at,
            updated_at=user.updated_at,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取用户详情失败: {str(e)}")


@router.put("/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: int,
    user_data: UserProfile = Body(...),
    session: Session = Depends(get_db_session),
):
    """更新用户信息"""
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")

        # 检查用户名是否被其他用户使用
        if user_data.username and user_data.username != user.username:
            existing_user = (
                session.query(User)
                .filter(User.username == user_data.username, User.id != user_id)
                .first()
            )
            if existing_user:
                raise ValidationError("用户名已被使用")
            user.username = user_data.username

        # 更新邮箱
        if user_data.email and user_data.email != user.email:
            # 检查邮箱是否已被其他用户使用
            existing_user = (
                session.query(User)
                .filter(User.email == user_data.email, User.id != user_id)
                .first()
            )
            if existing_user:
                raise ValidationError("邮箱已被使用")
            user.email = user_data.email
            user.is_verified = False  # 邮箱变更后需要重新验证

        session.commit()
        session.refresh(user)

        return UserResponse(
            id=user.id,
            email=user.email,
            username=user.username,
            is_active=user.is_active,
            is_verified=user.is_verified,
            created_at=user.created_at,
            updated_at=user.updated_at,
        )

    except HTTPException:
        raise
    except ValidationError:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"更新用户信息失败: {str(e)}")


@router.delete("/users/{user_id}")
async def delete_user(user_id: int, session: Session = Depends(get_db_session)):
    """删除用户"""
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")

        # 软删除：设置为非活跃状态
        user.is_active = False
        session.commit()

        # TODO: 删除相关的交易所账户
        # TODO: 发送删除确认邮件

        return {"message": "用户已删除"}

    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"删除用户失败: {str(e)}")


# 账户管理API
@router.post("/accounts", response_model=AccountResponse)
async def create_account(
    user_id: int,
    account_data: AccountCreate = Body(...),
    session: Session = Depends(get_db_session),
):
    """创建交易所账户"""
    try:
        # 验证用户存在
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")

        # 检查账户是否已存在
        existing_account = (
            session.query(Account)
            .filter(
                Account.user_id == user_id,
                Account.exchange == account_data.exchange,
                Account.account_type == account_data.account_type,
            )
            .first()
        )

        if existing_account:
            raise ValidationError("该账户已存在")

        # 创建账户
        account = Account(
            user_id=user_id,
            exchange=account_data.exchange,
            account_type=account_data.account_type,
            api_key=account_data.api_key,
            api_secret=account_data.api_secret,
            passphrase=account_data.passphrase,
            is_active=True,
            is_testnet=account_data.is_testnet,
        )

        session.add(account)
        session.commit()
        session.refresh(account)

        return AccountResponse(
            id=account.id,
            exchange=account.exchange,
            account_type=account.account_type,
            is_active=account.is_active,
            is_testnet=account.is_testnet,
            created_at=account.created_at,
            updated_at=account.updated_at,
        )

    except HTTPException:
        raise
    except ValidationError:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"创建交易所账户失败: {str(e)}")


@router.get("/accounts", response_model=List[AccountResponse])
async def get_user_accounts(
    user_id: Optional[int] = Query(None, description="用户ID"),
    exchange: Optional[str] = Query(None, description="交易所名称"),
    session: Session = Depends(get_db_session),
):
    """获取用户账户列表"""
    try:
        query = session.query(Account)

        if user_id:
            query = query.filter(Account.user_id == user_id)
        if exchange:
            query = query.filter(Account.exchange == exchange)

        accounts = query.all()

        return [
            AccountResponse(
                id=account.id,
                exchange=account.exchange,
                account_type=account.account_type,
                is_active=account.is_active,
                is_testnet=account.is_testnet,
                created_at=account.created_at,
                updated_at=account.updated_at,
            )
            for account in accounts
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取账户列表失败: {str(e)}")


@router.get("/accounts/{account_id}", response_model=AccountResponse)
async def get_account(account_id: int, session: Session = Depends(get_db_session)):
    """获取账户详情"""
    try:
        account = session.query(Account).filter(Account.id == account_id).first()
        if not account:
            raise HTTPException(status_code=404, detail="账户不存在")

        return AccountResponse(
            id=account.id,
            exchange=account.exchange,
            account_type=account.account_type,
            is_active=account.is_active,
            is_testnet=account.is_testnet,
            created_at=account.created_at,
            updated_at=account.updated_at,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取账户详情失败: {str(e)}")


@router.put("/accounts/{account_id}", response_model=AccountResponse)
async def update_account(
    account_id: int,
    account_data: AccountCreate = Body(...),
    session: Session = Depends(get_db_session),
):
    """更新交易所账户"""
    try:
        account = session.query(Account).filter(Account.id == account_id).first()
        if not account:
            raise HTTPException(status_code=404, detail="账户不存在")

        # 更新账户信息
        account.exchange = account_data.exchange
        account.account_type = account_data.account_type
        account.api_key = account_data.api_key
        account.api_secret = account_data.api_secret
        account.passphrase = account_data.passphrase
        account.is_testnet = account_data.is_testnet

        session.commit()
        session.refresh(account)

        return AccountResponse(
            id=account.id,
            exchange=account.exchange,
            account_type=account.account_type,
            is_active=account.is_active,
            is_testnet=account.is_testnet,
            created_at=account.created_at,
            updated_at=account.updated_at,
        )

    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"更新交易所账户失败: {str(e)}")


@router.delete("/accounts/{account_id}")
async def delete_account(account_id: int, session: Session = Depends(get_db_session)):
    """删除交易所账户"""
    try:
        account = session.query(Account).filter(Account.id == account_id).first()
        if not account:
            raise HTTPException(status_code=404, detail="账户不存在")

        # 软删除：设置为非活跃状态
        account.is_active = False
        session.commit()

        return {"message": "交易所账户已删除"}

    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"删除交易所账户失败: {str(e)}")


# 用户统计API
@router.get("/users/{user_id}/statistics")
async def get_user_statistics(user_id: int, session: Session = Depends(get_db_session)):
    """获取用户统计信息"""
    try:
        # 验证用户存在
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")

        # 获取账户数量
        accounts_count = (
            session.query(Account)
            .filter(Account.user_id == user_id, Account.is_active == True)
            .count()
        )

        # TODO: 获取其他统计信息
        # - 订单数量
        # - 交易量
        # - 持仓数量
        # - 盈亏统计

        return {
            "user_id": user_id,
            "accounts_count": accounts_count,
            "orders_count": 0,
            "trading_volume": 0.0,
            "positions_count": 0,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取用户统计失败: {str(e)}")


# 验证API
@router.post("/users/{user_id}/verify-email")
async def verify_user_email(user_id: int, session: Session = Depends(get_db_session)):
    """验证用户邮箱"""
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")

        if user.is_verified:
            return {"message": "邮箱已验证"}

        # TODO: 验证邮箱逻辑
        # user.is_verified = True
        # session.commit()

        return {"message": "邮箱验证成功"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"验证邮箱失败: {str(e)}")


@router.post("/users/{user_id}/change-password")
async def change_password(
    user_id: int,
    password_data: PasswordChange = Body(...),
    session: Session = Depends(get_db_session),
):
    """修改密码"""
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")

        # TODO: 验证旧密码
        # TODO: 加密新密码并保存
        # TODO: 退出所有活跃会话

        return {"message": "密码修改成功"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"修改密码失败: {str(e)}")