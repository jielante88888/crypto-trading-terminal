"""
持仓管理器模块
负责管理用户仓位、计算风险指标、执行止损止盈等功能
"""

import logging
import statistics
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import and_, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.src.core.models import (
    Account,
    AutoOrder,
    MarketData,
    Order,
    Position,
)
from backend.src.core.types import MarketType

logger = logging.getLogger(__name__)


class PositionRiskLevel(Enum):
    """仓位风险等级"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class PositionRiskMetrics:
    """仓位风险指标"""

    def __init__(self):
        self.unrealized_pnl: Optional[Decimal] = None
        self.unrealized_pnl_percent: Optional[Decimal] = None
        self.max_drawdown: Optional[Decimal] = None
        self.var_95: Optional[Decimal] = None
        self.cvar_95: Optional[Decimal] = None
        self.sharpe_ratio: Optional[float] = None
        self.concentration_risk: Optional[float] = None
        self.liquidity_score: Optional[float] = None
        self.risk_level: PositionRiskLevel = PositionRiskLevel.LOW
        self.margin_usage: Optional[Decimal] = None
        self.maintenance_margin: Optional[Decimal] = None
        self.timestamp: datetime = datetime.now()


class StopLossConfig:
    """止损配置"""

    def __init__(
        self,
        enabled: bool = False,
        stop_loss_percent: Optional[Decimal] = None,
        trailing_stop: bool = False,
        trailing_distance: Optional[Decimal] = None,
        emergency_stop: bool = False,
    ):
        self.enabled = enabled
        self.stop_loss_percent = stop_loss_percent
        self.trailing_stop = trailing_stop
        self.trailing_distance = trailing_distance
        self.emergency_stop = emergency_stop


class TakeProfitConfig:
    """止盈配置"""

    def __init__(
        self,
        enabled: bool = False,
        take_profit_percent: Optional[Decimal] = None,
        partial_close: bool = False,
        partial_close_percent: Optional[Decimal] = None,
    ):
        self.enabled = enabled
        self.take_profit_percent = take_profit_percent
        self.partial_close = partial_close
        self.partial_close_percent = partial_close_percent


class PositionManager:
    """仓位管理器"""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self._risk_callbacks: Dict[str, callable] = {}
        self._stop_loss_callbacks: Dict[str, callable] = {}
        self._take_profit_callbacks: Dict[str, callable] = {}

    async def get_user_positions(
        self, user_id: int, account_id: int, symbol: Optional[str] = None
    ) -> List[Position]:
        """获取用户仓位列表"""
        try:
            query = (
                select(Position)
                .join(Account)
                .where(
                    and_(
                        Account.user_id == user_id,
                        Position.account_id == account_id,
                        Position.is_active == True,
                    )
                )
                .order_by(Position.created_at.desc())
            )

            if symbol:
                query = query.where(Position.symbol == symbol)

            result = await self.db_session.execute(query)
            positions = result.scalars().all()

            logger.info(
                f"获取用户仓位成功: user_id={user_id}, account_id={account_id}, "
                f"symbol={symbol}, count={len(positions)}"
            )

            return list(positions)

        except Exception as e:
            logger.error(f"获取用户仓位失败: {e}")
            return []

    async def get_position_risk_metrics(
        self, position_id: int, user_id: int
    ) -> PositionRiskMetrics:
        """获取仓位风险指标"""
        try:
            # 获取仓位信息
            query = select(Position).where(Position.id == position_id)
            result = await self.db_session.execute(query)
            position = result.scalar_one_or_none()

            if not position:
                raise ValueError(f"仓位不存在: {position_id}")

            # 获取当前价格
            current_price = await self._get_current_price(
                position.symbol, position.account_id
            )

            # 计算风险指标
            risk_metrics = PositionRiskMetrics()

            # 未实现盈亏
            if current_price:
                if position.quantity > 0:  # 多仓
                    unrealized_pnl = (current_price - position.avg_price) * position.quantity
                else:  # 空仓
                    unrealized_pnl = (position.avg_price - current_price) * abs(position.quantity)

                risk_metrics.unrealized_pnl = unrealized_pnl
                risk_metrics.unrealized_pnl_percent = (
                    (unrealized_pnl / (abs(position.quantity) * position.avg_price)) * 100
                    if position.avg_price > 0
                    else Decimal("0")
                )

            # 计算其他风险指标
            risk_metrics.var_95 = await self._calculate_var_95(position.symbol, position.account_id)
            risk_metrics.cvar_95 = await self._calculate_cvar_95(position.symbol, position.account_id)
            risk_metrics.sharpe_ratio = await self._calculate_sharpe_ratio_for_position(position)
            risk_metrics.concentration_risk = await self._calculate_concentration_risk(
                user_id, position.account_id, position.symbol
            )
            risk_metrics.liquidity_score = await self._calculate_liquidity_score(position.symbol)
            risk_metrics.margin_usage = await self._calculate_margin_usage(position)
            risk_metrics.maintenance_margin = await self._calculate_maintenance_margin(position)

            # 确定风险等级
            risk_metrics.risk_level = self._determine_risk_level(
                position,
                risk_metrics.unrealized_pnl_percent or Decimal("0"),
                risk_metrics.concentration_risk or 0.0,
                risk_metrics.var_95,
            )

            logger.debug(f"计算仓位风险指标完成: position_id={position_id}")

            return risk_metrics

        except Exception as e:
            logger.error(f"计算仓位风险指标失败: {e}")
            return PositionRiskMetrics()

    async def calculate_portfolio_risk(
        self, user_id: int, account_id: int
    ) -> Dict[str, Any]:
        """计算投资组合风险"""
        try:
            # 获取所有仓位
            positions = await self.get_user_positions(user_id, account_id)

            if not positions:
                return {"total_value": Decimal("0"), "risk_level": PositionRiskLevel.LOW.value}

            # 计算各仓位风险和市场数据
            market_data = {}
            total_value = Decimal("0")

            for position in positions:
                current_price = await self._get_current_price(
                    position.symbol, position.account_id
                )

                if current_price:
                    position_value = abs(position.quantity) * current_price
                    total_value += position_value

                    market_data[position.symbol] = {
                        "position": position,
                        "current_price": current_price,
                        "position_value": position_value,
                        "quantity": abs(position.quantity),
                    }

            # 计算投资组合风险指标
            portfolio_var = await self._calculate_portfolio_var(market_data)
            portfolio_max_drawdown = await self._calculate_portfolio_max_drawdown(
                user_id, account_id
            )
            portfolio_sharpe = await self._calculate_portfolio_sharpe_ratio(market_data)
            total_concentration_risk = await self._calculate_total_concentration_risk(
                market_data, total_value
            )
            correlation_risk = await self._calculate_correlation_risk(positions)

            # 确定投资组合风险等级
            portfolio_risk_level = await self._calculate_portfolio_risk_level(
                user_id, account_id, market_data
            )

            # 计算风险分解
            risk_breakdown = await self._calculate_risk_breakdown(positions)

            result = {
                "total_value": total_value,
                "position_count": len(positions),
                "portfolio_var": portfolio_var,
                "portfolio_max_drawdown": portfolio_max_drawdown,
                "portfolio_sharpe_ratio": portfolio_sharpe,
                "total_concentration_risk": total_concentration_risk,
                "correlation_risk": correlation_risk,
                "risk_level": portfolio_risk_level.value,
                "risk_breakdown": risk_breakdown,
                "positions_detail": market_data,
            }

            logger.info(
                f"计算投资组合风险完成: user_id={user_id}, account_id={account_id}, "
                f"total_value={total_value}, risk_level={portfolio_risk_level.value}"
            )

            return result

        except Exception as e:
            logger.error(f"计算投资组合风险失败: {e}")
            return {"error": str(e)}

    async def _get_current_price(
        self, symbol: str, account_id: int
    ) -> Optional[Decimal]:
        """获取当前价格"""
        try:
            query = (
                select(MarketData.current_price)
                .where(
                    and_(
                        MarketData.symbol == symbol,
                        MarketData.account_id == account_id,
                    )
                )
                .order_by(desc(MarketData.timestamp))
                .limit(1)
            )

            result = await self.db_session.execute(query)
            price = result.scalar()

            return Decimal(str(price)) if price else None

        except Exception as e:
            logger.error(f"获取当前价格失败: {e}")
            return None

    async def _calculate_var_95(
        self, symbol: str, account_id: int, confidence_level: float = 0.95
    ) -> Optional[Decimal]:
        """计算95% VaR"""
        try:
            # 获取最近30天的价格数据
            since = datetime.now() - timedelta(days=30)

            query = (
                select(MarketData.current_price)
                .where(
                    and_(
                        MarketData.symbol == symbol,
                        MarketData.account_id == account_id,
                        MarketData.timestamp >= since,
                    )
                )
                .order_by(MarketData.timestamp)
            )

            result = await self.db_session.execute(query)
            prices = result.scalars().all()

            if len(prices) < 2:
                return None

            # 计算收益率
            returns = []
            for i in range(1, len(prices)):
                daily_return = (prices[i] - prices[i - 1]) / prices[i - 1]
                returns.append(daily_return)

            if not returns:
                return None

            # 排序并计算分位数
            returns.sort()
            index = int((1 - confidence_level) * len(returns))
            var_95 = abs(returns[index]) * 100  # 转换为百分比

            return Decimal(str(var_95))

        except Exception as e:
            logger.error(f"计算VaR失败: {e}")
            return None

    async def _calculate_cvar_95(
        self, symbol: str, account_id: int
    ) -> Optional[Decimal]:
        """计算期望损失(CVaR)"""
        try:
            # 获取最近30天的价格数据
            since = datetime.now() - timedelta(days=30)

            query = (
                select(MarketData.current_price)
                .where(
                    and_(
                        MarketData.symbol == symbol,
                        MarketData.account_id == account_id,
                        MarketData.timestamp >= since,
                    )
                )
                .order_by(MarketData.timestamp)
            )

            result = await self.db_session.execute(query)
            prices = result.scalars().all()

            if len(prices) < 2:
                return None

            # 计算收益率
            returns = []
            for i in range(1, len(prices)):
                daily_return = (prices[i] - prices[i - 1]) / prices[i - 1]
                returns.append(daily_return)

            if not returns:
                return None

            # 排序并计算5%尾部平均损失
            returns.sort()
            tail_size = int(len(returns) * 0.05)
            if tail_size == 0:
                return Decimal("0")

            tail_losses = returns[:tail_size]
            expected_shortfall = sum(abs(loss) for loss in tail_losses) / tail_size

            return expected_shortfall * 100

        except Exception as e:
            logger.error(f"计算CVaR失败: {e}")
            return None

    async def _calculate_sharpe_ratio_for_position(
        self, position: Position
    ) -> Optional[float]:
        """计算仓位夏普比率"""
        try:
            # 获取历史价格数据
            price_history = []
            
            # 获取当前价格
            current_price = await self._get_current_price(
                position.symbol, position.account_id
            )
            
            if not current_price:
                return None
            
            # 简化实现：使用平均价格作为历史价格
            price_history = [position.avg_price, current_price]
            
            return self._calculate_sharpe_ratio(price_history)

        except Exception as e:
            logger.error(f"计算夏普比率失败: {e}")
            return None

    def _calculate_max_drawdown(
        self, price_history: List[Decimal], initial_price: Decimal
    ) -> Decimal:
        """计算最大回撤"""
        if not price_history:
            return Decimal("0")

        max_drawdown = Decimal("0")
        peak = initial_price

        for price in price_history:
            if price > peak:
                peak = price

            drawdown = (peak - price) / peak * 100
            if drawdown > max_drawdown:
                max_drawdown = drawdown

        return max_drawdown

    def _calculate_sharpe_ratio(self, price_history: List[Decimal]) -> Optional[float]:
        """计算夏普比率"""
        if len(price_history) < 2:
            return None

        # 计算收益率
        returns = []
        for i in range(1, len(price_history)):
            daily_return = (price_history[i] - price_history[i - 1]) / price_history[
                i - 1
            ]
            returns.append(daily_return)

        if not returns:
            return None

        if len(returns) < 2:
            return 0.0

        # 计算平均收益率和标准差
        mean_return = statistics.mean(returns)
        std_return = statistics.stdev(returns)

        if std_return == 0:
            return 0.0

        # 年化夏普比率（假设252个交易日）
        sharpe_ratio = (mean_return / std_return) * (252**0.5)

        return round(sharpe_ratio, 4)

    async def _calculate_concentration_risk(
        self, user_id: int, account_id: int, symbol: str
    ) -> float:
        """计算单个仓位的集中度风险"""
        try:
            # 获取所有仓位
            positions = await self.get_user_positions(user_id, account_id)

            if not positions:
                return 0.0

            # 计算总持仓价值
            total_value = Decimal("0")
            symbol_value = Decimal("0")

            for position in positions:
                current_price = await self._get_current_price(
                    position.symbol, account_id
                )
                if current_price:
                    position_value = abs(position.quantity) * current_price
                    total_value += position_value

                    if position.symbol == symbol:
                        symbol_value = position_value

            if total_value == 0:
                return 0.0

            concentration = float(symbol_value / total_value)
            return min(concentration, 1.0)

        except Exception as e:
            logger.error(f"计算集中度风险失败: {e}")
            return 0.0

    async def _calculate_liquidity_score(self, symbol: str) -> float:
        """计算流动性分数"""
        try:
            # 获取最近的市场数据
            since = datetime.now() - timedelta(days=1)

            query = (
                select(MarketData.volume_24h)
                .where(and_(MarketData.symbol == symbol, MarketData.timestamp >= since))
                .order_by(desc(MarketData.timestamp))
                .limit(1)
            )

            result = await self.db_session.execute(query)
            volume = result.scalar()

            if not volume:
                return 0.5  # 默认中等流动性

            # 基于交易量计算流动性分数（归一化）
            volume_score = min(float(volume) / 1000000, 1.0)  # 假设100万为满分

            return volume_score

        except Exception as e:
            logger.error(f"计算流动性分数失败: {e}")
            return 0.5

    def _determine_risk_level(
        self,
        position: Position,
        unrealized_pnl_percent: Decimal,
        concentration_risk: float,
        var_95: Optional[Decimal],
    ) -> PositionRiskLevel:
        """确定风险等级"""
        risk_score = 0

        # 基于盈亏百分比评分
        pnl_percent = abs(unrealized_pnl_percent)
        if pnl_percent > 30:
            risk_score += 3
        elif pnl_percent > 20:
            risk_score += 2
        elif pnl_percent > 10:
            risk_score += 1

        # 基于集中度评分
        if concentration_risk > 0.5:
            risk_score += 3
        elif concentration_risk > 0.3:
            risk_score += 2
        elif concentration_risk > 0.2:
            risk_score += 1

        # 基于VaR评分
        if var_95 and var_95 > 15:
            risk_score += 3
        elif var_95 and var_95 > 10:
            risk_score += 2
        elif var_95 and var_95 > 5:
            risk_score += 1

        # 确定风险等级
        if risk_score >= 6:
            return PositionRiskLevel.CRITICAL
        elif risk_score >= 4:
            return PositionRiskLevel.HIGH
        elif risk_score >= 2:
            return PositionRiskLevel.MEDIUM
        else:
            return PositionRiskLevel.LOW

    async def _calculate_portfolio_risk_level(
        self, user_id: int, account_id: int, market_data: Dict[str, Any]
    ) -> PositionRiskLevel:
        """计算投资组合风险等级"""
        if not market_data:
            return PositionRiskLevel.LOW

        total_positions = len(market_data)
        risk_scores = []

        for symbol, data in market_data.items():
            # 获取仓位风险指标
            positions = await self.get_user_positions(user_id, account_id, symbol)
            if positions:
                position = positions[0]
                risk_metrics = await self.get_position_risk_metrics(
                    position.id, user_id
                )

                # 转换风险等级为数值
                level_scores = {
                    PositionRiskLevel.LOW: 1,
                    PositionRiskLevel.MEDIUM: 2,
                    PositionRiskLevel.HIGH: 3,
                    PositionRiskLevel.CRITICAL: 4,
                }
                risk_scores.append(level_scores.get(risk_metrics.risk_level, 2))

        if not risk_scores:
            return PositionRiskLevel.LOW

        # 计算加权平均风险分数
        avg_risk_score = statistics.mean(risk_scores)

        if avg_risk_score >= 3.5:
            return PositionRiskLevel.CRITICAL
        elif avg_risk_score >= 2.5:
            return PositionRiskLevel.HIGH
        elif avg_risk_score >= 1.5:
            return PositionRiskLevel.MEDIUM
        else:
            return PositionRiskLevel.LOW

    async def _calculate_correlation_risk(self, positions: List[Position]) -> float:
        """计算相关性风险"""
        # 简化实现：假设相同市场的币种有较高相关性
        if len(positions) < 2:
            return 0.0

        # 按市场类型分组
        market_groups = {}
        for position in positions:
            market_type = position.market_type.value
            if market_type not in market_groups:
                market_groups[market_type] = 0
            market_groups[market_type] += 1

        # 计算市场集中度
        total_positions = len(positions)
        max_group_size = max(market_groups.values()) if market_groups else 0

        concentration = max_group_size / total_positions if total_positions > 0 else 0

        return concentration

    async def _calculate_portfolio_var(
        self, market_data: Dict[str, Any]
    ) -> Optional[Decimal]:
        """计算投资组合VaR"""
        if not market_data:
            return None

        # 简化实现：假设所有资产的相关性为中等水平
        # 实际应该计算实际相关性
        total_var = Decimal("0")
        total_value = Decimal("0")

        for symbol, data in market_data.items():
            position_value = data["position_value"]
            total_value += position_value

            # 简化的VaR计算（假设每个资产Var为10%）
            var_contribution = position_value * Decimal("0.10")
            total_var += var_contribution**2  # 方差加法

        # 取平方根得到标准差，然后乘以1.65得到95%VaR
        portfolio_std = total_var ** Decimal("0.5")
        portfolio_var = portfolio_std * Decimal("1.65")

        return portfolio_var

    async def _calculate_portfolio_max_drawdown(
        self, user_id: int, account_id: int
    ) -> Decimal:
        """计算投资组合最大回撤"""
        # 简化实现：基于最近的市场数据
        try:
            # 获取最近30天的数据
            since = datetime.now() - timedelta(days=30)

            # 获取所有币种的价格数据
            symbols = await self._get_user_symbols(user_id, account_id)

            if not symbols:
                return Decimal("0")

            # 计算每个币种的回撤
            max_drawdowns = []
            for symbol in symbols:
                query = (
                    select(MarketData.current_price)
                    .where(
                        and_(
                            MarketData.symbol == symbol,
                            MarketData.account_id == account_id,
                            MarketData.timestamp >= since,
                        )
                    )
                    .order_by(MarketData.timestamp)
                )

                result = await self.db_session.execute(query)
                prices = result.scalars().all()

                if len(prices) > 1:
                    price_list = [Decimal(str(price)) for price in prices]
                    max_drawdown = self._calculate_max_drawdown(
                        price_list, price_list[0]
                    )
                    max_drawdowns.append(max_drawdown)

            if not max_drawdowns:
                return Decimal("0")

            # 返回平均最大回撤
            return Decimal(str(statistics.mean(max_drawdowns)))

        except Exception as e:
            logger.error(f"计算投资组合最大回撤失败: {e}")
            return Decimal("0")

    async def _calculate_portfolio_sharpe_ratio(
        self, market_data: Dict[str, Any]
    ) -> Optional[float]:
        """计算投资组合夏普比率"""
        # 简化实现：基于各资产夏普比率的加权平均
        if not market_data:
            return None

        # 这里应该获取实际的夏普比率数据
        # 简化返回中等夏普比率
        return 1.2

    async def _calculate_total_concentration_risk(
        self, market_data: Dict[str, Any], total_value: Decimal
    ) -> float:
        """计算总集中度风险"""
        if total_value == 0:
            return 0.0

        # 计算HHI（赫芬达尔-赫尔曼指数）
        hhi = Decimal("0")

        for symbol, data in market_data.items():
            weight = data["position_value"] / total_value
            hhi += weight**2

        # HHI越高，集中度风险越大
        concentration = float(hhi)
        return min(concentration, 1.0)

    async def _calculate_risk_breakdown(
        self, positions: List[Position]
    ) -> Dict[str, Any]:
        """计算风险分解"""
        breakdown = {"by_market_type": {}, "by_risk_level": {}, "by_symbol": {}}

        # 按市场类型分组
        for position in positions:
            market_type = position.market_type.value
            if market_type not in breakdown["by_market_type"]:
                breakdown["by_market_type"][market_type] = 0
            breakdown["by_market_type"][market_type] += 1

        return breakdown

    async def _calculate_margin_usage(self, position: Position) -> Optional[Decimal]:
        """计算保证金使用率"""
        if position.market_type != MarketType.FUTURES:
            return None

        # 简化实现：假设保证金率为10%
        position_value = abs(position.quantity) * position.avg_price
        required_margin = position_value * Decimal("0.1")  # 10%保证金

        # 这里应该获取实际可用保证金
        available_margin = Decimal("10000")  # 假设可用保证金

        if available_margin > 0:
            return (required_margin / available_margin) * 100

        return None

    async def _calculate_maintenance_margin(
        self, position: Position
    ) -> Optional[Decimal]:
        """计算维持保证金"""
        if position.market_type != MarketType.FUTURES:
            return None

        # 简化实现：假设维持保证金率为5%
        position_value = abs(position.quantity) * position.avg_price
        maintenance_margin = position_value * Decimal("0.05")  # 5%维持保证金

        return maintenance_margin

    def _calculate_stop_price(
        self, position: Position, stop_percent: Decimal
    ) -> Decimal:
        """计算止损价格"""
        if position.quantity > 0:  # 多仓
            return position.avg_price * (1 - stop_percent / 100)
        else:  # 空仓
            return position.avg_price * (1 + stop_percent / 100)

    def _should_trigger_stop_loss(
        self, position: Position, current_price: Decimal, stop_price: Decimal
    ) -> bool:
        """判断是否触发止损"""
        if position.quantity > 0:  # 多仓
            return current_price <= stop_price
        else:  # 空仓
            return current_price >= stop_price

    async def _calculate_trailing_stop_price(
        self, position: Position, trailing_distance: Decimal
    ) -> Decimal:
        """计算跟踪止损价格"""
        # 这里需要获取历史最高价/最低价
        # 简化实现：使用入场价格
        return self._calculate_stop_price(position, trailing_distance)

    def _should_trigger_trailing_stop(
        self, position: Position, current_price: Decimal, trailing_stop_price: Decimal
    ) -> bool:
        """判断是否触发跟踪止损"""
        return self._should_trigger_stop_loss(
            position, current_price, trailing_stop_price
        )

    def _calculate_take_profit_price(
        self, position: Position, profit_percent: Decimal
    ) -> Decimal:
        """计算止盈价格"""
        if position.quantity > 0:  # 多仓
            return position.avg_price * (1 + profit_percent / 100)
        else:  # 空仓
            return position.avg_price * (1 - profit_percent / 100)

    def _should_trigger_take_profit(
        self, position: Position, current_price: Decimal, target_price: Decimal
    ) -> bool:
        """判断是否触发止盈"""
        if position.quantity > 0:  # 多仓
            return current_price >= target_price
        else:  # 空仓
            return current_price <= target_price

    async def _get_stop_loss_config(self, position_id: int) -> StopLossConfig:
        """获取止损配置"""
        # 这里应该从数据库或配置中获取
        # 简化返回默认配置
        return StopLossConfig(
            enabled=True,
            stop_loss_percent=Decimal("5.0"),
            trailing_stop=False,
            emergency_stop=False,
        )

    async def _get_take_profit_config(self, position_id: int) -> TakeProfitConfig:
        """获取止盈配置"""
        # 这里应该从数据库或配置中获取
        # 简化返回默认配置
        return TakeProfitConfig(
            enabled=True, take_profit_percent=Decimal("10.0"), partial_close=False
        )

    async def _create_risk_alert(
        self,
        user_id: int,
        account_id: int,
        position_id: Optional[int],
        symbol: str,
        severity: str,
        message: str,
        alert_type: str,
        details: Dict[str, Any],
    ) -> Dict[str, Any]:
        """创建风险警告"""
        # 这里应该调用风险检查器创建警告
        alert_data = {
            "user_id": user_id,
            "account_id": account_id,
            "position_id": position_id,
            "symbol": symbol,
            "severity": severity,
            "message": message,
            "alert_type": alert_type,
            "details": details,
            "timestamp": datetime.now(),
        }

        logger.warning(f"创建风险警告: {message}", extra=alert_data)

        return alert_data

    async def _get_user_symbols(self, user_id: int, account_id: int) -> List[str]:
        """获取用户交易的所有币种"""
        query = (
            select(Position.symbol)
            .join(Account)
            .where(
                and_(
                    Account.user_id == user_id,
                    Position.account_id == account_id,
                    Position.is_active == True,
                )
            )
            .distinct()
        )

        result = await self.db_session.execute(query)
        return result.scalars().all()