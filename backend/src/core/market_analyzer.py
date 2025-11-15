"""
市场分析器
负责市场数据分析和异常检测
"""

import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any, AsyncGenerator
from dataclasses import dataclass, asdict
from enum import Enum
from collections import deque
import statistics

import structlog
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from ..adapters.base import MarketData, OrderBook, Trade, Exchange
from ..utils.exceptions import MarketAnalyzerError

logger = structlog.get_logger(__name__)


class HealthStatus(Enum):
    """交易所健康状态"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    DOWN = "down"


class AlertLevel(Enum):
    """预警级别"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class PerformanceMetrics:
    """性能指标"""
    exchange: str
    response_time: float
    success_rate: float
    error_count: int
    total_requests: int
    timestamp: datetime

    @property
    def is_healthy(self) -> bool:
        """检查是否健康"""
        return (
            self.response_time < 2.0
            and self.success_rate > 0.95
            and self.error_count < 10
        )


@dataclass
class HealthCheckResult:
    """健康检查结果"""
    exchange: str
    status: HealthStatus
    latency: float
    success_rate: float
    error_message: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


@dataclass
class PositionAlert:
    """持仓预警"""
    id: str
    type: AlertLevel
    message: str
    data: Dict[str, Any]
    timestamp: datetime


class ExchangeHealthMonitor:
    """交易所健康监控"""

    def __init__(self):
        self.exchanges: Dict[str, Dict[str, Any]] = {}
        self.performance_history: Dict[str, deque] = {}
        self.health_checks: Dict[str, HealthCheckResult] = {}
        self.monitoring_active = False
        self._monitoring_task: Optional[asyncio.Task] = None

        # 健康阈值配置
        self.thresholds = {
            "response_time_warning": 1.0,
            "response_time_critical": 2.0,
            "success_rate_warning": 0.95,
            "success_rate_critical": 0.90,
            "error_rate_warning": 0.05,
            "error_rate_critical": 0.10,
        }

    async def start_monitoring(self):
        """启动监控"""
        if self.monitoring_active:
            return

        self.monitoring_active = True
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())

        logger.info("交易所健康监控已启动")

    async def stop_monitoring(self):
        """停止监控"""
        self.monitoring_active = False

        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        logger.info("交易所健康监控已停止")

    async def _monitoring_loop(self):
        """监控循环"""
        while self.monitoring_active:
            try:
                await self._check_all_exchanges()
                await asyncio.sleep(30)  # 每30秒检查一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                await asyncio.sleep(10)

    async def _check_all_exchanges(self):
        """检查所有交易所"""
        for exchange_name in list(self.exchanges.keys()):
            try:
                await self._check_exchange_health(exchange_name)
            except Exception as e:
                logger.error(f"检查交易所健康状态失败 {exchange_name}: {e}")

    async def _check_exchange_health(self, exchange_name: str):
        """检查单个交易所健康状态"""
        try:
            # 获取性能数据
            metrics = await self._get_exchange_metrics(exchange_name)

            if not metrics:
                # 设置为离线状态
                self.health_checks[exchange_name] = HealthCheckResult(
                    exchange=exchange_name,
                    status=HealthStatus.DOWN,
                    latency=float('inf'),
                    success_rate=0.0,
                    error_message="无法获取性能数据"
                )
                return

            # 计算健康状态
            status = self._calculate_health_status(metrics)
            latency = metrics.response_time
            success_rate = metrics.success_rate

            # 记录健康检查结果
            self.health_checks[exchange_name] = HealthCheckResult(
                exchange=exchange_name,
                status=status,
                latency=latency,
                success_rate=success_rate
            )

            # 添加到历史记录
            if exchange_name not in self.performance_history:
                self.performance_history[exchange_name] = deque(maxlen=100)

            self.performance_history[exchange_name].append(metrics)

            logger.debug(f"交易所健康状态 {exchange_name}: {status.value}")

        except Exception as e:
            logger.error(f"检查交易所健康状态异常 {exchange_name}: {e}")

    async def _get_exchange_metrics(self, exchange_name: str) -> Optional[PerformanceMetrics]:
        """获取交易所性能指标"""
        try:
            exchange_config = self.exchanges.get(exchange_name)
            if not exchange_config:
                return None

            # 这里应该实现真实的健康检查逻辑
            # 当前返回模拟数据用于测试
            import random
            import time

            start_time = time.time()
            
            # 模拟API调用延迟
            await asyncio.sleep(random.uniform(0.1, 0.5))
            
            response_time = time.time() - start_time
            success_rate = random.uniform(0.85, 0.99)
            error_count = random.randint(0, 5)
            total_requests = random.randint(50, 200)

            return PerformanceMetrics(
                exchange=exchange_name,
                response_time=response_time,
                success_rate=success_rate,
                error_count=error_count,
                total_requests=total_requests,
                timestamp=datetime.utcnow()
            )

        except Exception as e:
            logger.error(f"获取交易所性能指标失败 {exchange_name}: {e}")
            return None

    def _calculate_health_status(self, metrics: PerformanceMetrics) -> HealthStatus:
        """计算健康状态"""
        # 检查响应时间
        if metrics.response_time >= self.thresholds["response_time_critical"]:
            return HealthStatus.CRITICAL
        elif metrics.response_time >= self.thresholds["response_time_warning"]:
            if metrics.success_rate < self.thresholds["success_rate_critical"]:
                return HealthStatus.CRITICAL
            elif metrics.success_rate < self.thresholds["success_rate_warning"]:
                return HealthStatus.WARNING
            else:
                return HealthStatus.WARNING

        # 检查成功率
        if metrics.success_rate < self.thresholds["success_rate_critical"]:
            return HealthStatus.CRITICAL
        elif metrics.success_rate < self.thresholds["success_rate_warning"]:
            return HealthStatus.WARNING

        # 检查错误率
        error_rate = metrics.error_count / max(metrics.total_requests, 1)
        if error_rate >= self.thresholds["error_rate_critical"]:
            return HealthStatus.CRITICAL
        elif error_rate >= self.thresholds["error_rate_warning"]:
            return HealthStatus.WARNING

        return HealthStatus.HEALTHY

    def add_exchange(self, exchange_name: str, config: Dict[str, Any]):
        """添加交易所"""
        self.exchanges[exchange_name] = config
        logger.info(f"添加交易所监控: {exchange_name}")

    def remove_exchange(self, exchange_name: str):
        """移除交易所"""
        if exchange_name in self.exchanges:
            del self.exchanges[exchange_name]
        if exchange_name in self.performance_history:
            del self.performance_history[exchange_name]
        if exchange_name in self.health_checks:
            del self.health_checks[exchange_name]

        logger.info(f"移除交易所监控: {exchange_name}")

    def get_health_status(self) -> Dict[str, HealthCheckResult]:
        """获取健康状态"""
        return self.health_checks.copy()

    def get_performance_history(self, exchange_name: str) -> List[PerformanceMetrics]:
        """获取性能历史"""
        return list(self.performance_history.get(exchange_name, []))


class MarketAnalyzer:
    """市场分析器"""

    def __init__(self):
        self.health_monitor = ExchangeHealthMonitor()
        
        # 数据存储
        self.market_data_cache: Dict[str, deque] = {}
        self.order_book_cache: Dict[str, deque] = {}
        self.trade_cache: Dict[str, deque] = {}
        
        # 分析配置
        self.max_cache_size = 1000
        self.analysis_interval = 60  # 秒
        
        # 异常检测器
        self.anomaly_detectors = {
            "response_time": self._detect_response_time_anomalies,
            "success_rate": self._detect_success_rate_anomalies,
            "data_consistency": self._detect_data_consistency_anomalies
        }
        
        # 分析状态
        self.is_running = False
        self._analysis_task: Optional[asyncio.Task] = None
        
        # 预警回调
        self.alert_callbacks: List[Callable] = []
        
        logger.info("市场分析器初始化完成")

    async def start_analysis(self):
        """启动分析"""
        if self.is_running:
            return

        self.is_running = True
        
        # 启动健康监控
        await self.health_monitor.start_monitoring()
        
        # 启动分析循环
        self._analysis_task = asyncio.create_task(self._analysis_loop())

        logger.info("市场分析器已启动")

    async def stop_analysis(self):
        """停止分析"""
        self.is_running = False
        
        # 停止分析循环
        if self._analysis_task:
            self._analysis_task.cancel()
            try:
                await self._analysis_task
            except asyncio.CancelledError:
                pass
        
        # 停止健康监控
        await self.health_monitor.stop_monitoring()

        logger.info("市场分析器已停止")

    async def _analysis_loop(self):
        """分析循环"""
        while self.is_running:
            try:
                await self._perform_analysis()
                await asyncio.sleep(self.analysis_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"分析循环异常: {e}")
                await asyncio.sleep(10)

    async def _perform_analysis(self):
        """执行分析"""
        try:
            # 执行异常检测
            await self._run_anomaly_detection()
            
            # 更新健康状态
            await self._update_health_status()
            
            # 生成分析报告
            await self._generate_analysis_report()

        except Exception as e:
            logger.error(f"执行分析失败: {e}")

    async def _run_anomaly_detection(self):
        """运行异常检测"""
        for detector_name, detector_func in self.anomaly_detectors.items():
            try:
                anomalies = await detector_func()
                if anomalies:
                    await self._handle_anomalies(detector_name, anomalies)
            except Exception as e:
                logger.error(f"异常检测器执行失败 {detector_name}: {e}")

    async def _detect_response_time_anomalies(self) -> List[Dict[str, Any]]:
        """检测响应时间异常"""
        anomalies = []
        
        for exchange_name, history in self.health_monitor.performance_history.items():
            if len(history) < 10:
                continue
                
            # 获取最近的响应时间数据
            recent_times = [m.response_time for m in list(history)[-20:]]
            
            if len(recent_times) < 10:
                continue
                
            # 计算统计指标
            mean_time = statistics.mean(recent_times)
            std_time = statistics.stdev(recent_times) if len(recent_times) > 1 else 0
            
            # 检测异常
            if std_time > 0:
                z_scores = [(t - mean_time) / std_time for t in recent_times]
                
                for i, z_score in enumerate(z_scores):
                    if abs(z_score) > 2.5:  # 2.5倍标准差阈值
                        anomaly = {
                            "type": "response_time_spike",
                            "exchange": exchange_name,
                            "value": recent_times[i],
                            "z_score": z_score,
                            "timestamp": datetime.utcnow(),
                            "severity": "high" if abs(z_score) > 3.0 else "medium"
                        }
                        anomalies.append(anomaly)
        
        return anomalies

    async def _detect_success_rate_anomalies(self) -> List[Dict[str, Any]]:
        """检测成功率异常"""
        anomalies = []
        
        for exchange_name, history in self.health_monitor.performance_history.items():
            if len(history) < 5:
                continue
                
            # 获取最近的成功率数据
            recent_rates = [m.success_rate for m in list(history)[-10:]]
            
            # 检测成功率下降
            if len(recent_rates) >= 5:
                recent_avg = statistics.mean(recent_rates[-5:])
                overall_avg = statistics.mean(recent_rates)
                
                if recent_avg < overall_avg * 0.95:  # 5%下降
                    anomaly = {
                        "type": "success_rate_decline",
                        "exchange": exchange_name,
                        "recent_avg": recent_avg,
                        "overall_avg": overall_avg,
                        "decline_percent": (overall_avg - recent_avg) / overall_avg * 100,
                        "timestamp": datetime.utcnow(),
                        "severity": "high" if recent_avg < 0.90 else "medium"
                    }
                    anomalies.append(anomaly)
        
        return anomalies

    async def _detect_data_consistency_anomalies(self) -> List[Dict[str, Any]]:
        """检测数据一致性异常"""
        anomalies = []
        
        # 检查不同交易所间的价格差异
        for symbol in self.market_data_cache.keys():
            prices = []
            for exchange, cache in self.market_data_cache.items():
                if cache:
                    latest_data = cache[-1]
                    if hasattr(latest_data, 'price'):
                        prices.append((exchange, latest_data.price))
            
            # 如果有多个交易所的价格数据，检查差异
            if len(prices) >= 2:
                price_values = [p[1] for p in prices]
                max_price = max(price_values)
                min_price = min(price_values)
                
                # 如果价格差异超过5%，认为存在异常
                if max_price > 0 and (max_price - min_price) / min_price > 0.05:
                    anomaly = {
                        "type": "price_discrepancy",
                        "symbol": symbol,
                        "prices": dict(prices),
                        "max_diff_percent": (max_price - min_price) / min_price * 100,
                        "timestamp": datetime.utcnow(),
                        "severity": "high"
                    }
                    anomalies.append(anomaly)
        
        return anomalies

    async def _handle_anomalies(self, detector_name: str, anomalies: List[Dict[str, Any]]):
        """处理异常"""
        for anomaly in anomalies:
            try:
                # 创建预警
                alert = PositionAlert(
                    id=f"{detector_name}_{int(time.time())}",
                    type=AlertLevel.WARNING if anomaly.get("severity") == "medium" else AlertLevel.ERROR,
                    message=f"{detector_name}检测到异常: {anomaly.get('type', 'unknown')}",
                    data=anomaly,
                    timestamp=datetime.utcnow()
                )
                
                # 通知回调
                await self._notify_alerts([alert])
                
                logger.warning(f"检测到异常: {alert.message}", extra={"anomaly": anomaly})
                
            except Exception as e:
                logger.error(f"处理异常失败: {e}")

    async def _update_health_status(self):
        """更新健康状态"""
        # 这里可以实现更复杂的健康状态更新逻辑
        # 目前健康监控已经处理了基础的健康检查
        pass

    async def _generate_analysis_report(self):
        """生成分析报告"""
        try:
            # 收集分析数据
            report_data = await self._collect_analysis_data()
            
            # 这里可以添加报告生成逻辑
            # 例如发送到监控面板、发送邮件等
            logger.debug("生成分析报告", extra={"report": report_data})
            
        except Exception as e:
            logger.error(f"生成分析报告失败: {e}")

    async def _collect_analysis_data(self) -> Dict[str, Any]:
        """收集分析数据"""
        health_status = self.health_monitor.get_health_status()
        
        # 汇总健康状态统计
        health_summary = {
            "healthy": len([h for h in health_status.values() if h.status == HealthStatus.HEALTHY]),
            "warning": len([h for h in health_status.values() if h.status == HealthStatus.WARNING]),
            "critical": len([h for h in health_status.values() if h.status == HealthStatus.CRITICAL]),
            "down": len([h for h in health_status.values() if h.status == HealthStatus.DOWN])
        }
        
        return {
            "timestamp": datetime.utcnow(),
            "health_summary": health_summary,
            "total_exchanges": len(health_status),
            "market_data_symbols": len(self.market_data_cache),
            "order_book_symbols": len(self.order_book_cache),
            "trade_cache_symbols": len(self.trade_cache)
        }

    async def add_market_data(self, exchange: str, symbol: str, data: MarketData):
        """添加市场数据"""
        cache_key = f"{exchange}:{symbol}"
        
        if cache_key not in self.market_data_cache:
            self.market_data_cache[cache_key] = deque(maxlen=self.max_cache_size)
        
        self.market_data_cache[cache_key].append(data)

    async def add_order_book(self, exchange: str, symbol: str, order_book: OrderBook):
        """添加订单簿数据"""
        cache_key = f"{exchange}:{symbol}"
        
        if cache_key not in self.order_book_cache:
            self.order_book_cache[cache_key] = deque(maxlen=self.max_cache_size)
        
        self.order_book_cache[cache_key].append(order_book)

    async def add_trade(self, exchange: str, symbol: str, trade: Trade):
        """添加交易数据"""
        cache_key = f"{exchange}:{symbol}"
        
        if cache_key not in self.trade_cache:
            self.trade_cache[cache_key] = deque(maxlen=self.max_cache_size)
        
        self.trade_cache[cache_key].append(trade)

    def add_alert_callback(self, callback: Callable):
        """添加预警回调"""
        self.alert_callbacks.append(callback)

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

    async def get_market_analysis_report(self) -> Dict[str, Any]:
        """获取市场分析报告"""
        try:
            # 收集所有数据
            health_status = self.health_monitor.get_health_status()
            analysis_data = await self._collect_analysis_data()
            
            # 构建报告
            report = {
                "timestamp": datetime.utcnow().isoformat(),
                "health_monitoring": {
                    "total_exchanges": len(health_status),
                    "status_summary": {
                        status.value: len([h for h in health_status.values() if h.status == status])
                        for status in HealthStatus
                    },
                    "detailed_status": [
                        {
                            "exchange": result.exchange,
                            "status": result.status.value,
                            "latency": result.latency,
                            "success_rate": result.success_rate,
                            "error_message": result.error_message,
                            "timestamp": result.timestamp.isoformat()
                        }
                        for result in health_status.values()
                    ]
                },
                "data_cache": {
                    "market_data_symbols": len(self.market_data_cache),
                    "order_book_symbols": len(self.order_book_cache),
                    "trade_cache_symbols": len(self.trade_cache),
                    "total_cached_items": sum(len(cache) for cache in self.market_data_cache.values())
                },
                "analysis_summary": analysis_data
            }
            
            return report
            
        except Exception as e:
            logger.error(f"获取市场分析报告失败: {e}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}


# 全局市场分析器实例
_market_analyzer: Optional[MarketAnalyzer] = None


async def get_market_analyzer() -> MarketAnalyzer:
    """获取全局市场分析器实例"""
    global _market_analyzer

    if _market_analyzer is None:
        _market_analyzer = MarketAnalyzer()

    return _market_analyzer


async def shutdown_market_analyzer():
    """关闭市场分析器"""
    global _market_analyzer

    if _market_analyzer:
        await _market_analyzer.stop_analysis()
        _market_analyzer = None


# 主函数测试代码
if __name__ == "__main__":
    print("测试市场分析器...")
    
    async def test_market_analyzer():
        try:
            # 创建分析器实例
            analyzer = await get_market_analyzer()
            
            # 添加一些测试交易所
            analyzer.health_monitor.add_exchange("binance", {"api_key": "test", "api_secret": "test"})
            analyzer.health_monitor.add_exchange("okx", {"api_key": "test", "api_secret": "test"})
            analyzer.health_monitor.add_exchange("huobi", {"api_key": "test", "api_secret": "test"})
            
            # 添加预警回调
            def alert_handler(alerts):
                print(f"收到 {len(alerts)} 个预警:")
                for alert in alerts:
                    print(f"  - {alert.type.value}: {alert.message}")
            
            analyzer.add_alert_callback(alert_handler)
            
            # 启动分析
            await analyzer.start_analysis()
            
            # 等待一段时间
            await asyncio.sleep(30)
            
            # 获取分析报告
            report = await analyzer.get_market_analysis_report()
            print("市场分析报告:")
            print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
            
        except Exception as e:
            print(f"测试失败: {e}")
        finally:
            await shutdown_market_analyzer()
    
    # 运行测试
    asyncio.run(test_market_analyzer())