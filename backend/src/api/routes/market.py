"""
市场数据API路由模块
提供现货和期货市场数据获取、订单簿查询、K线数据、资金费率等功能
"""

from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Query, HTTPException, Depends
import logging

from ..models.market import (
    MarketDataResponse,
    OrderBookResponse,
    TradeResponse,
    CandleResponse,
    FundingRateResponse,
    OpenInterestResponse,
)
from ..dependencies import get_data_aggregator, get_redis_cache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/market", tags=["market-data"])


@router.get("/spot/ticker/{symbol}", response_model=MarketDataResponse)
async def get_spot_ticker(
    symbol: str,
    exchange: str = Query("binance", description="交易所名称"),
    data_aggregator=Depends(get_data_aggregator),
    redis_cache=Depends(get_redis_cache),
):
    """获取单个现货交易对的价格和统计数据"""
    try:
        # 从缓存获取数据
        cache_key = f"spot_ticker:{exchange}:{symbol}"
        cached_data = await redis_cache.get(cache_key)
        if cached_data:
            return MarketDataResponse(**cached_data)

        # 从数据聚合器获取数据
        market_data = await data_aggregator.get_market_data(exchange, "spot", symbol)

        if market_data is None:
            raise HTTPException(
                status_code=404, detail=f"未找到 {symbol} 的市场数据"
            )

        response_data = MarketDataResponse(
            symbol=market_data.symbol,
            price=float(market_data.current_price),
            price_change_24h=float(market_data.price_change_24h),
            price_change_percent_24h=float(market_data.price_change_percent),
            volume_24h=float(market_data.volume_24h),
            high_24h=float(market_data.high_24h),
            low_24h=float(market_data.low_24h),
            bid=float(market_data.bid_price) if market_data.bid_price else None,
            ask=float(market_data.ask_price) if market_data.ask_price else None,
            timestamp=datetime.utcnow(),
        )

        # 缓存数据（5分钟过期）
        await redis_cache.setex(
            cache_key, 300, response_data.dict()
        )

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取现货ticker失败: {str(e)}")


@router.get("/spot/tickers", response_model=List[MarketDataResponse])
async def get_spot_tickers(
    symbols: List[str] = Query(..., description="交易对符号列表"),
    exchange: str = Query("binance", description="交易所名称"),
    data_aggregator=Depends(get_data_aggregator),
):
    """获取多个现货交易对的价格和统计数据"""
    try:
        # 从数据聚合器获取多个市场数据
        market_data_list = await data_aggregator.get_multiple_market_data(
            exchange, "spot", symbols
        )

        response_data = []
        for symbol, market_data in market_data_list.items():
            if market_data is not None:
                response_data.append(
                    MarketDataResponse(
                        symbol=market_data.symbol,
                        price=float(market_data.current_price),
                        price_change_24h=float(market_data.price_change_24h),
                        price_change_percent_24h=float(
                            market_data.price_change_percent
                        ),
                        volume_24h=float(market_data.volume_24h),
                        high_24h=float(market_data.high_24h),
                        low_24h=float(market_data.low_24h),
                        bid=float(market_data.bid_price)
                        if market_data.bid_price
                        else None,
                        ask=float(market_data.ask_price)
                        if market_data.ask_price
                        else None,
                        timestamp=datetime.utcnow(),
                    )
                )

        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取现货tickers失败: {str(e)}")


@router.get("/spot/orderbook/{symbol}", response_model=OrderBookResponse)
async def get_spot_orderbook(
    symbol: str,
    exchange: str = Query("binance", description="交易所名称"),
    limit: int = Query(100, ge=1, le=1000, description="订单簿深度"),
    data_aggregator=Depends(get_data_aggregator),
):
    """获取现货订单簿数据"""
    try:
        # 获取订单簿数据
        orderbook_data = await data_aggregator.get_orderbook_data(
            exchange, "spot", symbol, limit
        )

        if orderbook_data is None:
            raise HTTPException(
                status_code=404, detail=f"未找到 {symbol} 的订单簿数据"
            )

        return OrderBookResponse(
            symbol=symbol,
            bids=[[float(bid[0]), float(bid[1])] for bid in orderbook_data.bids],
            asks=[[float(ask[0]), float(ask[1])] for ask in orderbook_data.asks],
            timestamp=datetime.utcnow(),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取现货订单簿失败: {str(e)}")


@router.get("/spot/trades", response_model=List[TradeResponse])
async def get_spot_trades(
    symbol: str = Query(..., description="交易对符号"),
    exchange: str = Query("binance", description="交易所名称"),
    limit: int = Query(100, ge=1, le=1000, description="返回记录数"),
    data_aggregator=Depends(get_data_aggregator),
):
    """获取现货交易记录"""
    try:
        # 获取交易记录数据
        trades_data = await data_aggregator.get_trades_data(
            exchange, "spot", symbol, limit
        )

        if trades_data is None:
            raise HTTPException(
                status_code=404, detail=f"未找到 {symbol} 的交易记录"
            )

        response_data = []
        for trade in trades_data:
            response_data.append(
                TradeResponse(
                    id=str(trade.id),
                    price=float(trade.price),
                    quantity=float(trade.quantity),
                    timestamp=datetime.fromtimestamp(trade.timestamp),
                    side=trade.side,
                )
            )

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取现货交易记录失败: {str(e)}")


@router.get("/spot/klines", response_model=List[CandleResponse])
async def get_spot_klines(
    symbol: str = Query(..., description="交易对符号"),
    interval: str = Query(..., description="时间间隔，如1m, 5m, 1h, 1d"),
    exchange: str = Query("binance", description="交易所名称"),
    limit: int = Query(100, ge=1, le=1000, description="K线数量"),
    start_time: Optional[datetime] = Query(None, description="开始时间"),
    end_time: Optional[datetime] = Query(None, description="结束时间"),
    data_aggregator=Depends(get_data_aggregator),
):
    """获取现货K线数据"""
    try:
        # 获取K线数据
        klines_data = await data_aggregator.get_klines_data(
            exchange, "spot", symbol, interval, limit, start_time, end_time
        )

        if klines_data is None:
            raise HTTPException(status_code=404, detail=f"未找到 {symbol} 的K线数据")

        response_data = []
        for kline in klines_data:
            response_data.append(
                CandleResponse(
                    open_time=datetime.fromtimestamp(kline.open_time),
                    close_time=datetime.fromtimestamp(kline.close_time),
                    open=float(kline.open),
                    high=float(kline.high),
                    low=float(kline.low),
                    close=float(kline.close),
                    volume=float(kline.volume),
                    quote_asset_volume=float(kline.quote_asset_volume),
                    number_of_trades=int(kline.number_of_trades),
                )
            )

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取现货K线数据失败: {str(e)}")


# ================================
# 期货市场数据API
# ================================


@router.get("/futures/ticker/{symbol}", response_model=MarketDataResponse)
async def get_futures_ticker(
    symbol: str,
    exchange: str = Query("binance", description="交易所名称"),
    data_aggregator=Depends(get_data_aggregator),
    redis_cache=Depends(get_redis_cache),
):
    """获取单个期货交易对的价格和统计数据"""
    try:
        # 从缓存获取数据
        cache_key = f"futures_ticker:{exchange}:{symbol}"
        cached_data = await redis_cache.get(cache_key)
        if cached_data:
            return MarketDataResponse(**cached_data)

        # 从期货数据聚合器获取数据
        futures_data = await data_aggregator.futures_aggregator.get_futures_market_data(
            exchange, symbol
        )

        if futures_data is None:
            raise HTTPException(
                status_code=404, detail=f"未找到 {symbol} 的期货市场数据"
            )

        response_data = MarketDataResponse(
            symbol=futures_data.symbol,
            price=float(futures_data.current_price),
            price_change_24h=float(futures_data.price_change_24h),
            price_change_percent_24h=float(futures_data.price_change_percent),
            volume_24h=float(futures_data.volume_24h),
            high_24h=float(futures_data.high_24h),
            low_24h=float(futures_data.low_24h),
            bid=float(futures_data.bid_price) if futures_data.bid_price else None,
            ask=float(futures_data.ask_price) if futures_data.ask_price else None,
            timestamp=datetime.utcnow(),
        )

        # 缓存数据（5分钟过期）
        await redis_cache.setex(
            cache_key, 300, response_data.dict()
        )

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取期货ticker失败: {str(e)}")


@router.get("/futures/tickers", response_model=List[MarketDataResponse])
async def get_futures_tickers(
    symbols: List[str] = Query(..., description="交易对符号列表"),
    exchange: str = Query("binance", description="交易所名称"),
    data_aggregator=Depends(get_data_aggregator),
):
    """获取多个期货交易对的价格和统计数据"""
    try:
        response_data = []
        for symbol in symbols:
            try:
                futures_data = (
                    await data_aggregator.futures_aggregator.get_futures_market_data(
                        exchange, symbol
                    )
                )
                if futures_data:
                    response_data.append(
                        MarketDataResponse(
                            symbol=futures_data.symbol,
                            price=float(futures_data.current_price),
                            price_change_24h=float(futures_data.price_change_24h),
                            price_change_percent_24h=float(
                                futures_data.price_change_percent
                            ),
                            volume_24h=float(futures_data.volume_24h),
                            high_24h=float(futures_data.high_24h),
                            low_24h=float(futures_data.low_24h),
                            bid=float(futures_data.bid_price)
                            if futures_data.bid_price
                            else None,
                            ask=float(futures_data.ask_price)
                            if futures_data.ask_price
                            else None,
                            timestamp=datetime.utcnow(),
                        )
                    )
            except Exception as e:
                logger.warning(f"获取 {symbol} 的期货数据失败: {e}")
                continue

        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取期货tickers失败: {str(e)}")


@router.get("/futures/orderbook/{symbol}", response_model=OrderBookResponse)
async def get_futures_orderbook(
    symbol: str,
    exchange: str = Query("binance", description="交易所名称"),
    limit: int = Query(100, ge=1, le=1000, description="订单簿深度"),
    data_aggregator=Depends(get_data_aggregator),
):
    """获取期货订单簿数据"""
    try:
        # TODO: 实现期货订单簿获取逻辑
        raise HTTPException(status_code=501, detail="期货订单簿API正在实现中")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取期货订单簿失败: {str(e)}")


@router.get("/futures/trades", response_model=List[TradeResponse])
async def get_futures_trades(
    symbol: str = Query(..., description="交易对符号"),
    exchange: str = Query("binance", description="交易所名称"),
    limit: int = Query(100, ge=1, le=1000, description="返回记录数"),
):
    """获取期货交易记录"""
    try:
        # TODO: 实现交易记录获取逻辑
        raise HTTPException(status_code=501, detail="期货交易记录API正在实现中")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取期货交易记录失败: {str(e)}")


@router.get("/futures/klines", response_model=List[CandleResponse])
async def get_futures_klines(
    symbol: str = Query(..., description="交易对符号"),
    interval: str = Query(..., description="时间间隔，如1m, 5m, 1h, 1d"),
    exchange: str = Query("binance", description="交易所名称"),
    limit: int = Query(100, ge=1, le=1000, description="K线数量"),
    start_time: Optional[datetime] = Query(None, description="开始时间"),
    end_time: Optional[datetime] = Query(None, description="结束时间"),
):
    """获取期货K线数据"""
    try:
        # TODO: 实现K线数据获取逻辑
        raise HTTPException(status_code=501, detail="期货K线数据API正在实现中")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取期货K线数据失败: {str(e)}")


@router.get("/futures/funding-rate", response_model=FundingRateResponse)
async def get_futures_funding_rate(
    symbol: str = Query(..., description="交易对符号"),
    exchange: str = Query("binance", description="交易所名称"),
):
    """获取期货资金费率"""
    try:
        # 获取数据聚合器
        data_aggregator = await get_data_aggregator()

        # 获取资金费率数据
        funding_data = await data_aggregator.futures_aggregator.get_funding_rate_data(
            exchange, symbol
        )

        if funding_data is None:
            raise HTTPException(
                status_code=404, detail=f"未找到 {symbol} 的资金费率数据"
            )

        return FundingRateResponse(
            symbol=funding_data.get("symbol", symbol),
            funding_rate=float(funding_data.get("last_funding_rate", 0)),
            next_funding_time=funding_data.get("next_funding_time", datetime.utcnow()),
            exchange=exchange,
            timestamp=datetime.utcnow(),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取资金费率失败: {str(e)}")


@router.get("/futures/open-interest", response_model=OpenInterestResponse)
async def get_futures_open_interest(
    symbol: str = Query(..., description="交易对符号"),
    exchange: str = Query("binance", description="交易所名称"),
):
    """获取期货持仓量"""
    try:
        # 获取数据聚合器
        data_aggregator = await get_data_aggregator()

        # 获取持仓量数据
        oi_data = await data_aggregator.futures_aggregator.get_open_interest_data(
            exchange, symbol
        )

        if oi_data is None:
            raise HTTPException(status_code=404, detail=f"未找到 {symbol} 的持仓量数据")

        return OpenInterestResponse(
            symbol=oi_data.get("symbol", symbol),
            open_interest=float(oi_data.get("open_interest", 0)),
            open_interest_value=float(oi_data.get("open_interest_value", 0)),
            exchange=exchange,
            timestamp=datetime.utcnow(),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取持仓量失败: {str(e)}")


@router.get("/futures/summary")
async def get_futures_summary(
    symbols: Optional[List[str]] = Query(None, description="交易对符号列表"),
    exchange: str = Query("binance", description="交易所名称"),
):
    """获取期货市场摘要信息"""
    try:
        # 默认获取热门期货交易对
        if symbols is None:
            symbols = ["BTCUSDT-PERP", "ETHUSDT-PERP", "BNBUSDT-PERP"]

        # 获取数据聚合器
        data_aggregator = await get_data_aggregator()

        # 获取期货摘要数据
        summary = await data_aggregator.futures_aggregator.get_futures_summary(
            exchange, symbols
        )

        return {
            "exchange": exchange,
            "summary": summary,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取期货摘要失败: {str(e)}")


# 市场概览API
@router.get("/overview")
async def get_market_overview():
    """获取市场概览信息"""
    try:
        # 获取数据聚合器
        data_aggregator = await get_data_aggregator()

        # 获取主流现货交易对数据
        spot_symbols = [
            "BTCUSDT",
            "ETHUSDT",
            "BNBUSDT",
            "ADAUSDT",
            "SOLUSDT",
            "MATICUSDT",
            "DOTUSDT",
            "LINKUSDT",
        ]
        futures_symbols = [
            "BTCUSDT-PERP",
            "ETHUSDT-PERP",
            "BNBUSDT-PERP",
            "ADAUSDT-PERP",
            "SOLUSDT-PERP",
        ]

        # 获取现货市场数据
        spot_data = await data_aggregator.get_multiple_market_data(
            "binance", "spot", spot_symbols
        )

        # 处理现货市场数据
        spot_markets = []
        spot_gainers = []
        spot_losers = []

        for symbol, market_data in spot_data.items():
            if market_data is not None:
                market_info = {
                    "symbol": symbol,
                    "price": float(market_data.current_price),
                    "change_percent": float(market_data.price_change_percent),
                    "volume": float(market_data.volume_24h),
                    "high_24h": float(market_data.high_24h),
                    "low_24h": float(market_data.low_24h),
                }
                spot_markets.append(market_info)

                # 分类涨跌
                if market_data.price_change_percent > 0:
                    spot_gainers.append(market_info)
                elif market_data.price_change_percent < 0:
                    spot_losers.append(market_info)

        # 获取期货市场数据
        futures_markets = []
        futures_gainers = []
        futures_losers = []

        try:
            for symbol in futures_symbols:
                futures_data = (
                    await data_aggregator.futures_aggregator.get_futures_market_data(
                        "binance", symbol
                    )
                )
                if futures_data:
                    market_info = {
                        "symbol": symbol,
                        "price": float(futures_data.current_price),
                        "change_percent": float(futures_data.price_change_percent),
                        "volume": float(futures_data.volume_24h),
                        "high_24h": float(futures_data.high_24h),
                        "low_24h": float(futures_data.low_24h),
                        "funding_rate": (
                            float(futures_data.funding_rate)
                            if futures_data.funding_rate
                            else None
                        ),
                        "open_interest": (
                            float(futures_data.open_interest)
                            if futures_data.open_interest
                            else None
                        ),
                    }
                    futures_markets.append(market_info)

                    # 分类涨跌
                    if futures_data.price_change_percent > 0:
                        futures_gainers.append(market_info)
                    elif futures_data.price_change_percent < 0:
                        futures_losers.append(market_info)
        except Exception as e:
            logger.warning(f"获取期货市场数据失败: {e}")

        # 按涨跌幅排序
        spot_gainers.sort(key=lambda x: x["change_percent"], reverse=True)
        spot_losers.sort(key=lambda x: x["change_percent"])
        futures_gainers.sort(key=lambda x: x["change_percent"], reverse=True)
        futures_losers.sort(key=lambda x: x["change_percent"])

        return {
            "spot_markets": {
                "count": len(spot_markets),
                "markets": spot_markets,
                "top_gainers": spot_gainers[:5],  # 前5个涨幅
                "top_losers": spot_losers[:5],  # 前5个跌幅
            },
            "futures_markets": {
                "count": len(futures_markets),
                "markets": futures_markets,
                "top_gainers": futures_gainers[:5],
                "top_losers": futures_losers[:5],
            },
            "global_market_status": "open",
            "timestamp": datetime.utcnow().isoformat(),
            "total_market_cap": 1000000000,  # 模拟数据
            "bitcoin_dominance": 45.2,  # 模拟数据
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取市场概览失败: {str(e)}")


# 支持的交易对API
@router.get("/symbols")
async def get_supported_symbols(
    market_type: str = Query(..., regex="^(spot|futures)$", description="市场类型"),
    exchange: str = Query("binance", description="交易所名称"),
):
    """获取支持的交易对列表"""
    try:
        # TODO: 实现交易对获取逻辑
        return {
            "exchange": exchange,
            "market_type": market_type,
            "symbols": [],
            "count": 0,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取交易对列表失败: {str(e)}")


# 交易所状态API
@router.get("/exchanges/status")
async def get_exchange_status():
    """获取交易所连接状态"""
    try:
        # TODO: 实现交易所状态检查逻辑
        return {
            "binance": {
                "status": "operational",
                "spot_api": "connected",
                "futures_api": "connected",
                "websocket": "connected",
            },
            "okx": {
                "status": "operational",
                "spot_api": "connected",
                "futures_api": "connected",
                "websocket": "connected",
            },
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取交易所状态失败: {str(e)}")


# WebSocket升级端点
@router.websocket("/spot/ws/{symbol}")
async def spot_websocket_endpoint(websocket, symbol: str):
    """现货市场WebSocket端点"""
    # TODO: 实现WebSocket逻辑
    await websocket.close()


@router.websocket("/futures/ws/{symbol}")
async def futures_websocket_endpoint(websocket, symbol: str):
    """期货市场WebSocket端点"""
    # TODO: 实现WebSocket逻辑
    await websocket.close()