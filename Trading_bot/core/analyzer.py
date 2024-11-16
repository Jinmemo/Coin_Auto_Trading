from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
import logging
from Trading_bot.config.settings import settings

logger = logging.getLogger(__name__)

@dataclass
class MarketState:
    """시장 상태 정보"""
    market: str
    current_price: float
    rsi: float
    bb_upper: float
    bb_middle: float
    bb_lower: float
    volume_ratio: float
    price_change: float
    is_valid: bool
    is_oversold: bool = False
    is_overbought: bool = False
    ma5: float = 0.0   # 5일 이동평균
    ma10: float = 0.0  # 10일 이동평균
    ma20: float = 0.0  # 20일 이동평균
    ma50: float = 0.0  # 50일 이동평균
    ma60: float = 0.0  # 60일 이동평균
    ma120: float = 0.0 # 120일 이동평균

class MarketAnalyzer:
    def __init__(self, upbit_api=None):
        self.upbit = upbit_api
        self.rsi_period = settings.RSI_PERIOD
        self.bb_period = settings.BOLLINGER_PERIOD
        self.bb_std = settings.BOLLINGER_STD
        self.volume_threshold = settings.VOLUME_THRESHOLD
        self._initialized = False
        logger.info("MarketAnalyzer 객체 생성")

    async def initialize(self, upbit_api) -> bool:
        """분석기 초기화"""
        try:
            self.upbit = upbit_api
            self._initialized = True
            logger.info("MarketAnalyzer 초기화 완료")
            return True
        except Exception as e:
            logger.error(f"MarketAnalyzer 초기화 실패: {str(e)}")
            return False

    def calculate_moving_averages(self, prices: pd.Series) -> Dict[str, float]:
        """이동평균선 계산"""
        try:
            ma5 = prices.rolling(window=5).mean().iloc[-1]
            ma10 = prices.rolling(window=10).mean().iloc[-1]
            ma20 = prices.rolling(window=20).mean().iloc[-1]
            ma50 = prices.rolling(window=50).mean().iloc[-1]
            ma60 = prices.rolling(window=60).mean().iloc[-1]
            ma120 = prices.rolling(window=120).mean().iloc[-1]
            
            return {
                'ma5': float(ma5),
                'ma10': float(ma10),
                'ma20': float(ma20),
                'ma50': float(ma50),
                'ma60': float(ma60),
                'ma120': float(ma120)
            }
        except Exception as e:
            logger.error(f"이동평균선 계산 실패: {str(e)}")
            return {
                'ma5': 0.0,
                'ma10': 0.0,
                'ma20': 0.0,
                'ma50': 0.0,
                'ma60': 0.0,
                'ma120': 0.0
            }

    async def analyze_market(self, market: str) -> Optional[MarketState]:
        """시장 분석"""
        if not self._initialized:
            logger.error("MarketAnalyzer가 초기화되지 않았습니다")
            return None

        try:
            # OHLCV 데이터 조회 (충분한 데이터를 위해 count 증가)
            ohlcv = await self.upbit.get_ohlcv(market, count=200)
            if ohlcv is None or len(ohlcv) < 120:  # 최소 120개 데이터 필요
                logger.warning(f"{market} OHLCV 데이터 부족")
                return None

            # RSI 계산
            rsi = self.calculate_rsi(ohlcv['close'])
            
            # RSI 과매도/과매수 판단
            is_oversold = rsi <= settings.RSI_OVERSOLD
            is_overbought = rsi >= settings.RSI_OVERBOUGHT
            
            # 볼린저 밴드 계산
            bb_upper, bb_middle, bb_lower = self.calculate_bollinger_bands(ohlcv['close'])

            # 이동평균선 계산
            mas = self.calculate_moving_averages(ohlcv['close'])

            # 거래량 분석
            volume_ma = ohlcv['volume'].rolling(window=self.bb_period).mean()
            volume_ratio = ohlcv['volume'].iloc[-1] / volume_ma.iloc[-1]
            is_volume_valid = volume_ratio >= self.volume_threshold

            # 가격 변화율 계산
            current_price = ohlcv['close'].iloc[-1]
            prev_price = ohlcv['close'].iloc[-2]
            price_change = (current_price - prev_price) / prev_price * 100

            return MarketState(
                market=market,
                current_price=current_price,
                rsi=rsi,
                bb_upper=bb_upper,
                bb_middle=bb_middle,
                bb_lower=bb_lower,
                volume_ratio=volume_ratio,
                price_change=price_change,
                is_valid=is_volume_valid,
                is_oversold=is_oversold,
                is_overbought=is_overbought,
                ma5=mas['ma5'],
                ma10=mas['ma10'],
                ma20=mas['ma20'],
                ma50=mas['ma50'],
                ma60=mas['ma60'],
                ma120=mas['ma120']
            )

        except Exception as e:
            logger.error(f"시장 분석 실패 ({market}): {str(e)}")
            return None

    def calculate_rsi(self, prices: pd.Series) -> float:
        """RSI 계산 (Upbit 방식)"""
        try:
            # 가격 변화
            delta = prices.diff()
            
            # gains (상승분), losses (하락분) 계산
            gains = delta.copy()
            losses = delta.copy()
            
            gains[gains < 0] = 0
            losses[losses > 0] = 0
            losses = abs(losses)
            
            # 첫 평균 계산
            avg_gain = gains.iloc[:self.rsi_period].mean()
            avg_loss = losses.iloc[:self.rsi_period].mean()
            
            # 전체 기간에 대한 계산을 벡터화
            for idx in range(self.rsi_period, len(gains)):
                avg_gain = ((avg_gain * (self.rsi_period - 1) + gains.iloc[idx]) / 
                           self.rsi_period)
                avg_loss = ((avg_loss * (self.rsi_period - 1) + losses.iloc[idx]) / 
                           self.rsi_period)
            
            if avg_loss == 0:
                return 100.0
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            return round(float(rsi), 2)
            
        except Exception as e:
            logger.error(f"RSI 계산 실패: {str(e)}")
            return 50.0

    def calculate_bollinger_bands(self, prices: pd.Series) -> Tuple[float, float, float]:
        """볼린저 밴드 계산"""
        try:
            middle = prices.rolling(window=self.bb_period).mean()
            std = prices.rolling(window=self.bb_period).std()
            
            upper = middle + (std * self.bb_std)
            lower = middle - (std * self.bb_std)
            
            return (
                float(upper.iloc[-1]),
                float(middle.iloc[-1]),
                float(lower.iloc[-1])
            )
        except Exception as e:
            logger.error(f"볼린저 밴드 계산 실패: {str(e)}")
            current_price = float(prices.iloc[-1])
            return (current_price, current_price, current_price)  # 기본값 반환
