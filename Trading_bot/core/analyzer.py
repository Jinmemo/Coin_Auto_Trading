from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

@dataclass
class MarketState:
    """시장 상태 정보"""
    coin: str
    current_price: float
    ohlcv: pd.DataFrame
    volume: float
    volume_ma: float
    volatility: float
    rsi: float
    ma20: float
    ma50: float
    ma120: float
    bb_upper: float
    bb_middle: float
    bb_lower: float
    macd: float
    macd_signal: float
    stoch_k: float
    stoch_d: float
    atr: float
    trend: str
    pattern_signals: Dict
    support_resistance: Dict
    is_valid: bool = True

class MarketAnalyzer:
    def __init__(self):
        self._initialize_parameters()

    def _initialize_parameters(self):
        """분석 파라미터 초기화"""
        self.volume_ma_period = 20
        self.volatility_period = 20
        self.rsi_period = 14
        self.macd_fast = 12
        self.macd_slow = 26
        self.macd_signal = 9
        self.stoch_period = 14
        self.stoch_slowk = 3
        self.stoch_slowd = 3
        self.atr_period = 14

    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> float:
        """RSI 계산"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs)).iloc[-1]

    def calculate_macd(self, prices: pd.Series) -> tuple:
        """MACD 계산"""
        exp1 = prices.ewm(span=self.macd_fast).mean()
        exp2 = prices.ewm(span=self.macd_slow).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=self.macd_signal).mean()
        return macd.iloc[-1], signal.iloc[-1]

    def calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, std: int = 2) -> tuple:
        """볼린저 밴드 계산"""
        middle = prices.rolling(window=period).mean()
        std_dev = prices.rolling(window=period).std()
        upper = middle + (std_dev * std)
        lower = middle - (std_dev * std)
        return upper.iloc[-1], middle.iloc[-1], lower.iloc[-1]

    def calculate_stochastic(self, high: pd.Series, low: pd.Series, close: pd.Series) -> tuple:
        """스토캐스틱 계산"""
        lowest_low = low.rolling(window=self.stoch_period).min()
        highest_high = high.rolling(window=self.stoch_period).max()
        k = 100 * (close - lowest_low) / (highest_high - lowest_low)
        k = k.rolling(window=self.stoch_slowk).mean()
        d = k.rolling(window=self.stoch_slowd).mean()
        return k.iloc[-1], d.iloc[-1]

    async def analyze_market(self, coin: str, ohlcv: pd.DataFrame) -> Optional[MarketState]:
        """시장 상태 분석"""
        try:
            if len(ohlcv) < 120:
                return None

            closes = ohlcv['close']
            volumes = ohlcv['volume']
            
            current_price = closes.iloc[-1]
            volume = volumes.iloc[-1]
            volume_ma = volumes.rolling(window=self.volume_ma_period).mean().iloc[-1]
            
            # 이동평균
            ma20 = closes.rolling(window=20).mean().iloc[-1]
            ma50 = closes.rolling(window=50).mean().iloc[-1]
            ma120 = closes.rolling(window=120).mean().iloc[-1]
            
            # 기술적 지표 계산
            rsi = self.calculate_rsi(closes)
            macd, macd_signal = self.calculate_macd(closes)
            bb_upper, bb_middle, bb_lower = self.calculate_bollinger_bands(closes)
            stoch_k, stoch_d = self.calculate_stochastic(ohlcv['high'], ohlcv['low'], closes)
            
            # 변동성
            returns = np.log(closes / closes.shift(1))
            volatility = returns.std() * np.sqrt(self.volatility_period)
            
            # ATR
            high_low = ohlcv['high'] - ohlcv['low']
            high_close = np.abs(ohlcv['high'] - closes.shift())
            low_close = np.abs(ohlcv['low'] - closes.shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = ranges.max(axis=1)
            atr = true_range.rolling(window=self.atr_period).mean().iloc[-1]
            
            # 추세 판단
            trend = "상승" if current_price > ma20 > ma50 else "하락"

            return MarketState(
                coin=coin,
                current_price=current_price,
                ohlcv=ohlcv,
                volume=volume,
                volume_ma=volume_ma,
                volatility=volatility,
                rsi=rsi,
                ma20=ma20,
                ma50=ma50,
                ma120=ma120,
                bb_upper=bb_upper,
                bb_middle=bb_middle,
                bb_lower=bb_lower,
                macd=macd,
                macd_signal=macd_signal,
                stoch_k=stoch_k,
                stoch_d=stoch_d,
                atr=atr,
                trend=trend,
                pattern_signals={},
                support_resistance={},
                is_valid=True
            )
            
        except Exception as e:
            logger.error(f"시장 분석 실패 ({coin}): {str(e)}")
            return None
