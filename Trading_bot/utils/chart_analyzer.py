from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from datetime import datetime
import pandas_ta as ta
from ..core.analyzer import MarketState
import logging

logger = logging.getLogger(__name__)

class ChartPattern:
    """차트 패턴 정보"""
    def __init__(self, pattern_type: str, strength: float, price_target: float):
        self.pattern_type = pattern_type
        self.strength = strength  # 0.0 ~ 1.0
        self.price_target = price_target
        self.timestamp = datetime.now()

class ChartAnalyzer:
    """차트 패턴 분석기"""
    
    def __init__(self):
        self.pattern_configs = {
            'double_bottom': {
                'min_depth': 0.02,  # 최소 깊이
                'max_deviation': 0.01,  # 최대 허용 편차
                'min_days': 5,  # 최소 형성 기간
                'max_days': 30  # 최대 형성 기간
            },
            'double_top': {
                'min_height': 0.02,
                'max_deviation': 0.01,
                'min_days': 5,
                'max_days': 30
            },
            'head_and_shoulders': {
                'min_height': 0.03,
                'max_deviation': 0.015,
                'min_days': 10,
                'max_days': 60
            },
            'triangle': {
                'min_points': 5,
                'min_days': 7,
                'max_days': 45,
                'min_slope': 0.001
            }
        }
        self.support_resistance_levels: Dict[str, List[float]] = {}
        self.detected_patterns: Dict[str, List[ChartPattern]] = {}

    async def analyze_chart(self, market_state: MarketState) -> Dict:
        """차트 분석 수행"""
        try:
            patterns = []
            
            # 캔들스틱 패턴 분석
            candlestick_patterns = await self._analyze_candlestick_patterns(market_state)
            patterns.extend(candlestick_patterns)
            
            # 차트 패턴 분석
            chart_patterns = await self._analyze_chart_patterns(market_state)
            patterns.extend(chart_patterns)
            
            # 지지/저항 레벨 업데이트
            support_resistance = await self._update_support_resistance(market_state)
            
            # 추세선 분석
            trendlines = await self._analyze_trendlines(market_state)
            
            return {
                'patterns': patterns,
                'support_resistance': support_resistance,
                'trendlines': trendlines,
                'strength': self._calculate_pattern_strength(patterns)
            }
            
        except Exception as e:
            logger.error(f"차트 분석 실패: {str(e)}")
            return {}

    async def _analyze_candlestick_patterns(self, market_state: MarketState) -> List[ChartPattern]:
        """캔들스틱 패턴 분석"""
        try:
            patterns = []
            df = market_state.ohlcv
            
            # pandas_ta를 사용한 캔들스틱 패턴 분석
            doji = ta.cdl_pattern(df['open'], df['high'], df['low'], df['close'], name='doji')
            hammer = ta.cdl_pattern(df['open'], df['high'], df['low'], df['close'], name='hammer')
            engulfing = ta.cdl_pattern(df['open'], df['high'], df['low'], df['close'], name='engulfing')
            
            # 패턴 감지 및 추가
            if doji.iloc[-1] != 0:
                patterns.append(ChartPattern('Doji', 0.3, self._calculate_pattern_target('Doji', 
                              'bullish' if doji.iloc[-1] > 0 else 'bearish', 
                              market_state.current_price, market_state.ohlcv)))
            
            if hammer.iloc[-1] != 0:
                patterns.append(ChartPattern('Hammer', 0.5, self._calculate_pattern_target('Hammer',
                              'bullish' if hammer.iloc[-1] > 0 else 'bearish',
                              market_state.current_price, market_state.ohlcv)))
            
            if engulfing.iloc[-1] != 0:
                patterns.append(ChartPattern('Engulfing', 0.6, self._calculate_pattern_target('Engulfing',
                              'bullish' if engulfing.iloc[-1] > 0 else 'bearish',
                              market_state.current_price, market_state.ohlcv)))
            
            return patterns
            
        except Exception as e:
            logger.error(f"캔들스틱 패턴 분석 실패: {str(e)}")
            return []

    async def _analyze_chart_patterns(self, market_state: MarketState) -> List[ChartPattern]:
        """차트 패턴 분석"""
        try:
            patterns = []
            
            # 더블 바텀/탑 검사
            if double_bottom := await self._detect_double_bottom(market_state):
                patterns.append(double_bottom)
            if double_top := await self._detect_double_top(market_state):
                patterns.append(double_top)
            
            # 헤드앤숄더 검사
            if head_shoulders := await self._detect_head_and_shoulders(market_state):
                patterns.append(head_shoulders)
            
            # 삼각형 패턴 검사
            if triangle := await self._detect_triangle(market_state):
                patterns.append(triangle)
            
            return patterns
            
        except Exception as e:
            logger.error(f"차트 패턴 분석 실패: {str(e)}")
            return []

    async def _update_support_resistance(self, market_state: MarketState) -> Dict[str, List[float]]:
        """지지/저항 레벨 업데이트"""
        try:
            highs = market_state.ohlcv['high']
            lows = market_state.ohlcv['low']
            
            # 피봇 포인트 계산
            pivot = (highs[-1] + lows[-1] + market_state.current_price) / 3
            
            # 지지/저항 레벨 계산
            r1 = 2 * pivot - lows[-1]
            r2 = pivot + (highs[-1] - lows[-1])
            s1 = 2 * pivot - highs[-1]
            s2 = pivot - (highs[-1] - lows[-1])
            
            # 볼린저 밴드 레벨
            upper, middle, lower = ta.bbands(
                np.array(market_state.ohlcv['close']),
                length=20,
                std=2
            )
            
            return {
                'resistance': [r1, r2, upper[-1]],
                'support': [s1, s2, lower[-1]],
                'pivot': pivot,
                'middle': middle[-1]
            }
            
        except Exception as e:
            logger.error(f"지지/저항 레벨 업데이트 실패: {str(e)}")
            return {}

    async def _analyze_trendlines(self, market_state: MarketState) -> Dict:
        """추세선 분석"""
        try:
            closes = np.array(market_state.ohlcv['close'])
            
            # 단기/중기/장기 추세 계산
            short_trend = self._calculate_trend_slope(closes[-10:])
            medium_trend = self._calculate_trend_slope(closes[-30:])
            long_trend = self._calculate_trend_slope(closes[-90:])
            
            # 추세 강도 계산
            trend_strength = abs(medium_trend) * (
                1 + 0.5 * (1 if np.sign(short_trend) == np.sign(medium_trend) else -1) +
                0.3 * (1 if np.sign(medium_trend) == np.sign(long_trend) else -1)
            )
            
            return {
                'short_trend': short_trend,
                'medium_trend': medium_trend,
                'long_trend': long_trend,
                'strength': trend_strength,
                'direction': 'up' if medium_trend > 0 else 'down'
            }
            
        except Exception as e:
            logger.error(f"추세선 분석 실패: {str(e)}")
            return {}

    def _calculate_trend_slope(self, prices: np.ndarray) -> float:
        """추세선 기울기 계산"""
        try:
            x = np.arange(len(prices))
            slope, _ = np.polyfit(x, prices, 1)
            return slope
            
        except Exception:
            return 0.0

    def _calculate_pattern_strength(self, patterns: List[ChartPattern]) -> float:
        """패턴 강도 종합 계산"""
        if not patterns:
            return 0.0
            
        total_strength = sum(pattern.strength for pattern in patterns)
        return min(1.0, total_strength / len(patterns))

    def _calculate_pattern_target(self, pattern_type: str, direction: str,
                                current_price: float, ohlcv: pd.DataFrame) -> float:
        """패턴 기반 가격 목표 계산"""
        try:
            # 패턴별 목표가 계산 로직
            if pattern_type in ['Morning Star', 'Hammer']:
                return current_price * 1.02  # 2% 상승 목표
            elif pattern_type in ['Evening Star', 'Shooting Star']:
                return current_price * 0.98  # 2% 하락 목표
            elif pattern_type == 'Engulfing':
                return current_price * (1.03 if direction == 'bullish' else 0.97)
            else:
                return current_price * (1.01 if direction == 'bullish' else 0.99)
                
        except Exception:
            return current_price
