from typing import Dict, Optional
from Trading_bot.core.analyzer import MarketState
from .base import BaseStrategy, Position, PositionType
import logging
import numpy as np

logger = logging.getLogger(__name__)

class SwingStrategy(BaseStrategy):
    """스윙 트레이딩 전략 클래스"""
    
    def __init__(self, name: str, config: Dict):
        super().__init__(name, config)
        self._initialize_swing_parameters()
    
    def _initialize_swing_parameters(self):
        """스윙 전용 파라미터 초기화"""
        self.trend_confirmation_period = self.config.get('trend_confirmation_period', 3)
        self.min_trend_strength = self.config.get('min_trend_strength', 0.02)
        self.volume_surge_threshold = self.config.get('volume_surge_threshold', 2.0)
        self.profit_target_multiplier = self.config.get('profit_target_multiplier', 2.0)
        self.ma_crossover_threshold = self.config.get('ma_crossover_threshold', 0.01)
        self.rsi_trend_threshold = self.config.get('rsi_trend_threshold', 40)

    async def analyze(self, market_state: MarketState) -> Dict:
        """시장 상태 분석"""
        try:
            # 추세 강도 계산
            trend_strength = abs(market_state.ma20 - market_state.ma50) / market_state.ma50
            
            # 이동평균 크로스오버 체크
            ma_crossover = (market_state.ma20 - market_state.ma50) / market_state.ma50
            
            # 볼륨 트렌드 체크
            volume_trend = market_state.volume / market_state.volume_ma
            
            # 추세 점수 계산
            trend_scores = {
                "강세상승": 1.0,
                "상승": 0.7,
                "중립": 0.5,
                "하락": 0.3,
                "강세하락": 0.0
            }
            trend_score = trend_scores.get(market_state.trend, 0.5)
            
            # RSI 트렌드 점수
            rsi_trend_score = 0.0
            if market_state.rsi < 30:  # 과매도
                rsi_trend_score = 1.0
            elif market_state.rsi > 70:  # 과매수
                rsi_trend_score = -1.0
            else:
                rsi_trend_score = (market_state.rsi - 50) / 20  # -1.0 to 1.0
            
            # 종합 점수 계산
            total_score = (
                trend_score * 0.4 +
                abs(ma_crossover) * 0.2 +
                volume_trend * 0.2 +
                (1 + rsi_trend_score) * 0.2
            )
            
            signal_strength = "WEAK"
            if total_score >= 0.8:
                signal_strength = "STRONG"
            elif total_score >= 0.6:
                signal_strength = "NORMAL"
            
            return {
                'score': total_score,
                'strength': signal_strength,
                'trend_strength': trend_strength,
                'ma_crossover': ma_crossover,
                'volume_trend': volume_trend,
                'rsi_trend': rsi_trend_score
            }
            
        except Exception as e:
            logger.error(f"시장 분석 실패: {str(e)}")
            return {}

    async def should_enter(self, market_state: MarketState) -> bool:
        """진입 조건 확인"""
        try:
            analysis = await self.analyze(market_state)
            if not analysis or analysis['score'] < 0.6:  # 스윙은 더 높은 신뢰도 요구
                return False
            
            # 추세 강도 확인
            if analysis['trend_strength'] < self.min_trend_strength:
                return False
            
            # 이동평균 크로스오버 확인
            if abs(analysis['ma_crossover']) < self.ma_crossover_threshold:
                return False
            
            # 거래량 서지 확인
            if analysis['volume_trend'] < self.volume_surge_threshold:
                return False
            
            # RSI 트렌드 확인
            if market_state.trend in ["상승", "강세상승"]:
                if market_state.rsi < self.rsi_trend_threshold:
                    return False
            elif market_state.trend in ["하락", "강세하락"]:
                if market_state.rsi > (100 - self.rsi_trend_threshold):
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"진입 조건 확인 실패: {str(e)}")
            return False

    async def should_exit(self, position: Position, market_state: MarketState) -> bool:
        """청산 조건 확인"""
        try:
            analysis = await self.analyze(market_state)
            
            # 추세 반전 확인
            if position.position_type == PositionType.LONG:
                if market_state.trend in ["강세하락"] and analysis['trend_strength'] > self.min_trend_strength:
                    return True
            else:
                if market_state.trend in ["강세상승"] and analysis['trend_strength'] > self.min_trend_strength:
                    return True
            
            # 이동평균 크로스오버 확인
            if abs(analysis['ma_crossover']) > self.ma_crossover_threshold:
                if (position.position_type == PositionType.LONG and analysis['ma_crossover'] < 0) or \
                   (position.position_type == PositionType.SHORT and analysis['ma_crossover'] > 0):
                    return True
            
            # 목표 수익률 도달 확인
            current_profit = (market_state.current_price - position.entry_price) / position.entry_price
            if abs(current_profit) >= (self.profit_rate * self.profit_target_multiplier):
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"청산 조건 확인 실패: {str(e)}")
            return False

    async def calculate_position_size(self, market_state: MarketState) -> float:
        """포지션 크기 계산"""
        try:
            base_size = await super().calculate_position_size(market_state)
            analysis = await self.analyze(market_state)
            
            # 시그널 강도에 따른 조정
            strength_multiplier = {
                'STRONG': 1.5,
                'NORMAL': 1.2,
                'WEAK': 1.0
            }.get(analysis.get('strength', 'NORMAL'), 1.0)
            
            # 추세 강도에 따른 조정
            trend_multiplier = min(1.5, max(0.5, 1 + analysis['trend_strength']))
            
            # 변동성에 따른 조정
            volatility_multiplier = 1.0
            if market_state.volatility > 0.05:  # 높은 변동성
                volatility_multiplier = 0.7
            elif market_state.volatility < 0.02:  # 낮은 변동성
                volatility_multiplier = 1.3
            
            return base_size * strength_multiplier * trend_multiplier * volatility_multiplier
            
        except Exception as e:
            logger.error(f"포지션 크기 계산 실패: {str(e)}")
            return 0
