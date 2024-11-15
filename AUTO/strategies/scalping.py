from datetime import datetime
from typing import Dict, Optional
from Trading_bot.core.analyzer import MarketState
from .base import BaseStrategy, Position, PositionType
import logging
import numpy as np

logger = logging.getLogger(__name__)

class ScalpingStrategy(BaseStrategy):
    """스캘핑 전략 클래스"""
    
    def __init__(self, name: str, config: Dict):
        super().__init__(name, config)
        self._initialize_scalping_parameters()
    
    def _initialize_scalping_parameters(self):
        """스캘핑 전용 파라미터 초기화"""
        self.volume_multiplier = self.config.get('volume_multiplier', 1.5)
        self.min_volatility = self.config.get('min_volatility', 0.003)
        self.max_volatility = self.config.get('max_volatility', 0.02)
        self.rsi_oversold = self.config.get('rsi_oversold', 30)
        self.rsi_overbought = self.config.get('rsi_overbought', 70)
        self.trend_strength_threshold = self.config.get('trend_strength_threshold', 0.01)

    async def analyze(self, market_state: MarketState) -> Dict:
        """시장 상태 분석"""
        try:
            # 변동성 점수 (0-1)
            volatility_score = min(1, max(0, (market_state.volatility - self.min_volatility) / 
                                        (self.max_volatility - self.min_volatility)))
            
            # RSI 점수 (0-1)
            rsi_score = 1 - abs(50 - market_state.rsi) / 50
            
            # 거래량 점수 (0-1)
            volume_score = min(1, market_state.volume / (market_state.volume_ma * self.volume_multiplier))
            
            # 추세 점수 (-1 to 1)
            trend_scores = {
                "강세상승": 1.0,
                "상승": 0.5,
                "중립": 0.0,
                "하락": -0.5,
                "강세하락": -1.0
            }
            trend_score = trend_scores.get(market_state.trend, 0.0)
            
            # 종합 점수 계산
            total_score = (
                volatility_score * 0.4 +
                rsi_score * 0.3 +
                volume_score * 0.2 +
                abs(trend_score) * 0.1  # 추세의 강도만 고려
            )
            
            signal_strength = "WEAK"
            if total_score >= 0.8:
                signal_strength = "STRONG"
            elif total_score >= 0.6:
                signal_strength = "NORMAL"
            
            return {
                'score': total_score,
                'strength': signal_strength,
                'volatility_score': volatility_score,
                'rsi_score': rsi_score,
                'volume_score': volume_score,
                'trend_score': trend_score
            }
            
        except Exception as e:
            logger.error(f"시장 분석 실패: {str(e)}")
            return {}

    async def should_enter(self, market_state: MarketState) -> bool:
        """진입 조건 확인"""
        try:
            # 기본 분석 수행
            analysis = await self.analyze(market_state)
            if not analysis or analysis['score'] < 0.5:
                return False
            
            # RSI 기반 과매수/과매도 체크
            if market_state.rsi <= self.rsi_oversold:  # 과매도 상태
                if market_state.trend in ["상승", "강세상승"]:  # 반등 기대
                    return True
            elif market_state.rsi >= self.rsi_overbought:  # 과매수 상태
                if market_state.trend in ["하락", "강세하락"]:  # 하락 반전 기대
                    return True
            
            # 변동성 체크
            if not (self.min_volatility <= market_state.volatility <= self.max_volatility):
                return False
            
            # 거래량 체크
            if market_state.volume < market_state.volume_ma * self.volume_multiplier:
                return False
            
            # 추세 강도 체크
            price_momentum = abs(market_state.current_price - market_state.ma20) / market_state.ma20
            if price_momentum < self.trend_strength_threshold:
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"진입 조건 확인 실패: {str(e)}")
            return False

    async def should_exit(self, position: Position, market_state: MarketState) -> bool:
        """청산 조건 확인"""
        try:
            # 기본 분석 수행
            analysis = await self.analyze(market_state)
            
            # 보유 시간 체크
            holding_duration = position.get_holding_duration()
            min_time, max_time = PositionType.get_holding_time(position.position_type)
            
            if holding_duration > max_time:
                return True
            
            # 추세 반전 체크
            if position.position_type == PositionType.LONG:
                if market_state.trend in ["강세하락", "하락"] and market_state.rsi > 70:
                    return True
            else:
                if market_state.trend in ["강세상승", "상승"] and market_state.rsi < 30:
                    return True
            
            # 변동성 급증 체크
            if market_state.volatility > self.max_volatility * 1.5:
                return True
            
            # 거래량 급감 체크
            if market_state.volume < market_state.volume_ma * 0.5:
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"청산 조건 확인 실패: {str(e)}")
            return False

    async def calculate_position_size(self, market_state: MarketState) -> float:
        """포지션 크기 계산"""
        try:
            # 기본 크기 계산
            base_size = await super().calculate_position_size(market_state)
            
            # 분석 결과에 따른 조정
            analysis = await self.analyze(market_state)
            
            # 시그널 강도에 따른 조정
            strength_multiplier = {
                'STRONG': 1.2,
                'NORMAL': 1.0,
                'WEAK': 0.8
            }.get(analysis.get('strength', 'NORMAL'), 1.0)
            
            # 변동성에 따른 조정
            volatility_multiplier = 1.0
            if market_state.volatility > self.max_volatility * 0.8:
                volatility_multiplier = 0.8
            elif market_state.volatility < self.min_volatility * 1.2:
                volatility_multiplier = 0.9
            
            return base_size * strength_multiplier * volatility_multiplier
            
        except Exception as e:
            logger.error(f"포지션 크기 계산 실패: {str(e)}")
            return 0
