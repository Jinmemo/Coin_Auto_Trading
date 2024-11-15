from typing import Dict, Optional, List
from datetime import datetime, timedelta
from Trading_bot.core.analyzer import MarketState
from .base import BaseStrategy, Position, PositionType
import logging
import numpy as np

logger = logging.getLogger(__name__)

class DCAStrategy(BaseStrategy):
    """DCA(Dollar Cost Averaging) 전략 클래스"""
    
    def __init__(self, name: str, config: Dict):
        super().__init__(name, config)
        self._initialize_dca_parameters()
    
    def _initialize_dca_parameters(self):
        """DCA 전용 파라미터 초기화"""
        self.entry_intervals = self.config.get('entry_intervals', [-0.05, -0.1, -0.15, -0.2])
        self.position_increase_factor = self.config.get('position_increase_factor', 1.5)
        self.max_dca_count = self.config.get('max_dca_count', 5)
        self.min_interval_hours = self.config.get('min_interval_hours', 24)
        self.recovery_target_rate = self.config.get('recovery_target_rate', 0.02)
        self.max_total_investment = self.config.get('max_total_investment', 1000000)
        self.trend_reversal_threshold = self.config.get('trend_reversal_threshold', 0.03)

    async def analyze(self, market_state: MarketState) -> Dict:
        """시장 상태 분석"""
        try:
            # 하락 깊이 계산
            price_from_ma50 = (market_state.current_price - market_state.ma50) / market_state.ma50
            price_from_ma20 = (market_state.current_price - market_state.ma20) / market_state.ma20
            
            # RSI 기반 과매도 점수
            oversold_score = max(0, (30 - market_state.rsi) / 30) if market_state.rsi < 30 else 0
            
            # 거래량 급증 여부
            volume_surge = market_state.volume / market_state.volume_ma
            
            # 변동성 점수
            volatility_score = min(1, market_state.volatility * 10)
            
            # 추세 점수
            trend_scores = {
                "강세하락": 1.0,  # DCA에선 하락이 기회
                "하락": 0.8,
                "중립": 0.5,
                "상승": 0.3,
                "강세상승": 0.0
            }
            trend_score = trend_scores.get(market_state.trend, 0.5)
            
            # 종합 점수 계산
            total_score = (
                abs(min(0, price_from_ma50)) * 0.3 +  # 하락 깊이
                oversold_score * 0.3 +  # 과매도 상태
                trend_score * 0.2 +  # 추세
                volume_surge * 0.1 +  # 거래량
                (1 - volatility_score) * 0.1  # 안정성
            )
            
            signal_strength = "WEAK"
            if total_score >= 0.8:
                signal_strength = "STRONG"
            elif total_score >= 0.6:
                signal_strength = "NORMAL"
            
            return {
                'score': total_score,
                'strength': signal_strength,
                'price_from_ma50': price_from_ma50,
                'price_from_ma20': price_from_ma20,
                'oversold_score': oversold_score,
                'volume_surge': volume_surge,
                'trend_score': trend_score
            }
            
        except Exception as e:
            logger.error(f"시장 분석 실패: {str(e)}")
            return {}

    async def should_enter(self, market_state: MarketState) -> bool:
        """진입 조건 확인"""
        try:
            analysis = await self.analyze(market_state)
            if not analysis:
                return False
            
            # 기존 포지션 확인
            existing_position = self.positions.get(market_state.coin)
            if existing_position:
                return await self._should_add_dca(existing_position, market_state)
            
            # 신규 진입 조건
            if analysis['score'] < 0.5:
                return False
            
            # MA50 대비 하락률 확인
            if analysis['price_from_ma50'] > -0.05:  # 5% 이상 하락하지 않았으면 진입 안함
                return False
            
            # 과매도 상태 확인
            if market_state.rsi > 35:  # RSI가 너무 높으면 진입 안함
                return False
            
            # 거래량 확인
            if analysis['volume_surge'] < 1.2:  # 거래량 증가가 부족하면 진입 안함
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"진입 조건 확인 실패: {str(e)}")
            return False

    async def _should_add_dca(self, position: Position, market_state: MarketState) -> bool:
        """DCA 추가 진입 조건 확인"""
        try:
            # 최대 DCA 횟수 체크
            if len(position.additional_entries) >= self.max_dca_count:
                return False
            
            # 마지막 진입으로부터의 시간 체크
            last_entry_time = position.timestamp
            if position.additional_entries:
                last_entry_time = position.additional_entries[-1]['timestamp']
            
            if datetime.now() - last_entry_time < timedelta(hours=self.min_interval_hours):
                return False
            
            # 현재 손실률 계산
            avg_price = position.calculate_average_price()
            current_loss = (market_state.current_price - avg_price) / avg_price
            
            # 다음 DCA 진입점 확인
            next_entry_level = self.entry_intervals[len(position.additional_entries)]
            
            return current_loss <= next_entry_level
            
        except Exception as e:
            logger.error(f"DCA 조건 확인 실패: {str(e)}")
            return False

    async def should_exit(self, position: Position, market_state: MarketState) -> bool:
        """청산 조건 확인"""
        try:
            # RSI 기반 매도
            if market_state.rsi >= 70:
                price_to_bb = (market_state.current_price - market_state.bb_upper) / market_state.bb_upper
                if abs(price_to_bb) < 0.005:  # 상단 밴드 근처
                    return True
            
            # RSI 하락 시 매도
            if position.last_rsi and market_state.rsi < position.last_rsi and position.last_rsi >= 70:
                return True
            
            # 목표 수익률 도달
            current_profit = (market_state.current_price - position.entry_price) / position.entry_price
            if current_profit >= self.recovery_target_rate:
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"청산 조건 확인 실패: {str(e)}")
            return False

    async def calculate_position_size(self, market_state: MarketState) -> float:
        """포지션 크기 계산"""
        try:
            base_size = await super().calculate_position_size(market_state)
            existing_position = self.positions.get(market_state.coin)
            
            if existing_position:
                # DCA 회차에 따른 크기 증가
                dca_count = len(existing_position.additional_entries)
                position_size = base_size * (self.position_increase_factor ** dca_count)
                
                # 총 투자금액 제한 확인
                total_invested = existing_position.calculate_total_amount() * \
                               existing_position.calculate_average_price()
                remaining_limit = self.max_total_investment - total_invested
                
                return min(position_size, remaining_limit)
            
            return base_size
            
        except Exception as e:
            logger.error(f"포지션 크기 계산 실패: {str(e)}")
            return 0

    async def update_position(self, position: Position, market_state: MarketState) -> Dict:
        """포지션 업데이트"""
        try:
            update_info = await super().update_position(position, market_state)
            if not update_info:
                return None
            
            # DCA 특화 정보 추가
            avg_entry_price = position.calculate_average_price()
            total_investment = position.calculate_total_amount() * avg_entry_price
            
            update_info.update({
                'dca_count': len(position.additional_entries),
                'avg_entry_price': avg_entry_price,
                'total_investment': total_investment,
                'remaining_dca': self.max_dca_count - len(position.additional_entries)
            })
            
            return update_info
            
        except Exception as e:
            logger.error(f"포지션 업데이트 실패: {str(e)}")
            return None

    async def should_add_position(self, position: Position, market_state: MarketState) -> bool:
        """추가 매수 조건 확인"""
        try:
            if not position.can_add_position():
                return False
            
            # 현재가가 진입가 대비 특정 비율 이상 하락했는지 확인
            current_drawdown = (market_state.current_price - position.entry_price) / position.entry_price
            
            # 추가 매수 조건
            if (current_drawdown <= self.entry_intervals[len(position.additional_entries)] and
                market_state.rsi <= 30 and
                market_state.current_price <= market_state.bb_lower):
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"추가 매수 조건 확인 실패: {str(e)}")
            return False
