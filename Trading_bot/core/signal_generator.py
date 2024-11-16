import logging
import pandas as pd
import numpy as np
from typing import Optional, Dict, List
from Trading_bot.config.settings import settings
from Trading_bot.core.analyzer import MarketState

logger = logging.getLogger(__name__)

class SignalGenerator:
    def __init__(self, notifier=None):
        self.notifier = notifier
        self.rsi_period = settings.RSI_PERIOD
        self.rsi_oversold = settings.RSI_OVERSOLD
        self.rsi_overbought = settings.RSI_OVERBOUGHT
        
        self.macd_fast = settings.MACD_FAST
        self.macd_slow = settings.MACD_SLOW
        self.macd_signal = settings.MACD_SIGNAL
        
        self.bb_period = settings.BOLLINGER_PERIOD
        self.bb_std = settings.BOLLINGER_STD
        
        self.price_history: Dict[str, List[float]] = {}
        self.min_data_points = self.rsi_period + 1

    async def generate_signal(self, market: str, market_state: MarketState) -> Optional[str]:
        """매매 신호 생성"""
        try:
            if not market_state or not market_state.is_valid:
                return None

            # 매수 신호 조건
            if self._check_buy_conditions(market_state):
                logger.info(
                    f"매수 신호 발생: {market} "
                    f"(RSI: {market_state.rsi:.1f}, "
                    f"BB하단: {market_state.bb_lower:.0f}, "
                    f"현재가: {market_state.current_price:.0f}, "
                    f"거래량: {market_state.volume/market_state.volume_ma:.1f}배)"
                )
                return "buy"

            # 매도 신호 조건
            if self._check_sell_conditions(market_state):
                logger.info(
                    f"매도 신호 발생: {market} "
                    f"(RSI: {market_state.rsi:.1f}, "
                    f"BB상단: {market_state.bb_upper:.0f}, "
                    f"현재가: {market_state.current_price:.0f})"
                )
                return "sell"

            return None

        except Exception as e:
            logger.error(f"신호 생성 실패 ({market}): {str(e)}")
            return None

    def _check_buy_conditions(self, market_state: MarketState) -> bool:
        """매수 조건 확인"""
        try:
            # 1. RSI 기본 조건
            rsi_condition = market_state.rsi <= self.rsi_oversold  # RSI 30 이하

            # 2. 볼린저 밴드 조건
            bb_condition = (
                market_state.current_price <= market_state.bb_lower * 1.005 and  # 하단 밴드 근처
                market_state.current_price > market_state.bb_lower * 0.995      # 하단 밴드 아래로 너무 멀지 않음
            )

            # 3. 거래량 조건
            volume_condition = (
                market_state.volume > market_state.volume_ma * 2.0 and  # 평균 거래량의 2배 이상
                market_state.volume < market_state.volume_ma * 5.0      # 비정상적인 급등 제외
            )

            # 4. 이동평균선 조건
            ma_condition = (
                market_state.current_price < market_state.ma20 and  # 20일선 아래
                market_state.ma20 > market_state.ma50              # 중기 상승 추세
            )

            # 매수 조건 조합
            return (
                rsi_condition and                    # RSI 과매도
                (bb_condition or volume_condition) and  # 볼린저 하단 터치 또는 거래량 급증
                ma_condition                         # 이동평균선 조건
            )

        except Exception as e:
            logger.error(f"매수 조건 확인 실패: {str(e)}")
            return False

    def _check_sell_conditions(self, market_state: MarketState) -> bool:
        """매도 조건 확인"""
        try:
            # 1. RSI 조건
            rsi_condition = market_state.rsi >= self.rsi_overbought  # RSI 70 이상

            # 2. 볼린저 밴드 조건
            bb_condition = (
                market_state.current_price >= market_state.bb_upper * 0.995 and  # 상단 밴드 근처
                market_state.current_price < market_state.bb_upper * 1.005      # 상단 밴드 위로 너무 멀지 않음
            )

            # 3. 이동평균선 조건
            ma_condition = (
                market_state.current_price > market_state.ma20 and   # 20일선 위
                market_state.current_price > market_state.ma50 and   # 50일선 위
                market_state.ma20 < market_state.ma50               # 하락 반전 조짐
            )

            # 4. 거래량 감소 조건
            volume_condition = market_state.volume < market_state.volume_ma * 0.7  # 거래량 감소

            # 매도 조건 조합
            return (
                (rsi_condition or bb_condition) and  # RSI 과매수 또는 볼린저 상단 터치
                (ma_condition or volume_condition)   # 이동평균선 반전 또는 거래량 감소
            )

        except Exception as e:
            logger.error(f"매도 조건 확인 실패: {str(e)}")
            return False

    async def _calculate_rsi(self, market: str, market_data: Dict) -> Optional[float]:
        """RSI 계산"""
        try:
            current_price = market_data['trade_price']
            
            # 가격 기록 초기화 또는 업데이트 
            if market not in self.price_history:
                self.price_history[market] = []
            
            self.price_history[market].append(current_price)
            
            # 최대 기간만큼만 데이터 유지
            if len(self.price_history[market]) > self.min_data_points:
                self.price_history[market].pop(0)
            
            # RSI 계산에 필요한 최소 데이터가 없으면 None 반환
            if len(self.price_history[market]) < self.min_data_points:
                logger.debug(f"{market} RSI 계산을 위한 데이터 수집 중... ({len(self.price_history[market])}/{self.min_data_points})")
                return None
            
            # 가격 변화 계산
            changes = []
            prices = self.price_history[market]
            for i in range(1, len(prices)):
                changes.append(prices[i] - prices[i-1])
            
            # 상승/하락 변화 분리
            gains = []
            losses = []
            for change in changes:
                if change > 0:
                    gains.append(change)
                    losses.append(0)
                else:
                    gains.append(0)
                    losses.append(abs(change))
                    
            # Wilder의 Smoothing 방식으로 평균 계산
            avg_gain = sum(gains[:self.rsi_period]) / self.rsi_period
            avg_loss = sum(losses[:self.rsi_period]) / self.rsi_period
            
            # 이후 데이터에 대해 Smoothing 적용
            for i in range(self.rsi_period, len(gains)):
                avg_gain = (avg_gain * (self.rsi_period - 1) + gains[i]) / self.rsi_period
                avg_loss = (avg_loss * (self.rsi_period - 1) + losses[i]) / self.rsi_period
            
            if avg_loss == 0:
                return 100.0
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            logger.debug(f"{market} RSI 계산 완료: {rsi:.2f}")
            return rsi
            
        except Exception as e:
            logger.error(f"RSI 계산 실패 ({market}): {str(e)}")
            return None 

    async def get_rsi(self, market: str, market_data: Dict) -> Optional[float]:
        """현재 RSI 값 반환"""
        try:
            rsi = await self._calculate_rsi(market, market_data)
            if rsi is not None:
                logger.debug(f"{market} RSI 조회: {rsi:.2f}")
                return rsi
            return None
            
        except Exception as e:
            logger.error(f"RSI 조회 실패 ({market}): {str(e)}")
            return None
