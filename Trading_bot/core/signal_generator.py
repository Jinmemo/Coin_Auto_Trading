import logging
import pandas as pd
import numpy as np
from typing import Optional, Dict, List
from Trading_bot.config.settings import settings

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

    async def generate_signal(self, market: str, market_data: Dict) -> Optional[str]:
        """기본 매매 신호 생성"""
        try:
            current_price = market_data['trade_price']
            change_rate = market_data['signed_change_rate'] * 100
            
            rsi = await self._calculate_rsi(market, market_data)
            
            if rsi is not None:
                logger.debug(f"{market} RSI: {rsi:.2f}, 변동률: {change_rate:.2f}%")
                
                # 매수 신호
                if rsi < self.rsi_oversold and change_rate < -2:
                    message = (
                        f"🔵 매수 신호 감지\n"
                        f"코인: {market}\n"
                        f"현재가: {current_price:,}원\n"
                        f"RSI: {rsi:.2f}\n"
                        f"변동률: {change_rate:.2f}%"
                    )
                    logger.info(message)
                    if self.notifier:  # notifier가 있을 때만 메시지 전송
                        await self.notifier.send_message(message)
                    return 'buy'
                    
                # 매도 신호
                elif rsi > self.rsi_overbought and change_rate > 2:
                    message = (
                        f"🔴 매도 신호 감지\n"
                        f"코인: {market}\n"
                        f"현재가: {current_price:,}원\n"
                        f"RSI: {rsi:.2f}\n"
                        f"변동률: {change_rate:.2f}%"
                    )
                    logger.info(message)
                    if self.notifier:  # notifier가 있을 때만 메시지 전송
                        await self.notifier.send_message(message)
                    return 'sell'
            return None
            
        except Exception as e:
            error_message = f"신호 생성 실패 ({market}): {str(e)}"
            logger.error(error_message)
            if self.notifier:  # notifier가 있을 때만 메시지 전송
                await self.notifier.send_message(f"⚠️ {error_message}")
            return None

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
            
            # RS 계산을 위한 평균 상승/하락
            avg_gain = sum(gains[-self.rsi_period:]) / self.rsi_period
            avg_loss = sum(losses[-self.rsi_period:]) / self.rsi_period
            
            if avg_loss == 0:
                return 100.0
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            logger.debug(f"{market} RSI 계산 완료: {rsi:.2f}")
            return rsi
            
        except Exception as e:
            logger.error(f"RSI 계산 실패 ({market}): {str(e)}")
            return None 