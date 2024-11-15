from typing import Dict, List
from datetime import datetime
from core.analyzer import MarketState
from strategies.base import BaseStrategy, Position

class CycleTradingStrategy(BaseStrategy):
    """순환매매 전략"""
    
    def __init__(self, name: str, config: Dict):
        super().__init__(name, config)
        self.cycle_points: List[float] = []  # 순환 지점 저장
        self.last_trade_price: float = 0
        self.cycle_count: int = 0
        
    async def analyze(self, market_state: MarketState) -> Dict:
        try:
            current_price = market_state.current_price
            
            # 볼린저 밴드 계산
            std = market_state.volatility * market_state.ma20
            upper_band = market_state.ma20 + (std * 2)
            lower_band = market_state.ma20 - (std * 2)
            
            # MACD 계산 (간단한 버전)
            ema12 = market_state.ema12
            ema26 = market_state.ema26
            macd = ema12 - ema26
            
            return {
                'bb_position': (current_price - lower_band) / (upper_band - lower_band),
                'macd_signal': macd > 0,
                'volume_ratio': market_state.volume / market_state.volume_ma,
                'price_position': (current_price - self.last_trade_price) / self.last_trade_price if self.last_trade_price else 0
            }
            
        except Exception as e:
            print(f"순환매매 분석 실패: {e}")
            return {}

    async def should_enter(self, market_state: MarketState) -> bool:
        try:
            analysis = await self.analyze(market_state)
            
            # 볼린저 밴드 하단, MACD 상승, 거래량 증가 시 매수
            return (
                analysis.get('bb_position', 0) < 0.2 and
                analysis.get('macd_signal', False) and
                analysis.get('volume_ratio', 0) > 1.2
            )
            
        except Exception as e:
            print(f"진입 조건 확인 실패: {e}")
            return False

    async def should_exit(self, position: Position, market_state: MarketState) -> bool:
        try:
            analysis = await self.analyze(market_state)
            current_price = market_state.current_price
            
            # 순환 지점 업데이트
            if not self.cycle_points or abs(self.cycle_points[-1] - current_price) / self.cycle_points[-1] > 0.02:
                self.cycle_points.append(current_price)
                
            # 볼린저 밴드 상단 도달 또는 일정 수익 달성 시 매도
            if analysis.get('bb_position', 0) > 0.8:
                self.last_trade_price = current_price
                self.cycle_count += 1
                return True
                
            return False
            
        except Exception as e:
            print(f"청산 조건 확인 실패: {e}")
            return False 