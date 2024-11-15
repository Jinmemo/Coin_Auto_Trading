from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional
from Trading_bot.core.analyzer import MarketState
import logging

logger = logging.getLogger(__name__)

class PositionType(Enum):
    SCALPING = "단타"  # 5분~1시간
    DAYTRADING = "일단위"  # 1일~3일
    SWING = "스윙"  # 3일~2주
    POSITION = "포지션"  # 2주 이상
    NONE = "없음"

    @classmethod
    def get_holding_time(cls, position_type) -> tuple:
        """포지션 타입별 예상 보유 시간"""
        holding_times = {
            cls.SCALPING: (5, 60),  # 5분~1시간
            cls.DAYTRADING: (60*24, 60*24*3),  # 1일~3일
            cls.SWING: (60*24*3, 60*24*14),  # 3일~2주
            cls.POSITION: (60*24*14, 60*24*30),  # 2주~1달
        }
        return holding_times.get(position_type, (0, 0))

@dataclass
class Position:
    """포지션 정보"""
    market: str
    entry_price: float
    amount: float
    position_type: PositionType
    entry_time: datetime = field(default_factory=datetime.now)
    additional_entries: List[Dict] = field(default_factory=list)  # 추가 매수 기록
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    last_rsi: Optional[float] = None
    highest_price: float = field(init=False)  # 트레일링 스탑용
    lowest_price: float = field(init=False)   # 트레일링 스탑용
    
    def __post_init__(self):
        self.highest_price = self.entry_price
        self.lowest_price = self.entry_price
    
    def update_price_extremes(self, current_price: float):
        """최고/최저가 업데이트"""
        self.highest_price = max(self.highest_price, current_price)
        self.lowest_price = min(self.lowest_price, current_price)
    
    def can_add_position(self) -> bool:
        """추가 매수 가능 여부 확인"""
        return len(self.additional_entries) < 3  # 최대 3번까지 추가 매수 가능
    
    def get_holding_duration(self) -> float:
        """보유 기간 계산 (시간)"""
        duration = datetime.now() - self.entry_time
        return duration.total_seconds() / 3600

class BaseStrategy(ABC):
    """기본 전략 클래스"""
    
    def __init__(self, name: str, config: Dict):
        self.name = name
        self.config = config
        self.positions: Dict[str, Position] = {}
        self._initialize_parameters()
    
    def _initialize_parameters(self):
        """전략 파라미터 초기화"""
        self.profit_rate = self.config.get('profit_rate', 0.02)
        self.loss_rate = self.config.get('loss_rate', -0.02)
        self.max_positions = self.config.get('max_positions', 5)
        self.use_trailing_stop = self.config.get('use_trailing_stop', True)
        self.trailing_stop_rate = self.config.get('trailing_stop_rate', 0.02)
        self.max_additional_entries = self.config.get('max_additional_entries', 3)
        self.add_position_threshold = self.config.get('add_position_threshold', -0.05)
        self.base_amount = self.config.get('base_amount', 100000)
        self.volume_threshold = self.config.get('volume_threshold', 1000000)
    
    @abstractmethod
    async def analyze(self, market_state: MarketState) -> Dict:
        """시장 상태 분석"""
        pass
    
    @abstractmethod
    async def should_enter(self, market_state: MarketState) -> bool:
        """진입 조건 확인"""
        pass
    
    @abstractmethod
    async def should_exit(self, position: Position, market_state: MarketState) -> bool:
        """청산 조건 확인"""
        pass
    
    async def calculate_position_size(self, market_state: MarketState) -> float:
        """포지션 크기 계산"""
        try:
            # 변동성에 따른 포지션 크기 조절
            volatility_factor = 1 - (market_state.volatility * 10)  # 변동성이 클수록 작은 포지션
            position_size = self.base_amount * max(0.2, min(1, volatility_factor))
            
            # 거래량에 따른 추가 조절
            volume_factor = min(1, market_state.volume / self.volume_threshold)
            position_size *= volume_factor
            
            # 시장 상황에 따른 추가 조절
            if market_state.rsi < 30:  # 과매도 상태
                position_size *= 1.2
            elif market_state.rsi > 70:  # 과매수 상태
                position_size *= 0.8
            
            return position_size
            
        except Exception as e:
            print(f"포지션 크기 계산 실패: {e}")
            return 0
    
    async def calculate_entry_points(self, market_state: MarketState) -> Dict:
        """진입 지점 계산"""
        try:
            entry_price = market_state.current_price
            
            # 변동성에 따른 목표가/손절가 조정
            volatility_factor = market_state.volatility * 2
            adjusted_profit_rate = self.profit_rate * (1 + volatility_factor)
            adjusted_loss_rate = self.loss_rate * (1 - volatility_factor)
            
            take_profit = entry_price * (1 + adjusted_profit_rate)
            stop_loss = entry_price * (1 + adjusted_loss_rate)
            
            return {
                'entry_price': entry_price,
                'take_profit': take_profit,
                'stop_loss': stop_loss,
                'trailing_stop': entry_price * (1 - self.trailing_stop_rate)
            }
            
        except Exception as e:
            print(f"진입 지점 계산 실패: {e}")
            return None
    
    async def update_position(self, position: Position, market_state: MarketState) -> Dict:
        """포지션 업데이트 개선"""
        try:
            update_info = await super().update_position(position, market_state)
            if not update_info:
                return None
                
            # 포지션 파라미터 조정 검사
            param_adjustments = await self.adjust_position_parameters(position, market_state)
            if param_adjustments:
                position.position_type = param_adjustments['position_type']
                position.take_profit = param_adjustments['take_profit']
                position.stop_loss = param_adjustments['stop_loss']
                self.trailing_stop_rate = param_adjustments['trailing_stop_rate']
                
                update_info.update({
                    'position_type': position.position_type.value,
                    'take_profit': position.take_profit,
                    'stop_loss': position.stop_loss,
                    'parameter_adjusted': True
                })
            
            return update_info
            
        except Exception as e:
            print(f"포지션 업데이트 실패: {e}")
            return None
    
    async def should_add_position(self, position: Position, market_state: MarketState) -> bool:
        """추가 매수 조건 확인"""
        try:
            if len(position.additional_entries) >= self.max_additional_entries:
                return False
            
            current_price = market_state.current_price
            avg_entry_price = position.calculate_average_price()
            loss_rate = (current_price - avg_entry_price) / avg_entry_price
            
            # 기본 조건 검사
            if loss_rate > self.add_position_threshold:
                return False
                
            # 시장 상황 검사
            if market_state.rsi >= 40:  # RSI가 너무 높으면 추가 매수 하지 않음
                return False
                
            # 거래량 검사
            if market_state.volume < market_state.volume_ma:  # 거래량이 평균보다 낮으면 매수 하지 않음
                return False
                
            # 추세 검사
            if market_state.ma20 < market_state.ma50:  # 하락 추세면 신중하게 접근
                if market_state.rsi >= 30:  # RSI가 더 낮아야 매수
                    return False
            
            return True
            
        except Exception as e:
            print(f"추가 매수 조건 확인 실패: {e}")
            return False

    async def determine_position_type(self, market_state: MarketState) -> PositionType:
        """시장 상황에 따른 포지션 타입 결정"""
        try:
            # 변동성 점수 (0-1)
            volatility_score = min(1, market_state.volatility * 10)
            
            # 추세 강도 점수 (0-1)
            trend_scores = {
                "강세상승": 1.0,
                "상승": 0.7,
                "중립": 0.5,
                "하락": 0.3,
                "강세하락": 0.0
            }
            trend_score = trend_scores.get(market_state.trend, 0.5)
            
            # RSI 점수 (0-1)
            rsi_score = market_state.rsi / 100
            
            # 거래량 점수 (0-1)
            volume_score = min(1, market_state.volume / market_state.volume_ma)
            
            # 종합 점수 계산
            total_score = (
                volatility_score * 0.3 +
                trend_score * 0.3 +
                rsi_score * 0.2 +
                volume_score * 0.2
            )
            
            # 점수에 따른 포지션 타입 결정
            if total_score >= 0.8:
                return PositionType.POSITION
            elif total_score >= 0.6:
                return PositionType.SWING
            elif total_score >= 0.4:
                return PositionType.DAYTRADING
            else:
                return PositionType.SCALPING
                
        except Exception as e:
            print(f"포지션 타입 결정 실패: {e}")
            return PositionType.SCALPING

    async def adjust_position_parameters(self, position: Position, market_state: MarketState) -> Dict:
        """포지션 파라미터 조정"""
        try:
            # 현재 포지션 타입 검사
            new_position_type = await self.determine_position_type(market_state)
            current_type = position.position_type
            
            if new_position_type != current_type:
                # 포지션 타입에 따른 파라미터 조정
                if new_position_type.value > current_type.value:  # 더 긴 텀으로 변경
                    return {
                        'take_profit': position.take_profit * 1.5,
                        'stop_loss': position.stop_loss * 0.8,
                        'trailing_stop_rate': self.trailing_stop_rate * 1.5,
                        'position_type': new_position_type
                    }
                else:  # 더 짧은 텀으로 변경
                    return {
                        'take_profit': position.take_profit * 0.8,
                        'stop_loss': position.stop_loss * 1.2,
                        'trailing_stop_rate': self.trailing_stop_rate * 0.8,
                        'position_type': new_position_type
                    }
            
            return None
            
        except Exception as e:
            print(f"포지션 파라미터 조정 실패: {e}")
            return None

    async def calculate_dynamic_parameters(self, market_state: MarketState) -> Dict:
        """시장 상황에 따른 동적 파라미터 계산"""
        try:
            volatility_factor = market_state.volatility * 2
            volume_factor = min(1.5, market_state.volume / market_state.volume_ma)
            
            # 동적 손익률 조정
            adjusted_profit_rate = self.profit_rate * (1 + volatility_factor)
            adjusted_loss_rate = self.loss_rate * (1 - volatility_factor)
            
            # RSI 기반 진입/청산 레벨 조정
            rsi_buy_level = 30 * (1 + volatility_factor * 0.2)
            rsi_sell_level = 70 * (1 - volatility_factor * 0.2)
            
            return {
                'profit_rate': adjusted_profit_rate,
                'loss_rate': adjusted_loss_rate,
                'rsi_buy_level': rsi_buy_level,
                'rsi_sell_level': rsi_sell_level,
                'position_size_multiplier': volume_factor
            }
            
        except Exception as e:
            logger.error(f"동적 파라미터 계산 실패: {str(e)}")
            return {}
