from typing import Dict, List, Optional
from datetime import datetime
from ..core.analyzer import MarketState
from .base import BaseStrategy
from .scalping import ScalpingStrategy
from .swing import SwingStrategy
from .dca_strategy import DCAStrategy

class StrategyManager:
    """전략 관리자 클래스"""
    
    def __init__(self):
        self.strategies: Dict[str, BaseStrategy] = {}
        self._initialize_strategies()
        self.active_strategy: Optional[BaseStrategy] = None

    def _initialize_strategies(self):
        """사용 가능한 전략들 초기화"""
        strategy_configs = {
            'SCALPING': {
                'name': 'Scalping Strategy',
                'profit_rate': 0.01,
                'loss_rate': 0.02,
                'volume_multiplier': 1.5,
                'min_volatility': 0.003,
                'max_volatility': 0.02
            },
            'SWING': {
                'name': 'Swing Strategy',
                'profit_rate': 0.03,
                'loss_rate': 0.05,
                'trend_confirmation_period': 3,
                'min_trend_strength': 0.02,
                'volume_surge_threshold': 2.0
            },
            'DCA': {
                'name': 'DCA Strategy',
                'entry_intervals': [-0.05, -0.1, -0.15, -0.2],
                'position_increase_factor': 1.5,
                'max_dca_count': 5,
                'min_interval_hours': 24,
                'recovery_target_rate': 0.02
            }
        }

        # 전략 인스턴스 생성
        self.strategies = {
            'SCALPING': ScalpingStrategy('SCALPING', strategy_configs['SCALPING']),
            'SWING': SwingStrategy('SWING', strategy_configs['SWING']),
            'DCA': DCAStrategy('DCA', strategy_configs['DCA'])
        }

        # 기본 전략 설정
        self.active_strategy = self.strategies['DCA']
