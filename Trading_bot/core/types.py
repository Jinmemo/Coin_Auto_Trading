from typing import Protocol, Dict, Optional

class TraderInterface(Protocol):
    """Trader 인터페이스 정의"""
    is_running: bool
    trading_coins: list
    
    async def get_balance(self) -> Dict:
        ...
    
    async def get_positions(self) -> Dict:
        ...
    
    async def start(self) -> None:
        ...
    
    async def stop(self) -> None:
        ...
