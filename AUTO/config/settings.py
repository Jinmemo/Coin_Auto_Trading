from typing import Dict, List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# 프로젝트 루트 디렉토리
BASE_DIR = Path(__file__).resolve().parent.parent.parent

class TradingSettings(BaseSettings):
    # API 키 설정
    UPBIT_ACCESS_KEY: str = Field(default=os.getenv('UPBIT_ACCESS_KEY', ''))
    UPBIT_SECRET_KEY: str = Field(default=os.getenv('UPBIT_SECRET_KEY', ''))
    
    # 텔레그램 설정
    TELEGRAM_TOKEN: str = Field(default=os.getenv('TELEGRAM_TOKEN', ''))
    TELEGRAM_CHAT_ID: str = Field(default=os.getenv('TELEGRAM_CHAT_ID', ''))
    
    # 로깅 설정
    LOG_LEVEL: str = Field(default='INFO')
    LOG_FORMAT: str = Field(default='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    LOG_DIR: str = Field(default=str(BASE_DIR / 'logs'))  # 로그 디렉토리 추가
    
    # 거래 설정
    TRADING_INTERVAL: int = Field(default=60)
    MAX_POSITIONS: int = Field(default=5)
    
    # 포지션 관리 설정
    POSITION_SIZE_RATIO: float = Field(default=0.1)
    MIN_TRADE_AMOUNT: float = Field(default=5000.0)
    STOP_LOSS_RATIO: float = Field(default=-0.02)
    TAKE_PROFIT_RATIO: float = Field(default=0.03)
    
    # 전략 설정
    RSI_PERIOD: int = Field(default=14)
    RSI_OVERSOLD: float = Field(default=30.0)
    RSI_OVERBOUGHT: float = Field(default=70.0)
    
    MACD_FAST: int = Field(default=12)
    MACD_SLOW: int = Field(default=26)
    MACD_SIGNAL: int = Field(default=9)
    
    BOLLINGER_PERIOD: int = Field(default=20)
    BOLLINGER_STD: float = Field(default=2.0)
    
    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        case_sensitive = True

# 설정 인스턴스 생성
settings = TradingSettings()

# 로그 디렉토리 생성
os.makedirs(settings.LOG_DIR, exist_ok=True)
