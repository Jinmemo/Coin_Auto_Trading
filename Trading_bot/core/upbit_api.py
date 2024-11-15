from jwt import encode as jwt_encode  # jwt.encode를 직접 import
import uuid
import hashlib
from urllib.parse import urlencode
import aiohttp
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, Optional, List, Union, Any
import certifi
import ssl
import json
import websockets
import asyncio
import pathlib
import time
import jwt
from decimal import Decimal
from aiohttp import TCPConnector
from asyncio import Lock, sleep

from Trading_bot.config.settings import settings

logger = logging.getLogger(__name__)

class UpbitAPI:
    def __init__(self):
        self.access_key = settings.UPBIT_ACCESS_KEY
        self.secret_key = settings.UPBIT_SECRET_KEY
        self.session = None
        self.markets = None
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        logger.info("UpbitAPI 객체 생성")
        self._request_lock = Lock()
        self._last_request_time = 0
        self._request_interval = 0.1  # 100ms
        self._cached_balances = {}
        self._last_balance_update = 0
        self._balance_update_interval = 5  # 5초

    async def initialize(self):
        """API 초기화"""
        try:
            logger.info("UpbitAPI 초기화 시작")
            
            # API 키 확인
            if not self.access_key or not self.secret_key:
                raise ValueError("API 키가 설정되지 않았습니다")
            
            # SSL 컨텍스트로 커넥터 생성
            connector = TCPConnector(ssl=self.ssl_context)
            
            # 세션 생성
            self.session = aiohttp.ClientSession(connector=connector)
            
            # 마켓 정보 초기화
            await self.update_markets()
            
            logger.info("UpbitAPI 초기화 완료")
            return True
            
        except Exception as e:
            logger.error(f"UpbitAPI 초기화 실패: {str(e)}")
            if self.session and not self.session.closed:
                await self.session.close()
            return False

    async def update_markets(self):
        """마켓 정보 업데이트"""
        try:
            url = "https://api.upbit.com/v1/market/all"
            async with self.session.get(url, ssl=self.ssl_context) as response:
                if response.status == 200:
                    self.markets = await response.json()
                    logger.info(f"마켓 정보 업데이트 완료: {len(self.markets)}개")
                else:
                    raise Exception(f"마켓 정보 조회 실패: {response.status}")
        except Exception as e:
            logger.error(f"마켓 정보 업데이트 실패: {str(e)}")
            raise

    async def _wait_for_rate_limit(self):
        """API 요청 간격 제어"""
        async with self._request_lock:
            current_time = time.time()
            time_since_last_request = current_time - self._last_request_time
            if time_since_last_request < self._request_interval:
                await sleep(self._request_interval - time_since_last_request)
            self._last_request_time = time.time()

    async def get_all_balances(self) -> Optional[Dict]:
        """전체 잔고 조회 (캐시 사용)"""
        try:
            current_time = time.time()
            
            # 캐시된 데이터가 있고 업데이트 간격이 지나지 않았으면 캐시 사용
            if self._cached_balances and \
               current_time - self._last_balance_update < self._balance_update_interval:
                return self._cached_balances

            await self._wait_for_rate_limit()

            # JWT 토큰 생성
            payload = {
                'access_key': self.access_key,
                'nonce': str(uuid.uuid4()),
            }
            jwt_token = jwt.encode(payload, self.secret_key)
            headers = {
                "Authorization": f"Bearer {jwt_token}",
                "Content-Type": "application/json"
            }

            # 잔고 조회 요청
            url = "https://api.upbit.com/v1/accounts"
            async with self.session.get(url, headers=headers, ssl=self.ssl_context) as response:
                if response.status == 200:
                    accounts = await response.json()
                    
                    # 잔고 데이터 캐시
                    self._cached_balances = {
                        account['currency']: {
                            'currency': account['currency'],
                            'total': account['balance'],
                            'locked': account['locked'],
                            'avg_buy_price': account['avg_buy_price'],
                            'unit_currency': 'KRW'
                        }
                        for account in accounts
                    }
                    self._last_balance_update = current_time
                    
                    return self._cached_balances
                else:
                    error_msg = await response.text()
                    raise Exception(f"API 요청 실패 (상태 코드: {response.status}): {error_msg}")

        except Exception as e:
            logger.error(f"전체 잔고 조회 실패: {str(e)}")
            return None

    async def get_balance(self) -> Optional[float]:
        """계좌 잔고 조회"""
        try:
            # API 키 검증
            if not self.access_key or not self.secret_key:
                raise ValueError("API 키가 설정되지 않았습니다")

            # JWT 토큰 생성
            payload = {
                'access_key': self.access_key,
                'nonce': str(uuid.uuid4()),
            }
            jwt_token = jwt.encode(payload, self.secret_key)
            headers = {
                "Authorization": f"Bearer {jwt_token}",
                "Content-Type": "application/json"
            }

            # 잔고 조회 요청
            url = "https://api.upbit.com/v1/accounts"
            async with self.session.get(url, headers=headers, ssl=self.ssl_context) as response:
                if response.status == 200:
                    accounts = await response.json()
                    
                    # KRW 잔고 찾기
                    for account in accounts:
                        if account['currency'] == 'KRW':
                            try:
                                balance = float(account['balance'])
                                logger.debug(f"KRW 잔고 조회 성공: {balance:,.0f}원")
                                return balance
                            except (ValueError, KeyError) as e:
                                logger.error(f"잔고 데이터 변환 실패: {str(e)}")
                                return None
                    
                    # KRW 계좌가 없는 경우
                    logger.warning("KRW 계좌를 찾을 수 없습니다")
                    return 0.0
                else:
                    error_msg = await response.text()
                    raise Exception(f"API 요청 실패 (상태 코드: {response.status}): {error_msg}")

        except aiohttp.ClientError as e:
            logger.error(f"API 연결 실패: {str(e)}")
            return None
        except jwt.PyJWTError as e:
            logger.error(f"JWT 토큰 생성 실패: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"잔고 조회 중 오류 발생: {str(e)}")
            return None

    async def get_top_volume_coins(self, limit: int = 20) -> List[str]:
        """거래량 상위 코인 조회"""
        try:
            if not self.markets:
                await self.update_markets()
                if not self.markets:
                    raise Exception("마켓 정보가 없습니다")

            # KRW 마켓만 필터링
            krw_markets = [m['market'] for m in self.markets if m['market'].startswith('KRW-')]
            if not krw_markets:
                raise Exception("KRW 마켓을 찾을 수 없습니다")

            # 티커 정보 조회
            url = "https://api.upbit.com/v1/ticker"
            params = {'markets': ','.join(krw_markets)}
            
            async with self.session.get(url, params=params, ssl=self.ssl_context) as response:
                if response.status == 200:
                    tickers = await response.json()
                    if not tickers:
                        raise Exception("티커 데이터가 비어있습니다")

                    # 거래량 기준 정렬
                    sorted_tickers = sorted(
                        tickers,
                        key=lambda x: float(x.get('acc_trade_price_24h', 0)),
                        reverse=True
                    )

                    # 상위 코인 추출
                    top_coins = [ticker['market'] for ticker in sorted_tickers[:limit]]
                    logger.debug(f"거래량 상위 {limit}개 코인 조회 성공")
                    return top_coins

                else:
                    error_msg = await response.text()
                    raise Exception(f"API 요청 실패 (상태 코드: {response.status}): {error_msg}")

        except aiohttp.ClientError as e:
            logger.error(f"API 연결 실패: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"거래량 상위 코인 조회 중 오류: {str(e)}")
            return []

    async def close(self):
        """API 세션 종료"""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
                logger.info("UpbitAPI 세션 종료")
        except Exception as e:
            logger.error(f"세션 종료 중 오류: {str(e)}")

    async def get_coin_balance(self, market: str) -> Optional[Dict]:
        """특정 코인의 잔고 조회 (캐시 사용)"""
        try:
            currency = market.split('-')[1]
            
            # 전체 잔고 조회
            balances = await self.get_all_balances()
            if not balances:
                return None

            # 해당 코인 잔고 반환
            return balances.get(currency, {
                'currency': currency,
                'total': '0',
                'locked': '0',
                'avg_buy_price': '0',
                'unit_currency': 'KRW'
            })

        except Exception as e:
            logger.error(f"{market} 잔고 조회 중 오류: {str(e)}")
            return None

    async def calculate_position_value(self, market: str) -> Optional[Dict]:
        """포지션 가치 계산"""
        try:
            balance_info = await self.get_coin_balance(market)
            if not balance_info:
                return None

            current_price = await self.get_current_price(market)
            if not current_price:
                return None

            total = float(balance_info['total'])
            avg_price = float(balance_info['avg_buy_price'])
            
            if total > 0 and avg_price > 0:
                current_value = total * current_price
                invested_value = total * avg_price
                profit_loss = current_value - invested_value
                profit_rate = (profit_loss / invested_value) * 100

                return {
                    'current_value': current_value,
                    'invested_value': invested_value,
                    'profit_loss': profit_loss,
                    'profit_rate': profit_rate
                }
            
            return {
                'current_value': 0,
                'invested_value': 0,
                'profit_loss': 0,
                'profit_rate': 0
            }

        except Exception as e:
            logger.error(f"{market} 포지션 가치 계산 실패: {str(e)}")
            return None