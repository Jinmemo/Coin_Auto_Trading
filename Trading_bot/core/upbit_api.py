from jwt import encode as jwt_encode  # jwt.encode를 직접 import
import uuid
import hashlib
from urllib.parse import urlencode
import aiohttp
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, Optional, List
import certifi
import ssl
import json
import websockets
import asyncio
import pathlib
import time

from Trading_bot.config.settings import settings

logger = logging.getLogger(__name__)

class UpbitAPI:
    def __init__(self, notifier=None):
        self.base_url = "https://api.upbit.com/v1"
        self.ws_url = "wss://api.upbit.com/websocket/v1"
        self.access_key = settings.UPBIT_ACCESS_KEY
        self.secret_key = settings.UPBIT_SECRET_KEY
        self.notifier = notifier
        
        # SSL 컨텍스트 설정
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.connector = aiohttp.TCPConnector(ssl=self.ssl_context)
        self.session = None
        
        # API 요청 제한 설정 수정
        self.request_limit = 8  # 초당 요청 수 제한 (10에서 8로 감소)
        self.request_window = 1
        self.last_request_time = time.time()
        self.request_count = 0
        self.min_request_interval = 0.15  # 최소 요청 간격 (150ms)

    async def _ensure_session(self):
        """세션이 없거나 닫혀있으면 새로 생성"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(connector=self.connector)
        return self.session

    async def close(self):
        """세션 정리"""
        if self.session and not self.session.closed:
            await self.session.close()
        if self.connector and not self.connector.closed:
            await self.connector.close()

    def _get_headers(self, query: Optional[Dict] = None) -> Dict:
        """JWT 인증 헤더 생성"""
        try:
            payload = {
                'access_key': self.access_key,
                'nonce': str(uuid.uuid4())
            }

            if query:
                query_string = urlencode(query)
                m = hashlib.sha512()
                m.update(query_string.encode())
                query_hash = m.hexdigest()
                payload['query_hash'] = query_hash
                payload['query_hash_alg'] = 'SHA512'

            jwt_token = jwt_encode(
                payload, 
                self.secret_key, 
                algorithm='HS256'
            )

            if isinstance(jwt_token, bytes):
                jwt_token = jwt_token.decode('utf-8')

            return {
                'Authorization': f'Bearer {jwt_token}',
                'Content-Type': 'application/json'
            }
        except Exception as e:
            logger.error(f"헤더 생성 실패: {str(e)}")
            return {}

    async def _handle_rate_limit(self):
        """요청 제한 처리"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time

        # 최소 요청 간격 보장
        if elapsed < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - elapsed)
            current_time = time.time()
            elapsed = current_time - self.last_request_time

        # 초당 요청 수 제한
        if elapsed < self.request_window:
            if self.request_count >= self.request_limit:
                wait_time = self.request_window - elapsed
                logger.debug(f"API 요청 제한 대기: {wait_time:.2f}초")
                await asyncio.sleep(wait_time)
                self.request_count = 0
                self.last_request_time = time.time()
            else:
                self.request_count += 1
        else:
            self.request_count = 1
            self.last_request_time = current_time

    async def get_top_volume_coins(self, limit: int = 20) -> List[str]:
        """REST API를 통한 거래량 상위 코인 조회"""
        try:
            conn = aiohttp.TCPConnector(ssl=False)  # SSL 검증 비활성화
            async with aiohttp.ClientSession(connector=conn) as session:
                # 마켓 코드 조회
                async with session.get(f"{self.base_url}/market/all") as response:
                    if response.status == 200:
                        markets_data = await response.json()
                        krw_markets = [m['market'] for m in markets_data if m['market'].startswith('KRW-')]
                        
                        if not krw_markets:
                            logger.warning("KRW 마켓을 찾을 수 없습니다")
                            return []
                        
                        # 티커 정보 조회
                        market_codes = ','.join(krw_markets)
                        async with session.get(f"{self.base_url}/ticker?markets={market_codes}") as ticker_response:
                            if ticker_response.status == 200:
                                ticker_data = await ticker_response.json()
                                # 거래량 기준 정렬
                                sorted_data = sorted(
                                    ticker_data,
                                    key=lambda x: float(x.get('acc_trade_price_24h', 0)),
                                    reverse=True
                                )
                                return [item['market'] for item in sorted_data[:limit]]
                            else:
                                logger.error(f"티커 정보 조회 실패: {ticker_response.status}")
                    else:
                        logger.error(f"마켓 정보 조회 실패: {response.status}")
            return []
            
        except Exception as e:
            logger.error(f"거래량 상위 코인 조회 실패: {str(e)}")
            return []

    async def get_ohlcv(self, market: str, interval: str = 'minute1', count: int = 200) -> Optional[pd.DataFrame]:
        """REST API를 통한 OHLCV 데이터 조회"""
        try:
            conn = aiohttp.TCPConnector(ssl=False)  # SSL 검증 비활성화
            async with aiohttp.ClientSession(connector=conn) as session:
                url = f"{self.base_url}/candles/{interval}"
                params = {
                    'market': market,
                    'count': count
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if not data:
                            logger.warning(f"{market}에 대한 데이터가 없습니다")
                            return None
                            
                        df = pd.DataFrame(data)
                        df = df.rename(columns={
                            'candle_date_time_utc': 'datetime',
                            'opening_price': 'open',
                            'high_price': 'high',
                            'low_price': 'low',
                            'trade_price': 'close',
                            'candle_acc_trade_volume': 'volume'
                        })
                        df['datetime'] = pd.to_datetime(df['datetime'])
                        df = df.set_index('datetime')
                        df = df[['open', 'high', 'low', 'close', 'volume']]
                        return df
                    else:
                        logger.error(f"OHLCV 데이터 조회 실패 ({market}): {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"OHLCV 데이터 조회 실패 ({market}): {str(e)}")
            return None

    async def get_current_price(self, market: str) -> Optional[float]:
        """REST API를 통한 현재가 조회"""
        try:
            conn = aiohttp.TCPConnector(ssl=False)  # SSL 검증 비활성화
            async with aiohttp.ClientSession(connector=conn) as session:
                url = f"{self.base_url}/ticker"
                params = {'markets': market}
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data and len(data) > 0:
                            return float(data[0]['trade_price'])
                    return None
                    
        except Exception as e:
            logger.error(f"현재가 조회 실패 ({market}): {str(e)}")
            return None

    async def update_trading_coins(self) -> List[str]:
        """거래 대상 코인 목록 업데이트"""
        try:
            url = f"{self.base_url}/market/all"
            
            connector = aiohttp.TCPConnector(ssl=self.ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        markets = await response.json()
                        krw_markets = [
                            market['market'] for market in markets 
                            if market['market'].startswith('KRW-')
                        ]
                        logger.info(f"거 대상 코인 업데이: {krw_markets}")
                        return krw_markets
                    return []
                    
        except Exception as e:
            logger.error(f"거래 대상 코인 목록 업데이트 실패: {str(e)}")
            return []

    async def get_markets_info(self, markets: str) -> Optional[List[Dict]]:
        """여러 마켓 정보 일괄 조회"""
        try:
            await self._handle_rate_limit()
            session = await self._ensure_session()
            
            url = f"{self.base_url}/ticker"
            params = {'markets': markets}
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list):
                        data.sort(key=lambda x: x['market'])
                        return data
                    logger.error("마켓 정보 형식 오류")
                    return None
                logger.error(f"마켓 정보 일괄 조회 실패: {response.status}")
                return None
                
        except aiohttp.ClientError as e:
            logger.error(f"마켓 정보 일괄 조회 중 네트워크 오류: {str(e)}")
            await self.close()  # 오류 발생 시 세션 재생성을 위해 닫기
            return None
        except Exception as e:
            logger.error(f"마켓 정보 일괄 조회 실패: {str(e)}")
            return None

    async def get_market_info(self, market: str) -> Optional[Dict]:
        """단일 마켓 정보 조회"""
        try:
            await self._handle_rate_limit()
            session = await self._ensure_session()
            
            url = f"{self.base_url}/ticker"
            params = {'markets': market}
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and isinstance(data, list):
                        return data[0]
                    return None
                elif response.status == 429:  # Too Many Requests
                    logger.warning("API 요청 제한 초과, 잠시 대기 후 재시도")
                    await asyncio.sleep(1)  # 1초 대기
                    return await self.get_market_info(market)  # 재시도
                else:
                    logger.error(f"마켓 정보 조회 실패: {response.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"마켓 정보 조회 실패 ({market}): {str(e)}")
            return None

    async def get_balance(self) -> Optional[float]:
        """KRW 잔고 조회"""
        try:
            await self._handle_rate_limit()
            
            headers = self._get_headers()
            url = f"{self.base_url}/accounts"
            
            connector = aiohttp.TCPConnector(ssl=self.ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        accounts = await response.json()
                        for account in accounts:
                            if account['currency'] == 'KRW':
                                balance = float(account['balance'])
                                logger.debug(f"잔고 조회 성공: {balance:,.0f}원")
                                return balance
                        return 0.0
                    else:
                        logger.error(f"잔고 조회 실패: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"잔고 조회 실패: {str(e)}")
            return None

    async def place_order(self, market: str, side: str, volume: Optional[float] = None, 
                         price: Optional[float] = None) -> Optional[Dict]:
        """주문 실행"""
        try:
            await self._handle_rate_limit()
            
            query = {
                'market': market,
                'side': side,
                'ord_type': 'limit' if price else 'market'
            }
            
            if volume:
                query['volume'] = str(volume)
            if price:
                query['price'] = str(price)
            
            headers = self._get_headers(query)
            url = f"{self.base_url}/orders"
            
            conn = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=conn) as session:
                async with session.post(url, headers=headers, json=query) as response:
                    if response.status == 201:
                        return await response.json()
                    else:
                        logger.error(f"주문 실패: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"주문 실패: {str(e)}")
            return None

    async def get_candles(self, market: str, unit: str = 'minutes', count: int = 200) -> Optional[List[Dict]]:
        """캔들 데이터 조회"""
        try:
            await self._handle_rate_limit()
            session = await self._ensure_session()
            
            url = f"{self.base_url}/candles/{unit}/1"
            params = {
                'market': market,
                'count': count
            }
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                logger.error(f"캔들 데이터 조회 실패: {response.status}")
                return None
                
        except Exception as e:
            logger.error(f"캔들 데이터 조회 실패 ({market}): {str(e)}")
            return None