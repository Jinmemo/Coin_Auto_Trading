from dotenv import load_dotenv
import os
import pyupbit
import jwt
import uuid
import requests
from datetime import datetime
import time
from datetime import datetime, timedelta
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np
import logging
import sqlite3
from contextlib import contextmanager

# .env 파일 로드
load_dotenv()

class UpbitAPI:
    def __init__(self):
        self.access_key = os.getenv('UPBIT_ACCESS_KEY')
        self.secret_key = os.getenv('UPBIT_SECRET_KEY')
        self.upbit = pyupbit.Upbit(self.access_key, self.secret_key)
        # 캐시 초기화 추가
        self.cache = {}

    def get_current_price(self, ticker):
        """현재가 조회"""
        try:
            if isinstance(ticker, tuple):
                ticker = ticker[0]
                
            if not ticker.startswith('KRW-'):
                ticker = f'KRW-{ticker}'

            cache_key = f"{ticker}_price"
            current_time = datetime.now()
            
            # 캐시 확인
            if cache_key in self.cache:
                cached_data = self.cache[cache_key]
                if (current_time - cached_data['timestamp']).total_seconds() < 1.0:
                    return cached_data['price']
            
            # 재시도 로직 추가
            max_retries = 3
            retry_delay = 0.5
            
            for attempt in range(max_retries):
                try:
                    url = f"https://api.upbit.com/v1/ticker?markets={ticker}"
                    response = requests.get(url, timeout=2)
                    
                    if response.status_code == 429:  # Rate limit
                        logger.warning(f"[WARNING] {ticker} Rate limit 발생, {attempt+1}번째 재시도...")
                        time.sleep(retry_delay * (attempt + 1))
                        continue
                        
                    if response.status_code == 200:
                        result = response.json()
                        if result and isinstance(result, list) and result[0]:
                            price = float(result[0]['trade_price'])
                            # 캐시 업데이트
                            self.cache[cache_key] = {
                                'timestamp': current_time,
                                'price': price
                            }
                            return price
                            
                    time.sleep(retry_delay)
                    
                except requests.exceptions.Timeout:
                    logger.warning(f"[WARNING] {ticker} 타임아웃, {attempt+1}번째 재시도...")
                    continue
                    
            logger.warning(f"[WARNING] {ticker} 현재가 조회 실패")
            return None
                
        except Exception as e:
            logger.error(f"[ERROR] {ticker} 현재가 조회 중 오류: {str(e)}")
            return None

    # get_balances 메서드 추가
    def get_balances(self):
        """전체 계좌 잔고 조회"""
        try:
            return self.upbit.get_balances()
        except Exception as e:
            logger.error(f"[ERROR] 잔고 조회 중 오류: {str(e)}")
            return []
        
    def get_balance(self, ticker="KRW"):
        """특정 코인/원화의 잔고 조회"""
        try:
            # KRW인 경우 원화 잔고 조회
            if ticker == "KRW":
                return self.upbit.get_balance()
                    
            coin = ticker.replace("KRW-", "")
            balance = self.upbit.get_balance(coin)
                
            if balance is not None:
                return float(balance)
            else:
                logger.error(f"[ERROR] {ticker} 잔고 조회 실패")
                return None
                    
        except Exception as e:
            logger.error(f"[ERROR] {ticker} 잔고 조회 중 오류: {str(e)}")
            return None    
            
    def sell_market_order(self, ticker, volume):
        """시장가 매도 주문"""
        try:
            print(f"[DEBUG] {ticker} 시장가 매도 주문 시도: {volume}")
            
            # 실제 보유 수량 다시 확인
            actual_volume = self.upbit.get_balance(ticker)
            if not actual_volume:
                logger.error(f"[ERROR] {ticker} 실제 보유 수량 조회 실패")
                return False, "보유 수량 조회 실패"
                
            # 수량이 다른 경우 로그 출력
            if abs(actual_volume - volume) > 0.00000001:
                logger.warning(f"[WARNING] {ticker} 수량 불일치 - 요청: {volume}, 실제: {actual_volume}")
                volume = actual_volume
            
            # 소수점 자리 조정 (코인마다 다름)
            volume = float(format(volume, '.8f'))  # 8자리로 조정
            
            if volume <= 0:
                logger.error(f"[ERROR] {ticker} 매도 수량이 0보다 작거나 같음")
                return False, "잘못된 매도 수량"
            
            # 주문 실행
            order = self.upbit.sell_market_order(ticker, volume)
            
            if order and 'uuid' in order:
                logger.info(f"[INFO] {ticker} 시장가 매도 주문 성공: {order['uuid']}")
                return True, order['uuid']
            else:
                logger.error(f"[ERROR] {ticker} 시장가 매도 주문 실패: {order}")
                return False, "주문 실패"
                
        except Exception as e:
            logger.error(f"[ERROR] {ticker} 시장가 매도 주문 중 오류: {str(e)}")
            return False, str(e)

    def buy_market_order(self, ticker, price):
        """시장가 매수 주문"""
        try:
            # 티커 형식 확인 및 수정
            if isinstance(ticker, tuple):
                ticker = ticker[0]
            if not ticker.startswith('KRW-'):
                ticker = f'KRW-{ticker}'
                
            # 최소 주문 금액 확인 (업비트 최소 주문 금액: 5,000원)
            if price < 5000:
                logger.error(f"[ERROR] {ticker} 최소 주문 금액(5,000원) 미만: {price}원")
                return False, "최소 주문 금액 미만"

            # 원화 잔고 확인
            krw_balance = self.get_balance("KRW")
            if krw_balance < price:
                logger.error(f"[ERROR] {ticker} 잔고 부족 (필요: {price:,}원, 보유: {krw_balance:,}원)")
                return False, "잔고 부족"

            # 주문 금액을 정수로 변환
            price = int(price)
                
            # 주문 실행
            order = self.upbit.buy_market_order(ticker, price)
            
            if order and 'uuid' in order:
                logger.info(f"[INFO] {ticker} 시장가 매수 주문 성공: {order['uuid']}")
                return True, order['uuid']
            else:
                logger.error(f"[ERROR] {ticker} 시장가 매수 주문 실패: {order}")
                return False, "주문 실패"
                
        except Exception as e:
            logger.error(f"[ERROR] {ticker} 시장가 매수 주문 중 오류: {str(e)}")
            return False, str(e)

    def get_balance(self, ticker="KRW"):
        """특정 코인/원화의 잔고 조회"""
        try:
            return self.upbit.get_balance(ticker)
        except Exception as e:
            logger.error(f"[ERROR] {ticker} 잔고 조회 중 오류: {str(e)}")
            return 0

class TelegramBot:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.last_message_time = datetime.now() - timedelta(seconds=30)
        self.message_cooldown = 0.5  # 메시지 간 최소 간격
        
        # 세션 설정 개선
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,  # 최대 재시도 횟수
            backoff_factor=0.5,  # 재시도 간격
            status_forcelist=[429, 500, 502, 503, 504]  # 재시도할 HTTP 상태 코드
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retry_strategy))
        
        # 타임아웃 증가
        self.timeout = (5, 10)  # (연결 타임아웃, 읽기 타임아웃)
        
        if not self.token or not self.chat_id:
            raise ValueError("텔레그램 토큰 또는 채팅 ID가 설정되지 않았습니다.")

    def send_message(self, message, parse_mode=None):
        """메시지 전송 - 재시도 로직 추가"""
        try:
            # 메시지 전송 간격 제어
            current_time = datetime.now()
            if (current_time - self.last_message_time).total_seconds() < self.message_cooldown:
                time.sleep(self.message_cooldown)
            
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            params = {
                'chat_id': self.chat_id,
                'text': message
            }
            if parse_mode:
                params['parse_mode'] = parse_mode
            
            response = self.session.post(
                url, 
                json=params, 
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                self.last_message_time = datetime.now()
                return True
                
            print(f"[WARNING] 텔레그램 응답 코드: {response.status_code}")
            return False
            
        except requests.exceptions.Timeout:
            print("[WARNING] 텔레그램 전송 타임아웃")
            return False
        except requests.exceptions.RequestException as e:
            print(f"[WARNING] 텔레그램 전송 오류: {str(e)}")
            return False
        except Exception as e:
            print(f"[ERROR] 텔레그램 기타 오류: {str(e)}")
            return False
            
    def __del__(self):
        """소멸자에서 세션 정리"""
        try:
            self.session.close()
        except:
            pass

class MarketAnalyzer:
    def __init__(self, upbit_api, position_manager):
        self.upbit = upbit_api
        self.position_manager = position_manager  # PositionManager 인스턴스 저장
        self.tickers = []  # 빈 리스트로 초기화
        self.timeframes = {'minute30': {'interval': 'minute30', 'count': 100}}
        self.trading_conditions = {
            'rsi_strong_oversold': 32,
            'rsi_oversold': 37,
            'rsi_overbought': 63,
            'rsi_strong_overbought': 68,
            'bb_squeeze': 0.5,
            'bb_expansion': 2.0,
            'position_size_strong': 1.2,
            'position_size_normal': 1.0
        }
        self.market_state = 'normal'
        # 캐시 관련 설정 수정
        self.cache = {}
        self.cache_duration = 300 
        self.last_analysis = {}
        self.analysis_interval = timedelta(seconds=60)
        self.analysis_count = 0
        self.max_analysis_per_cycle = 20
        self._api_calls = []  # API 호출 추적
        self._update_queue = set()  # 업데이트 대기열
        
        # API 요청 세션 최적화
        self.session = self._setup_session()
        
        # 초기 티커 목록 업데이트
        self.update_tickers()
        print(f"[INFO] 초기 티커 목록 로드됨: {len(self.tickers)}개")

        # ThreadPool 초기화 (티커 목록 업데이트 후)
        self.thread_pool = ThreadPoolExecutor(
            max_workers=max(5, min(10, len(self.tickers))),  # 최소 5개, 최대 10개
            thread_name_prefix="analyzer"
        )        

        # 신호 처리 이력 추가
        self.signal_history = {}
        self.signal_cooldown = 60.0  # 신호 재처리 대기 시간 (초)

    def _setup_session(self):
        """API 요청을 위한 최적화된 세션 설정"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10
        )
        session.mount('https://', adapter)
        return session    

    def update_tickers(self):
        """티커 목록 업데이트 (최적화 버전)"""
        try:
            print("[INFO] 티커 목록 업데이트 중...")
            # KRW 마켓의 티커만 가져오기
            all_tickers = pyupbit.get_tickers(fiat="KRW")

                    # BTC 제외
            if "KRW-BTC" in all_tickers:
                all_tickers.remove("KRW-BTC")
            
            if not all_tickers:
                print("[ERROR] 티커 목록 조회 실패")
                return
            
            # 24시간 거래량 한 번에 조회
            try:
                url = "https://api.upbit.com/v1/ticker"
                params = {"markets": ",".join(all_tickers)}
                response = self.session.get(url, params=params, timeout=5)
                
                if response.status_code == 200:
                    ticker_data = response.json()
                    # 거래량 기준으로 정렬
                    sorted_tickers = sorted(
                        ticker_data,
                        key=lambda x: float(x.get('acc_trade_price_24h', 0)),
                        reverse=True
                    )
                    
                    # 항상 상위 20개 티커만 선택
                    self.tickers = [ticker['market'] for ticker in sorted_tickers[:20]]
                    print(f"[INFO] 티커 목록 업데이트 완료: {len(self.tickers)}개")
                    
                    # 상위 20개 티커 정보 출력
                    print("[INFO] 상위 20개 티커 (24시간 거래대금):")
                    for i, ticker_info in enumerate(sorted_tickers[:20], 1):
                        volume = float(ticker_info.get('acc_trade_price_24h', 0)) / 1000000  # 백만원 단위
                        price = float(ticker_info.get('trade_price', 0))
                        print(f"    {i}. {ticker_info['market']}: "
                            f"거래대금 {volume:,.0f}백만원, "
                            f"현재가 {price:,.0f}원")
                    
                else:
                    logger.error(f"[ERROR] 거래량 조회 실패: {response.status_code}")
                    # 기본 티커 목록의 상위 20개만 사용
                    self.tickers = all_tickers[:20]
                    
            except Exception as e:
                print(f"[WARNING] 거래량 조회 중 오류: {e}")
                # 오류 발생 시 기본 티커 목록의 상위 20개만 사용
                self.tickers = all_tickers[:20]
                
            # 캐시 초기화
            self.cache = {}
            self.last_analysis = {}
            
        except Exception as e:
            print(f"[ERROR] 티커 업데이트 중 오류: {e}")
            print(traceback.format_exc())

    def __del__(self):
        """소멸자에서 스레드풀 정리"""
        if hasattr(self, 'thread_pool'):
            self.thread_pool.shutdown(wait=False)
        if hasattr(self, 'session'):
            self.session.close()

    def get_ohlcv(self, ticker):
        """OHLCV 데이터 조회 (최적화 버전)"""
        try:
            current_time = datetime.now()
            cache_key = f"{ticker}_ohlcv"
            
            # 캐시 확인 (메모리 효율성 개선)
            if cache_key in self.cache:
                cached_data = self.cache[cache_key]
                elapsed_time = (current_time - cached_data['timestamp']).total_seconds()
                
                # 캐시가 유효한 경우
                if elapsed_time < 180.0:  # 기본 캐시 유효시간
                    return cached_data['data']
                
                # 캐시 만료 시 점진적 업데이트 (배치 처리)
                if elapsed_time < 10.0:
                    if not hasattr(self, '_update_queue'):
                        self._update_queue = set()
                    
                    if ticker not in self._update_queue:
                        self._update_queue.add(ticker)
                        self.thread_pool.submit(self._batch_update_cache)
                    return cached_data['data']

            # API 호출 관리 (레이트 리밋 최적화)
            if not hasattr(self, '_api_calls'):
                self._api_calls = []
            
            # 1분당 최대 호출 수 제한
            current_minute = current_time.replace(second=0, microsecond=0)
            self._api_calls = [t for t in self._api_calls if (current_time - t).total_seconds() < 60]
            
            if len(self._api_calls) >= 60:  # 분당 60회 제한
                time.sleep(0.1)  # 기본 대기
                return self.cache.get(cache_key, {}).get('data')
            
            # 새로운 데이터 조회 (메모리 최적화)
            df = pyupbit.get_ohlcv(ticker, interval="minute30", count=100)
            self._api_calls.append(current_time)
            
            if df is not None and len(df) >= 20:
                # 데이터 전처리 (메모리 효율성)
                df = df.rename(columns={
                    'open': '시가', 'high': '고가', 
                    'low': '저가', 'close': '종가',
                    'volume': '거래량'
                })
                
                # 불필요한 컬럼 제거
                df = df[['시가', '고가', '저가', '종가', '거래량']]
                
                # 메모리 최적화
                for col in df.columns:
                    if df[col].dtype == 'float64':
                        df[col] = df[col].astype('float32')
                
                # 캐시 업데이트 (메모리 관리)
                self.cache[cache_key] = {
                    'timestamp': current_time,
                    'data': df
                }
                
                # 캐시 크기 관리
                if len(self.cache) > 100:  # 최대 캐시 항목 수
                    oldest = min(self.cache.items(), key=lambda x: x[1]['timestamp'])
                    del self.cache[oldest[0]]
                    
                return df
                
            return None

        except Exception as e:
            logger.error(f"[ERROR] {ticker} OHLCV 데이터 조회 실패: {str(e)}")
            return None

    def _batch_update_cache(self):
        """캐시 일괄 업데이트"""
        try:
            if hasattr(self, '_update_queue') and self._update_queue:
                tickers = list(self._update_queue)
                self._update_queue.clear()
                
                for ticker in tickers:
                    time.sleep(0.5)  # API 부하 방지
                    self.get_ohlcv(ticker)
                    
        except Exception as e:
            logger.error(f"[ERROR] 캐시 일괄 업데이트 실패: {str(e)}")

    def _calculate_indicators(self, df):
        """기술적 지표 계산"""
        try:
            if df is None or len(df) < 20:
                print("[WARNING] 충분한 데이터가 없습니다")
                return None
                
            # 데이터 복사 및 전처리
            df = df.copy()
            df['종가'] = pd.to_numeric(df['종가'], errors='coerce')
            df['고가'] = pd.to_numeric(df['고가'], errors='coerce')
            df['저가'] = pd.to_numeric(df['저가'], errors='coerce')
            df['거래량'] = pd.to_numeric(df['거래량'], errors='coerce')
            df = df.dropna()
            
            if len(df) < 20:
                print("[WARNING] 유효한 데이터가 부족합니다")
                return None

            # RSI 계산
            delta = df['종가'].diff()
            up = delta.copy()
            down = delta.copy()
            up[up < 0] = 0
            down[down > 0] = 0
            
            period = 14
            _gain = up.ewm(com=(period - 1), min_periods=period).mean()
            _loss = down.abs().ewm(com=(period - 1), min_periods=period).mean()
            
            RS = _gain / _loss
            df['RSI'] = 100 - (100 / (1 + RS))

            # 볼린저 밴드 계산
            unit = 2  # 표준편차 승수
            window = 20  # 기간
            
            df['중심선'] = df['종가'].rolling(window=window).mean()
            band = unit * df['종가'].rolling(window=window).std(ddof=0)
            
            df['상단밴드'] = df['중심선'] + band
            df['하단밴드'] = df['중심선'] - band
            
            # %B 계산
            df['%B'] = (df['종가'] - df['하단밴드']) / (df['상단밴드'] - df['하단밴드'])
            
            # 밴드폭 계산
            df['밴드폭'] = (df['상단밴드'] - df['하단밴드']) / df['중심선'] * 100

            # MACD 추가
            exp1 = df['종가'].ewm(span=12, adjust=False).mean()
            exp2 = df['종가'].ewm(span=26, adjust=False).mean()
            df['MACD'] = exp1 - exp2
            df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
            df['MACD_Hist'] = df['MACD'] - df['Signal']

            # 스토캐스틱 추가
            n = 14
            df['%K'] = ((df['종가'] - df['저가'].rolling(n).min()) / 
                      (df['고가'].rolling(n).max() - df['저가'].rolling(n).min())) * 100
            df['%D'] = df['%K'].rolling(3).mean()

            # 거래량 지표
            df['VMA5'] = df['거래량'].rolling(window=5).mean()
            df['Volume_Ratio'] = df['거래량'] / df['VMA5']


            # NaN 값 처리
            df = df.dropna()

            return df

        except Exception as e:
            print(f"[ERROR] 지표 계산 중 오류: {str(e)}")
            print(traceback.format_exc())
            return None

    def analyze_market(self, ticker):
        """시장 분석 수행 (최적화 버전)"""
        try:
            current_time = datetime.now()
            cache_key = f"{ticker}_analysis"
            
            # 캐시 확인 및 유효성 검사
            if cache_key in self.cache:
                cached_data = self.cache[cache_key]
                if isinstance(cached_data, dict) and 'timestamp' in cached_data:
                    elapsed_time = (current_time - cached_data['timestamp']).total_seconds()
                    if elapsed_time < 180.0: 
                        return cached_data['data']

            # OHLCV 데이터 조회 (자동으로 캐시 관리됨)
            df = self.get_ohlcv(ticker)
            if df is None:
                return None

            # 지표 계산
            analyzed_df = self._calculate_indicators(df)
            if analyzed_df is None:
                return None

            # 결과 생성 (메모리 최적화)
            last_row = analyzed_df.iloc[-1]
            analysis_result = {
                'ticker': ticker,
                'current_price': float(last_row['종가']),
                'timeframes': {
                    'minute30': {
                        'rsi': float(last_row['RSI']),
                        'bb_bandwidth': float(last_row['밴드폭']),
                        'percent_b': float(last_row['%B'])  # 이미 계산된 값 사용
                    }
                },
                'timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S')
            }

            # 캐시 관리
            self.cache[cache_key] = {
                'timestamp': current_time,
                'data': analysis_result
            }
            
            # 캐시 크기 제한
            if len(self.cache) > 100:
                oldest = min(self.cache.items(), key=lambda x: x[1]['timestamp'])
                del self.cache[oldest[0]]

            return analysis_result

        except Exception as e:
            logger.error(f"[ERROR] {ticker} 분석 중 오류: {str(e)}")
            return None

    def analyze_multiple_markets(self, tickers):
        """여러 시장 동시 분석 (최적화 버전)"""
        try:
            if not tickers:
                self.update_tickers()
                tickers = self.tickers
                
            # 항상 20개 유지
            if len(tickers) < 20:
                print("[WARNING] 티커 목록이 20개 미만입니다. 티커 목록을 업데이트합니다.")
                self.update_tickers()
                tickers = self.tickers
                
            results = {}
            current_time = datetime.now()
            
            # 분석이 필요한 티커만 필터링 (항상 20개 유지)
            analysis_tickers = tickers[:20]  # 상위 20개로 제한
            print(f"[INFO] 총 {len(analysis_tickers)}개 코인 분석 시작...")
            
            if not analysis_tickers:
                return {k: v['data'] for k, v in self.cache.items() if k.endswith('_analysis')}
                
            # 배치 처리로 변경
            batch_size = 5
            completed = 0
            
            for i in range(0, len(analysis_tickers), batch_size):
                batch = analysis_tickers[i:i + batch_size]
                futures = []
                
                # 배치 분석 실행
                for ticker in batch:
                    future = self.thread_pool.submit(self.analyze_market, ticker)
                    futures.append((ticker, future))
                
                # 배치 결과 수집
                for ticker, future in futures:
                    try:
                        result = future.result(timeout=3)
                        if result and 'timeframes' in result:
                            results[ticker] = result
                            completed += 1
                            self._print_analysis_result(ticker, result, completed, len(analysis_tickers))
                    except Exception as e:
                        logger.error(f"[ERROR] {ticker} 분석 실패: {e}")
                        
                time.sleep(0.5)  # API 레이트 리밋 방지
                
            return results
        except Exception as e:
            logger.error(f"[ERROR] 시장 분석 중 오류 발생: {e}")
            return {}

    def _print_analysis_result(self, ticker, result, completed, total):
        """분석 결과 출력 (분리된 메소드)"""
        try:
            timeframe_data = result['timeframes']['minute30']
            df = self.get_ohlcv(ticker)
            
            if df is None or len(df) < 3:
                return
                
            # 이동평균선 계산 전에 종가 데이터 숫자형으로 변환
            df['종가'] = pd.to_numeric(df['종가'], errors='coerce')
            df['MA5'] = df['종가'].rolling(window=5).mean()
            df['MA20'] = df['종가'].rolling(window=20).mean()
            
            last_row = df.iloc[-1]
            prev_row = df.iloc[-2]
            
            # 기본 지표 출력
            print(f"[INFO] {ticker} 분석 완료 ({completed}/{total})")
            print(f"  - RSI: {timeframe_data['rsi']:.1f}")
            print(f"  - %B: {timeframe_data['percent_b']:.2f}")
            print(f"  - 밴드폭: {timeframe_data['bb_bandwidth']:.1f}%")
            
            # 추세 판단
            if not pd.isna(last_row['MA5']) and not pd.isna(last_row['MA20']):
                is_uptrend = last_row['MA5'] > last_row['MA20']
                trend = "상승" if is_uptrend else "하락"
                print(f"  - 추세: {trend}")
            
            # 주목할 만한 상황 출력
            if timeframe_data['rsi'] <= 35:
                print(f"[SIGNAL] {ticker} 매수 관심 구간")
                print(f"  - RSI: {timeframe_data['rsi']:.1f}")
                print(f"  - %B: {timeframe_data['percent_b']:.2f}")
                    
            elif timeframe_data['rsi'] >= 65:
                print(f"[SIGNAL] {ticker} 매도 관심 구간")
                print(f"  - RSI: {timeframe_data['rsi']:.1f}")
                print(f"  - %B: {timeframe_data['percent_b']:.2f}")
            
            # 특별한 상황 출력
            if timeframe_data['bb_bandwidth'] > 1.0:
                print(f"[INFO] {ticker} 높은 변동성 감지 - 밴드폭: {timeframe_data['bb_bandwidth']:.1f}%")
                
        except Exception as e:
            logger.error(f"[ERROR] {ticker} 결과 출력 실패: {str(e)}")
            logger.debug(f"Error details: {traceback.format_exc()}")
    
    def analyze_market_state(self, df):
        """시장 상태 분석"""
        try:
            if df is None or len(df) < 20:
                return None
                
            # 변동성 계산
            df['daily_change'] = df['종가'].pct_change() * 100
            volatility = df['daily_change'].std()
            avg_volatility = df['daily_change'].rolling(window=20).std().mean()
            
            # 가격 추세 계산 (최근 20봉 기준)
            price_trend = ((df['종가'].iloc[-1] - df['종가'].iloc[-20]) / df['종가'].iloc[-20]) * 100
            
            # 볼린저 밴드 추세 계산
            df['밴드폭'] = ((df['상단밴드'] - df['하단밴드']) / df['중심선']) * 100
            bb_trend = df['밴드폭'].diff().mean()
            
            market_state = {
                'volatility': volatility,
                'avg_volatility': avg_volatility,
                'price_trend': price_trend,
                'bb_trend': bb_trend
            }
            
            # 시장 상태 판단
            if volatility > avg_volatility * 1.5:
                self.market_state = 'volatile'
            elif abs(price_trend) > 5:
                self.market_state = 'trend'
            else:
                self.market_state = 'normal'
                
            return market_state
            
        except Exception as e:
            print(f"[ERROR] 시장 상태 분석 중 오류: {str(e)}")
            return None

    def update_trading_conditions(self, market_status):
        """시장 상태에 따른 매매 조건 업데이트"""
        try:
            old_state = self.market_state
            old_conditions = self.trading_conditions.copy()
            
            if market_status:
                # 변동성이 높은 시장
                if market_status['volatility'] > market_status['avg_volatility'] * 1.5:
                    self.market_state = 'volatile'
                    self.trading_conditions.update({
                        'rsi_strong_oversold': 30,    # 강한 매수 신호
                        'rsi_oversold': 35,           # 일반 매수 신호
                        'rsi_overbought': 65,         # 일반 매도 신호
                        'rsi_strong_overbought': 70,  # 강한 매도 신호
                        'bb_squeeze': 0.3,
                        'bb_expansion': 2.5,
                        'position_size_strong': 1.5,   # 강한 신호시 포지션 크기
                        'position_size_normal': 1.0    # 일반 신호시 포지션 크기
                    })
                # 추세가 강한 시장
                elif abs(market_status['price_trend']) > 5:
                    self.market_state = 'trend'
                    self.trading_conditions.update({
                        'rsi_strong_oversold': 35,
                        'rsi_oversold': 40,
                        'rsi_overbought': 60,
                        'rsi_strong_overbought': 65,
                        'bb_squeeze': 0.7,
                        'bb_expansion': 1.8,
                        'position_size_strong': 1.3,
                        'position_size_normal': 1.0
                    })
                # 일반 시장
                else:
                    self.market_state = 'normal'
                    self.trading_conditions.update({
                        'rsi_strong_oversold': 32,
                        'rsi_oversold': 37,
                        'rsi_overbought': 63,
                        'rsi_strong_overbought': 68,
                        'bb_squeeze': 0.5,
                        'bb_expansion': 2.0,
                        'position_size_strong': 1.2,
                        'position_size_normal': 1.0
                    })
                
                # 조건이 변경되었을 때만 메시지 생성
                if old_state != self.market_state or old_conditions != self.trading_conditions:
                    message = f"🔄 매매 조건 업데이트\n\n"
                    message += f"시장 상태: {old_state} → {self.market_state}\n"
                    message += f"변동성: {market_status['volatility']:.2f}%\n"
                    message += f"가격 추세: {market_status['price_trend']:.2f}%\n"
                    message += f"밴드폭 추세: {market_status['bb_trend']:.2f}\n\n"
                    
                    message += "📊 매매 조건:\n"
                    message += f"RSI 과매도: {self.trading_conditions['rsi_oversold']}\n"
                    message += f"RSI 과매수: {self.trading_conditions['rsi_overbought']}\n"
                    message += f"밴드 수축: {self.trading_conditions['bb_squeeze']}\n"
                    message += f"밴드 확장: {self.trading_conditions['bb_expansion']}\n"
                    
                    print(f"[INFO] 매매 조건 업데이트됨: {self.market_state}")
                    return message
            
            return None
            
        except Exception as e:
            print(f"[ERROR] 매매 조건 업데이트 중 오류: {str(e)}")
            return None

    def _check_bottom_signal(self, df, last_idx=-1):
        """저점 매수 시그널 확인"""
        try:
            # 최근 3개 봉 데이터
            recent = df.iloc[last_idx-2:last_idx+1]
            if len(recent) < 3:
                return False

            # MACD 반등 확인
            macd_turning = (recent['MACD_Hist'].iloc[-1] > recent['MACD_Hist'].iloc[-2] and 
                          recent['MACD_Hist'].iloc[-2] < recent['MACD_Hist'].iloc[-3])

            # 스토캐스틱 과매도 반등
            stoch_turning = (recent['%K'].iloc[-1] > recent['%D'].iloc[-1] and 
                           recent['%K'].iloc[-1] < 30)

            # 거래량 증가 확인
            volume_increase = recent['Volume_Ratio'].iloc[-1] > 1.5

            # 양봉 확인
            bullish_candle = recent['종가'].iloc[-1] > recent['시가'].iloc[-1]

            # 점수 계산 (각 조건당 1점)
            score = sum([
                macd_turning,
                stoch_turning,
                volume_increase,
                bullish_candle
            ])

            return score >= 2  # 최소 2개 이상 조건 충족 필요

        except Exception as e:
            print(f"[ERROR] 저점 시그널 확인 중 오류: {str(e)}")
            return False

    def _check_top_signal(self, df, last_idx=-1):
        """고점 매도 시그널 확인"""
        try:
            # 최근 3개 봉 데이터
            recent = df.iloc[last_idx-2:last_idx+1]
            if len(recent) < 3:
                return False

            # MACD 하락 전환
            macd_turning = (recent['MACD_Hist'].iloc[-1] < recent['MACD_Hist'].iloc[-2] and 
                          recent['MACD_Hist'].iloc[-2] > recent['MACD_Hist'].iloc[-3])

            # 스토캐스틱 과매수 하락
            stoch_turning = (recent['%K'].iloc[-1] < recent['%D'].iloc[-1] and 
                           recent['%K'].iloc[-1] > 70)

            # 거래량 감소 확인
            volume_decrease = recent['Volume_Ratio'].iloc[-1] < 0.7

            # 음봉 확인
            bearish_candle = recent['종가'].iloc[-1] < recent['시가'].iloc[-1]

            # 점수 계산
            score = sum([
                macd_turning,
                stoch_turning,
                volume_decrease,
                bearish_candle
            ])

            return score >= 2  # 최소 2개 이상 조건 충족 필요

        except Exception as e:
            print(f"[ERROR] 고점 시그널 확인 중 오류: {str(e)}")
            return False
    
    def get_trading_signals(self, analysis):
        """매매 신호 생성"""
        try:
            signals = []
            if not analysis or 'timeframes' not in analysis:
                return signals

            ticker = analysis['ticker']
            timeframe_data = analysis['timeframes']['minute30']  # 30분봉 데이터 사용
            
            # 캐시된 신호 확인
            cache_key = f"{ticker}_signal"
            current_time = datetime.now()
            
            if cache_key in self.signal_history:
                last_signal_time = self.signal_history[cache_key]
                if (current_time - last_signal_time).total_seconds() < self.signal_cooldown:
                    return signals

            rsi = timeframe_data['rsi']
            bb_bandwidth = timeframe_data['bb_bandwidth']
            percent_b = timeframe_data['percent_b']
            
             # 지표 계산
            df = self.get_ohlcv(ticker)
            if df is None or len(df) < 30:
                return signals
                
            df = self._calculate_indicators(df)
            if df is None:
                return signals

            rsi = timeframe_data['rsi']
            bb_bandwidth = timeframe_data['bb_bandwidth']
            percent_b = timeframe_data['percent_b']
            
            # 추세 판단 (기존 코드 유지)
            df['MA5'] = df['종가'].rolling(window=5).mean()
            df['MA20'] = df['종가'].rolling(window=20).mean()
            
            last_data = df.iloc[-1]
            prev_data = df.iloc[-2]
            
            is_uptrend = last_data['MA5'] > last_data['MA20'] and prev_data['MA5'] > prev_data['MA20']
            is_downtrend = last_data['MA5'] < last_data['MA20'] and prev_data['MA5'] < prev_data['MA20']

            # 매수 신호 생성 (저점 확인 추가)
            if is_uptrend:
                if rsi <= 32:
                    if percent_b < 0.1 and self._check_bottom_signal(df):
                        signals.append(('매수', f'상승추세 + 저점확인 + RSI 과매도({rsi:.1f})', ticker, 2.0))
                    elif percent_b < 0.3 and self._check_bottom_signal(df):
                        signals.append(('매수', f'상승추세 + 저점확인 + RSI 과매도({rsi:.1f})', ticker, 1.5))
                            
                elif rsi <= 35 and self._check_bottom_signal(df):
                    if percent_b < 0.2 and bb_bandwidth > 0.8:
                        signals.append(('매수', f'상승추세 + 저점확인 + RSI 약과매도({rsi:.1f})', ticker, 1.2))
            
            elif is_downtrend:
                if rsi <= 30 and percent_b < 0.1 and bb_bandwidth > 1.0:
                    if self._check_bottom_signal(df):  # 강한 반등 신호 필요
                        signals.append(('매수', f'하락추세 + 저점확인 + RSI 극단과매도({rsi:.1f})', ticker, 1.0))
                
            # 매도 신호 생성 (분할 매도에 맞춰 조정)
            if is_downtrend:
                if rsi >= 68:
                    if percent_b > 0.9 and bb_bandwidth > 0.8 and self._check_top_signal(df):
                        signals.append(('매도', f'하락추세 + 고점확인 + RSI 과매수({rsi:.1f})', ticker, 1.5))  # 70% 매도
                    elif percent_b > 0.7 and self._check_top_signal(df):
                        signals.append(('매도', f'하락추세 + 고점확인 + RSI 과매수({rsi:.1f})', ticker, 1.2))  # 50% 매도
                elif rsi >= 65 and percent_b > 0.8 and self._check_top_signal(df):
                    signals.append(('매도', f'하락추세 + 고점확인 + RSI 약과매수({rsi:.1f})', ticker, 1.0))  # 30% 매도
            
            elif is_uptrend:
                if (rsi >= 70 and percent_b > 0.9 and bb_bandwidth > 0.8 and 
                    self._check_top_signal(df) and df['Volume_Ratio'].iloc[-1] < 0.5):
                    signals.append(('매도', f'상승추세 + 고점확인 + RSI 극단과매수({rsi:.1f})', ticker, 1.0))  # 30% 매도

            return signals
                    
        except Exception as e:
            print(f"[ERROR] 매매 신호 생성 중 오류: {str(e)}")
            return []
        
class MarketMonitor:
    def __init__(self, upbit_api, telegram_bot, market_analyzer):
        self.upbit = upbit_api
        self.telegram = telegram_bot
        self.analyzer = market_analyzer
        self.position_manager = PositionManager(upbit_api)
        self.report = TradingReport()
        self.commands = {
            '/start': self.start_bot,
            '/stop': self.stop_bot,
            '/daily_report': self.show_daily_report,
            '/monthly_report': self.show_monthly_report,
            '/help': self.show_help
        }        
        
        # 기존 포지션 로드 (명시적으로 호출)
        self.position_manager.load_positions()
        
        # 모니터링 상태 관리 변수들
        self.is_running = False
        self.last_market_analysis = datetime.now()
        self.market_analysis_interval = timedelta(hours=2)
        self.last_status_update = datetime.now()
        self.status_update_interval = timedelta(hours=1)
        
        # 에러 관련 변수들
        self.error_logs = []
        self.max_error_logs = 100
        self.last_error_notification = datetime.now()
        self.error_notification_cooldown = timedelta(minutes=5)
        
        # 텔레그램 명령어 처리 관련 변수
        self.last_error_notification = datetime.now()
        self.error_notification_cooldown = timedelta(minutes=5)
        self.last_processed_update_id = 0
        self.last_command_check = datetime.now()
        self.command_check_interval = timedelta(seconds=3)
        
        # 신호 처리 이력 추가
        self.signal_history = {}  # 여기에 추가
        self.signal_cooldown = 2.5  # 신호 재처리 대기 시간 (초)

        # 초기 시장 분석
        self.analyzer.update_tickers()  # 추가 필요

        logger.info("MarketMonitor 초기화 완료")

    def show_daily_report(self):
        """일일 거래 보고서 조회"""
        try:
            report = self.report.generate_daily_report()
            self.telegram.send_message(report)
            return True
        except Exception as e:
            error_msg = f"일일 보고서 생성 실패: {str(e)}"
            print(f"[ERROR] {error_msg}")
            self.telegram.send_message(f"⚠️ {error_msg}")
            return False

    def show_monthly_report(self):
        """월간 거래 보고서 조회"""
        try:
            report = self.report.generate_monthly_report()
            self.telegram.send_message(report)
            return True
        except Exception as e:
            error_msg = f"월간 보고서 생성 실패: {str(e)}"
            print(f"[ERROR] {error_msg}")
            self.telegram.send_message(f"⚠️ {error_msg}")
            return False

    def setup_logging(self):
        """로깅 설정"""
        # 로그 디렉토리 생성
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # 로거 생성
        logger = logging.getLogger('trading_bot')
        logger.setLevel(logging.INFO)
        
        # 이미 핸들러가 있다면 제거 (중복 방지)
        if logger.handlers:
            logger.handlers.clear()
        
        # 파일 핸들러 설정
        log_file = f'logs/trading_{datetime.now().strftime("%Y%m%d")}.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # 콘솔 핸들러 설정
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 포맷터 설정
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 핸들러 추가
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
        
    def log_error(self, error_type, error):
        """에러 로깅"""
        try:
            timestamp = datetime.now()
            error_detail = {
                'timestamp': timestamp,
                'type': error_type,
                'message': str(error),
                'traceback': traceback.format_exc()
            }
            
            # 에러 로그 저장
            self.error_logs.append(error_detail)
            
            # 최대 개수 유지
            if len(self.error_logs) > self.max_error_logs:
                self.error_logs.pop(0)
            
            # 파일에 로깅
            logger.error(f"{error_type}: {str(error)}\n{traceback.format_exc()}")
            
            # 심각한 에러는 텔레그램으로 알림
            if error_type.startswith("CRITICAL"):
                self.telegram.send_message(f"⚠️ 심각한 오류 발생:\n{str(error)}")
                
        except Exception as e:
            print(f"[ERROR] 에러 로깅 중 추가 에러 발생: {str(e)}")

    def process_command(self, command):
        """텔레그램 명령어 처리"""
        if command in self.commands:
            self.commands[command]()
            return True
        return False

    def check_telegram_commands(self):
        """텔레그램 명령어 확인 및 처리 (최적화)"""
        try:
            current_time = datetime.now()
            
            # 명령어 확인 간격을 3초로 증가
            if current_time - self.last_command_check < timedelta(seconds=3):
                return
            
            self.last_command_check = current_time
            
            # 텔레그램 업데이트 확인
            updates = self.get_telegram_updates()
            if not updates:
                return
            
            for update in updates:
                try:
                    # 이미 처리된 메시지 스킵
                    if update['update_id'] <= self.last_processed_update_id:
                        continue
                    
                    if 'message' in update and 'text' in update['message']:
                        command = update['message']['text'].lower().strip()
                        
                        if command in self.commands:
                            print(f"[INFO] 텔레그램 명령어 실행: {command}")
                            self.commands[command]()
                        
                    self.last_processed_update_id = update['update_id']
                    
                except Exception as e:
                    print(f"[ERROR] 개별 명령어 처리 중 오류: {str(e)}")
                    continue
                
        except Exception as e:
            print(f"[ERROR] 텔레그램 명령어 확인 중 오류: {str(e)}")

    def get_telegram_updates(self):
        """텔레그램 업데이트 조회 (안정성 개선)"""
        try:
            # 세션 설정
            session = requests.Session()
            retry_strategy = Retry(
                total=3,  # 최대 재시도 횟수
                backoff_factor=0.5,  # 재시도 간격
                status_forcelist=[429, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount('https://', adapter)
            
            url = f"https://api.telegram.org/bot{self.telegram.token}/getUpdates"
            params = {
                'offset': self.last_processed_update_id + 1,
                'timeout': 1,
                'allowed_updates': ['message']  # 메시지 업데이트만 받기
            }
            
            # 타임아웃 증가 및 연결 타임아웃 분리
            response = session.get(
                url, 
                params=params, 
                timeout=(5, 10)  # (연결 타임아웃, 읽기 타임아웃)
            )
            
            if response.status_code == 200:
                data = response.json()
                if data['ok']:
                    return data['result']
                else:
                    print(f"[WARNING] 텔레그램 API 응답 오류: {data.get('description', '알 수 없는 오류')}")
            else:
                print(f"[WARNING] 텔레그램 API 상태 코드: {response.status_code}")
            
            return []
            
        except requests.exceptions.Timeout:
            print("[WARNING] 텔레그램 업데이트 조회 타임아웃")
            return []
        except requests.exceptions.RequestException as e:
            print(f"[WARNING] 텔레그램 업데이트 조회 네트워크 오류: {str(e)}")
            return []
        except Exception as e:
            print(f"[ERROR] 텔레그램 업데이트 조회 중 오류: {str(e)}")
            return []
        finally:
            session.close()

    def process_buy_signals(self, signals):
        """여러 매매 신호 동시 처리"""
        try:
            logging.debug(f"====== 매매 신호 일괄 처리 시작: {len(signals)}개 ======")
            
            # 매수/매도 신호 분리 및 중복 신호 필터링
            buy_signals = []
            sell_signals = []
            current_time = datetime.now()
            
            # 로그 추가
            logging.debug(f"수신된 신호: {signals}")
            
            for signal in signals:
                # 신호 형식 로깅
                logging.debug(f"처리 중인 신호: {signal}")
                
                # 신호 구조 확인
                if len(signal) != 4:
                    logging.error(f"잘못된 신호 형식: {signal}")
                    continue
                    
                action, message, ticker, strength = signal
                # 최근 처리된 신호인지 확인
                last_processed = self.signal_history.get(f"{ticker}_{action}")
                if last_processed and (current_time - last_processed).total_seconds() < self.signal_cooldown:
                    logging.info(f"{ticker} {action} 신호 무시 (대기시간: {self.signal_cooldown}초)")
                    continue
                    
                if action == '매수':
                    if len(self.position_manager.positions) >= self.position_manager.max_positions:
                        logging.info("최대 포지션 수 도달")
                        continue
                    buy_signals.append((ticker, strength))
                    self.signal_history[f"{ticker}_매수"] = current_time
                    
                elif action == '매도':
                    if ticker in self.position_manager.positions:
                        sell_signals.append((ticker, strength))
                        self.signal_history[f"{ticker}_매도"] = current_time
                    else:
                        logging.info(f"{ticker} 미보유 코인 매도 신호 무시")
            
            results = {}
            
                # ThreadPoolExecutor 사용 시 position_manager 접근에 대한 동기화 처리 필요
            with ThreadPoolExecutor(max_workers=5) as executor:
                # 락(Lock) 메커니즘 추가 필요
                self._position_lock = threading.Lock()  # 클래스 초기화 시 추가
                if sell_signals:
                    sell_futures = {
                        executor.submit(self.execute_sell, ticker, strength): ticker 
                        for ticker in sell_signals
                    }
                    
                    for future in as_completed(sell_futures):
                        ticker = sell_futures[future]
                        try:
                            success, message = future.result(timeout=2)
                            results[ticker] = {'action': '매도', 'success': success, 'message': message}
                            logging.info(f"{ticker} 매도 처리 완료: {success}")
                        except Exception as e:
                            logging.error(f"{ticker} 매도 처리 실패: {e}")
                            results[ticker] = {'action': '매도', 'success': False, 'message': str(e)}
                
                # 매수 신호 처리
                if buy_signals:
                    buy_futures = {
                        executor.submit(self.execute_buy, ticker, strength): ticker
                        for ticker in buy_signals
                    }
                    
                    for future in as_completed(buy_futures):
                        ticker = buy_futures[future]
                        try:
                            success, message = future.result(timeout=2)
                            results[ticker] = {'action': '매수', 'success': success, 'message': message}
                            logging.info(f"{ticker} 매수 처리 완료: {success}")
                        except Exception as e:
                            logging.error(f"{ticker} 매수 처리 실패: {e}")
                            results[ticker] = {'action': '매수', 'success': False, 'message': str(e)}
            
            return results
                
        except Exception as e:
            logging.error(f"매매 신호 일괄 처리 중 오류: {str(e)}")
            return {}

    def execute_sell(self, ticker, strength):
        """매도 실행"""
        try:
            # DB와 positions 동기화
            self.position_manager.load_positions()
            
            # ticker가 튜플로 전달된 경우 처리
            if isinstance(ticker, tuple):
                ticker = ticker[0]  # 첫 번째 요소를 ticker로 사용
            else:
                ticker = ticker

            logging.debug(f"{ticker} 매도 시도...")
            
            # 1. 실제 보유 수량 확인 (소수점 8자리까지 처리)
            coin = ticker.replace("KRW-", "")
            actual_quantity = float(format(self.upbit.get_balance(coin), '.8f'))
            
            if actual_quantity <= 0.00000001:
                if ticker in self.position_manager.positions:
                    self.position_manager.close_position(ticker)
                return False, "보유하지 않은 코인"

            # 2. positions 딕셔너리 동기화 확인
            if actual_quantity > 0:
                if ticker not in self.position_manager.positions:
                    # DB에서 포지션 정보 다시 로드
                    self.position_manager.load_positions()
                    
                if ticker not in self.position_manager.positions:
                    # 여전히 없다면 새로운 포지션 생성
                    current_price = self.upbit.get_current_price(ticker)
                    success, message = self.position_manager.open_position(ticker, current_price, actual_quantity)
                    if not success:
                        logging.error(f"{ticker} 포지션 생성 실패: {message}")
                        return False, "포지션 동기화 실패"
            else:
                logging.info(f"{ticker} 보유하지 않은 코인")
                return False, "보유하지 않은 코인"
                
            # 3. position 객체 가져오기
            position = self.position_manager.positions[ticker]
                
            # 현재가 조회
            current_price = self.upbit.get_current_price(ticker)
            profit_rate = position.calculate_profit(current_price)
            
            # 분할 매도 로직
            sell_quantity = 0
            reason = ""
            
            if strength >= 1.5:  # 강한 매도 신호
                sell_quantity = actual_quantity  # 전량 매도
                reason = "강한 매도 신호로 전량 매도"
            elif strength >= 1.2:  # 중간 매도 신호
                if profit_rate >= 3.0:
                    sell_quantity = float(format(actual_quantity * 0.7, '.8f'))  # 70% 매도
                    reason = f"수익률 {profit_rate:.1f}% 달성으로 70% 매도"
                elif profit_rate >= 2.0:
                    sell_quantity = float(format(actual_quantity * 0.5, '.8f'))  # 50% 매도
                    reason = f"수익률 {profit_rate:.1f}% 달성으로 50% 매도"
            elif strength >= 1.0:  # 약한 매도 신호
                if profit_rate >= 4.0:
                    sell_quantity = float(format(actual_quantity * 0.3, '.8f'))  # 30% 매도
                    reason = f"수익률 {profit_rate:.1f}% 달성으로 30% 매도"
            
            if sell_quantity <= 0:
                return False, "매도 조건 미충족"
                
            # 최소 매도 수량 확인
            if sell_quantity < actual_quantity * 0.1:
                return False, "최소 매도 수량 미달"
            
                        # 매도 금액 확인
            expected_sell_amount = current_price * sell_quantity
            if expected_sell_amount < 6000:
                logging.info(f"{ticker} 매도 금액 부족 (예상: {expected_sell_amount:,.0f}원)")
                # 최소 주문 금액보다 작으면 전량 매도로 변경
                sell_quantity = actual_quantity
                reason = f"{reason} (최소 주문 금액 미달로 전량 매도)"
                
            # 매도 주문 실행
            success, order_id = self.upbit.sell_market_order(ticker, sell_quantity)
            if not success:
                return False, f"매도 주문 실패: {order_id}"
                
            time.sleep(0.5)  # 체결 대기
            
            # 매도 후 처리
            if sell_quantity == actual_quantity:  # 전량 매도
                self.position_manager.close_position(ticker)
                hold_time = datetime.now() - position.entry_time
                hold_hours = hold_time.total_seconds() / 3600
                
                profit_amount = int((current_price - position.average_price) * sell_quantity)
                profit_sign = "+" if profit_amount > 0 else ""  # 양수일 때만 + 표시
                
                self.telegram.send_message(
                    f"💰 전량 매도 완료: {ticker}\n"
                    f"매도가: {format(int(current_price), ',')}원\n"
                    f"매도량: {sell_quantity:.8f}\n"
                    f"수익률: {profit_sign}{profit_rate:.2f}%\n"
                    f"수익금액: {profit_sign}{format(profit_amount, ',')}원\n"
                    f"매도사유: {reason}\n"
                    f"보유기간: {hold_hours:.1f}시간\n"
                    f"매수횟수: {position.buy_count}회"
                )
            else:  # 부분 매도
                position.update_quantity(actual_quantity - sell_quantity)
                hold_time = datetime.now() - position.entry_time
                hold_hours = hold_time.total_seconds() / 3600
                
                profit_amount = int((current_price - position.average_price) * sell_quantity)
                profit_sign = "+" if profit_amount > 0 else ""  # 양수일 때만 + 표시
                
                self.telegram.send_message(
                    f"💰 부분 매도 완료: {ticker}\n"
                    f"매도가: {format(int(current_price), ',')}원\n"
                    f"매도량: {sell_quantity:.8f} ({(sell_quantity/actual_quantity)*100:.0f}%)\n"
                    f"수익률: {profit_sign}{profit_rate:.2f}%\n"
                    f"수익금액: {profit_sign}{format(profit_amount, ',')}원\n"
                    f"매도사유: {reason}\n"
                    f"보유기간: {hold_hours:.1f}시간"
                )
            
            return True, reason
            
        except Exception as e:
            logging.error(f"{ticker} 매도 실행 중 오류: {str(e)}")
            return False, str(e)
    
    def execute_buy(self, ticker, strength):
        """매수 실행 (고정 금액: 5,500원)"""
        try:
            # ticker가 튜플로 전달된 경우 처리
            if isinstance(ticker, tuple):
                ticker = ticker[0]  # 첫 번째 요소를 ticker로 사용

            # 현재가 조회
            current_price = self.upbit.get_current_price(ticker)
            if not current_price:
                return False, "현재가 조회 실패"
            
            logging.debug(f"{ticker} 매수 시도 (현재가: {format(int(current_price), ',')}원)")

            # 기본 투자 금액
            base_invest_amount = 100000  

            # 이미 보유 중인지 확인 및 추가매수 전략 적용
            if ticker in self.position_manager.positions:
                position = self.position_manager.positions[ticker]
                if position.buy_count >= 3:
                    logging.info(f"{ticker} 이미 최대 매수 횟수에 도달")
                    return False, "최대 매수 횟수 초과"
                
                # 마지막 매수 시간 체크
                time_since_last_buy = (datetime.now() - position.last_buy_time).total_seconds()
                if time_since_last_buy < 30:
                    logging.info(f"{ticker} 최근 매수 이력 있음 (대기시간: {30-time_since_last_buy:.0f}초)")
                    return False, "매수 대기시간"
                
                # 추가매수 전략 적용
                price_drop = ((position.average_price - current_price) / position.average_price) * 100
                logging.info(f"{ticker} 가격 하락률: {price_drop:.2f}%")
                
                if position.buy_count == 1 and price_drop >= 1.2:
                    base_invest_amount *= 1.0  # 첫 번째 추가매수
                    logging.info(f"{ticker} 첫 번째 추가매수 조건 충족 (하락률 {price_drop:.2f}%)")
                elif position.buy_count == 2 and price_drop >= 2.0:
                    base_invest_amount *= 1.2  # 두 번째 추가매수
                    logging.info(f"{ticker} 두 번째 추가매수 조건 충족 (하락률 {price_drop:.2f}%)")
                else:
                    return False, f"추가매수 조건 미충족 (하락률 {price_drop:.2f}%)"


              # 매수 가능한 KRW 잔고 확인 (수정)
            krw_balance = self.upbit.get_balance("KRW")
            required_amount = base_invest_amount * strength
            if krw_balance < required_amount:
                logging.info(f"{ticker} 잔고 부족 (필요: {required_amount:,.0f}원, 보유: {krw_balance:,.0f}원)")
                return False, "잔고 부족"
            
            # 신호 강도에 따른 투자 금액 조정
            invest_amount = base_invest_amount * strength
            logging.info(f"{ticker} 매수 시도 - 기본금액: {base_invest_amount:,}원, 신호강도: {strength}, 실제매수금액: {invest_amount:,}원")
            
            # 시장가 매수 주문 (수정된 부분)
            success, order_info = self.upbit.buy_market_order(ticker, invest_amount)
            
            if success:  # 주문 성공
                logging.info(f"{ticker} 매수 주문 성공: {order_info}")
                
                # 주문 체결 확인 개선
                max_wait = 10  # 최대 대기 시간
                wait_count = 0
                while wait_count < max_wait:
                    time.sleep(1)
                    actual_quantity = float(format(self.upbit.get_balance(ticker), '.8f'))
                    if actual_quantity > 0:
                        break
                    wait_count += 1
                    
                if actual_quantity <= 0:
                    logging.error(f"{ticker} 체결 실패 (대기시간 {max_wait}초 초과)")
                    return False, "체결 시간 초과"
                    
                # 실제 체결가 계산 개선
                executed_price = self.upbit.get_current_price(ticker)  # 현재가를 체결가로 사용
                if not executed_price:
                    logging.error(f"{ticker} 체결가 조회 실패")
                    return False, "체결가 조회 실패"
                    
                logging.info(f"{ticker} 매수 체결 - 수량: {actual_quantity:.8f}, 체결가: {executed_price:,}원")
                
                # 포지션 처리 (실제 체결가 사용)
                if ticker in self.position_manager.positions:
                    success, message = self.position_manager.add_to_position(ticker, executed_price, actual_quantity)
                else:
                    success, message = self.position_manager.open_position(ticker, executed_price, actual_quantity)
                
                if not success:
                    logging.warning(f"{ticker} 포지션 처리 실패: {message}")
                    return False, f"포지션 처리 실패: {message}"
                
                return True, order_info
            else:
                # 주문 실패
                logging.error(f"{ticker} 매수 주문 실패: {order_info}")
                return False, f"주문 실패: {order_info}"
        
        except Exception as e:
            logging.error(f"{ticker} 매수 실행 중 오류: {str(e)}")
            return False, str(e)
    
    def start_bot(self):
        """봇 시작"""
        if not self.is_running:
            self.is_running = True
            self.telegram.send_message("✅ 자동매매 봇이 시작되었습니다.")
        else:
            self.telegram.send_message("❗ 봇이 이미 실행 중입니다.")
    
    def stop_bot(self):
        """봇 중지"""
        if self.is_running:
            self.is_running = False
            self.telegram.send_message("🛑 자동매매 봇이 중지되었습니다.")
        else:
            self.telegram.send_message("❗ 봇이 이미 중지된 상태입니다.")

    def log_error(self, location, error, notify=True):
        """에러 로깅 및 알림 처리"""
        try:
            timestamp = datetime.now()
            error_detail = {
                'timestamp': timestamp,
                'location': location,
                'error': str(error),
                'traceback': traceback.format_exc()
            }
            self.error_logs.append(error_detail)
            
            # 콘솔 로깅
            print(f"\n[{timestamp}] 에러 발생 위치: {location}")
            print(f"에러 내용: {error}")
            print(f"상세 정보:\n{error_detail['traceback']}\n")
            
            # 텔레그램 알림 (쿨다운 적용)
            if notify and timestamp - self.last_error_notification >= self.error_notification_cooldown:
                message = f"⚠️ 에러 발생\n\n"
                message += f"위치: {location}\n"
                message += f"시간: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n"
                message += f"내용: {error}\n"
                self.telegram.send_message(message)
                self.last_error_notification = timestamp
                
        except Exception as e:
            print(f"에러 로깅 중 추가 에러 발생: {e}")

    def monitor_market(self):
        """시장 모니터링 (병렬 처리 최적화)"""
        try:
            current_time = datetime.now()

            # 시장 상태 분석
            market_states = []
            
            # 시장 분석 주기 체크 (1시간)
            if current_time - self.last_market_analysis >= self.market_analysis_interval:
                logging.info("시장 전체 분석 시작...")
                
                # 상위 거래량 코인 가져오기
                top_10_tickers = self.analyzer.tickers[:10]
                
                # 병렬로 여러 코인 분석
                analysis_results = self.analyzer.analyze_multiple_markets(top_10_tickers)
                
                for ticker, analysis in analysis_results.items():
                    if analysis and 'minute1' in analysis['timeframes']:
                        df = self.analyzer.get_ohlcv(ticker)
                        if df is not None:
                            market_state = self.analyzer.analyze_market_state(df)
                            if market_state:
                                market_states.append(market_state)
                
            # 시장 상태 업데이트 및 매매 조건 조정
            if market_states:
                # 거래량 가중치를 적용한 평균 계산
                total_volume = sum(state.get('volume', 0) for state in market_states)
                
                if total_volume > 0:  # 거래량이 있는 경우에만 계산
                    weighted_volatility = sum(state['volatility'] * state.get('volume', 0) for state in market_states) / total_volume
                    weighted_price_trend = sum(state['price_trend'] * state.get('volume', 0) for state in market_states) / total_volume
                    weighted_bb_trend = sum(state['bb_trend'] * state.get('volume', 0) for state in market_states) / total_volume
                    
                    # 변동성 표준편차 계산 (이상치 탐지용)
                    volatility_std = np.std([state['volatility'] for state in market_states])
                    
                    # 최근 N개 시간의 추세 방향성 계산
                    recent_trends = [1 if state['price_trend'] > 0 else -1 for state in market_states[-10:]]
                    trend_strength = sum(recent_trends) / len(recent_trends)  # -1 ~ 1 사이 값
                    
                    combined_market_status = {
                        'volatility': weighted_volatility,
                        'avg_volatility': sum(state['avg_volatility'] for state in market_states) / len(market_states),
                        'volatility_std': volatility_std,
                        'price_trend': weighted_price_trend,
                        'bb_trend': weighted_bb_trend,
                        'trend_strength': trend_strength,
                        'total_volume': total_volume,
                        'market_count': len(market_states),
                        'timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    # 이상치 제거 및 필터링
                    if (abs(weighted_volatility) < weighted_volatility * 3 and  # 변동성 이상치 제거
                        total_volume > 1000000):  # 최소 거래량 기준
                        
                        update_message = self.analyzer.update_trading_conditions(combined_market_status)
                        if update_message:
                            # 추가 정보를 포함한 메시지
                            update_message += f"\n📈 시장 추가 정보:\n"
                            update_message += f"추세 강도: {trend_strength:.2f}\n"
                            update_message += f"거래량: {total_volume:,.0f}\n"
                            update_message += f"변동성 표준편차: {volatility_std:.2f}\n"
                            self.telegram.send_message(update_message)
                    
                    # 디버그 로깅
                    logging.debug(f"시장 상태 업데이트:")
                    logging.debug(f"- 가중 변동성: {weighted_volatility:.2f}%")
                    logging.debug(f"- 가중 가격추세: {weighted_price_trend:.2f}%")
                    logging.debug(f"- 추세 강도: {trend_strength:.2f}")
                    logging.debug(f"- 총 거래량: {total_volume:,.0f}")

            self.last_market_analysis = current_time

            # 개별 코인 분석 (병렬 처리)
            analysis_results = self.analyzer.analyze_multiple_markets(self.analyzer.tickers)
            
            # 모든 매매 신호 수집
            all_signals = []
            for ticker, analysis in analysis_results.items():
                if analysis:
                    signals = self.analyzer.get_trading_signals(analysis)
                    all_signals.extend(signals)
            
            # 매매 신호가 있으면 한번에 처리
            if all_signals:
                # 전체 신호 정보를 그대로 전달
                results = self.process_buy_signals(all_signals)
                
                # 결과 처리 (이 부분만 수정)
                for ticker, result in results.items():
                    try:
                        signal_matches = [s for s in all_signals if s[2] == ticker]
                        if signal_matches:  # 매칭되는 신호가 있는 경우에만 처리
                            signal_info = signal_matches[0]
                            action, reason, _, strength = signal_info
                            
                            if result['success']:
                                self.telegram.send_message(
                                    f"✅ {ticker} {action} 성공: {reason}\n"
                                    f"신호 강도: {strength}배"
                                )
                            else:
                                logging.error(f"{ticker} {action} 실패: {result['message']}")
                    except Exception as e:
                        logging.error(f"{ticker} 신호 처리 중 오류: {str(e)}")
                        continue

            # 포지션 관리
            self.check_position_conditions()

            self.check_telegram_commands()

        except Exception as e:
            logging.error(f"모니터링 중 오류: {str(e)}")
            self.log_error("모니터링 중 오류", e)

    def check_position_conditions(self):
        """포지션의 손절/익절/강제매도 조건 체크"""
        try:
            for ticker, position in list(self.position_manager.positions.items()):
                try:
                    # should_force_sell 메소드를 통해 매도 조건 체크
                    if position.should_force_sell():
                        current_price = position.get_current_price()
                        profit = position.calculate_profit(current_price)
                        hold_time = datetime.now() - position.entry_time
                        
                        # 매도 사유 결정
                        if profit <= position.stop_loss:
                            reason = f"손절 조건 도달 (수익률: {profit:.2f}%)"
                        elif profit >= position.take_profit:
                            reason = f"익절 조건 도달 (수익률: {profit:.2f}%)"
                        elif hold_time >= position.max_hold_time and profit > 0:
                            reason = f"보유시간 초과 (시간: {hold_time.total_seconds()/3600:.1f}시간)"
                        else:
                            reason = f"매도 조건 충족 (수익률: {profit:.2f}%)"
                        
                        logging.info(f"{ticker} 강제 매도 시도")
                        success, message = self.execute_sell(ticker, 1.5)
                        
                        if success:
                            logging.info(f"{ticker} 강제 매도 성공")
                            self.telegram.send_message(
                                f"⚠️ 강제 매도 실행: {ticker}\n"
                                f"사유: {reason}"
                            )
                        else:
                            logging.warning(f"{ticker} 강제 매도 실패: {message}")
                            
                except Exception as e:
                    logging.error(f"{ticker} 개별 포지션 체크 중 오류: {str(e)}")
                    continue
                    
        except Exception as e:
            logging.error(f"포지션 조건 체크 중 오류: {e}")
            logging.debug("상세 오류 정보:")
            logging.debug(traceback.format_exc())

    def show_help(self):
        """봇 사용법 안내"""
        message = "🤖 자동매매 봇 사용법\n\n"
        message += "/start - 봇 시작\n"
        message += "/stop - 봇 중지\n"
        message += "/daily_report - 일일 보고서\n"
        message += "/monthly_report - 월간 보고서\n"
        
        self.telegram.send_message(message)

class Position:
    def __init__(self, ticker, entry_price, quantity, buy_count=None, entry_time=None, last_buy_time=None):
        self.ticker = ticker
        self.entries = [(entry_price, quantity)]
        self.buy_count = buy_count if buy_count is not None else 1
        self.status = 'active'
        self.entry_time = entry_time if entry_time is not None else datetime.now()
        self.last_buy_time = last_buy_time if last_buy_time is not None else datetime.now()
        self.stop_loss = -2.5
        self.take_profit = 5.0
        self.max_hold_time = timedelta(hours=6)
        # DB 경로를 상대 경로로 변경
        self.db_path = os.path.join(os.path.dirname(__file__), 'positions.db')
        self.save_position()

    def should_force_sell(self):
        """강제 매도 조건 확인"""
        try:
            # 현재가 조회 (재시도 로직 포함)
            current_price = None
            max_retries = 3
            retry_delay = 1.0  # 1초로 증가
            
            # 세션 재사용을 위한 설정
            session = requests.Session()
            retry_strategy = Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504]
            )
            session.mount('https://', HTTPAdapter(max_retries=retry_strategy))
            
            for attempt in range(max_retries):
                try:
                    # 캐시 키 생성
                    cache_key = f"{self.ticker}_price"
                    current_time = datetime.now()
                    
                    # 캐시 확인
                    if hasattr(self, '_price_cache') and cache_key in self._price_cache:
                        cached_data = self._price_cache[cache_key]
                        if (current_time - cached_data['timestamp']).total_seconds() < 1.0:
                            current_price = cached_data['price']
                            break
                    
                    url = f"https://api.upbit.com/v1/ticker?markets={self.ticker}"
                    response = session.get(url, timeout=5)
                    
                    if response.status_code == 429:  # Rate limit
                        logging.warning(f"{self.ticker} Rate limit 발생, {attempt+1}번째 재시도...")
                        time.sleep(retry_delay * (2 ** attempt))  # 지수 백오프
                        continue
                        
                    if response.status_code == 200:
                        result = response.json()
                        if result and isinstance(result, list) and result[0]:
                            current_price = result[0].get('trade_price')
                            
                            # 캐시 업데이트
                            if not hasattr(self, '_price_cache'):
                                self._price_cache = {}
                            self._price_cache[cache_key] = {
                                'timestamp': current_time,
                                'price': current_price
                            }
                            break
                            
                    time.sleep(retry_delay)
                    
                except Exception as e:
                    logging.warning(f"{self.ticker} 현재가 조회 실패: {str(e)}")
                    time.sleep(retry_delay)
                    
            # 손실률 계산
            if not self.average_price or self.average_price <= 0:
                logging.warning(f"{self.ticker} 평균단가 오류: {self.average_price}")
                return False
                    
            loss_rate = ((current_price - self.average_price) / self.average_price) * 100
                
            # 보유 시간 계산
            if not self.entry_time:
                logging.warning(f"{self.ticker} 매수 시간 정보 없음")
                return False
                    
            hold_time = datetime.now() - self.entry_time
            hold_hours = hold_time.total_seconds() / 3600
                
            # 가격 표시 포맷 개선
            if current_price >= 1000:
                price_format = "{:,.0f}원"  # 1000 이상은 정수 형태로
            else:
                price_format = "{:.4f}원"  # 1000 미만은 소수점 4자리까지
                
            print(f"[DEBUG] {self.ticker} 강제매도 조건 체크:")
            print(f"- 현재가: {price_format.format(current_price)}")
            print(f"- 평균단가: {price_format.format(self.average_price)}")
            print(f"- 손실률: {loss_rate:.2f}%")
            print(f"- 보유시간: {hold_hours:.1f}시간")
            print(f"- 매수시간: {self.entry_time}")  # 디버깅용 로그 추가
                
            # 강제 매도 조건 (백테스팅과 동일)
            if loss_rate <= -3.0:  # 손절: -3.0%
                print(f"[INFO] {self.ticker} 강제 매도 조건 충족: 손절률(-3.0%) 도달")
                return True
                
            if loss_rate >= 10.0:  # 익절: 10.0%
                print(f"[INFO] {self.ticker} 강제 매도 조건 충족: 익절률(10.0%) 도달")
                return True
                
            if hold_hours >= 12 and loss_rate > 0:  # 12시간 초과 & 수익 중
                print(f"[INFO] {self.ticker} 강제 매도 조건 충족: 6시간 초과 & 수익 실현")
                return True

            if hold_hours >= 24 and loss_rate > -0.5: # 24시간 초과 & 손절 대기 
                print(f"[INFO] {self.ticker} 강제 매도 조건 충족: 12시간 초과 & 손절 대기")
                return True
                
            return False
                
        except Exception as e:
            print(f"[ERROR] {self.ticker} 강제 매도 조건 확인 중 오류: {str(e)}")
            print(f"[DEBUG] 상세 오류 정보:")
            print(traceback.format_exc())
            return False
        
    def get_current_price(self):
        """현재가 조회"""
        try:
            max_retries = 3
            retry_delay = 0.5
            
            for attempt in range(max_retries):
                try:
                    url = f"https://api.upbit.com/v1/ticker?markets={self.ticker}"
                    response = requests.get(url)
                    
                    if response.status_code == 429:  # Rate limit
                        print(f"[WARNING] {self.ticker} Rate limit 발생, {attempt+1}번째 재시도...")
                        time.sleep(retry_delay * (attempt + 1))
                        continue
                        
                    if response.status_code != 200:
                        print(f"[WARNING] {self.ticker} API 응답 오류: {response.status_code}")
                        time.sleep(retry_delay)
                        continue
                        
                    result = response.json()
                    if result and isinstance(result, list) and result[0]:
                        current_price = result[0].get('trade_price')
                        if current_price and current_price > 0:
                            return current_price
                            
                    time.sleep(retry_delay)
                    
                except Exception as e:
                    print(f"[WARNING] {self.ticker} 현재가 조회 실패: {str(e)}")
                    time.sleep(retry_delay)
            
            return None
            
        except Exception as e:
            print(f"[ERROR] {self.ticker} 현재가 조회 중 오류: {str(e)}")
            return None

    def save_position(self):
        """포지션 정보를 데이터베이스에 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 트랜잭션 시작
                cursor.execute('BEGIN')
                
                try:
                    # 포지션 정보 저장/업데이트
                    cursor.execute('''
                        INSERT OR REPLACE INTO positions 
                        (ticker, status, entry_time, last_buy_time, buy_count)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        self.ticker,
                        self.status,
                        self.entry_time.isoformat(),
                        self.last_buy_time.isoformat(),
                        self.buy_count
                    ))
                    
                    # 기존 엔트리 삭제 후 새로 추가
                    cursor.execute('DELETE FROM entries WHERE ticker = ?', (self.ticker,))
                    
                    # 새 엔트리 추가
                    for price, quantity in self.entries:
                        cursor.execute('''
                            INSERT INTO entries (ticker, price, quantity, timestamp)
                            VALUES (?, ?, ?, ?)
                        ''', (self.ticker, price, quantity, datetime.now().isoformat()))
                    
                    conn.commit()
                    print(f"[INFO] {self.ticker} 포지션 저장 완료")
                    
                except Exception as e:
                    conn.rollback()
                    raise e
                    
        except Exception as e:
            print(f"[ERROR] {self.ticker} 포지션 저장 실패: {str(e)}")
    
    def calculate_profit(self, current_price):
        """수익률 계산 (업비트 방식)"""
        try:
            if not current_price or current_price <= 0 or not self.average_price:
                return 0.0
                
            # 업비트 방식으로 수익률 계산 (소수점 자리수 조정)
            profit = ((current_price - self.average_price) / self.average_price) * 100
            
            # 1000원 이상일 경우 소수점 둘째자리, 미만일 경우 넷째자리까지
            if self.average_price >= 1000:
                return round(profit, 2)
            return round(profit, 4)
            
        except Exception as e:
            print(f"[ERROR] 수익률 계산 중 오류: {e}")
            return 0.0
        
    def update_quantity(self, new_quantity):
        """포지션 수량 업데이트"""
        try:
            if new_quantity <= 0:
                return False, "잘못된 수량"
                
            self.total_quantity = new_quantity
            self.save_position()  # DB에 변경사항 저장
            return True, "수량 업데이트 성공"
            
        except Exception as e:
            logging.error(f"수량 업데이트 중 오류: {str(e)}")
            return False, str(e)  

    @property
    def average_price(self):
        """평균 매수가 계산 (업비트 방식)"""
        try:
            total_value = sum(price * qty for price, qty in self.entries)
            total_quantity = sum(qty for _, qty in self.entries)
            if total_quantity > 0:
                # 업비트 방식으로 평균단가 계산
                avg = total_value / total_quantity
                # 1000원 기준으로 소수점 자리수 다르게 처리
                if avg >= 1000:
                    return round(avg)  # 1000원 이상은 정수
                return round(avg, 4)   # 1000원 미만은 소수점 4자리
            return 0
        except Exception as e:
            print(f"[ERROR] 평균단가 계산 중 오류: {e}")
            return 0
    
    @property
    def total_quantity(self):
        """총 보유 수량"""
        return sum(qty for _, qty in self.entries)

class PositionManager:
    def __init__(self, upbit_api):
        self.upbit = upbit_api
        self.positions = {}
        self.max_positions = 10
        self.db_path = os.path.join(os.path.dirname(__file__), 'positions.db')
        print(f"[DEBUG] DB 경로: {self.db_path}")

        self.init_database()
        self.init_closed_positions_table()
        self.load_positions()
        print(f"[INFO] PositionManager 초기화 완료 (보유 포지션: {len(self.positions)}개)")

    def init_database(self):
        """데이터베이스 초기화"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 포지션 테이블 생성
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS positions (
                        ticker TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        entry_time TIMESTAMP NOT NULL,
                        last_buy_time TIMESTAMP NOT NULL,
                        buy_count INTEGER NOT NULL DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # 거래 내역 테이블 생성
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS entries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ticker TEXT NOT NULL,
                        price REAL NOT NULL,
                        quantity REAL NOT NULL,
                        timestamp TIMESTAMP NOT NULL,
                        FOREIGN KEY (ticker) REFERENCES positions (ticker) ON DELETE CASCADE
                    )
                ''')
                
                conn.commit()
                print("[INFO] 데이터베이스 테이블 초기화 완료")
                
        except Exception as e:
            print(f"[ERROR] 데이터베이스 초기화 실패: {str(e)}")
            print(traceback.format_exc())

    def init_closed_positions_table(self):
        """종료된 포지션을 저장할 테이블 생성"""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS closed_positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    status TEXT NOT NULL,
                    entry_time TIMESTAMP NOT NULL,
                    close_time TIMESTAMP NOT NULL,
                    last_buy_time TIMESTAMP NOT NULL,
                    buy_count INTEGER NOT NULL,
                    profit_rate REAL,
                    close_price REAL,
                    entry_price REAL,
                    total_volume REAL,
                    total_amount REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()       

    @contextmanager
    def get_db_connection(self):
        conn = None
        try:
            conn = sqlite3.connect(
                self.db_path,
                timeout=10,
                isolation_level=None,  # 자동 커밋
                check_same_thread=False  # 다중 스레드 지원
            )
            conn.execute('PRAGMA journal_mode=WAL')  # Write-Ahead Logging 모드
            conn.execute('PRAGMA synchronous=NORMAL')  # 동기화 수준 조정
            conn.row_factory = sqlite3.Row  # 모든 연결에 Row Factory 설정
            yield conn
        finally:
            if conn:
                conn.close()

    def load_positions(self):
        """데이터베이스에서 포지션 정보 로드"""
        try:
            self.positions = {}
            
            with self.get_db_connection() as conn:
                conn.row_factory = sqlite3.Row  # 딕셔너리 형태로 결과 반환
                cursor = conn.cursor()
                
                # 모든 활성 포지션 조회
                cursor.execute('''
                    SELECT p.*, GROUP_CONCAT(e.price || ',' || e.quantity) as entries
                    FROM positions p
                    LEFT JOIN entries e ON p.ticker = e.ticker
                    WHERE p.status = 'active'
                    GROUP BY p.ticker
                ''')
                
                for row in cursor.fetchall():
                    try:
                        row_dict = dict(row)  # Row 객체를 딕셔너리로 변환
                        
                        # 엔트리 데이터 파싱
                        entries = []
                        if row_dict['entries']:
                            entries_data = row_dict['entries'].split(',')
                            entries = [(float(entries_data[i]), float(entries_data[i+1])) 
                                    for i in range(0, len(entries_data), 2)]
                        
                        # Position 객체 생성
                        position = Position(
                            row_dict['ticker'],
                            entries[0][0] if entries else 0,
                            entries[0][1] if entries else 0,
                            row_dict['buy_count'],
                            datetime.fromisoformat(row_dict['entry_time']),
                            datetime.fromisoformat(row_dict['last_buy_time'])
                        )
                        position.entries = entries
                        position.buy_count = row_dict['buy_count']
                        position.status = row_dict['status']
                        position.entry_time = datetime.fromisoformat(row_dict['entry_time'])
                        position.last_buy_time = datetime.fromisoformat(row_dict['last_buy_time'])
                        
                        self.positions[row_dict['ticker']] = position
                        
                    except Exception as e:
                        print(f"[ERROR] {row_dict['ticker']} 포지션 로드 실패: {e}")
                        continue
                        
                print(f"[INFO] 총 {len(self.positions)}개의 포지션 로드 완료")
                
        except Exception as e:
            print(f"[ERROR] 포지션 로드 실패: {e}")
            self.positions = {}

    def save_position(self, ticker, position):
        """포지션 정보를 데이터베이스에 저장"""
        try:
            if position.entry_time is None:
                position.entry_time = datetime.now()
            if position.last_buy_time is None:
                position.last_buy_time = datetime.now()

            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('BEGIN')
                
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO positions 
                        (ticker, status, entry_time, last_buy_time, buy_count)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        ticker,
                        position.status,
                        position.entry_time.isoformat(),
                        position.last_buy_time.isoformat(),
                        position.buy_count
                    ))
                    
                    cursor.execute('DELETE FROM entries WHERE ticker = ?', (ticker,))
                    for price, quantity in position.entries:
                        cursor.execute('''
                            INSERT INTO entries (ticker, price, quantity, timestamp)
                            VALUES (?, ?, ?, ?)
                        ''', (ticker, price, quantity, datetime.now().isoformat()))
                    
                    print(f"[INFO] {ticker} 포지션 저장 완료")
                    return True
                    
                except Exception as e:
                    print(f"[ERROR] {ticker} 포지션 저장 실패: {str(e)}")
                    return False
                    
        except Exception as e:
            print(f"[ERROR] {ticker} 포지션 저장 중 오류: {str(e)}")
            return False
        
    def open_position(self, ticker, price, quantity):
        """새 포지션 오픈"""
        try:    
            position = Position(ticker, price, quantity)
            self.positions[ticker] = position
            self.save_position(ticker, position)
            
            print(f"[INFO] {ticker} 신규 포지션 오픈 (가격: {price:,.0f}, 수량: {quantity:.8f})")
            return True, "포지션 오픈 성공"
            
        except Exception as e:
            print(f"[ERROR] {ticker} 포지션 오픈 실패: {e}")
            return False, str(e)
    
    def add_to_position(self, ticker, price, quantity):
        """기존 포지션에 추가매수"""
        try:
            if ticker not in self.positions:
                return False, "보유하지 않은 코인"
            
            position = self.positions[ticker]
            max_retries = 3
            retry_delay = 0.5
            
            for attempt in range(max_retries):
                try:
                    # 데이터베이스 연결에 timeout 설정 추가
                    with sqlite3.connect(self.db_path, timeout=20) as conn:
                        cursor = conn.cursor()
                        
                        # 트랜잭션 시작
                        cursor.execute('BEGIN IMMEDIATE')
                        
                        try:
                            # 포지션 정보 업데이트
                            cursor.execute('''
                                UPDATE positions 
                                SET buy_count = buy_count + 1,
                                    last_buy_time = ?
                                WHERE ticker = ?
                            ''', (datetime.now().isoformat(), ticker))
                            
                            # 새로운 거래 내역 추가
                            cursor.execute('''
                                INSERT INTO entries (ticker, price, quantity, timestamp)
                                VALUES (?, ?, ?, ?)
                            ''', (
                                ticker,
                                float(price),
                                float(quantity),
                                datetime.now().isoformat()
                            ))
                            
                            # 트랜잭션 커밋
                            conn.commit()
                            break
                            
                        except sqlite3.Error as e:
                            conn.rollback()
                            if attempt == max_retries - 1:
                                raise e
                            time.sleep(retry_delay * (attempt + 1))
                            continue
                            
                except sqlite3.Error as e:
                    if attempt == max_retries - 1:
                        raise e
                    time.sleep(retry_delay * (attempt + 1))
                    continue
            
            # 메모리 상의 포지션 업데이트
            position.entries.append((price, quantity))
            position.buy_count += 1
            position.last_buy_time = datetime.now()
            
            print(f"[INFO] {ticker} 추가매수 완료 (가격: {price:,.0f}, 수량: {quantity:.8f}, 횟수: {position.buy_count})")
            return True, "추가 매수 성공"
            
        except Exception as e:
            print(f"[ERROR] {ticker} 추가매수 실패: {str(e)}")
            print(traceback.format_exc())
            return False, str(e)
    
    def get_position_status(self, ticker):
        """포지션 상태 조회"""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT p.*, GROUP_CONCAT(e.price || ',' || e.quantity) as entries
                    FROM positions p
                    LEFT JOIN entries e ON p.ticker = e.ticker
                    WHERE p.ticker = ? AND p.status = 'active'
                    GROUP BY p.ticker
                ''', (ticker,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                    
                row_dict = dict(row)
                current_price = self.upbit.get_current_price(ticker)
                
                if not current_price:
                    return None
                    
                return {
                    'ticker': row_dict['ticker'],
                    'status': row_dict['status'],
                    'entry_time': datetime.fromisoformat(row_dict['entry_time']),
                    'buy_count': row_dict['buy_count'],
                    'current_price': current_price,
                    'profit_rate': self.positions[ticker].calculate_profit(current_price) if ticker in self.positions else 0
                }
                
        except Exception as e:
            print(f"[ERROR] {ticker} 상태 조회 실패: {e}")
            return None

    def get_positions(self):
        """모든 포지션 상태 조회"""
        positions = {}
        for ticker in list(self.positions.keys()):  # 복사본으로 순회
            status = self.get_position_status(ticker)
            if status:
                positions[ticker] = status
            else:
                print(f"[WARNING] {ticker} 상태 조회 실패")
                
        return positions

    def close_position(self, ticker):
        """포지션 종료"""
        try:
            if ticker not in self.positions:
                return False, "보유하지 않은 코인"
            
            position = self.positions[ticker]
            current_price = self.upbit.get_current_price(ticker)
            
            if not current_price:
                return False, "현재가 조회 실패"

            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                try:
                    # 포지션 상태 업데이트
                    cursor.execute('''
                        UPDATE positions 
                        SET status = 'closed'
                        WHERE ticker = ?
                    ''', (ticker,))
                    
                    # 종료된 포지션 기록
                    cursor.execute('''
                        INSERT INTO closed_positions (
                            ticker, status, entry_time, close_time, last_buy_time,
                            buy_count, profit_rate, close_price, entry_price,
                            total_volume, total_amount
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        ticker,
                        'closed',
                        position.entry_time.isoformat(),
                        datetime.now().isoformat(),
                        position.last_buy_time.isoformat(),
                        position.buy_count,
                        position.calculate_profit(current_price),
                        current_price,
                        position.average_price,
                        position.total_quantity,
                        position.total_quantity * current_price
                    ))
                    
                    # 메모리에서 포지션 제거
                    del self.positions[ticker]
                    print(f"[INFO] {ticker} 포지션 종료 완료")
                    return True, "포지션 종료 성공"
                    
                except Exception as e:
                    print(f"[ERROR] {ticker} 포지션 종료 중 오류: {str(e)}")
                    return False, str(e)
                    
        except Exception as e:
            print(f"[ERROR] {ticker} 포지션 종료 실패: {str(e)}")
            return False, str(e)
        
class TradingReport:
    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(__file__), 'positions.db')

    def generate_daily_report(self, date=None):
        """일일 거래 보고서 생성"""
        try:
            if date is None:
                date = datetime.now().date()
                
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 일일 거래 통계
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN profit_rate > 0 THEN 1 ELSE 0 END) as profitable_trades,
                        AVG(profit_rate) as avg_profit,
                        MAX(profit_rate) as max_profit,
                        MIN(profit_rate) as max_loss
                    FROM closed_positions
                    WHERE date(close_time) = ?
                ''', (date.isoformat(),))
                
                stats = cursor.fetchone()
                
                # 일일 거래 내역
                cursor.execute('''
                    SELECT 
                        ticker,
                        entry_time,
                        close_time,
                        buy_count,
                        profit_rate,
                        close_price
                    FROM closed_positions
                    WHERE date(close_time) = ?
                    ORDER BY profit_rate DESC
                ''', (date.isoformat(),))
                
                trades = cursor.fetchall()
                
                # 보고서 생성
                report = f"📊 {date.strftime('%Y-%m-%d')} 거래 보고서\n\n"
                
                if stats[0]:  # 거래가 있는 경우
                    win_rate = (stats[1] / stats[0]) * 100 if stats[0] > 0 else 0
                    report += f"총 거래: {stats[0]}건\n"
                    report += f"승률: {win_rate:.1f}%\n"
                    report += f"평균 수익률: {stats[2]:.2f}%\n"
                    report += f"최대 수익: {stats[3]:.2f}%\n"
                    report += f"최대 손실: {stats[4]:.2f}%\n\n"
                    
                    report += "🔄 거래 내역:\n"
                    for trade in trades:
                        hold_time = datetime.fromisoformat(trade[2]) - datetime.fromisoformat(trade[1])
                        report += f"- {trade[0]}\n"
                        report += f"  수익률: {trade[4]:.2f}%\n"
                        report += f"  매수횟수: {trade[3]}회\n"
                        report += f"  보유시간: {str(hold_time).split('.')[0]}\n"
                        report += f"  종료가격: {format(int(trade[5]), ',')}원\n\n"
                else:
                    report += "해당 일자의 거래 내역이 없습니다."
                    
                return report
                
        except Exception as e:
            print(f"[ERROR] 일일 보고서 생성 실패: {str(e)}")
            return f"보고서 생성 중 오류 발생: {str(e)}"

    def generate_monthly_report(self, year=None, month=None):
        """월간 거래 보고서 생성"""
        try:
            if year is None or month is None:
                today = datetime.now()
                year = today.year
                month = today.month
                
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 월간 거래 통계
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN profit_rate > 0 THEN 1 ELSE 0 END) as profitable_trades,
                        AVG(profit_rate) as avg_profit,
                        SUM(profit_rate) as total_profit,
                        MAX(profit_rate) as max_profit,
                        MIN(profit_rate) as max_loss
                    FROM closed_positions
                    WHERE strftime('%Y', close_time) = ? 
                    AND strftime('%m', close_time) = ?
                ''', (str(year), f"{month:02d}"))
                
                stats = cursor.fetchone()
                
                # 코인별 통계
                cursor.execute('''
                    SELECT 
                        ticker,
                        COUNT(*) as trade_count,
                        AVG(profit_rate) as avg_profit,
                        SUM(CASE WHEN profit_rate > 0 THEN 1 ELSE 0 END) as wins
                    FROM closed_positions
                    WHERE strftime('%Y', close_time) = ?
                    AND strftime('%m', close_time) = ?
                    GROUP BY ticker
                    ORDER BY avg_profit DESC
                ''', (str(year), f"{month:02d}"))
                
                coin_stats = cursor.fetchall()
                
                # 보고서 생성
                report = f"📈 {year}년 {month}월 거래 보고서\n\n"
                
                if stats[0]:  # 거래가 있는 경우
                    win_rate = (stats[1] / stats[0]) * 100 if stats[0] > 0 else 0
                    report += f"총 거래: {stats[0]}건\n"
                    report += f"승률: {win_rate:.1f}%\n"
                    report += f"평균 수익률: {stats[2]:.2f}%\n"
                    report += f"총 수익률: {stats[3]:.2f}%\n"
                    report += f"최대 수익: {stats[4]:.2f}%\n"
                    report += f"최대 손실: {stats[5]:.2f}%\n\n"
                    
                    report += "🪙 코인별 성과:\n"
                    for coin in coin_stats:
                        coin_win_rate = (coin[3] / coin[1]) * 100
                        report += f"- {coin[0]}\n"
                        report += f"  거래수: {coin[1]}건\n"
                        report += f"  승률: {coin_win_rate:.1f}%\n"
                        report += f"  평균수익: {coin[2]:.2f}%\n\n"
                else:
                    report += "해당 월의 거래 내역이 없습니다."
                    
                return report
                
        except Exception as e:
            print(f"[ERROR] 월간 보고서 생성 실패: {str(e)}")
            return f"보고서 생성 중 오류 발생: {str(e)}"

if __name__ == "__main__":
    monitor = None
    try:
        # 기본 로깅 설정
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        print("[INFO] 봇 초기화 중...")
        upbit = UpbitAPI()
        telegram = TelegramBot()
        position_manager = PositionManager(upbit)  # PositionManager 먼저 생성
        analyzer = MarketAnalyzer(upbit, position_manager)
        monitor = MarketMonitor(upbit, telegram, analyzer)
        
        # 시작 메시지 전송
        print("[INFO] 봇 시작...")
        telegram.send_message("🤖 자동매매 봇이 시작되었습니다.\n명령어 목록을 보려면 /help를 입력하세요.")
        
        # 봇 자동 시작
        monitor.is_running = True
        print("[INFO] 봇 자동 시작됨")
        
        # monitor_market 메소드 실행
        while True:
            try:
                if monitor.is_running:
                    monitor.monitor_market()
                time.sleep(1)  # CPU 사용량 감소
            except KeyboardInterrupt:
                print("\n[INFO] 프로그램 종료 요청됨...")
                if monitor:
                    monitor.is_running = False
                telegram.send_message("🔴 봇이 수동으로 종료되었습니다.")
                break
            except Exception as e:
                logger.error(f"[ERROR] 모니터링 중 오류 발생: {e}")
                telegram.send_message(f"⚠️ 오류 발생: {str(e)}\n재시작을 시도합니다.")
                time.sleep(5)
                continue
                
    except Exception as e:
        error_message = f"프로그램 초기화 중 치명적 오류: {e}"
        logger.critical(f"[CRITICAL] {error_message}")
        if 'telegram' in locals():
            telegram.send_message(f"⚠️ {error_message}")
    
    finally:
        # 프로그램 종료 시 정리 작업
        if monitor:
            monitor.is_running = False
        if 'analyzer' in locals() and hasattr(analyzer, 'thread_pool'):
            analyzer.thread_pool.shutdown(wait=False)
        print("[INFO] 프로그램이 종료되었습니다.")