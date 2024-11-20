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
    
    def get_balances(self):
        """계좌 잔고 조회"""
        try:
            return self.upbit.get_balances()
        except Exception as e:
            print(f"잔고 조회 실패: {e}")
            return None

    def sell_limit_order(self, ticker, volume, price):
        """지정가 매도 주문"""
        try:
            print(f"[DEBUG] {ticker} 매도 주문 시도: {volume} @ {price:,}원")
            
            # 주문 실행
            order = self.upbit.sell_limit_order(ticker, price, volume)
            
            if order and 'uuid' in order:
                print(f"[INFO] {ticker} 매도 주문 성공: {order['uuid']}")
                return True, order['uuid']
            else:
                print(f"[ERROR] {ticker} 매도 주문 실패: {order}")
                return False, "주문 실패"
                
        except Exception as e:
            print(f"[ERROR] {ticker} 매도 주문 중 오류: {str(e)}")
            return False, str(e)
            
    def sell_market_order(self, ticker, volume):
        """시장가 매도 주문"""
        try:
            print(f"[DEBUG] {ticker} 시장가 매도 주문 시도: {volume}")
            
            # 주문 실행
            order = self.upbit.sell_market_order(ticker, volume)
            
            if order and 'uuid' in order:
                print(f"[INFO] {ticker} 시장가 매도 주문 성공: {order['uuid']}")
                return True, order['uuid']
            else:
                print(f"[ERROR] {ticker} 시장가 매도 주문 실패: {order}")
                return False, "주문 실패"
                
        except Exception as e:
            print(f"[ERROR] {ticker} 시장가 매도 주문 중 오류: {str(e)}")
            return False, str(e)
            
    def get_order_status(self, uuid):
        """주문 상태 조회"""
        try:
            order = self.upbit.get_order(uuid)
            return order
        except Exception as e:
            print(f"[ERROR] 주문 상태 조회 중 오류: {str(e)}")
            return None

    def buy_market_order(self, ticker, price):
        """시장가 매수 주문"""
        try:
            print(f"[DEBUG] {ticker} 시장가 매수 주문 시도: {price:,}원")
            
            # 주문 실행
            order = self.upbit.buy_market_order(ticker, price)
            
            if order and 'uuid' in order:
                print(f"[INFO] {ticker} 시장가 매수 주문 성공: {order['uuid']}")
                return True, order['uuid']
            else:
                print(f"[ERROR] {ticker} 시장가 매수 주문 실패: {order}")
                return False, "주문 실패"
                
        except Exception as e:
            print(f"[ERROR] {ticker} 시장가 매수 주문 중 오류: {str(e)}")
            return False, str(e)
            
    def buy_limit_order(self, ticker, price, volume):
        """지정가 매수 주문"""
        try:
            print(f"[DEBUG] {ticker} 매수 주문 시도: {volume} @ {price:,}원")
            
            # 주문 실행
            order = self.upbit.buy_limit_order(ticker, price, volume)
            
            if order and 'uuid' in order:
                print(f"[INFO] {ticker} 매수 주문 성공: {order['uuid']}")
                return True, order['uuid']
            else:
                print(f"[ERROR] {ticker} 매수 주문 실패: {order}")
                return False, "주문 실패"
                
        except Exception as e:
            print(f"[ERROR] {ticker} 매수 주문 중 오류: {str(e)}")
            return False, str(e)

    def get_current_price(self, ticker):
        """현재가 조회"""
        try:
            price = pyupbit.get_current_price(ticker)
            if price is None:
                print(f"[WARNING] {ticker} 현재가 조회 실패")
                return None
                
            return float(price)
            
        except Exception as e:
            print(f"[ERROR] {ticker} 현재가 조회 중 오류: {str(e)}")
            return None

    def get_current_prices(self, tickers):
        """여러 종목의 현재가 한 번에 조회"""
        try:
            if not isinstance(tickers, list):
                tickers = [tickers]
                
            prices = pyupbit.get_current_price(tickers)
            if not prices:
                return {}
                
            # 단일 종목 조회 시 딕셔너리로 변환
            if isinstance(prices, (int, float)):
                return {tickers[0]: float(prices)}
                
            return {ticker: float(price) for ticker, price in prices.items() if price is not None}
            
        except Exception as e:
            print(f"[ERROR] 현재가 일괄 조회 중 오류: {str(e)}")
            return {}

    def execute_sell(self, ticker, volume=None):
        """매도 실행"""
        try:
            # volume이 지정되지 않은 경우 전체 보유량 조회
            if volume is None:
                volume = self.get_balance(ticker)
                if not volume:
                    return False, "보유량 조회 실패"

            # 최소 거래 가능 수량 확인
            if volume < 0.00000001:  # 업비트 최소 거래 수량
                return False, "최소 거래 수량 미달"

            # 시장가 매도 주문
            order = self.upbit.sell_market_order(ticker, volume)
            
            if order and 'uuid' in order:
                return True, order['uuid']
            else:
                print(f"[ERROR] {ticker} 매도 주문 실패: {order}")
                return False, "주문 실패"
            
        except Exception as e:
            print(f"[ERROR] {ticker} 매도 실행 중 오류: {str(e)}")
            return False, str(e)

    def execute_buy(self, ticker):
        """매수 실행 (고정 금액: 5,500원)"""
        try:
            # 현재가 조회
            current_price = self.get_current_price(ticker)
            if not current_price:
                return False, "현재가 조회 실패"
            
            # 매수 가능한 KRW 잔고 확인
            krw_balance = self.get_balance("KRW")
            if krw_balance < 5500:  # 최소 주문 금액
                return False, "잔고 부족"
            
            # 고정 투자 금액 설정
            invest_amount = 5500  # 5,500원으로 고정
            
            # 시장가 매수 주문
            order = self.upbit.buy_market_order(ticker, invest_amount)
            
            if order and 'uuid' in order:
                # 성공 로그는 상위 메서드에서 처리하도록 제거
                return True, order['uuid']
            else:
                print(f"[ERROR] {ticker} 매수 주문 실패: {order}")
                return False, "주문 실패"
        
        except Exception as e:
            print(f"[ERROR] {ticker} 매수 실행 중 오류: {str(e)}")
            return False, str(e)

    def get_balance(self, ticker="KRW"):
        """특정 코인/원화의 잔고 조회"""
        try:
            return self.upbit.get_balance(ticker)
        except Exception as e:
            print(f"[ERROR] {ticker} 잔고 조회 중 오류: {str(e)}")
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
    def __init__(self, upbit_api):
        self.upbit = upbit_api
        self.tickers = []  # 빈 리스트로 초기화
        self.timeframes = {'minute1': {'interval': 'minute1', 'count': 100}}
        self.trading_conditions = {
            'rsi_oversold': 30,
            'rsi_overbought': 70,
            'bb_squeeze': 0.5,
            'bb_expansion': 2.0
        }
        self.market_state = 'normal'
        self.cache = {}
        self.cache_duration = 5
        self.last_analysis = {}
        self.analysis_interval = timedelta(seconds=3)
        self.analysis_count = 0
        self.max_analysis_per_cycle = 20
        
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
                    
                    self.tickers = [ticker['market'] for ticker in sorted_tickers]
                    print(f"[INFO] 티커 목록 업데이트 완료: {len(self.tickers)}개")
                    
                    # 상위 10개 티커 정보 출력
                    print("[INFO] 상위 20개 티커 (24시간 거래대금):")
                    for i, ticker_info in enumerate(sorted_tickers[:20], 1):
                        volume = float(ticker_info.get('acc_trade_price_24h', 0)) / 1000000  # 백만원 단위
                        price = float(ticker_info.get('trade_price', 0))
                        print(f"    {i}. {ticker_info['market']}: "
                              f"거래대금 {volume:,.0f}백만원, "
                              f"현재가 {price:,.0f}원")
                    
                else:
                    print(f"[ERROR] 거래량 조회 실패: {response.status_code}")
                    # 기본 티커 목록만 저장
                    self.tickers = all_tickers
                    
            except Exception as e:
                print(f"[WARNING] 거래량 조회 중 오류: {e}")
                # 오류 발생 시 기본 티커 목록 사용
                self.tickers = all_tickers
                
            # 분석할 최대 코인 수 설정
            self.tickers = self.tickers[:20]  # 상위 20개만 분석
            print(f"[INFO] 분석 대상 코인 수: {len(self.tickers)}개")
            
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
        """OHLCV 데이터 조회 (캐시 활용)"""
        try:
            # 캐시 키 생성
            cache_key = f"{ticker}_ohlcv"
            current_time = datetime.now()
            
            # 캐시된 데이터 확인
            if cache_key in self.cache:
                cached_data = self.cache[cache_key]
                if isinstance(cached_data, dict) and 'timestamp' in cached_data:
                    elapsed_time = (current_time - cached_data['timestamp']).total_seconds()
                    if elapsed_time < self.cache_duration:
                        return cached_data['data']

            # OHLCV 데이터 조회
            df = pyupbit.get_ohlcv(ticker, interval="minute1", count=100)
            if df is None or len(df) < 20:
                print(f"[WARNING] {ticker} OHLCV 데이터 부족")
                return None

            # 데이터 전처리
            df = df.rename(columns={
                'open': '시가',
                'high': '고가',
                'low': '저가',
                'close': '종가',
                'volume': '거래량'
            })

            # 캐시 업데이트
            self.cache[cache_key] = {
                'timestamp': current_time,
                'data': df
            }

            return df

        except Exception as e:
            print(f"[ERROR] {ticker} OHLCV 데이터 조회 실패: {str(e)}")
            return None

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

            # NaN 값 처리
            df = df.dropna()

            return df

        except Exception as e:
            print(f"[ERROR] 지표 계산 중 오류: {str(e)}")
            print(traceback.format_exc())
            return None

    def analyze_market(self, ticker):
        """시장 분석 수행 (병렬 처리 최적화)"""
        try:
            current_time = datetime.now()
            cache_key = f"{ticker}_analysis"
            
            # 캐시 확인
            if cache_key in self.cache:
                cached_data = self.cache[cache_key]
                if isinstance(cached_data, dict) and 'timestamp' in cached_data:
                    elapsed_time = (current_time - cached_data['timestamp']).total_seconds()
                    if elapsed_time < self.cache_duration:
                        print(f"[DEBUG] {ticker} 캐시된 결과 사용 (경과: {int(elapsed_time)}초)")
                        return cached_data['data']

            print(f"[INFO] {ticker} 분석 시작...")

            # OHLCV 데이터 조회
            print(f"[DEBUG] {ticker} OHLCV 데이터 조회 중...")
            df = self.get_ohlcv(ticker)
            if df is None:
                print(f"[ERROR] {ticker} OHLCV 데이터 조회 실패")
                return None

            # 지표 계산
            print(f"[DEBUG] {ticker} 기술적 지표 계산 중...")
            analyzed_df = self._calculate_indicators(df)
            if analyzed_df is None:
                print(f"[ERROR] {ticker} 지표 계산 실패")
                return None

            # 결과 생성
            print(f"[DEBUG] {ticker} 분석 결과 생성 중...")
            last_row = analyzed_df.iloc[-1]
            analysis_result = {
                'ticker': ticker,
                'current_price': float(last_row['종가']),
                'timeframes': {
                    'minute1': {
                        'rsi': float(last_row['RSI']),
                        'bb_bandwidth': float(last_row['밴드폭']),
                        'percent_b': (float(last_row['종가']) - float(last_row['하단밴드'])) / 
                                   (float(last_row['상단밴드']) - float(last_row['하단밴드']))
                    }
                },
                'timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S')
            }

            # 캐시 업데이트 (형식 변경)
            self.cache[cache_key] = {
                'timestamp': current_time,
                'data': analysis_result
            }
            self.last_analysis[ticker] = current_time
            self.analysis_count += 1

            print(f"[INFO] {ticker} 분석 완료 - RSI: {analysis_result['timeframes']['minute1']['rsi']:.1f}, "
                  f"%B: {analysis_result['timeframes']['minute1']['percent_b']:.2f}, "
                  f"밴드폭: {analysis_result['timeframes']['minute1']['bb_bandwidth']:.1f}%")

            return analysis_result

        except Exception as e:
            print(f"[ERROR] {ticker} 분석 중 오류: {str(e)}")
            print(f"[DEBUG] {ticker} 상세 오류:")
            print(traceback.format_exc())
            return None

    def analyze_multiple_markets(self, tickers):
        """여러 시장 동시 분석"""
        if not tickers:
            print("[WARNING] 분석할 티커 목록이 비어있음")
            self.update_tickers()  # 티커 목록 업데이트 시도
            tickers = self.tickers  # 업데이트된 티커 목록 사용
            
        results = {}
        futures = []
        
        analysis_tickers = tickers[:self.max_analysis_per_cycle]
        print(f"[INFO] 총 {len(analysis_tickers)}개 코인 병렬 분석 시작...")
        
        # 병렬로 분석 작업 제출
        for ticker in analysis_tickers:
            print(f"[DEBUG] {ticker} 분석 작업 제출...")
            future = self.thread_pool.submit(self.analyze_market, ticker)
            futures.append((ticker, future))
        
        # 결과 수집
        completed = 0
        for ticker, future in futures:
            try:
                print(f"[DEBUG] {ticker} 분석 결과 대기 중...")
                result = future.result(timeout=2)
                if result:
                    results[ticker] = result
                    completed += 1
                    print(f"[INFO] {ticker} 분석 완료 ({completed}/{len(futures)})")
                else:
                    print(f"[WARNING] {ticker} 분석 결과 없음")
            except Exception as e:
                print(f"[ERROR] {ticker} 분석 결과 처리 실패: {e}")
                print(f"[DEBUG] {ticker} 상세 오류:")
                print(traceback.format_exc())
                continue
        
        print(f"[INFO] 병렬 분석 완료 - 성공: {completed}/{len(futures)}")
        return results
    
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
            
            # 시장 상태에 따른 조건 업데이트
            if market_status:
                # 변동성이 높은 시장
                if market_status['volatility'] > market_status['avg_volatility'] * 1.5:
                    self.market_state = 'volatile'
                    self.trading_conditions.update({
                        'rsi_oversold': 25,
                        'rsi_overbought': 75,
                        'bb_squeeze': 0.3,
                        'bb_expansion': 2.5
                    })
                # 추세가 강한 시장
                elif abs(market_status['price_trend']) > 5:
                    self.market_state = 'trend'
                    self.trading_conditions.update({
                        'rsi_oversold': 35,
                        'rsi_overbought': 65,
                        'bb_squeeze': 0.7,
                        'bb_expansion': 1.8
                    })
                # 일반 시장
                else:
                    self.market_state = 'normal'
                    self.trading_conditions.update({
                        'rsi_oversold': 30,
                        'rsi_overbought': 70,
                        'bb_squeeze': 0.5,
                        'bb_expansion': 2.0
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

    def get_top_volume_tickers(self, limit=20):  # 상위 20개로 수정
        """거래량 상위 코인 목록 조회"""
        try:
            all_tickers = pyupbit.get_tickers(fiat="KRW")
            volume_data = []
            
            for ticker in all_tickers:
                try:
                    df = pyupbit.get_ohlcv(ticker, interval="day", count=1)
                    if df is not None and not df.empty:
                        trade_price = df['volume'].iloc[-1] * df['close'].iloc[-1]
                        volume_data.append((ticker, trade_price))
                    time.sleep(0.1)
                except Exception as e:
                    print(f"[ERROR] {ticker} 거래량 조회 실패: {e}")
                    continue
            
            volume_data.sort(key=lambda x: x[1], reverse=True)
            top_tickers = [ticker for ticker, volume in volume_data[:limit]]
            
            if top_tickers:
                print(f"[INFO] 거래량 상위 {limit}개 코인 목록 갱신됨")
                print(f"코인 목록: {', '.join(top_tickers)}")
                return top_tickers
            else:
                print("[WARNING] 거래량 데이터 조회 실패, 기본 티커 사용")
                return self.tickers if hasattr(self, 'tickers') else all_tickers[:limit]
            
        except Exception as e:
            print(f"[ERROR] 거래량 상위 코인 조회 실패: {e}")
            return self.tickers if hasattr(self, 'tickers') else all_tickers[:limit]
    
    def get_trading_signals(self, analysis):
        """매매 신호 생성"""
        try:
            signals = []
            if not analysis or 'timeframes' not in analysis:
                return signals

            timeframe_data = analysis['timeframes']['minute1']
            ticker = analysis['ticker']
            
            # RSI 기반 신호
            rsi = timeframe_data['rsi']
            if rsi <= self.trading_conditions['rsi_oversold']:
                signals.append(('매수', f'RSI 과매도({rsi:.1f})', ticker))
            elif rsi >= self.trading_conditions['rsi_overbought']:
                signals.append(('매도', f'RSI 과매수({rsi:.1f})', ticker))
            
            # 볼린저 밴드 기반 신호
            bb_bandwidth = timeframe_data['bb_bandwidth']
            percent_b = timeframe_data['percent_b']
            
            # 밴드 스퀴즈 상태에서 돌파
            if bb_bandwidth < self.trading_conditions['bb_squeeze']:
                if percent_b > 1:  # 상단 돌파
                    signals.append(('매수', f'밴드 스퀴즈 상향 돌파(밴드폭:{bb_bandwidth:.1f}%)', ticker))
                elif percent_b < 0:  # 하단 돌파
                    signals.append(('매수', f'밴드 스퀴즈 하향 돌파(밴드폭:{bb_bandwidth:.1f}%)', ticker))
            
            # 과도한 밴드 확장
            elif bb_bandwidth > self.trading_conditions['bb_expansion']:
                if percent_b > 0.95:  # 상단 접근
                    signals.append(('매도', f'밴드 상단 접근(밴드폭:{bb_bandwidth:.1f}%)', ticker))
                elif percent_b < 0.05:  # 하단 접근
                    signals.append(('매수', f'밴드 하단 접근(밴드폭:{bb_bandwidth:.1f}%)', ticker))
            
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
        self.commands = {
            '/start': self.start_bot,
            '/stop': self.stop_bot,
            '/status': self.show_positions,
            '/profit': self.show_profit,
            '/market': self.show_market_analysis,
            '/sell_all': self.sell_all_positions,
            '/help': self.show_help
        }
        
        # 기존 포지션 로드 (명시적으로 호출)
        self.position_manager.load_positions()
        
        # 모니터링 상태 관리 변수들
        self.is_running = False
        self.last_market_analysis = datetime.now()
        self.market_analysis_interval = timedelta(hours=1)
        self.last_status_update = datetime.now()
        self.status_update_interval = timedelta(minutes=30)
        
        # 에러 관련 변수들
        self.error_logs = []
        self.max_error_logs = 100
        self.last_error_notification = datetime.now()
        self.error_notification_cooldown = timedelta(minutes=5)
        # 로깅 설정
        self.setup_logging()
        
        # 텔레그램 명령어 처리 관련 변수
        self.last_error_notification = datetime.now()
        self.error_notification_cooldown = timedelta(minutes=5)
        self.last_processed_update_id = 0
        self.last_command_check = datetime.now()
        self.command_check_interval = timedelta(seconds=3)
        
        # 초기 시장 분석
        self.analyzer.update_tickers()  # 추가 필요


    def setup_logging(self):
        """로깅 설정"""
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # 파일 핸들러 설정
        log_file = f'logs/trading_{datetime.now().strftime("%Y%m%d")}.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # 포맷터 설정
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # 로거 설정
        self.logger = logging.getLogger('trading_bot')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        
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
            self.logger.error(f"{error_type}: {str(error)}\n{traceback.format_exc()}")
            
            # 심각한 에러는 텔레그램으로 알림
            if error_type.startswith("CRITICAL"):
                self.telegram.send_message(f"⚠️ 심각한 오류 발생:\n{str(error)}")
                
        except Exception as e:
            print(f"[ERROR] 에러 로깅 중 추가 에러 발생: {str(e)}")

    def load_existing_positions(self):
        """기존 보유 코인을 포지션에 추가"""
        try:
            balances = self.upbit.get_balances()
            print("받은 데이터 형식:", type(balances))
            print("데이터 내용:", balances)
            if not balances:
                return

            loaded_positions = 0
            for balance in balances:
                try:
                    # balance가 문자열이 아닌 딕셔너리인지 확인
                    if not isinstance(balance, dict):
                        continue
                    
                    # 필수 필드 확인
                    currency = balance['currency']
                    balance_amt = balance['balance']
                    avg_price = balance['avg_buy_price']
                    
                    if not currency or currency == 'KRW':  # KRW는 건너기
                        continue

                    # KRW 마켓 티커로 변환
                    market_ticker = f"KRW-{currency}"
                    
                    # 수량과 평균단가 변환
                    quantity = float(balance_amt)
                    avg_price = float(avg_price)
                    
                    # 1000원 이상인 포지션만 불러오기
                    current_value = quantity * avg_price
                    if current_value < 1000:
                        continue

                    # 포지션 추가
                    success, message = self.position_manager.open_position(market_ticker, avg_price, quantity)
                    if success:
                        loaded_positions += 1
                        print(f"포지션 불러옴: {market_ticker}, 수량: {quantity}, 평균가: {avg_price}")  # 디버깅
                        self.telegram.send_message(
                            f"💼 기존 포지션 불러옴: {market_ticker}\n"
                            f"평균단가: {avg_price:,.0f}원\n"
                            f"수량: {quantity:.8f}"
                        )

                except KeyError as e:
                    print(f"포지션 데이터 형식 오류: {e}, 데이터: {balance}")
                    continue
                except Exception as e:
                    print(f"포지션 불러오기 중 개별 오류: {e}, 데이터: {balance}")
                    continue

            if loaded_positions > 0:
                self.telegram.send_message(f"✅ 총 {loaded_positions}개의 기존 포지션을 불러왔습니다.")
            
        except Exception as e:
            error_message = f"기존 포지션 불러오기 실패: {e}"
            print(error_message)
            self.telegram.send_message(f"⚠️ {error_message}")

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

    def process_buy_signal(self, ticker, action):
        """매매 신호 처리"""
        try:
            print(f"[DEBUG] ====== 매매 신호 처리 시작: {ticker} {action} ======")
            
            if action == '매수':
                # 기존 매수 로직 유지
                return self.execute_buy(ticker)
                
            elif action == '매도':
                return self.execute_sell(ticker)
                
            else:
                print(f"[WARNING] 알 수 없는 매매 신호: {action}")
                return False, f"알 수 없는 매매 신호: {action}"
            
        except Exception as e:
            print(f"[ERROR] 매매 신호 처리 중 오류: {str(e)}")
            return False, str(e)

    def execute_sell(self, ticker):
        """매도 실행"""
        try:
            print(f"[DEBUG] {ticker} 매도 시도...")
            
            if ticker not in self.position_manager.positions:
                return False, "보유하지 않은 코인"
                
            position = self.position_manager.positions[ticker]
            current_price = self.upbit.get_current_price(ticker)
            
            if not current_price:
                return False, "현재가 조회 실패"
                
            # 매도 주문 실행
            success, order_id = self.upbit.execute_sell(ticker)
            
            if success:
                # 주문 체결 대기
                time.sleep(1)
                order = self.upbit.get_order_status(order_id)
                
                if order and order['state'] == 'done':
                    # 실제 체결 정보 사용
                    executed_price = float(order['price'])
                    executed_volume = float(order['volume'])
                    
                    # 수익률 계산
                    profit = position.calculate_profit(executed_price)
                    
                    # 포지션 종료
                    self.position_manager.close_position(ticker)
                    
                    # 매도 알림 전송
                    hold_time = datetime.now() - position.entry_time
                    hold_hours = hold_time.total_seconds() / 3600
                    
                    print(f"[INFO] {ticker} 매도 성공: {format(int(executed_price), ',')}원 @ {executed_volume:.8f}")
                    self.telegram.send_message(
                        f"💰 매도 완료: {ticker}\n"
                        f"매도가: {format(int(executed_price), ',')}원\n"
                        f"매도량: {executed_volume:.8f}\n"
                        f"수익률: {profit:.2f}%\n"
                        f"보유기간: {hold_hours:.1f}시간\n"
                        f"매수횟수: {position.buy_count}회"
                    )
                    
                    return True, "매도 성공"
                else:
                    print(f"[ERROR] {ticker} 매도 주문 체결 실패")
                    return False, "매도 주문 체결 실패"
            else:
                return False, f"매도 주문 실패: {order_id}"
            
        except Exception as e:
            print(f"[ERROR] {ticker} 매도 실행 중 오류: {str(e)}")
            return False, str(e)

    def execute_buy(self, ticker):
        """매수 실행"""
        try:
            print(f"[DEBUG] {ticker} 매수 시도...")
            
            # 포지션 오픈 가능 여부 확인
            if ticker in self.position_manager.positions:
                # 기존 포지션이 있는 경우 추가매수 가능 여부 확인
                position = self.position_manager.positions[ticker]
                if position.buy_count >= 3:
                    return False, "최대 매수 횟수 초과"
                    
                # 마지막 매수로부터 3초 대기 확인
                time_since_last_buy = datetime.now() - position.last_buy_time
                if time_since_last_buy.total_seconds() < 3:
                    return False, "매수 대기 시간"
            else:
                # 새 포지션 오픈 가능 여부 확인
                can_open, message = self.position_manager.can_open_position(ticker)
                if not can_open:
                    return False, message

            # 매수 주문 실행
            success, order_id = self.upbit.execute_buy(ticker)
            
            if success:
                # 주문 체결 대기
                time.sleep(1)
                order = self.upbit.get_order_status(order_id)
                
                if order and order['state'] == 'done':
                    # 실제 체결 정보 사용
                    executed_price = float(order['price'])
                    executed_volume = float(order['volume'])
                    
                    if ticker in self.position_manager.positions:
                        # 기존 포지션에 추가
                        success, message = self.position_manager.add_to_position(ticker, executed_price, executed_volume)
                    else:
                        # 새 포지션 생성
                        success, message = self.position_manager.open_position(ticker, executed_price, executed_volume)
                    
                    if success:
                        buy_type = "추가매수" if ticker in self.position_manager.positions else "신규매수"
                        print(f"[INFO] {ticker} {buy_type} 성공: {format(int(executed_price), ',')}원 @ {executed_volume:.8f}")
                        self.telegram.send_message(
                            f"✅ {ticker} {buy_type} 성공\n"
                            f"매수가: {format(int(executed_price), ',')}원\n"
                            f"수량: {executed_volume:.8f}"
                        )
                        return True, f"{buy_type} 성공"
                    else:
                        return False, f"포지션 처리 실패: {message}"
                else:
                    print(f"[ERROR] {ticker} 매수 주문 체결 실패")
                    return False, "매수 주문 체결 실패"
            else:
                return False, f"매수 주문 실패: {order_id}"
                
        except Exception as e:
            print(f"[ERROR] {ticker} 매수 실행 중 오류: {str(e)}")
            return False, str(e)

    def send_position_update(self, ticker, action):
        """포지션 상태 업데이트 메시지 전송"""
        status = self.position_manager.get_position_status(ticker)
        if not status:
            return
            
        message = f"💼 포지션 업데이트 ({action})\n\n"
        message += f"코인: {ticker}\n"
        message += f"평균단가: {format(status['average_price'], ',')}원\n"  # 천단위 구분자 사용
        message += f"수량: {status['quantity']}\n"  # 소수점 표시 제거
        message += f"매수 횟수: {status['buy_count']}\n"
        message += f"수익률: {status['profit']:.2f}%\n"
        message += f"상태: {status['status']}\n"
        message += f"마지막 업데이트: {status['last_update'].strftime('%Y-%m-%d %H:%M:%S')}"
        
        # 마크다운 파싱 제거
        self.telegram.send_message(message, parse_mode=None)
    
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
    
    def show_positions(self):
        """현재 포지션 상태 표시"""
        try:
            if not self.position_manager.positions:
                self.telegram.send_message("📊 현재 보유 중인 포지션이 없습니다.")
                return

            message = "📊 현재 포지션 상태\n\n"
            total_profit = 0
            
            for ticker, position in self.position_manager.positions.items():
                current_price = pyupbit.get_current_price(ticker)
                if current_price:
                    profit = position.calculate_profit(current_price)
                    total_profit += profit
                    
                    message += f"🪙 {ticker}\n"
                    message += f"평균단가: {format(int(position.average_price), ',')}원\n"
                    message += f"현재가: {format(int(current_price), ',')}원\n"
                    message += f"수익률: {profit:+.2f}%\n"
                    message += f"매수횟수: {position.buy_count}/3\n"
                    message += f"보유시간: {(datetime.now() - position.entry_time).total_seconds()/3600:.1f}시간\n\n"
            
            message += f"전체 평균 수익률: {total_profit/len(self.position_manager.positions):+.2f}%"
            self.telegram.send_message(message)
            
        except Exception as e:
            self.telegram.send_message(f"⚠️ 포지션 상태 조회 중 오류: {str(e)}")
    
    def show_profit(self):
        """수익률 현황 표시"""
        try:
            if not self.position_manager.positions:
                self.telegram.send_message("📈 현재 보유 중인 포지션이 없습니다.")
                return

            message = "📈 수익률 현황\n\n"
            total_investment = 0
            total_current_value = 0
            
            for ticker, position in self.position_manager.positions.items():
                current_price = pyupbit.get_current_price(ticker)
                if current_price:
                    investment = position.average_price * position.total_quantity
                    current_value = current_price * position.total_quantity
                    
                    total_investment += investment
                    total_current_value += current_value
                    
                    profit_rate = ((current_value - investment) / investment) * 100
                    message += f"{ticker}: {profit_rate:+.2f}%\n"
            
            total_profit_rate = ((total_current_value - total_investment) / total_investment) * 100
            message += f"\n총 투자금액: {format(int(total_investment), ',')}원\n"
            message += f"총 평가금액: {format(int(total_current_value), ',')}원\n"
            message += f"총 수익률: {total_profit_rate:+.2f}%"
            
            self.telegram.send_message(message)
            
        except Exception as e:
            self.telegram.send_message(f"⚠️ 수익률 조회 중 오류: {str(e)}")
    
    def sell_all_positions(self):
        """전체 포지션 매도"""
        try:
            if not self.position_manager.positions:
                self.telegram.send_message("📊 매도할 포지션이 없습니다.")
                return

            success_count = 0
            fail_count = 0
            total_profit = 0
            
            for ticker in list(self.position_manager.positions.keys()):
                success, message = self.position_manager.close_position(ticker)
                if success:
                    success_count += 1
                else:
                    fail_count += 1
                    self.telegram.send_message(f"⚠️ {ticker} 매도 실패: {message}")

            result_message = f"📊 전체 매도 결과\n\n"
            result_message += f"성공: {success_count}건\n"
            result_message += f"실패: {fail_count}건"
            
            self.telegram.send_message(result_message)
            
        except Exception as e:
            self.telegram.send_message(f"⚠️ 전체 매도 중 오류: {str(e)}")
    
    def show_balance(self):
        """계좌 잔고 확인"""
        try:
            balances = self.upbit.get_balances()
            if not balances:
                self.telegram.send_message("❌ 잔고 조회 실패")
                return

            message = "💰 계좌 잔고 현황\n\n"
            total_balance = 0
            
            # KRW 잔고 표시
            krw_balance = float(next((b['balance'] for b in balances if b['currency'] == 'KRW'), 0))
            message += f"💵 KRW: {format(int(krw_balance), ',')}원\n\n"
            total_balance += krw_balance
            
            # 코인 잔고 표시
            for balance in balances:
                if balance['currency'] == 'KRW':
                    continue
                    
                currency = balance['currency']
                amount = float(balance['balance'])
                avg_price = float(balance['avg_buy_price'])
                
                if amount > 0:
                    ticker = f"KRW-{currency}"
                    current_price = pyupbit.get_current_price(ticker)
                    
                    if current_price:
                        current_value = amount * current_price
                        profit_rate = ((current_price - avg_price) / avg_price) * 100
                        total_balance += current_value
                        
                        message += f"🪙 {currency}\n"
                        message += f"수량: {amount:.8f}\n"
                        message += f"평단가: {format(int(avg_price), ',')}원\n"
                        message += f"현재가: {format(int(current_price), ',')}원\n"
                        message += f"평가금액: {format(int(current_value), ',')}원\n"
                        message += f"수익률: {profit_rate:+.2f}%\n\n"
            
            message += f"총 평가금액: {format(int(total_balance), ',')}원"
            self.telegram.send_message(message)
            
        except Exception as e:
            self.telegram.send_message(f"⚠️ 잔고 조회 중 오류: {str(e)}")

    def send_status_update(self):
        """상태 업데이트 전송"""
        try:
            current_time = datetime.now()
            if current_time - self.last_status_update >= self.status_update_interval:
                message = "🤖 자동매매 봇 상태 업데이트\n\n"
                message += f"실행 상태: {'실행 중 ✅' if self.is_running else '중지됨 ⛔'}\n"
                message += f"마지막 업데이트: {current_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                message += f"최근 에러 수: {len(self.error_logs[-10:]) if self.error_logs else 0}건\n"
                
                # 보유 포지션 정보
                positions = self.position_manager.get_positions()
                if positions:
                    message += f"\n💼 보유 포지션: {len(positions)}개\n"
                    total_profit = sum(pos['profit'] for pos in positions.values())
                    message += f"전체 수익률: {total_profit:.2f}%\n"
                
                self.telegram.send_message(message)
                self.last_status_update = current_time
                
        except Exception as e:
            self.log_error("상태 업데이트 전송", e)

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
            
            # 시장 분석 주기 체크 (1시간)
            if current_time - self.last_market_analysis >= self.market_analysis_interval:
                print("[INFO] 시장 전체 분석 시작...")
                
                # 상위 거래량 코인 가져오기
                top_10_tickers = self.analyzer.tickers[:10]
                
                # 병렬로 여러 코인 분석
                analysis_results = self.analyzer.analyze_multiple_markets(top_10_tickers)
                
                # 시장 상태 분석
                market_states = []
                for ticker, analysis in analysis_results.items():
                    if analysis and 'minute1' in analysis['timeframes']:
                        df = self.analyzer.get_ohlcv(ticker)
                        if df is not None:
                            market_state = self.analyzer.analyze_market_state(df)
                            if market_state:
                                market_states.append(market_state)
                
                # 시장 상태 업데이트 및 매매 조건 조정
                if market_states:
                    avg_volatility = sum(state['volatility'] for state in market_states) / len(market_states)
                    avg_price_trend = sum(state['price_trend'] for state in market_states) / len(market_states)
                    avg_bb_trend = sum(state['bb_trend'] for state in market_states) / len(market_states)
                    
                    combined_market_status = {
                        'volatility': avg_volatility,
                        'avg_volatility': sum(state['avg_volatility'] for state in market_states) / len(market_states),
                        'price_trend': avg_price_trend,
                        'bb_trend': avg_bb_trend
                    }
                    
                    update_message = self.analyzer.update_trading_conditions(combined_market_status)
                    if update_message:
                        self.telegram.send_message(update_message)
                
                self.last_market_analysis = current_time

            # 개별 코인 분석 (병렬 처리)
            analysis_results = self.analyzer.analyze_multiple_markets(self.analyzer.tickers)
            
            # 매매 신호 처리
            for ticker, analysis in analysis_results.items():
                if analysis:
                    signals = self.analyzer.get_trading_signals(analysis)
                    for signal in signals:
                        if signal:
                            action, reason, ticker = signal
                            success, message = self.process_buy_signal(ticker, action)
                            if success:
                                self.telegram.send_message(f"✅ {ticker} {action} 성공: {reason}")
                            else:
                                print(f"[DEBUG] {ticker} {action} 실패: {message}")

            # 포지션 관리
            self.check_position_conditions()
            self.check_position_hold_times()

            self.check_telegram_commands()

        except Exception as e:
            print(f"[ERROR] 모니터링 중 오류: {str(e)}")
            self.log_error("모니터링 중 오류", e)

    def check_position_conditions(self):
        """포지션의 손절/익절/강제매도 조건 체크"""
        try:
            positions_to_sell = []
            for ticker, position in self.position_manager.positions.items():
                current_price = pyupbit.get_current_price(ticker)
                if not current_price:
                    continue
                    
                profit = position.calculate_profit(current_price)
                hold_time = datetime.now() - position.entry_time
                
                # 손절/익절/강제매도 조건 체크
                reason = None
                if profit <= position.stop_loss:
                    reason = f"손절 조건 도달 (수익률: {profit:.2f}%)"
                elif profit >= position.take_profit:
                    reason = f"익절 조건 도달 (수익률: {profit:.2f}%)"
                elif hold_time >= position.max_hold_time:
                    reason = f"보유시간 초과 (시간: {hold_time.total_seconds()/3600:.1f}시간)"
                
                if reason:
                    positions_to_sell.append((ticker, reason))
            
            # 매도 실행
            for ticker, reason in positions_to_sell:
                success, message = self.process_buy_signal(ticker, '매도')
                if success:
                    self.telegram.send_message(
                        f"⚠️ {ticker} 자동 매도 실행\n"
                        f"사유: {reason}"
                    )
                else:
                    print(f"[ERROR] {ticker} 자동 매도 실패: {message}")
                
        except Exception as e:
            print(f"[ERROR] 포지션 조건 체크 중 오류: {e}")
            self.log_error("포지션 조건 체크 중 오류", e)

    def analyze_single_ticker(self, ticker):
        """단일 티커 분석 및 매매 신호 처리"""
        try:
            analysis = self.analyzer.analyze_market(ticker)
            if analysis:
                signals = self.analyzer.get_trading_signals(analysis)
                if signals:
                    for signal in signals:
                        if signal:
                            action, reason, ticker = signal
                            print(f"[DEBUG] {ticker} 신호 처리 시작: {action}, 사유: {reason}")
                            
                            # 매도 신호 우선 처리
                            if action == '매도':
                                if ticker in self.position_manager.positions:
                                    print(f"[DEBUG] {ticker} 매도 신호 처리 시작")
                                    success, message = self.process_buy_signal(ticker, action)
                                    if success:
                                        self.telegram.send_message(f"✅ {ticker} 매도 성공: {reason}")
                                    else:
                                        print(f"[DEBUG] {ticker} 매도 실패: {message}")
                                continue  # 매도 처리 후 다음 신호로
                            
                            # 매수 신호 처리
                            elif action == '매수':
                                # 포지션이 최대치일 때는 추가매수만
                                if len(self.position_manager.positions) >= self.position_manager.max_positions:
                                    if ticker in self.position_manager.positions and \
                                    self.position_manager.positions[ticker].buy_count < 3:
                                        success, message = self.process_buy_signal(ticker, action)
                                        if success:
                                            self.telegram.send_message(f"✅ {ticker} 추가매수 성공: {reason}")
                                        else:
                                            print(f"[DEBUG] {ticker} 추가매수 실패: {message}")
                                    continue
                                
                                # 포지션에 여유가 있을 때의 매수
                                success, message = self.process_buy_signal(ticker, action)
                                if success:
                                    self.telegram.send_message(f"✅ {ticker} 매수 성공: {reason}")
                                else:
                                    print(f"[DEBUG] {ticker} 매수 실패: {message}")
                                
        except Exception as e:
            print(f"[ERROR] {ticker} 분석 중 오류: {str(e)}")
            self.log_error(f"{ticker} 매매 신호 처리 중 오류", e)
            return False, str(e)

    def show_market_analysis(self):
        """현재 시장 상황 분석 결과 전송 (주요 코인 + 거래량 상위)"""
        try:
            message = "🔍 현재 시장 상황 분석\n\n"
            
            # 주요 코인 목록
            major_coins = [
                'KRW-BTC', 'KRW-ETH', 'KRW-XRP', 'KRW-SOL', 
                'KRW-ADA', 'KRW-DOGE', 'KRW-MATIC', 'KRW-DOT',
                'KRW-TRX', 'KRW-LINK'
            ]
            
            # 상위 거래량 코인 추가 (주요 코인 제외)
            volume_leaders = []
            for ticker in self.analyzer.tickers:
                if ticker not in major_coins:
                    try:
                        current_volume = pyupbit.get_current_price(ticker) * \
                                    pyupbit.get_ohlcv(ticker, interval="day", count=1)['volume'].iloc[-1]
                        volume_leaders.append((ticker, current_volume))
                    except:
                        continue
            
            # 거래량 기준 상위 5개 코인 선택
            volume_leaders.sort(key=lambda x: x[1], reverse=True)
            top_volume_coins = [coin[0] for coin in volume_leaders[:10]]
            
            # 분석 대상 코인 목록
            analysis_targets = major_coins + top_volume_coins
            
            # 코인별 분석
            for ticker in analysis_targets:
                print(f"[INFO] {ticker} 분석 시작...")
                try:
                    analysis = self.analyzer.analyze_market(ticker)
                    if not analysis:
                        continue
                    
                    message += f"🪙 {ticker}\n"
                    message += f"💰 현재가: {format(int(analysis['current_price']), ',')}원\n"
                    
                    # RSI 분석
                    rsi = analysis['timeframes']['minute1']['rsi']
                    message += f"RSI: {rsi:.1f}"
                    if rsi <= self.analyzer.trading_conditions['rsi_oversold']:
                        message += " (과매도⤴️)"
                    elif rsi >= self.analyzer.trading_conditions['rsi_overbought']:
                        message += " (과매수⤵️)"
                    message += "\n"
                    
                    # 볼린저밴드 분석
                    bb_width = analysis['timeframes']['minute1']['bb_bandwidth']
                    message += f"밴드폭: {bb_width:.1f}%"
                    if bb_width < self.analyzer.trading_conditions['bb_squeeze']:
                        message += " (수축💫)"
                    elif bb_width > self.analyzer.trading_conditions['bb_expansion']:
                        message += " (확장↔️)"
                    message += "\n"
                    
                    # %B 분석
                    percent_b = analysis['timeframes']['minute1']['percent_b']
                    message += f"%B: {percent_b:.2f}"
                    if percent_b <= 0.05:
                        message += " (하단돌파⚠️)"
                    elif percent_b >= 0.95:
                        message += " (상단돌파⚠️)"
                    message += "\n"
                    
                    # 보유 상태 확인
                    if ticker in self.position_manager.positions:
                        position = self.position_manager.get_position_status(ticker)
                        message += f"\n💼 보유 중:\n"
                        message += f"평균단가: {format(int(position['average_price']), ',')}원\n"
                        message += f"수익률: {position['profit']:.2f}%\n"
                        message += f"매수횟수: {position['buy_count']}/3\n"
                    
                    message += "\n" + "─" * 20 + "\n\n"
                    
                except Exception as e:
                    print(f"[ERROR] {ticker} 분석 중 오류: {e}")
                    continue
            
            # 전체 시장 상태 추가
            message += f"\n🌍 전체 시장 상태: {self.analyzer.market_state}\n"
            message += f"📊 현재 매매 조건:\n"
            message += f"- RSI 과매도: {self.analyzer.trading_conditions['rsi_oversold']}\n"
            message += f"- RSI 과매수: {self.analyzer.trading_conditions['rsi_overbought']}\n"
            message += f"- 밴드 수축: {self.analyzer.trading_conditions['bb_squeeze']}\n"
            message += f"- 밴드 확장: {self.analyzer.trading_conditions['bb_expansion']}\n"
            
            message += f"\n분석 대상: 주요 코인 {len(major_coins)}개 + 거래량 상위 {len(top_volume_coins)}개"
            
            # 메시지 분할 전송
            max_length = 4096
            if len(message) > max_length:
                messages = [message[i:i+max_length] for i in range(0, len(message), max_length)]
                for msg in messages:
                    self.telegram.send_message(msg)
            else:
                self.telegram.send_message(message)
            
        except Exception as e:
            error_msg = f"시장 분석 중 오류 발생: {str(e)}"
            print(f"[ERROR] {error_msg}")
            self.telegram.send_message(f"⚠️ {error_msg}")

    def show_help(self):
        """봇 사용법 안내"""
        message = "🤖 자동매매 봇 사용법\n\n"
        message += "/start - 봇 시작\n"
        message += "/stop - 봇 중지\n"
        message += "/status - 포지션 상태 확인\n"
        message += "/profit - 수익률 확인\n"
        message += "/market - 시장 상황 분석\n"
        message += "/sell_all - 전체 포지션 매도\n"
        
        self.telegram.send_message(message)

    def check_position_hold_times(self):
        """포지션 보유 시간 체크 및 강제 매도"""
        try:
            positions_to_sell = []
            for ticker, position in self.position_manager.positions.items():
                if position.should_force_sell():
                    positions_to_sell.append(ticker)
            
            for ticker in positions_to_sell:
                try:
                    current_price = pyupbit.get_current_price(ticker)
                    if current_price:
                        position = self.position_manager.positions[ticker]
                        profit = position.calculate_profit(current_price)
                        
                        # 강제 매도 처리
                        success, message = self.process_buy_signal(ticker, '매도')
                        if success:
                            hold_time = datetime.now() - position.entry_time
                            hold_hours = hold_time.total_seconds() / 3600
                            
                            self.telegram.send_message(
                                f"⏰ 보유시간 초과로 강제 매도\n\n"
                                f"코인: {ticker}\n"
                                f"보유기간: {hold_hours:.1f}시간\n"
                                f"수익률: {profit:.2f}%\n"
                                f"매수횟수: {position.buy_count}회"
                            )
                        else:
                            print(f"[ERROR] {ticker} 강제 매도 실패: {message}")
                
                except Exception as e:
                    print(f"[ERROR] {ticker} 강제 매도 처리 중 오류: {e}")
                    self.log_error(f"{ticker} 강제 매도 처리 중 오류", e)
                    continue
                
        except Exception as e:
            print(f"[ERROR] 포지션 보유 시간 체크 중 오류: {e}")
            self.log_error("포지션 보유 시간 체크 중 오류", e)

class Position:
    def __init__(self, ticker, entry_price, quantity):
        self.ticker = ticker
        self.entries = [(entry_price, quantity)]
        self.buy_count = 1
        self.status = 'active'
        self.entry_time = datetime.now()
        self.last_buy_time = datetime.now()
        self.stop_loss = -3.0
        self.take_profit = 5.0
        self.max_hold_time = timedelta(hours=3)
        self.db_path = 'positions.db'
        self.save_position()

    def should_force_sell(self):
        """강제 매도 조건 확인 (손절/익절/시간 초과)"""
        current_time = datetime.now()
        hold_time = current_time - self.entry_time
        
        # 현재가로 수익률 계산
        current_price = pyupbit.get_current_price(self.ticker)
        if current_price:
            profit = self.calculate_profit(current_price)
            
            # 손절/익절 조건 추가
            if profit <= self.stop_loss or profit >= self.take_profit:
                return True
                
        # 보유 시간 초과 체크
        return hold_time >= self.max_hold_time

    def save_position(self):
        """포지션 정보를 데이터베이스에 저장"""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
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
                
                # 기존 엔트리 삭제
                cursor.execute('DELETE FROM entries WHERE ticker = ?', (self.ticker,))
                
                # 새 엔트리 추가
                for price, quantity in self.entries:
                    cursor.execute('''
                        INSERT INTO entries (ticker, price, quantity, timestamp)
                        VALUES (?, ?, ?, ?)
                    ''', (self.ticker, price, quantity, datetime.now().isoformat()))
                
                conn.commit()
                
        except Exception as e:
            print(f"[ERROR] {self.ticker} 포지션 저장 실패: {e}")

    def add_position(self, price, quantity):
        """추가 매수"""
        if self.buy_count >= 3:
            return False, "최대 매수 횟수 초과"
        
        self.entries.append((price, quantity))
        self.buy_count += 1
        self.last_buy_time = datetime.now()
        self.save_position()  # 추가 매수 후 저장
        return True, "추가 매수 성공"
    
    def calculate_profit(self, current_price):
        """수익률 계산"""
        try:
            if not current_price or current_price <= 0 or not self.average_price:
                return 0.0
            return ((current_price - self.average_price) / self.average_price) * 100
        except Exception as e:
            print(f"[ERROR] 수익률 계산 중 오류: {e}")
            return 0.0
    
    @property
    def average_price(self):
        """평균 매수가 계산"""
        total_value = sum(price * qty for price, qty in self.entries)
        total_quantity = sum(qty for _, qty in self.entries)
        return total_value / total_quantity if total_quantity > 0 else 0
    
    @property
    def total_quantity(self):
        """총 보유 수량"""
        return sum(qty for _, qty in self.entries)

class PositionManager:
    def __init__(self, upbit_api):
        self.upbit = upbit_api
        self.positions = {}
        self.max_positions = 10
        self.db_path = 'positions.db'
        # 데이터베이스 초기화
        self.init_database()
        
        # 기존 포지션 로드
        self.load_positions()
        
        # closed_positions 테이블 추가 필요
        self.init_closed_positions_table()  # 새로 추가

    def init_closed_positions_table(self):
        """종료된 포지션을 저장할 테이블 생성"""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS closed_positions (
                    ticker TEXT,
                    status TEXT,
                    entry_time TIMESTAMP,
                    last_buy_time TIMESTAMP,
                    buy_count INTEGER,
                    close_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()       

    @contextmanager
    def get_db_connection(self):
        """데이터베이스 연결 컨텍스트 매니저"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def init_database(self):
        """데이터베이스 초기화"""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 포지션 테이블 생성
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS positions (
                    ticker TEXT PRIMARY KEY,
                    status TEXT,
                    entry_time TIMESTAMP,
                    last_buy_time TIMESTAMP,
                    buy_count INTEGER
                )
            ''')
            
            # 거래 내역 테이블 생성
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT,
                    price REAL,
                    quantity REAL,
                    timestamp TIMESTAMP,
                    FOREIGN KEY (ticker) REFERENCES positions (ticker)
                )
            ''')
            
            conn.commit()

    def load_positions(self):
        """데이터베이스에서 포지션 정보 로드"""
        try:
            self.positions = {}
            
            with self.get_db_connection() as conn:
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
                        # 엔트리 데이터 파싱
                        entries = []
                        if row['entries']:
                            entries_data = row['entries'].split(',')
                            entries = [(float(entries_data[i]), float(entries_data[i+1])) 
                                     for i in range(0, len(entries_data), 2)]
                        
                        # Position 객체 생성
                        position = Position(
                            row['ticker'],
                            entries[0][0] if entries else 0,
                            entries[0][1] if entries else 0
                        )
                        position.entries = entries
                        position.buy_count = row['buy_count']
                        position.status = row['status']
                        position.entry_time = datetime.fromisoformat(row['entry_time'])
                        position.last_buy_time = datetime.fromisoformat(row['last_buy_time'])
                        
                        self.positions[row['ticker']] = position
                        
                    except Exception as e:
                        print(f"[ERROR] {row['ticker']} 포지션 로드 실패: {e}")
                        continue
                        
            print(f"[INFO] 총 {len(self.positions)}개의 포지션 로드 완료")
            
        except Exception as e:
            print(f"[ERROR] 포지션 로드 실패: {e}")
            self.positions = {}

    def save_position(self, ticker, position):
        """포지션 정보를 데이터베이스에 저장"""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                # 포지션 정보 저장/업데이트
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
                
                # 기존 엔트리 삭제
                cursor.execute('DELETE FROM entries WHERE ticker = ?', (ticker,))
                
                # 새 엔트리 추가
                for price, quantity in position.entries:
                    cursor.execute('''
                        INSERT INTO entries (ticker, price, quantity, timestamp)
                        VALUES (?, ?, ?, ?)
                    ''', (ticker, price, quantity, datetime.now().isoformat()))
                
                conn.commit()
                
        except Exception as e:
            print(f"[ERROR] {ticker} 포지션 저장 실패: {e}")
            raise

    def can_open_position(self, ticker):
        """새 포지션 오픈 가능 여부 확인"""
        if ticker in self.positions:
            return False, "이미 보유 중인 코인"
        if len(self.positions) >= self.max_positions:
            return False, "최대 포지션 수 도달"
        return True, "포지션 오픈 가능"
    
    def open_position(self, ticker, price, quantity):
        """새 포지션 오픈"""
        try:
            can_open, message = self.can_open_position(ticker)
            if not can_open:
                return False, message
                
            position = Position(ticker, price, quantity)
            self.positions[ticker] = position
            self.save_position(ticker, position)
            
            print(f"[INFO] {ticker} 신규 포지션 오픈 (가격: {price:,.0f}, 수량: {quantity:.8f})")
            return True, "포지션 오픈 성공"
            
        except Exception as e:
            print(f"[ERROR] {ticker} 포지션 오픈 실패: {e}")
            return False, str(e)
    
    def add_to_position(self, ticker, price, quantity):
        """기존 포지션에 추가"""
        try:
            if ticker not in self.positions:
                return False, "보유하지 않은 코인"
                
            position = self.positions[ticker]
            if position.buy_count >= 3:
                return False, "최대 매수 횟수 초과"
                
            success, message = position.add_position(price, quantity)
            if success:
                self.save_position(ticker, position)
                print(f"[INFO] {ticker} 추가매수 완료 (가격: {price:,.0f}, 수량: {quantity:.8f}, 횟수: {position.buy_count})")
            
            return success, message
            
        except Exception as e:
            print(f"[ERROR] {ticker} 추가매수 실패: {e}")
            return False, str(e)
    
    def get_position_status(self, ticker):
        """포지션 상태 조회"""
        try:
            if ticker not in self.positions:
                return None
                
            position = self.positions[ticker]
            current_price = self.upbit.get_current_price(ticker)
            
            if not current_price:
                return None
                
            return {
                'ticker': ticker,
                'average_price': position.average_price,
                'total_quantity': position.total_quantity,
                'buy_count': position.buy_count,
                'profit': position.calculate_profit(current_price),
                'status': position.status,
                'entry_time': position.entry_time,
                'last_buy_time': position.last_buy_time,
                'current_price': current_price
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
        """포지션 종료 (데이터베이스 업데이트)"""
        try:
            if ticker not in self.positions:
                return False, "보유하지 않은 코인"
            
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                # 포지션 상태 업데이트
                cursor.execute('''
                    UPDATE positions 
                    SET status = 'closed'
                    WHERE ticker = ?
                ''', (ticker,))
                
                # 거래 이력 보관
                cursor.execute('''
                    INSERT INTO closed_positions
                    SELECT * FROM positions WHERE ticker = ?
                ''', (ticker,))
                
                conn.commit()
            
            # 메모리에서 제거
            del self.positions[ticker]
            print(f"[INFO] {ticker} 포지션 종료")
            
            return True, "포지션 종료 성공"
            
        except Exception as e:
            print(f"[ERROR] {ticker} 포지션 종료 실패: {e}")
            return False, str(e)

    def get_position_history(self, ticker=None, start_date=None, end_date=None):
        """포지션 거래 이력 조회"""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                query = '''
                    SELECT p.*, e.price, e.quantity, e.timestamp
                    FROM positions p
                    JOIN entries e ON p.ticker = e.ticker
                    WHERE 1=1
                '''
                params = []
                
                if ticker:
                    query += ' AND p.ticker = ?'
                    params.append(ticker)
                
                if start_date:
                    query += ' AND e.timestamp >= ?'
                    params.append(start_date.isoformat())
                
                if end_date:
                    query += ' AND e.timestamp <= ?'
                    params.append(end_date.isoformat())
                
                cursor.execute(query, params)
                return cursor.fetchall()
                
        except Exception as e:
            print(f"[ERROR] 거래 이력 조회 실패: {e}")
            return []

if __name__ == "__main__":
    monitor = None
    try:
        print("[INFO] 봇 초기화 중...")
        upbit = UpbitAPI()
        telegram = TelegramBot()
        analyzer = MarketAnalyzer(upbit)
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
                print(f"[ERROR] 모니터링 중 오류 발생: {e}")
                telegram.send_message(f"⚠️ 오류 발생: {str(e)}\n재시작을 시도합니다.")
                time.sleep(5)
                continue
                
    except Exception as e:
        error_message = f"프로그램 초기화 중 치명적 오류: {e}"
        print(f"[CRITICAL] {error_message}")
        if 'telegram' in locals():
            telegram.send_message(f"⚠️ {error_message}")
    
    finally:
        # 프로그램 종료 시 정리 작업
        if monitor:
            monitor.is_running = False
        if 'analyzer' in locals() and hasattr(analyzer, 'thread_pool'):
            analyzer.thread_pool.shutdown(wait=False)
        print("[INFO] 프로그램이 종료되었습니다.")