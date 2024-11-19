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
import json
import psutil
import glob
import logging
from logging.handlers import RotatingFileHandler
from collections import defaultdict
import sqlite3

# .env 파일 로드
load_dotenv()

class UpbitAPI:
    def __init__(self):
        self.access_key = os.getenv('UPBIT_ACCESS_KEY')
        self.secret_key = os.getenv('UPBIT_SECRET_KEY')
        self.upbit = pyupbit.Upbit(self.access_key, self.secret_key)
        self._balance_cache = None
        self._last_balance_update = None
        self.balance_cache_timeout = timedelta(seconds=10)
        self._jwt_token_cache = None
        self._last_jwt_update = None
        self.jwt_cache_timeout = timedelta(minutes=5)

    def create_jwt_token(self):
        """JWT 토큰 생성 최적화"""
        current_time = datetime.now()
        
        if (self._jwt_token_cache is not None and 
            self._last_jwt_update is not None and 
            current_time - self._last_jwt_update < self.jwt_cache_timeout):
            return self._jwt_token_cache
            
        payload = {
            'access_key': self.access_key,
            'nonce': str(uuid.uuid4()),
        }
        jwt_token = jwt.encode(payload, self.secret_key)
        
        self._jwt_token_cache = jwt_token
        self._last_jwt_update = current_time
        
        return jwt_token

    def get_headers(self):
        jwt_token = self.create_jwt_token()
        return {
            'Authorization': f'Bearer {jwt_token}',
            'Content-Type': 'application/json'
        }
    
    def get_balances(self):
        """계좌 잔고 조회 최적화"""
        current_time = datetime.now()
        
        if (self._balance_cache is not None and 
            self._last_balance_update is not None and 
            current_time - self._last_balance_update < self.balance_cache_timeout):
            return self._balance_cache
            
        try:
            balances = self.upbit.get_balances()
            if balances:
                self._balance_cache = balances
                self._last_balance_update = current_time
            return balances
        except Exception as e:
            print(f"잔고 조회 실패: {e}")
            return None

class TelegramBot:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.message_queue = []
        self.priority_queue = []
        self.last_message_time = datetime.now()
        self.message_cooldown = timedelta(seconds=1)
        self.queue_processor = threading.Thread(target=self._process_message_queue, daemon=True)
        self.queue_processor.start()
        self.message_lock = threading.Lock()
        self.batch_size = 5  # 한 번에 처리할 메시지 수
        
        if not self.token or not self.chat_id:
            raise ValueError("텔레그램 토큰 또는 채팅 ID가 설정되지 않았습니다.")
        print(f"[INFO] 텔레그램 봇 초기화 완료")

    def send_message(self, message, parse_mode=None, priority=False):
        """메시지 전송 최적화"""
        try:
            current_time = datetime.now()
            
            with self.message_lock:
                if priority:
                    return self._send_telegram_message(message, parse_mode)
                    
                if current_time - self.last_message_time < self.message_cooldown:
                    self.message_queue.append((message, parse_mode))
                    return True
                    
                if self.message_queue:
                    # 큐에 있는 메시지 먼저 처리
                    queued_message, queued_parse_mode = self.message_queue.pop(0)
                    success = self._send_telegram_message(queued_message, queued_parse_mode)
                    if not success:
                        self.message_queue.insert(0, (queued_message, queued_parse_mode))
                
                return self._send_telegram_message(message, parse_mode)
                
        except Exception as e:
            print(f"[ERROR] 메시지 전송 실패: {str(e)}")
            return False

    def _process_message_queue(self):
        """메시지 큐 처리 최적화"""
        while True:
            try:
                current_time = datetime.now()
                
                # 우선순위 메시지 처리
                if self.priority_queue:
                    message, parse_mode = self.priority_queue.pop(0)
                    self._send_telegram_message(message, parse_mode)
                    time.sleep(0.2)
                    continue

                # 일반 메시지 처리
                if self.message_queue and current_time - self.last_message_time >= self.message_cooldown:
                    message, parse_mode = self.message_queue.pop(0)
                    if self._send_telegram_message(message, parse_mode):
                        self.last_message_time = current_time
                    else:
                        # 실패한 메시지 재시도 큐에 추가
                        self.message_queue.insert(0, (message, parse_mode))
                
                # 큐 크기 제한
                if len(self.message_queue) > 100:
                    self.message_queue = self.message_queue[-100:]
                
                time.sleep(0.1)

            except Exception as e:
                print(f"[ERROR] 메시지 큐 처리 중 오류: {e}")
                time.sleep(1)

    def _send_telegram_message(self, message, parse_mode=None):
        """실제 텔레그램 API 호출"""
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            params = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': parse_mode or 'HTML'
            }
            
            response = requests.post(url, json=params, timeout=10)
            self.last_message_time = datetime.now()
            
            if response.status_code == 200:
                print(f"[DEBUG] 텔레그램 메시지 전송 성공")
                return True
            else:
                print(f"텔레그램 메시지 전송 실패: {response.status_code}")
                print(f"응답 내용: {response.text}")
                return False
            
        except Exception as e:
            print(f"텔레그램 메시지 전송 중 오류: {str(e)}")
            print(f"전체 오류 정보:\n{traceback.format_exc()}")
            return False

class MarketAnalyzer:
    def __init__(self, upbit_api):
        self.upbit = upbit_api
        self.tickers = pyupbit.get_tickers(fiat="KRW")
        # 분석할 시간대 설정
        self.timeframes = {
            'minute1': {'interval': 'minute1', 'count': 300}
        }
        # 기본 매매 조건 설정
        self.trading_conditions = {
            'rsi_oversold': 30,
            'rsi_overbought': 70,
            'bb_squeeze': 0.5,  # 밴드 수축 기준
            'bb_expansion': 2.0  # 밴드 확장 기준
        }
        self.market_state = 'normal'  # 시장 상태: normal, volatile, trend
        self.alert_sent = {}  # 알림 중복 방지를 위한 딕셔너리
        self.alert_cooldown = timedelta(minutes=30)  # 동일 코인 알림 제한 시간
        self._cache = {}
        self.cache_timeout = timedelta(minutes=1)
        self._ohlcv_cache = {}
        self._indicator_cache = {}
        self.cache_timeout = timedelta(minutes=1)
        self._market_state_cache = None
        self._last_market_state_update = None
        self.market_state_timeout = timedelta(minutes=5)
        
    def analyze_market_state(self, df):
        """시장 상태 분석 최적화"""
        current_time = datetime.now()
        
        # 캐시된 시장 상태 확인
        if (self._market_state_cache is not None and 
            self._last_market_state_update is not None and 
            current_time - self._last_market_state_update < self.market_state_timeout):
            return self._market_state_cache

        try:
            current = df.iloc[-1]
            
            # 변동성 체크
            volatility = (current['고가'] - current['저가']) / current['시가'] * 100
            avg_volatility = df['종가'].pct_change().std() * 100
            
            # 추세 체크
            price_trend = df['종가'].iloc[-5:].pct_change().mean() * 100
            
            # 밴드폭 추세
            bb_trend = df['밴드폭'].iloc[-5:].mean()
            
            # 시장 상태 판단
            if volatility > 3 or avg_volatility > 2:
                self.market_state = 'volatile'
            elif abs(price_trend) > 2:
                self.market_state = 'trend'
            else:
                self.market_state = 'normal'
                
            market_status = {
                'volatility': volatility,
                'avg_volatility': avg_volatility,
                'price_trend': price_trend,
                'bb_trend': bb_trend,
                'state': self.market_state
            }
            
            # 캐시 업데이트
            self._market_state_cache = market_status
            self._last_market_state_update = current_time
            
            return market_status
            
        except Exception as e:
            print(f"시장 상태 분석 중 오류: {e}")
            return None

    def update_trading_conditions(self, market_status):
        """매매 조건 업데이트 최적화"""
        old_state = self.market_state
        old_conditions = self.trading_conditions.copy()
        
        try:
            # 시장 상태에 따른 조건 업데이트
            conditions_map = {
                'volatile': {
                    'rsi_oversold': 25,
                    'rsi_overbought': 75,
                    'bb_squeeze': 0.3,
                    'bb_expansion': 2.5
                },
                'trend': {
                    'rsi_oversold': 35,
                    'rsi_overbought': 65,
                    'bb_squeeze': 0.7,
                    'bb_expansion': 1.8
                },
                'normal': {
                    'rsi_oversold': 30,
                    'rsi_overbought': 70,
                    'bb_squeeze': 0.5,
                    'bb_expansion': 2.0
                }
            }
            
            # 조건 업데이트
            self.trading_conditions.update(conditions_map.get(self.market_state, conditions_map['normal']))
            
            # 변경사항이 있을 때만 메시지 생성
            if old_state != self.market_state or old_conditions != self.trading_conditions:
                return self._format_condition_update_message(old_state, market_status)
            
            return None
            
        except Exception as e:
            print(f"[ERROR] 매매 조건 업데이트 중 오류: {str(e)}")
            return None

    def _format_condition_update_message(self, old_state, market_status):
        """매매 조건 업데이트 메시지 포맷팅"""
        try:
            message = [
                "🔄 매매 조건 업데이트",
                "",
                f"시장 상태: {old_state} → {self.market_state}",
                f"변동성: {market_status['volatility']:.2f}%",
                f"가격 추세: {market_status['price_trend']:.2f}%",
                f"밴드폭 추세: {market_status['bb_trend']:.2f}",
                "",
                "📊 매매 조건:",
                f"RSI 과매도: {self.trading_conditions['rsi_oversold']}",
                f"RSI 과매수: {self.trading_conditions['rsi_overbought']}",
                f"밴드 수축: {self.trading_conditions['bb_squeeze']}",
                f"밴드 확장: {self.trading_conditions['bb_expansion']}"
            ]
            
            return "\n".join(message)
            
        except Exception as e:
            print(f"[ERROR] 조건 업데이트 메시지 포맷팅 중 오류: {str(e)}")
            return None

    def get_ohlcv(self, ticker, interval="minute1", count=300):
        """OHLCV 데이터 캐시 활용"""
        cache_key = f"{ticker}_{interval}_{count}"
        current_time = datetime.now()
        
        if cache_key in self._ohlcv_cache:
            data, cache_time = self._ohlcv_cache[cache_key]
            if current_time - cache_time < self.cache_timeout:
                return data
        
        try:
            df = pyupbit.get_ohlcv(ticker, interval=interval, count=count)
            if df is not None:
                df.columns = ['시가', '고가', '저가', '종가', '거래량', '거래금액']
                self._ohlcv_cache[cache_key] = (df, current_time)
                return df
        except Exception as e:
            print(f"{ticker} OHLCV 데이터 조회 실패: {e}")
        return None

    def calculate_indicators(self, df, ticker):
        """지표 계산 최적화"""
        cache_key = f"{ticker}_{df.index[-1]}"
        
        if cache_key in self._indicator_cache:
            return self._indicator_cache[cache_key]
            
        try:
            # RSI 계산
            df = self.calculate_rsi(df)
            
            # 볼린저 밴드 계산
            df = self.calculate_bollinger_bands(df)
            
            # 거래량 분석
            df = self.analyze_volume(df)
            
            # 캐시 저장
            self._indicator_cache[cache_key] = df
            
            # 오래된 캐시 정리
            self._cleanup_indicator_cache()
            
            return df
            
        except Exception as e:
            print(f"지표 계산 중 오류: {e}")
            return None

    def _cleanup_indicator_cache(self):
        """오래된 지표 캐시 정리"""
        current_time = datetime.now()
        expired_keys = [k for k, (_, t) in self._indicator_cache.items() 
                       if current_time - t >= self.cache_timeout]
        for k in expired_keys:
            del self._indicator_cache[k]

    def calculate_rsi(self, df, period=14):
        """RSI 계산 최적화"""
        try:
            cache_key = f"RSI_{df.index[-1]}_{period}"
            
            if cache_key in self._indicator_cache:
                return self._indicator_cache[cache_key]
            
            # 변화량 계산
            df['변화량'] = df['종가'].diff()
            
            # 상승폭과 하락폭 계산
            df['상승폭'] = df['변화량'].apply(lambda x: x if x > 0 else 0)
            df['하락폭'] = df['변화량'].apply(lambda x: -x if x < 0 else 0)
            
            # 지수이동평균 계산
            df['AU'] = df['상승폭'].ewm(alpha=1/period, min_periods=period).mean()
            df['AD'] = df['하락폭'].ewm(alpha=1/period, min_periods=period).mean()
            
            # RSI 계산
            df['RSI'] = df['AU'] / (df['AU'] + df['AD']) * 100
            
            # 캐시 저장
            self._indicator_cache[cache_key] = df
            
            return df
            
        except Exception as e:
            print(f"RSI 계산 중 오류: {e}")
            return None

    def calculate_bollinger_bands(self, df, n=20, k=2):
        """볼린저 밴드 계산 최적화"""
        try:
            cache_key = f"BB_{df.index[-1]}_{n}_{k}"
            
            if cache_key in self._indicator_cache:
                return self._indicator_cache[cache_key]
                
            if len(df) < n:
                return None

            # 컬럼명 통일
            df['종가'] = df['close'] if 'close' in df.columns else df['종가']
            
            # 중심선 계산 (이동평균)
            df['중심선'] = df['종가'].rolling(window=n).mean()
            
            # 표준편차 계산
            df['표준편차'] = df['종가'].rolling(window=n).std()
            
            # 밴드 계산
            df['상단밴드'] = df['중심선'] + (df['표준편차'] * k)
            df['하단밴드'] = df['중심선'] - (df['표준편차'] * k)
            df['밴드폭'] = (df['상단밴드'] - df['하단밴드']) / df['중심선'] * 100
            
            # 캐시 저장
            self._indicator_cache[cache_key] = df
            
            return df
            
        except Exception as e:
            print(f"볼린저 밴드 계산 중 오류: {e}")
            return None

    def analyze_volume(self, df):
        """거래량 분석 최적화"""
        try:
            cache_key = f"VOL_{df.index[-1]}"
            
            if cache_key in self._indicator_cache:
                return self._indicator_cache[cache_key]
            
            # 거래량 이동평균 계산
            df['거래량MA5'] = df['거래량'].rolling(window=5).mean()
            df['거래량MA20'] = df['거래량'].rolling(window=20).mean()
            
            # 거래량 증가율 계산
            df['거래량증가율'] = (df['거래량'] / df['거래량MA5'] - 1) * 100
            
            # 거래량 급증 여부 체크
            df['거래량급증'] = df['거래량'] > df['거래량MA5'] * 2
            
            # 캐시 저장
            self._indicator_cache[cache_key] = df
            
            return df
            
        except Exception as e:
            print(f"거래량 분석 중 오류: {e}")
            return None

    def analyze_market(self, ticker):
        """캐시를 활용한 시장 분석"""
        try:
            current_time = datetime.now()
            cache_key = f"{ticker}_{current_time.strftime('%Y%m%d%H%M')}"
            
            # 캐시 확인
            if cache_key in self._cache:
                cached_data, cache_time = self._cache[cache_key]
                if current_time - cache_time < self.cache_timeout:
                    return cached_data
            
            # 새로운 분석 수행
            analysis = self._perform_market_analysis(ticker)
            if analysis:
                self._cache[cache_key] = (analysis, current_time)
                
                # 오래된 캐시 정리
                self._cleanup_cache()
                
            return analysis
            
        except Exception as e:
            print(f"[ERROR] {ticker} 분석 중 오류: {e}")
            return None

    def _perform_market_analysis(self, ticker):
        """실제 분석 수행"""
        try:
            analysis_results = {}
            
            for timeframe, config in self.timeframes.items():
                try:
                    df = self.get_ohlcv(ticker, interval=config['interval'], count=config['count'])
                    if df is None or df.empty:
                        continue

                    # 지표 계산
                    df = self.calculate_indicators(df, ticker)
                    
                    if df is None or df.empty or df.iloc[-1].isnull().any():
                        continue

                    current = df.iloc[-1]
                    
                    percent_b = (current['종가'] - current['하단밴드']) / (current['상단밴드'] - current['하단밴드'])
                    print(f"[DEBUG] {ticker} {timeframe} 분석 완료: RSI={current.get('RSI', 0):.2f}, %B={percent_b:.2f}")

                    analysis_results[timeframe] = {
                        'rsi': current.get('RSI', 0),
                        'bb_bandwidth': current.get('밴드폭', 0),
                        'percent_b': percent_b,
                        'volume_increase': current.get('거래량증가율', 0)
                    }

                except Exception as e:
                    print(f"[DEBUG] {ticker} {timeframe} 분석 중 오류 발생: {str(e)}")
                    continue

            if not analysis_results:
                return None

            # 현재가 조회 재시도 로직 추가
            max_retries = 3
            current_price = None
            
            for i in range(max_retries):
                try:
                    current_price = pyupbit.get_current_price(ticker)
                    if current_price and current_price > 0:
                        break
                    time.sleep(0.2)
                except Exception as e:
                    print(f"[DEBUG] {ticker} 현재가 조회 재시도 {i+1}/{max_retries}")
                    if i == max_retries - 1:
                        print(f"[ERROR] {ticker} 현재가 조회 최종 실패")
                        return None
                    time.sleep(0.2)
            
            if not current_price:
                print(f"[ERROR] {ticker} 유효하지 않은 현재가")
                return None
            
            return {
                'ticker': ticker,
                'current_price': current_price,
                'timeframes': analysis_results,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            print(f"[ERROR] {ticker} 분석 중 오류: {e}")
            return None

    def get_trading_signals(self, analysis):
        """매매 신호 생성 최적화"""
        signals = []
        
        timeframe = 'minute1'
        if timeframe not in analysis['timeframes']:
            return signals
        
        data = analysis['timeframes'][timeframe]
        
        try:
            # RSI + 볼린저밴드 복합 신호
            if data['rsi'] >= self.trading_conditions['rsi_overbought']:
                if data['percent_b'] >= 0.9:  # 상단밴드 근접
                    print(f"[DEBUG] {analysis['ticker']} 매도 신호 감지: RSI={data['rsi']:.2f}, %B={data['percent_b']:.2f}")
                    signals.append(('매도', f'RSI 과매수({data["rsi"]:.1f}) + 상단밴드 근접(%B:{data["percent_b"]:.2f})', analysis['ticker']))
            
            elif data['rsi'] <= self.trading_conditions['rsi_oversold']:
                if data['percent_b'] <= 0.1:  # 하단밴드 근접
                    print(f"[DEBUG] {analysis['ticker']} 매수 신호 감지: RSI={data['rsi']:.2f}, %B={data['percent_b']:.2f}")
                    signals.append(('매수', f'RSI 과매도({data["rsi"]:.1f}) + 하단밴드 근접(%B:{data["percent_b"]:.2f})', analysis['ticker']))
                
            if signals:
                print(f"[DEBUG] {analysis['ticker']} 매매 신호 생성됨: {signals}")
            
            return signals
            
        except Exception as e:
            print(f"[ERROR] 매매 신호 생성 중 오류: {str(e)}")
            return []

    def format_analysis_message(self, analysis):
        """분석 결과 메시지 포맷팅 최적화"""
        try:
            message = [
                f"🔍 {analysis['ticker']} 분석 결과",
                f"💰 현재가: {analysis['current_price']:,.0f}원",
                f"📊 RSI: {analysis['timeframes']['minute1']['rsi']:.2f}",
                "",
                "📈 볼린저 밴드",
                f"상단: {analysis['bb_upper']:,.0f}원",
                f"중심: {analysis['bb_middle']:,.0f}원",
                f"하단: {analysis['bb_lower']:,.0f}원",
                f"밴드폭: {analysis['timeframes']['minute1']['bb_bandwidth']:.2f}%",
                f"%B: {analysis['timeframes']['minute1']['percent_b']:.2f}",
                "",
                "📊 거래량",
                f"증가율: {analysis['timeframes']['minute1'].get('volume_increase', 0):.2f}%"
            ]
            
            return "\n".join(message)
            
        except Exception as e:
            print(f"[ERROR] 분석 메시지 포맷팅 중 오류: {str(e)}")
            return f"⚠️ {analysis['ticker']} 분석 결과 포맷팅 실패"

    def check_trading_alerts(self, analysis):
        """매매 조건 접근 알림 체크 최적화"""
        ticker = analysis['ticker']
        current_time = datetime.now()
        
        # 알림 쿨다운 체크
        if ticker in self.alert_sent:
            if current_time - self.alert_sent[ticker] < self.alert_cooldown:
                return None

        alerts = []
        alert_conditions = {
            'RSI': {
                'oversold': (32, 35, '과매도'),
                'overbought': (65, 68, '과매수')
            },
            'BB': {
                'lower': (0.05, 0.1, '하단'),
                'upper': (0.9, 0.95, '상단')
            }
        }
        
        try:
            # 여러 시간대의 지표 확인
            for timeframe, data in analysis['timeframes'].items():
                # RSI 알림 체크
                rsi_value = data.get('rsi', 0)
                for condition, (low, high, type_str) in alert_conditions['RSI'].items():
                    if low <= rsi_value <= high:
                        alerts.append(f"{timeframe} RSI {type_str} 구간 접근 중 ({rsi_value:.2f})")
                
                # 볼린저 밴드 알림 체크
                bb_value = data.get('percent_b', 0)
                for condition, (low, high, type_str) in alert_conditions['BB'].items():
                    if low <= bb_value <= high:
                        alerts.append(f"{timeframe} {type_str} 밴드 접근 중")
            
            if alerts:
                self.alert_sent[ticker] = current_time
                message = [
                    f"⚠️ {ticker} 매매 시그널 접근 알림",
                    "",
                    f"현재가: {format(int(analysis['current_price']), ',')}원",
                    "감지된 신호:"
                ]
                message.extend([f"- {alert}" for alert in alerts])
                return "\n".join(message)
            
            return None
            
        except Exception as e:
            print(f"[ERROR] 매매 알림 체크 중 오류: {str(e)}")
            return None

    def get_top_volume_tickers(self, limit=40):
        """거래량 상위 코인 목록 조회 최적화"""
        try:
            cache_key = f"top_volume_{datetime.now().strftime('%Y%m%d%H')}"
            
            # 캐시 확인
            if cache_key in self._cache:
                return self._cache[cache_key]

            all_tickers = pyupbit.get_tickers(fiat="KRW")
            volume_data = []
            
            # 병렬 처리를 위한 함수
            def get_volume_data(ticker):
                try:
                    df = pyupbit.get_ohlcv(ticker, interval="day", count=1)
                    if df is not None and not df.empty:
                        trade_price = df['volume'].iloc[-1] * df['close'].iloc[-1]
                        return (ticker, trade_price)
                except Exception:
                    return None

            # ThreadPoolExecutor 사용
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(get_volume_data, ticker) for ticker in all_tickers]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        volume_data.append(result)
            
            # 거래대금 기준 정렬
            volume_data.sort(key=lambda x: x[1], reverse=True)
            top_tickers = [ticker for ticker, _ in volume_data[:limit]]
            
            # 캐시 저장
            self._cache[cache_key] = top_tickers
            
            return top_tickers
            
        except Exception as e:
            print(f"[ERROR] 거래량 상위 코인 조회 실패: {e}")
            return self.tickers if hasattr(self, 'tickers') else all_tickers[:limit]

    def _cleanup_cache(self):
        """오래된 캐시 정리"""
        current_time = datetime.now()
        expired_keys = [k for k, (_, t) in self._cache.items() 
                       if current_time - t >= self.cache_timeout]
        for k in expired_keys:
            del self._cache[k]

    def _cleanup_caches(self):
        """모든 캐시 정리 최적화"""
        try:
            current_time = datetime.now()
            
            # 캐시 타임아웃 설정
            timeouts = {
                'analysis': self.cache_timeout,
                'ohlcv': timedelta(minutes=1),
                'indicator': timedelta(minutes=5),
                'market_state': timedelta(minutes=5),
                'volume': timedelta(hours=1)
            }
            
            # 각 캐시 정리
            caches = {
                'analysis': self._cache,
                'ohlcv': self._ohlcv_cache,
                'indicator': self._indicator_cache
            }
            
            for cache_type, cache_dict in caches.items():
                timeout = timeouts[cache_type]
                expired_keys = [
                    k for k, (_, t) in cache_dict.items() 
                    if current_time - t >= timeout
                ]
                for k in expired_keys:
                    del cache_dict[k]
                    
            # 메모리 사용량 로깅
            process = psutil.Process()
            memory_usage = process.memory_info().rss / 1024 / 1024  # MB
            print(f"[INFO] 메모리 사용량: {memory_usage:.2f}MB")
            
        except Exception as e:
            print(f"[ERROR] 캐시 정리 중 오류: {str(e)}")

    def analyze_market_trend(self, ticker):
        """시장 추세 분석 최적화"""
        try:
            cache_key = f"trend_{ticker}_{datetime.now().strftime('%Y%m%d%H%M')}"
            
            if cache_key in self._cache:
                return self._cache[cache_key]

            # 다중 시간대 데이터 수집
            timeframes = {
                'minute1': 60,
                'minute3': 60,
                'minute5': 60,
                'minute15': 48,
                'minute30': 48,
                'minute60': 24,
                'day': 10
            }
            
            trend_data = {}
            
            for timeframe, count in timeframes.items():
                df = self.get_ohlcv(ticker, interval=timeframe, count=count)
                if df is None or df.empty:
                    continue
                    
                trend_data[timeframe] = {
                    'price_trend': self._calculate_price_trend(df),
                    'volume_trend': self._calculate_volume_trend(df),
                    'volatility': self._calculate_volatility(df),
                    'momentum': self._calculate_momentum(df)
                }

            # 종합 분석
            analysis = self._analyze_trends(trend_data)
            
            # 캐시 저장
            self._cache[cache_key] = analysis
            
            return analysis

        except Exception as e:
            print(f"[ERROR] 시장 추세 분석 실패: {e}")
            return None

    def _calculate_price_trend(self, df):
        """가격 추세 계산"""
        try:
            # 단기/중기/장기 이동평균
            df['MA5'] = df['종가'].rolling(window=5).mean()
            df['MA20'] = df['종가'].rolling(window=20).mean()
            df['MA60'] = df['종가'].rolling(window=60).mean()
            
            current = df.iloc[-1]
            
            trend = {
                'short_term': (current['MA5'] / current['MA20'] - 1) * 100,
                'mid_term': (current['MA20'] / current['MA60'] - 1) * 100,
                'price_momentum': df['종가'].pct_change(5).iloc[-1] * 100
            }
            
            return trend
            
        except Exception as e:
            print(f"[ERROR] 가격 추세 계산 실패: {e}")
            return None

    def _calculate_volume_trend(self, df):
        """거래량 추세 계산"""
        try:
            # 거래량 이동평균
            df['VMA5'] = df['거래량'].rolling(window=5).mean()
            df['VMA20'] = df['거래량'].rolling(window=20).mean()
            
            current = df.iloc[-1]
            
            trend = {
                'volume_change': (current['거래량'] / df['거래량'].mean() - 1) * 100,
                'volume_trend': (current['VMA5'] / current['VMA20'] - 1) * 100
            }
            
            return trend
            
        except Exception as e:
            print(f"[ERROR] 거래량 추세 계산 실패: {e}")
            return None

    def _calculate_momentum(self, df):
        """모멘텀 지표 계산"""
        try:
            # RSI
            df['RSI'] = self.calculate_rsi(df)['RSI']
            
            # MACD
            exp1 = df['종가'].ewm(span=12, adjust=False).mean()
            exp2 = df['종가'].ewm(span=26, adjust=False).mean()
            df['MACD'] = exp1 - exp2
            df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
            
            current = df.iloc[-1]
            
            momentum = {
                'rsi': current['RSI'],
                'macd': current['MACD'],
                'macd_signal': current['Signal'],
                'macd_hist': current['MACD'] - current['Signal']
            }
            
            return momentum
            
        except Exception as e:
            print(f"[ERROR] 모멘텀 계산 실패: {e}")
            return None

    def _analyze_trends(self, trend_data):
        """추세 종합 분석"""
        try:
            analysis = {
                'overall_trend': 'neutral',
                'strength': 0,
                'signals': []
            }
            
            # 가중치 설정
            weights = {
                'minute1': 0.05,
                'minute3': 0.10,
                'minute5': 0.15,
                'minute15': 0.20,
                'minute30': 0.20,
                'minute60': 0.20,
                'day': 0.10
            }
            
            trend_score = 0
            
            for timeframe, data in trend_data.items():
                weight = weights.get(timeframe, 0.1)
                
                if data['price_trend']['short_term'] > 0:
                    trend_score += weight
                elif data['price_trend']['short_term'] < 0:
                    trend_score -= weight
                    
                # 거래량 확인
                if data['volume_trend']['volume_change'] > 50:
                    analysis['signals'].append(f"{timeframe} 거래량 급증")
                    
                # 모멘텀 확인
                if data['momentum']['rsi'] > 70:
                    analysis['signals'].append(f"{timeframe} RSI 과매수")
                elif data['momentum']['rsi'] < 30:
                    analysis['signals'].append(f"{timeframe} RSI 과매도")
            
            # 종합 추세 판단
            if trend_score > 0.3:
                analysis['overall_trend'] = 'bullish'
            elif trend_score < -0.3:
                analysis['overall_trend'] = 'bearish'
                
            analysis['strength'] = abs(trend_score)
            
            return analysis
            
        except Exception as e:
            print(f"[ERROR] 추세 종합 분석 실패: {e}")
            return None

class SystemMonitor:
    def __init__(self):
        self.start_time = datetime.now()
        self.last_check = datetime.now()
        self.check_interval = timedelta(minutes=30)
        self._status_cache = {}
        self.status_cache_timeout = timedelta(minutes=1)

    def check_system_status(self):
        """시스템 상태 체크 최적화"""
        try:
            current_time = datetime.now()
            
            # 캐시된 상태 확인
            if self._status_cache.get('last_check'):
                if current_time - self._status_cache['last_check'] < self.status_cache_timeout:
                    return self._status_cache['status']

            # CPU 사용량
            cpu_usage = psutil.cpu_percent(interval=1)
            
            # 메모리 사용량
            memory = psutil.virtual_memory()
            memory_usage = memory.percent
            
            # 디스크 사용량
            disk = psutil.disk_usage('/')
            disk_usage = disk.percent
            
            # 실행 시간
            uptime = current_time - self.start_time
            
            status = {
                'cpu_usage': cpu_usage,
                'memory_usage': memory_usage,
                'disk_usage': disk_usage,
                'uptime': uptime,
                'timestamp': current_time
            }
            
            # 캐시 업데이트
            self._status_cache = {
                'status': status,
                'last_check': current_time
            }
            
            return status
            
        except Exception as e:
            print(f"[ERROR] 시스템 상태 체크 실패: {e}")
            return None

    def format_status_message(self, status):
        """시스템 상태 메시지 포맷팅"""
        try:
            if not status:
                return "⚠️ 시스템 상태 정보 없음"
                
            uptime_str = self._format_uptime(status['uptime'])
            
            message = [
                "🖥️ 시스템 상태 보고",
                "",
                f"CPU 사용량: {status['cpu_usage']}%",
                f"메모리 사용량: {status['memory_usage']}%",
                f"디스크 사용량: {status['disk_usage']}%",
                f"실행 시간: {uptime_str}",
                "",
                f"마지막 체크: {status['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}"
            ]
            
            return "\n".join(message)
            
        except Exception as e:
            print(f"[ERROR] 상태 메시지 포맷팅 실패: {e}")
            return "⚠️ 상태 메시지 생성 실패"

    def _format_uptime(self, uptime):
        """실행 시간 포맷팅"""
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        parts = []
        if days > 0:
            parts.append(f"{days}일")
        if hours > 0:
            parts.append(f"{hours}시간")
        if minutes > 0:
            parts.append(f"{minutes}분")
        parts.append(f"{seconds}초")
        
        return " ".join(parts)

class PerformanceMonitor:
    def __init__(self):
        self.log_path = 'logs/performance/'
        self.metrics = {
            'api_calls': defaultdict(int),
            'response_times': defaultdict(list),
            'errors': defaultdict(int),
            'memory_usage': [],
            'cpu_usage': []
        }
        self.start_time = datetime.now()
        self._setup_logging()
        
    def _setup_logging(self):
        """로깅 설정 초기화"""
        try:
            os.makedirs(self.log_path, exist_ok=True)
            
            # 성능 로그 설정
            perf_logger = logging.getLogger('performance')
            perf_logger.setLevel(logging.INFO)
            
            # 파일 핸들러 설정
            log_file = os.path.join(self.log_path, f'performance_{datetime.now().strftime("%Y%m%d")}.log')
            handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            perf_logger.addHandler(handler)
            
            self.logger = perf_logger
            
        except Exception as e:
            print(f"[ERROR] 로깅 설정 실패: {e}")

    def record_api_call(self, endpoint, response_time, success=True):
        """API 호출 기록"""
        try:
            self.metrics['api_calls'][endpoint] += 1
            self.metrics['response_times'][endpoint].append(response_time)
            
            if not success:
                self.metrics['errors'][endpoint] += 1
                
            # 로그 기록
            self.logger.info(f"API Call - Endpoint: {endpoint}, Time: {response_time:.3f}s, Success: {success}")
            
        except Exception as e:
            print(f"[ERROR] API 호출 기록 실패: {e}")

    def record_system_metrics(self):
        """시스템 메트릭 기록"""
        try:
            process = psutil.Process()
            
            # CPU 사용량
            cpu_percent = process.cpu_percent()
            self.metrics['cpu_usage'].append(cpu_percent)
            
            # 메모리 사용량
            memory_info = process.memory_info()
            memory_usage_mb = memory_info.rss / 1024 / 1024
            self.metrics['memory_usage'].append(memory_usage_mb)
            
            # 로그 기록
            self.logger.info(f"System Metrics - CPU: {cpu_percent}%, Memory: {memory_usage_mb:.2f}MB")
            
            # 메트릭 정리 (최근 100개만 유지)
            if len(self.metrics['cpu_usage']) > 100:
                self.metrics['cpu_usage'] = self.metrics['cpu_usage'][-100:]
            if len(self.metrics['memory_usage']) > 100:
                self.metrics['memory_usage'] = self.metrics['memory_usage'][-100:]
                
        except Exception as e:
            print(f"[ERROR] 시스템 메트릭 기록 실패: {e}")

    def generate_performance_report(self):
        """성능 보고서 생성"""
        try:
            report = {
                'timestamp': datetime.now(),
                'uptime': datetime.now() - self.start_time,
                'api_stats': {},
                'system_stats': {},
                'error_stats': {}
            }
            
            # API 통계
            for endpoint, calls in self.metrics['api_calls'].items():
                response_times = self.metrics['response_times'][endpoint]
                if response_times:
                    avg_time = sum(response_times) / len(response_times)
                    max_time = max(response_times)
                    error_rate = (self.metrics['errors'][endpoint] / calls) * 100 if calls > 0 else 0
                    
                    report['api_stats'][endpoint] = {
                        'total_calls': calls,
                        'avg_response_time': avg_time,
                        'max_response_time': max_time,
                        'error_rate': error_rate
                    }
            
            # 시스템 통계
            if self.metrics['cpu_usage']:
                report['system_stats']['cpu'] = {
                    'current': self.metrics['cpu_usage'][-1],
                    'average': sum(self.metrics['cpu_usage']) / len(self.metrics['cpu_usage']),
                    'max': max(self.metrics['cpu_usage'])
                }
                
            if self.metrics['memory_usage']:
                report['system_stats']['memory'] = {
                    'current_mb': self.metrics['memory_usage'][-1],
                    'average_mb': sum(self.metrics['memory_usage']) / len(self.metrics['memory_usage']),
                    'max_mb': max(self.metrics['memory_usage'])
                }
            
            # 에러 통계
            report['error_stats'] = dict(self.metrics['errors'])
            
            return report
            
        except Exception as e:
            print(f"[ERROR] 성능 보고서 생성 실패: {e}")
            return None

    def format_report_message(self, report):
        """성능 보고서 메시지 포맷팅"""
        try:
            if not report:
                return "⚠️ 성능 보고서 생성 실패"
                
            message = [
                "📊 성능 모니터링 보고서",
                f"📅 생성 시간: {report['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}",
                f"⏱️ 가동 시간: {report['uptime']}",
                "",
                "🔄 API 통계:"
            ]
            
            for endpoint, stats in report['api_stats'].items():
                message.extend([
                    f"  • {endpoint}:",
                    f"    - 총 호출: {stats['total_calls']}회",
                    f"    - 평균 응답시간: {stats['avg_response_time']:.3f}초",
                    f"    - 에러율: {stats['error_rate']:.2f}%"
                ])
            
            message.extend([
                "",
                "💻 시스템 상태:",
                f"  • CPU 사용률: {report['system_stats']['cpu']['current']}%",
                f"  • 메모리 사용량: {report['system_stats']['memory']['current_mb']:.1f}MB"
            ])
            
            return "\n".join(message)
            
        except Exception as e:
            print(f"[ERROR] 보고서 메시지 포맷팅 실패: {e}")
            return "⚠️ 보고서 포맷팅 실패"

class MarketMonitor:
    def __init__(self, upbit_api, telegram_bot, market_analyzer):
        self.upbit = upbit_api
        self.telegram = telegram_bot
        self.analyzer = market_analyzer
        self.position_manager = PositionManager(upbit_api)
        self.command_handlers = {
            '/start': self.handle_start,
            '/stop': self.handle_stop,
            '/status': self.handle_status,
            '/profit': self.handle_profit,
            '/market': self.handle_market,
            '/coins': self.handle_coins,
            '/sell_all': self.handle_sell_all,
            '/help': self.handle_help
        }
        self.command_lock = threading.Lock()
        self.is_running = False
        # 시작 시 기존 포지션 불러오기
        self.load_existing_positions()
        self.last_processed_update_id = 0  # 마지막으로 처리한 업데이트 ID 저장
        self.last_status_update = datetime.now()
        self.status_update_interval = timedelta(minutes=5)  # 상태 업데이트 주기
        self.error_count = 0  # 에러 카운터
        self.error_logs = []  # 에러 로그 저장
        self.last_error_notification = datetime.now()
        self.error_notification_cooldown = timedelta(minutes=5)  # 에러 알림 주기
        self.last_tickers_update = None  # 마지막 티커 업데이트 시간 추가
        self.last_analysis_time = {}  # 코인별 마지막 분석 시간
        self.analysis_interval = timedelta(seconds=30)  # 분석 주기
        self.error_cooldown = timedelta(minutes=5)  # 에러 알림 주기
        self.last_error_time = {}  # 코인별 마지막 에러 시간
        self._order_cache = {}
        self.order_cache_timeout = timedelta(minutes=1)
        self._market_state_cache = None
        self._last_market_state_update = None
        self.market_state_timeout = timedelta(minutes=5)

    def _process_buy_order(self, ticker):
        """매수 주문 처리 최적화"""
        try:
            # 현재가 조회 재시도 로직
            current_price = self._get_current_price_with_retry(ticker)
            if not current_price:
                return False, "현재가 조회 실패"

            # 주문 가능 금액 계산
            available_krw = self.upbit.get_balance("KRW")
            if available_krw < 5000:  # 최소 주문 금액
                return False, "주문 가능 금액 부족"

            # 시장 상태에 따른 주문 금액 조정
            market_state = self.analyzer.get_market_state(ticker)
            order_amount = self.calculate_order_amount(market_state)
            
            # 주문 금액 제한
            order_amount = min(order_amount, available_krw * 0.9)  # 여유자금 10% 확보
            
            # 주문 실행
            order = self.upbit.buy_market_order(ticker, order_amount)
            if not order:
                return False, "주문 실행 실패"

            # 주문 체결 확인
            time.sleep(0.5)  # 체결 대기
            order_info = self.upbit.get_order(order['uuid'])
            if order_info['state'] != 'done':
                return False, "주문 미체결"

            return True, "매수 주문 성공"

        except Exception as e:
            return False, f"매수 주문 처리 중 오류: {str(e)}"

    def _process_sell_order(self, ticker):
        """매도 주문 처리 최적화"""
        try:
            # 보유 수량 확인
            balance = self.upbit.get_balance(ticker.split('-')[1])
            if not balance:
                return False, "보유 수량 없음"

            # 최소 주문 금액 확인
            current_price = self._get_current_price_with_retry(ticker)
            if not current_price:
                return False, "현재가 조회 실패"

            order_value = current_price * balance
            if order_value < 5000:
                return False, "최소 주문 금액 미달"

            # 주문 실행
            order = self.upbit.sell_market_order(ticker, balance)
            if not order:
                return False, "주문 실행 실패"

            # 주문 체결 확인
            time.sleep(0.5)  # 체결 대기
            order_info = self.upbit.get_order(order['uuid'])
            if order_info['state'] != 'done':
                return False, "주문 미체결"

            return True, "매도 주문 성공"

        except Exception as e:
            return False, f"매도 주문 처리 중 오류: {str(e)}"

    def _get_current_price_with_retry(self, ticker, max_retries=3):
        """현재가 조회 재시도 로직"""
        for i in range(max_retries):
            try:
                price = pyupbit.get_current_price(ticker)
                if price and price > 0:
                    return price
                time.sleep(0.2)
            except Exception as e:
                print(f"[DEBUG] {ticker} 현재가 조회 재시도 {i+1}/{max_retries}")
                if i == max_retries - 1:
                    print(f"[ERROR] {ticker} 현재가 조회 최종 실패: {e}")
                    return None
                time.sleep(0.2)
        return None
    
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

    def handle_command(self, command, chat_id):
        """스레드 안전한 명령어 처리"""
        with self.command_lock:
            try:
                if command in self.command_handlers:
                    handler = self.command_handlers[command]
                    threading.Thread(target=self._execute_handler, 
                                  args=(handler, command, chat_id)).start()
                    return True
                return False
            except Exception as e:
                self.log_error(f"명령어 처리 오류: {command}", e)
                return False
                
    def _execute_handler(self, handler, command, chat_id):
        """명령어 핸들러 실행"""
        try:
            print(f"[INFO] 명령어 실행: {command}")
            handler()
        except Exception as e:
            error_msg = f"명령어 실행 실패: {command}\n오류: {str(e)}"
            self.telegram.send_message(error_msg, priority=True)

    def check_telegram_commands(self):
        """텔레그램 명령어 확인"""
        try:
            url = f"https://api.telegram.org/bot{self.telegram.token}/getUpdates"
            params = {
                'offset': self.last_processed_update_id + 1,
                'timeout': 1  # timeout 값을 1초로 줄임
            }
            
            response = requests.get(url, params=params, timeout=3)  # timeout 3초로 설정
            if response.status_code == 200:
                updates = response.json()
                if 'result' in updates and updates['result']:
                    for update in updates['result']:
                        self.last_processed_update_id = update['update_id']
                        
                        if 'message' in update and 'text' in update['message']:
                            command = update['message']['text']
                            if command.startswith('/'):
                                # 명령어 처리를 별도 스레드로 실행
                                threading.Thread(target=self.handle_command, args=(command, self.telegram.chat_id)).start()
                                
        except requests.exceptions.RequestException as e:
            print(f"텔레그램 API 연결 오류: {e}")
            time.sleep(1)
        except Exception as e:
            print(f"텔레그램 명령어 확인 중 오류: {e}")
            time.sleep(1)

    def calculate_split_orders(self, market_state):
        """시장 태에 따른 분할 매수/매도 금액 계산"""
        base_amount = 5500  # 최소 주문금액(5000원) + 수수료 여유분
        
        if market_state == 'volatile':
            # 변동성 장: 첫 주문 작게, 나중 주문 크게
            return [base_amount, base_amount * 1.2, base_amount * 1.4]
        elif market_state == 'trend':
            # 추세장: 첫 주문 크게, 나중 주문 작게
            return [base_amount * 1.4, base_amount * 1.2, base_amount]
        else:
            # 일반장: 균등 분할
            return [base_amount, base_amount, base_amount]

    def process_buy_signal(self, ticker, signal_type):
        """매매 신호 처리 최적화"""
        try:
            # 주문 중복 체크
            if self._is_recent_order(ticker):
                return False, "최근 주문 내역 있음"

            if signal_type == '매도':
                success, message = self._process_sell_order(ticker)
            else:  # 매수
                success, message = self._process_buy_order(ticker)

            if success:
                self._update_order_cache(ticker)
            return success, message

        except Exception as e:
            return False, f"주문 처리 중 오류: {str(e)}"

    def _is_recent_order(self, ticker):
        """최근 주문 여부 확인"""
        current_time = datetime.now()
        if ticker in self._order_cache:
            last_order_time = self._order_cache[ticker]
            return current_time - last_order_time < self.order_cache_timeout
        return False

    def _update_order_cache(self, ticker):
        """주문 캐시 업데이트"""
        self._order_cache[ticker] = datetime.now()
        # 오래된 캐시 정리
        self._cleanup_order_cache()

    def _cleanup_order_cache(self):
        """오래된 주문 캐시 정리"""
        current_time = datetime.now()
        expired_keys = [k for k, v in self._order_cache.items() 
                       if current_time - v >= self.order_cache_timeout]
        for k in expired_keys:
            del self._order_cache[k]

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
            self.telegram.send_message("🤖 자동매매 봇이 시작되었습니다.")
    
    def stop_bot(self):
        """봇 중지"""
        if self.is_running:
            self.is_running = False
            self.telegram.send_message("🤖 자동매매 봇이 중지되었습니다.")
    
    def show_positions(self):
        """포지션 상태 조회"""
        positions = self.position_manager.get_positions()
        if not positions:
            self.telegram.send_message("🔍 현재 보유 중인 코인이 없습니다.")
            return
            
        message = "💼 포지션 상태\n\n"
        for ticker, status in positions.items():
            message += f"코인: {ticker}\n"
            message += f"평균단가: {status['average_price']:,.0f}원\n"
            message += f"수량: {status['quantity']:.8f}\n"
            message += f"매수 횟수: {status['buy_count']}\n"
            message += f"수익률: {status['profit']:.2f}%\n"
            message += f"상태: {status['status']}\n"
            message += f"마지막 업데이트: {status['last_update']}\n\n"
        
        self.telegram.send_message(message)
    
    def show_profit(self):
        """수익률 조회"""
        positions = self.position_manager.get_positions()
        if not positions:
            self.telegram.send_message("🔍 현재 보유 중인 코인이 없습니다.")
            return
            
        total_profit = 0
        for ticker, status in positions.items():
            profit = status['profit']
            total_profit += profit
            
        message = f"💰 총 수익률: {total_profit:.2f}%\n"
        self.telegram.send_message(message)
    
    def sell_all_positions(self):
        """모든 포지션 매도"""
        positions = self.position_manager.get_positions()
        if not positions:
            self.telegram.send_message("🔍 현재 보유 중인 코인이 없습니다.")
            return
            
        for ticker, status in positions.items():
            self.process_buy_signal(ticker, '매도')
        
        self.telegram.send_message("🎉 모든 포지션이 성공적으로 매도되었습니다.")

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

    def log_error(self, message, error):
        """에러 로깅 최적화"""
        try:
            error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            error_msg = f"[{error_time}] {message}: {str(error)}"
            
            # 콘솔 로깅
            print(f"[ERROR] {error_msg}")
            
            # 텔레그램 알림 (중요 에러만)
            if isinstance(error, (ConnectionError, TimeoutError)) or "API" in str(error):
                self.telegram.send_message(f"⚠️ {error_msg}", priority=True)
            
            # 스택 트레이스 로깅
            print(f"상세 에러 정보:\n{traceback.format_exc()}")
            
        except Exception as e:
            print(f"에러 로깅 중 오류 발생: {e}")

    def monitor_market(self):
        """최적화된 시장 모니터링"""
        try:
            current_time = datetime.now()
            
            # 포지션 체크
            self.check_position_hold_times()
            
            # 코인별 분석
            for ticker in self.analyzer.tickers:
                try:
                    # 분석 주기 체크
                    if ticker in self.last_analysis_time:
                        if current_time - self.last_analysis_time[ticker] < self.analysis_interval:
                            continue
                    
                    analysis = self.analyzer.analyze_market(ticker)
                    if not analysis:
                        continue
                        
                    signals = self.analyzer.get_trading_signals(analysis)
                    if signals:
                        for signal in signals:
                            if signal:
                                action, reason, ticker = signal
                                success, message = self.process_buy_signal(ticker, action)
                                if success:
                                    self.telegram.send_message(
                                        f"✅ {ticker} {action} 성공: {reason}",
                                        priority=True
                                    )
                    
                    self.last_analysis_time[ticker] = current_time
                    
                except Exception as e:
                    self._handle_error(ticker, e)
                    continue
                
                time.sleep(0.1)  # API 제한 방지
                
        except Exception as e:
            print(f"[ERROR] 모니터링 중 오류: {str(e)}")
            self.telegram.send_message(f"⚠️ 모니터링 오류: {str(e)}")

    def _handle_error(self, ticker, error):
        """에러 처리 최적화"""
        current_time = datetime.now()
        
        # 에러 알림 쿨다운 체크
        if ticker in self.last_error_time:
            if current_time - self.last_error_time[ticker] < self.error_cooldown:
                print(f"[ERROR] {ticker} 처리 중 오류: {str(error)}")
                return
        
        self.last_error_time[ticker] = current_time
        error_msg = f"[ERROR] {ticker} 처리 중 오류: {str(error)}"
        print(error_msg)
        self.telegram.send_message(f"⚠️ {error_msg}")

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
                                f"보유기간: {hold_hours:.1f}간\n"
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
        self.last_update = datetime.now()
        self.entry_time = datetime.now()  # 첫 진입 시간 추가
        self.profit_target = 5.0  # 익절 목표
        self.stop_loss = -5.0     # 손절 기준
        self._last_profit_check = None
        self._cached_profit = None
        self.profit_cache_timeout = timedelta(seconds=5)
        self.max_hold_time = timedelta(hours=6)  # 최대 보유 시간 설정
        self._cached_average_price = None
        self._cached_total_quantity = None
        self._last_cache_update = None
        self.cache_timeout = timedelta(seconds=10)

    def calculate_profit(self, current_price):
        """수익률 계산 최적화"""
        try:
            current_time = datetime.now()
            
            if (self._cached_profit is not None and 
                self._last_profit_check is not None and 
                current_time - self._last_profit_check < self.profit_cache_timeout):
                return self._cached_profit
                
            if not current_price or current_price <= 0 or not self.average_price:
                return 0.0
                
            profit = ((current_price - self.average_price) / self.average_price) * 100
            
            self._cached_profit = profit
            self._last_profit_check = current_time
            
            return profit
            
        except Exception as e:
            print(f"수익률 계산 중 오류: {e}")
            return 0.0

    def should_force_sell(self):
        """강제 매도 조건 확인 최적화"""
        current_time = datetime.now()
        hold_time = current_time - self.entry_time
        
        # 최대 보유 시간 초과
        if hold_time >= self.max_hold_time:
            return True
            
        # 현재가 조회
        current_price = pyupbit.get_current_price(self.ticker)
        if not current_price:
            return False
            
        # 수익률 계산
        profit = self.calculate_profit(current_price)
        
        # 손절 조건
        if profit <= self.stop_loss:
            return True
            
        # 익절 조건
        if profit >= self.profit_target:
            return True
            
        return False

class PositionManager:
    def __init__(self, upbit_api):
        self.upbit = upbit_api
        self.positions = {}
        self.position_lock = threading.Lock()
        self.max_positions = 10
        self._position_cache = {}
        self.cache_timeout = timedelta(seconds=30)
        self.position_updates = []
        self.backup_path = 'data/positions/'
        self.backup_interval = timedelta(minutes=30)
        self.last_backup = datetime.now()
        os.makedirs(self.backup_path, exist_ok=True)

    def get_position_status(self, ticker):
        """캐시를 활용한 포지션 상태 조회"""
        current_time = datetime.now()
        
        # 캐시 확인
        if ticker in self._position_cache:
            status, cache_time = self._position_cache[ticker]
            if current_time - cache_time < self.cache_timeout:
                return status
        
        # 새로운 상태 조회
        if ticker not in self.positions:
            return None
            
        position = self.positions[ticker]
        current_price = pyupbit.get_current_price(ticker)
        
        status = {
            'ticker': ticker,
            'average_price': position.average_price,
            'quantity': position.total_quantity,
            'buy_count': position.buy_count,
            'profit': position.calculate_profit(current_price),
            'status': position.status,
            'last_update': position.last_update
        }
        
        self._position_cache[ticker] = (status, current_time)
        return status

    def can_open_position(self, ticker):
        """새 포지션 오픈 가능 여부 확인"""
        if ticker in self.positions:
            return False, "이미 보유 중인 코인"
        if len(self.positions) >= self.max_positions:
            return False, "최대 포지션 수 도달"
        return True, "포지션 오픈 가능"
    
    def open_position(self, ticker, price, quantity):
        """새 포지션 오픈"""
        can_open, message = self.can_open_position(ticker)
        if not can_open:
            return False, message
            
        self.positions[ticker] = Position(ticker, price, quantity)
        return True, "포지션 오픈 성공"
    
    def add_to_position(self, ticker, price, quantity):
        """기존 포지션에 추가"""
        if ticker not in self.positions:
            return False, "보유하지 않은 코인"
            
        return self.positions[ticker].add_position(price, quantity)
    
    def get_positions(self):
        """모든 포지션 상태 조회 최적화"""
        positions = {}
        current_time = datetime.now()
        
        # 백업 처리
        if current_time - self.last_backup >= self.backup_interval:
            self._backup_positions()
            self.last_backup = current_time
        
        for ticker in self.positions:
            status = self.get_position_status(ticker)
            if status:
                positions[ticker] = status
                
        return positions

    def get_position_summary(self):
        """포지션 요약 정보 생성"""
        try:
            summary = {
                'total_positions': len(self.positions),
                'total_profit': 0.0,
                'positions': []
            }
            
            for ticker in self.positions:
                position = self.positions[ticker]
                current_price = pyupbit.get_current_price(ticker)
                
                if current_price:
                    profit = position.calculate_profit(current_price)
                    summary['positions'].append({
                        'ticker': ticker,
                        'average_price': position.average_price,
                        'quantity': position.total_quantity,
                        'profit': profit,
                        'buy_count': position.buy_count,
                        'hold_time': (datetime.now() - position.entry_time).total_seconds() / 3600
                    })
                    summary['total_profit'] += profit
                    
            return summary
            
        except Exception as e:
            print(f"[ERROR] 포지션 요약 생성 실패: {str(e)}")
            return None

    def close_position(self, ticker):
        """포지션 종료"""
        if ticker not in self.positions:
            return False, "보유하지 않은 코인"
        
        try:
            position = self.positions[ticker]
            current_price = pyupbit.get_current_price(ticker)
            
            if not current_price:
                return False, "현재가 조회 실패"
            
            quantity = position.total_quantity
            if quantity <= 0:
                return False, "잘못된 수량"
            
            # 매도 주문 실행
            order = self.upbit.upbit.sell_market_order(ticker, quantity)
            if not order or 'error' in order:
                return False, f"매도 주문 실패: {order}"
            
            # 포지션 제거
            del self.positions[ticker]
            return True, "포지션 종료 성공"
        except Exception as e:
            return False, f"매도 실패: {str(e)}"

    def _backup_positions(self):
        """포지션 백업"""
        try:
            current_time = datetime.now()
            
            # 백업 주기 체크
            if current_time - self.last_backup < self.backup_interval:
                return True

            # 백업 데이터 생성
            backup_data = {
                'timestamp': current_time.isoformat(),
                'positions': {}
            }
            
            for ticker, position in self.positions.items():
                backup_data['positions'][ticker] = {
                    'entry_price': position.average_price,
                    'quantity': position.total_quantity,
                    'buy_count': position.buy_count,
                    'entry_time': position.entry_time.isoformat(),
                    'last_update': position.last_update.isoformat()
                }

            # 백업 파일 저장
            filename = f"positions_{current_time.strftime('%Y%m%d_%H%M%S')}.json"
            filepath = os.path.join(self.backup_path, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2)

            # 오래된 백업 파일 정리
            self._cleanup_old_backups()
            
            self.last_backup = current_time
            return True

        except Exception as e:
            print(f"[ERROR] 포지션 백업 실패: {e}")
            return False

    def restore_positions(self):
        """포지션 복구 최적화"""
        try:
            # 최신 백업 파일 찾기
            backup_files = glob.glob(os.path.join(self.backup_path, 'positions_*.json'))
            if not backup_files:
                return False

            latest_backup = max(backup_files, key=os.path.getctime)
            
            with open(latest_backup, 'r', encoding='utf-8') as f:
                backup_data = json.load(f)

            # 포지션 복구
            restored_positions = {}
            for ticker, data in backup_data['positions'].items():
                try:
                    entry_time = datetime.fromisoformat(data['entry_time'])
                    last_update = datetime.fromisoformat(data['last_update'])
                    
                    position = Position(
                        ticker=ticker,
                        entry_price=data['entry_price'],
                        quantity=data['quantity']
                    )
                    position.buy_count = data['buy_count']
                    position.entry_time = entry_time
                    position.last_update = last_update
                    
                    restored_positions[ticker] = position
                    
                except Exception as e:
                    print(f"[ERROR] {ticker} 포지션 복구 실패: {e}")
                    continue

            self.positions = restored_positions
            return True

        except Exception as e:
            print(f"[ERROR] 포지션 복구 실패: {e}")
            return False

    def _cleanup_old_backups(self):
        """오래된 백업 파일 정리"""
        try:
            backup_files = glob.glob(os.path.join(self.backup_path, 'positions_*.json'))
            if len(backup_files) <= 10:  # 최소 10개 유지
                return

            # 생성일 기준 정렬
            backup_files.sort(key=os.path.getctime)
            
            # 오래된 파일 삭제
            for file in backup_files[:-10]:  # 최근 10개만 남기고 삭제
                try:
                    os.remove(file)
                except Exception as e:
                    print(f"[ERROR] 백업 파일 삭제 실패 ({file}): {e}")

        except Exception as e:
            print(f"[ERROR] 백업 파일 정리 중 오류: {e}")

class OrderHistory:
    def __init__(self):
        self.orders = []
        self.max_history = 1000
        self._order_cache = {}
        self.cache_timeout = timedelta(minutes=5)

    def add_order(self, order_info):
        """주문 이력 추가"""
        try:
            order_data = {
                'timestamp': datetime.now(),
                'ticker': order_info['market'],
                'type': order_info['side'],
                'price': float(order_info['price']),
                'volume': float(order_info['volume']),
                'uuid': order_info['uuid']
            }
            
            self.orders.append(order_data)
            
            # 최대 이력 개수 제한
            if len(self.orders) > self.max_history:
                self.orders = self.orders[-self.max_history:]
                
            # 캐시 업데이트
            self._order_cache[order_info['uuid']] = order_data
            
            return True
            
        except Exception as e:
            print(f"[ERROR] 주문 이력 추가 실패: {e}")
            return False

    def get_recent_orders(self, ticker=None, limit=10):
        """최근 주문 이력 조회"""
        try:
            if ticker:
                filtered_orders = [order for order in self.orders if order['ticker'] == ticker]
            else:
                filtered_orders = self.orders
                
            return filtered_orders[-limit:]
            
        except Exception as e:
            print(f"[ERROR] 주문 이력 조회 실패: {e}")
            return []

    def get_order_details(self, uuid):
        """주문 상세 정보 조회"""
        try:
            # 캐시 확인
            if uuid in self._order_cache:
                return self._order_cache[uuid]
                
            # DB나 API에서 조회
            for order in self.orders:
                if order['uuid'] == uuid:
                    self._order_cache[uuid] = order
                    return order
                    
            return None
            
        except Exception as e:
            print(f"[ERROR] 주문 상세 조회 실패: {e}")
            return None

class VolumeAnalyzer:
    def __init__(self):
        self.volume_cache = {}
        self.cache_timeout = timedelta(minutes=1)
        self.anomaly_thresholds = {
            'sudden_increase': 200,  # 갑작스러운 거래량 증가 (%)
            'sustained_increase': 150,  # 지속적 거래량 증가 (%)
            'volume_dry_up': 50  # 거래량 고갈 (%)
        }

    def analyze_volume_patterns(self, ticker, df):
        """거래량 패턴 분석 최적화"""
        try:
            cache_key = f"vol_{ticker}_{datetime.now().strftime('%Y%m%d%H%M')}"
            
            if cache_key in self.volume_cache:
                return self.volume_cache[cache_key]

            analysis = {
                'patterns': [],
                'alerts': [],
                'metrics': {}
            }

            # 기본 거래량 메트릭 계산
            df['VMA5'] = df['거래량'].rolling(window=5).mean()
            df['VMA20'] = df['거래량'].rolling(window=20).mean()
            
            current_volume = df['거래량'].iloc[-1]
            avg_volume = df['VMA20'].iloc[-1]

            # 거래량 증가율 계산
            volume_increase = ((current_volume / avg_volume) - 1) * 100
            
            # 거래량 패턴 감지
            self._detect_volume_patterns(df, analysis)
            
            # 이상 징후 감지
            self._detect_volume_anomalies(df, analysis)
            
            # 메트릭 저장
            analysis['metrics'] = {
                'current_volume': current_volume,
                'average_volume': avg_volume,
                'volume_increase': volume_increase,
                'volume_trend': self._calculate_volume_trend(df)
            }

            # 캐시 저장
            self.volume_cache[cache_key] = analysis
            
            return analysis

        except Exception as e:
            print(f"[ERROR] 거래량 패턴 분석 실패: {e}")
            return None

    def _detect_volume_patterns(self, df, analysis):
        """거래량 패턴 감지"""
        try:
            # 거래량 증가 패턴
            if (df['거래량'].iloc[-1] > df['VMA5'].iloc[-1] * 2 and
                df['거래량'].iloc[-2] > df['VMA5'].iloc[-2] * 1.5):
                analysis['patterns'].append('volume_surge')
                analysis['alerts'].append('🚨 연속적인 거래량 급증')

            # 거래량 감소 패턴
            if (df['거래량'].iloc[-1] < df['VMA5'].iloc[-1] * 0.5 and
                df['거래량'].iloc[-2] < df['VMA5'].iloc[-2] * 0.5):
                analysis['patterns'].append('volume_dry_up')
                analysis['alerts'].append('⚠️ 거래량 고갈 징후')

            # 거래량 집중 패턴
            recent_volumes = df['거래량'].iloc[-5:]
            if (recent_volumes.max() > df['VMA20'].iloc[-1] * 3 and
                recent_volumes.mean() > df['VMA20'].iloc[-1] * 2):
                analysis['patterns'].append('volume_concentration')
                analysis['alerts'].append('📊 거래량 집중 발생')

        except Exception as e:
            print(f"[ERROR] 거래량 패턴 감지 실패: {e}")

    def _detect_volume_anomalies(self, df, analysis):
        """거래량 이상 징후 감지"""
        try:
            current_volume = df['거래량'].iloc[-1]
            avg_volume = df['VMA20'].iloc[-1]

            # 갑작스러운 거래량 증가
            if current_volume > avg_volume * (1 + self.anomaly_thresholds['sudden_increase'] / 100):
                analysis['patterns'].append('sudden_volume_spike')
                analysis['alerts'].append('⚠️ 갑작스러운 거래량 급증')

            # 지속적인 거래량 증가
            recent_volumes = df['거래량'].iloc[-5:]
            if all(vol > avg_volume * (1 + self.anomaly_thresholds['sustained_increase'] / 100) 
                  for vol in recent_volumes):
                analysis['patterns'].append('sustained_volume_increase')
                analysis['alerts'].append('📈 지속적인 거래량 증가')

            # 거래량 고갈
            if current_volume < avg_volume * (self.anomaly_thresholds['volume_dry_up'] / 100):
                analysis['patterns'].append('volume_exhaustion')
                analysis['alerts'].append('📉 거래량 고갈')

        except Exception as e:
            print(f"[ERROR] 거래량 이상 징후 감지 실패: {e}")

    def _calculate_volume_trend(self, df):
        """거래량 추세 계산"""
        try:
            recent_volumes = df['거래량'].iloc[-5:]
            volume_changes = recent_volumes.pct_change()
            
            trend = {
                'direction': 'neutral',
                'strength': 0,
                'consistency': 0
            }
            
            # 추세 방향과 강도 계산
            avg_change = volume_changes.mean() * 100
            if abs(avg_change) > 10:
                trend['direction'] = 'up' if avg_change > 0 else 'down'
                trend['strength'] = abs(avg_change)
            
            # 추세 일관성 계산
            positive_changes = (volume_changes > 0).sum()
            negative_changes = (volume_changes < 0).sum()
            trend['consistency'] = max(positive_changes, negative_changes) / len(volume_changes)
            
            return trend
            
        except Exception as e:
            print(f"[ERROR] 거래량 추세 계산 실패: {e}")
            return None

class TradingStrategy:
    def __init__(self):
        self.strategy_params = {
            'rsi_buy': 30,
            'rsi_sell': 70,
            'bb_buy': 0.2,  # 하단 밴드 접근
            'bb_sell': 0.8,  # 상단 밴드 접근
            'volume_threshold': 150  # 거래량 증가 기준 (%)
        }
        self._strategy_cache = {}
        self.cache_timeout = timedelta(minutes=1)

    def evaluate_trading_signals(self, ticker, analysis_data):
        """매매 신호 평가 최적화"""
        try:
            cache_key = f"signals_{ticker}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            if cache_key in self._strategy_cache:
                return self._strategy_cache[cache_key]

            signals = {
                'buy_signals': [],
                'sell_signals': [],
                'strength': 0,
                'confidence': 0
            }

            # RSI 기반 신호
            self._evaluate_rsi_signals(analysis_data, signals)
            
            # 볼린저 밴드 기반 신호
            self._evaluate_bb_signals(analysis_data, signals)
            
            # 거래량 기반 신호
            self._evaluate_volume_signals(analysis_data, signals)
            
            # 신호 강도 및 신뢰도 계산
            self._calculate_signal_metrics(signals)
            
            # 캐시 저장
            self._strategy_cache[cache_key] = signals
            
            return signals

        except Exception as e:
            print(f"[ERROR] 매매 신호 평가 실패: {e}")
            return None

    def _evaluate_rsi_signals(self, data, signals):
        """RSI 기반 신호 평가"""
        try:
            rsi = data['indicators']['rsi']
            
            if rsi <= self.strategy_params['rsi_buy']:
                signals['buy_signals'].append({
                    'type': 'RSI',
                    'value': rsi,
                    'strength': (self.strategy_params['rsi_buy'] - rsi) / 10
                })
                
            elif rsi >= self.strategy_params['rsi_sell']:
                signals['sell_signals'].append({
                    'type': 'RSI',
                    'value': rsi,
                    'strength': (rsi - self.strategy_params['rsi_sell']) / 10
                })
                
        except Exception as e:
            print(f"[ERROR] RSI 신호 평가 실패: {e}")

    def _evaluate_bb_signals(self, data, signals):
        """볼린저 밴드 기반 신호 평가"""
        try:
            bb = data['indicators']['bollinger']
            
            if bb['percent_b'] <= self.strategy_params['bb_buy']:
                signals['buy_signals'].append({
                    'type': 'BB',
                    'value': bb['percent_b'],
                    'strength': (self.strategy_params['bb_buy'] - bb['percent_b']) * 2
                })
                
            elif bb['percent_b'] >= self.strategy_params['bb_sell']:
                signals['sell_signals'].append({
                    'type': 'BB',
                    'value': bb['percent_b'],
                    'strength': (bb['percent_b'] - self.strategy_params['bb_sell']) * 2
                })
                
        except Exception as e:
            print(f"[ERROR] BB 신호 평가 실패: {e}")

    def _evaluate_volume_signals(self, data, signals):
        """거래량 기반 신호 평가"""
        try:
            volume = data['volume']
            
            if volume['increase'] > self.strategy_params['volume_threshold']:
                # 가격 추세와 결합하여 신호 생성
                if data['price']['trend'] > 0:
                    signals['buy_signals'].append({
                        'type': 'Volume',
                        'value': volume['increase'],
                        'strength': volume['increase'] / 100
                    })
                elif data['price']['trend'] < 0:
                    signals['sell_signals'].append({
                        'type': 'Volume',
                        'value': volume['increase'],
                        'strength': volume['increase'] / 100
                    })
                    
        except Exception as e:
            print(f"[ERROR] 거래량 신호 평가 실패: {e}")

    def _calculate_signal_metrics(self, signals):
        """신호 강도 및 신뢰도 계산"""
        try:
            # 신호 강도 계산
            buy_strength = sum(signal['strength'] for signal in signals['buy_signals'])
            sell_strength = sum(signal['strength'] for signal in signals['sell_signals'])
            
            signals['strength'] = buy_strength - sell_strength
            
            # 신호 신뢰도 계산
            total_signals = len(signals['buy_signals']) + len(signals['sell_signals'])
            if total_signals > 0:
                max_signals = 3  # RSI, BB, Volume
                signals['confidence'] = (total_signals / max_signals) * 100
                
        except Exception as e:
            print(f"[ERROR] 신호 메트릭 계산 실패: {e}")

class RiskManager:
    def __init__(self):
        self.risk_params = {
            'max_position_size': 0.1,  # 전체 자산의 최대 10%
            'max_daily_loss': 0.05,    # 일일 최대 손실 5%
            'stop_loss': 0.03,         # 개별 포지션 손절 3%
            'take_profit': 0.05        # 개별 포지션 익절 5%
        }
        self.daily_stats = {
            'start_balance': 0,
            'current_balance': 0,
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0
        }
        self._risk_cache = {}
        self.cache_timeout = timedelta(minutes=5)

    def evaluate_position_risk(self, ticker, current_price, position_size, total_balance):
        """포지션 리스크 평가"""
        try:
            cache_key = f"risk_{ticker}_{datetime.now().strftime('%Y%m%d%H%M')}"
            
            if cache_key in self._risk_cache:
                return self._risk_cache[cache_key]

            risk_assessment = {
                'risk_level': 'low',
                'warnings': [],
                'position_allowed': True,
                'max_position_size': 0,
                'stop_loss_price': 0,
                'take_profit_price': 0
            }

            # 포지션 크기 검증
            position_ratio = position_size / total_balance
            if position_ratio > self.risk_params['max_position_size']:
                risk_assessment['warnings'].append('포지션 크기 초과')
                risk_assessment['position_allowed'] = False
                risk_assessment['risk_level'] = 'high'

            # 일일 손실 한도 검증
            daily_loss_ratio = (self.daily_stats['current_balance'] - self.daily_stats['start_balance']) / self.daily_stats['start_balance']
            if abs(daily_loss_ratio) > self.risk_params['max_daily_loss']:
                risk_assessment['warnings'].append('일일 손실 한도 도달')
                risk_assessment['position_allowed'] = False
                risk_assessment['risk_level'] = 'extreme'

            # 손절/익절 가격 계산
            risk_assessment['stop_loss_price'] = current_price * (1 - self.risk_params['stop_loss'])
            risk_assessment['take_profit_price'] = current_price * (1 + self.risk_params['take_profit'])
            
            # 최대 포��션 크기 계산
            risk_assessment['max_position_size'] = total_balance * self.risk_params['max_position_size']

            # 캐시 저장
            self._risk_cache[cache_key] = risk_assessment
            
            return risk_assessment

        except Exception as e:
            print(f"[ERROR] 리스크 평가 실패: {e}")
            return None

    def update_trade_stats(self, trade_result):
        """거래 통계 업데이트"""
        try:
            self.daily_stats['total_trades'] += 1
            
            if trade_result['profit'] > 0:
                self.daily_stats['winning_trades'] += 1
            else:
                self.daily_stats['losing_trades'] += 1
                
            self.daily_stats['current_balance'] = trade_result['current_balance']
            
            # 승률 계산
            win_rate = (self.daily_stats['winning_trades'] / self.daily_stats['total_trades']) * 100 if self.daily_stats['total_trades'] > 0 else 0
            
            return {
                'win_rate': win_rate,
                'total_trades': self.daily_stats['total_trades'],
                'daily_pnl': (self.daily_stats['current_balance'] - self.daily_stats['start_balance']) / self.daily_stats['start_balance'] * 100
            }
            
        except Exception as e:
            print(f"[ERROR] 거래 통계 업데이트 실패: {e}")
            return None

    def check_risk_limits(self, position):
        """리스크 한도 체크"""
        try:
            current_price = position.current_price
            entry_price = position.average_price
            
            # 손절 체크
            if current_price < entry_price * (1 - self.risk_params['stop_loss']):
                return {
                    'action': 'close',
                    'reason': 'stop_loss',
                    'limit_price': entry_price * (1 - self.risk_params['stop_loss'])
                }
                
            # 익절 체크
            if current_price > entry_price * (1 + self.risk_params['take_profit']):
                return {
                    'action': 'close',
                    'reason': 'take_profit',
                    'limit_price': entry_price * (1 + self.risk_params['take_profit'])
                }
                
            return {
                'action': 'hold',
                'reason': None,
                'limit_price': None
            }
            
        except Exception as e:
            print(f"[ERROR] 리스크 한도 체크 실패: {e}")
            return None

class DatabaseManager:
    def __init__(self):
        self.db_path = 'data/trading.db'
        self._setup_database()
        self._connection = None
        self._lock = threading.Lock()

    def _setup_database(self):
        """데이터베이스 초기 설정"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # 거래 기록 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME,
                        ticker TEXT,
                        type TEXT,
                        price REAL,
                        volume REAL,
                        total_amount REAL,
                        fee REAL,
                        status TEXT
                    )
                ''')
                
                # 포지션 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS positions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ticker TEXT,
                        entry_price REAL,
                        current_price REAL,
                        quantity REAL,
                        entry_time DATETIME,
                        last_update DATETIME
                    )
                ''')
                
                # 성능 메트릭 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME,
                        metric_type TEXT,
                        value REAL
                    )
                ''')
                
                conn.commit()
                
        except Exception as e:
            print(f"[ERROR] 데이터베이스 설정 실패: {e}")

    def _get_connection(self):
        """데이터베이스 연결 관리"""
        if not self._connection:
            self._connection = sqlite3.connect(self.db_path)
            self._connection.row_factory = sqlite3.Row
        return self._connection

    def record_trade(self, trade_data):
        """거래 기록 저장"""
        try:
            with self._lock:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO trades (
                            timestamp, ticker, type, price, volume, 
                            total_amount, fee, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        datetime.now(),
                        trade_data['ticker'],
                        trade_data['type'],
                        trade_data['price'],
                        trade_data['volume'],
                        trade_data['total_amount'],
                        trade_data['fee'],
                        trade_data['status']
                    ))
                    conn.commit()
                    return cursor.lastrowid
                    
        except Exception as e:
            print(f"[ERROR] 거래 기록 저장 실패: {e}")
            return None

    def update_position(self, position_data):
        """포지션 정보 업데이트"""
        try:
            with self._lock:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT OR REPLACE INTO positions (
                            ticker, entry_price, current_price, quantity,
                            entry_time, last_update
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        position_data['ticker'],
                        position_data['entry_price'],
                        position_data['current_price'],
                        position_data['quantity'],
                        position_data['entry_time'],
                        datetime.now()
                    ))
                    conn.commit()
                    
        except Exception as e:
            print(f"[ERROR] 포지션 업데이트 실패: {e}")

    def record_metric(self, metric_type, value):
        """성능 메트릭 기록"""
        try:
            with self._lock:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO metrics (timestamp, metric_type, value)
                        VALUES (?, ?, ?)
                    ''', (datetime.now(), metric_type, value))
                    conn.commit()
                    
        except Exception as e:
            print(f"[ERROR] 메트릭 기록 실패: {e}")

    def get_trade_history(self, ticker=None, start_date=None, end_date=None):
        """거래 기록 조회"""
        try:
            query = "SELECT * FROM trades WHERE 1=1"
            params = []
            
            if ticker:
                query += " AND ticker = ?"
                params.append(ticker)
            
            if start_date:
                query += " AND timestamp >= ?"
                params.append(start_date)
                
            if end_date:
                query += " AND timestamp <= ?"
                params.append(end_date)
                
            query += " ORDER BY timestamp DESC"
            
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                return cursor.fetchall()
                
        except Exception as e:
            print(f"[ERROR] 거래 기록 조회 실패: {e}")
            return []

    def get_active_positions(self):
        """활성 포지션 조회"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM positions WHERE quantity > 0")
                return cursor.fetchall()
                
        except Exception as e:
            print(f"[ERROR] 활성 포지션 조회 실패: {e}")
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
                monitor.monitor_market()
                time.sleep(1)
            except KeyboardInterrupt:
                print("\n[INFO] 프로그램 종료 요청됨...")
                if monitor:
                    monitor.is_running = False
                telegram.send_message("🔴 봇이 수동으로 종료되었습니다.")
                break
            except Exception as e:
                print(f"[ERROR] 모니터링 중 오류 발생: {e}")
                telegram.send_message(f"⚠️ 모니터링 오류: {str(e)}")
                
    except Exception as e:
        error_message = f"프로그램 초기화 중 치명적 오류: {e}"
        print(error_message)
        if 'telegram' in locals():
            telegram.send_message(f"⚠️ {error_message}")
    
    finally:
        # 프로그램 종료 시 정리 작업
        if monitor:
            monitor.is_running = False
        print("[INFO] 프로그램이 종료되었습니다.")