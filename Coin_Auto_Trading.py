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

# .env 파일 로드
load_dotenv()

class UpbitAPI:
    def __init__(self):
        self.access_key = os.getenv('UPBIT_ACCESS_KEY')
        self.secret_key = os.getenv('UPBIT_SECRET_KEY')
        self.upbit = pyupbit.Upbit(self.access_key, self.secret_key)

    def create_jwt_token(self):
        payload = {
            'access_key': self.access_key,
            'nonce': str(uuid.uuid4()),
        }
        jwt_token = jwt.encode(payload, self.secret_key)
        return jwt_token

    def get_headers(self):
        jwt_token = self.create_jwt_token()
        return {
            'Authorization': f'Bearer {jwt_token}',
            'Content-Type': 'application/json'
        }
    
    def get_balances(self):
        """계좌 잔고 조회"""
        try:
            return self.upbit.get_balances()
        except Exception as e:
            print(f"잔고 조회 실패: {e}")
            return None

class TelegramBot:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # 초기화 시 토큰과 채팅 ID 확인
        if not self.token or not self.chat_id:
            raise ValueError("텔레그램 토큰 또는 채팅 ID가 설정되지 않았습니다.")
        print(f"텔레그램 봇 초기화 - 채팅 ID: {self.chat_id}")

    def send_message(self, message, parse_mode=None):
        """텔레그램으로 메시지를 보내는 함수"""
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            
            params = {
                'chat_id': self.chat_id,
                'text': message
            }
            
            if parse_mode:
                params['parse_mode'] = parse_mode
            
            # URL 인코딩 제거 (불필요한 처리였음)
            print(f"\n[DEBUG] 텔레그램 메시지 전송 시도:")
            print(f"메시지: {message[:100]}...")  # 메시지 앞부분만 출력
            
            response = requests.post(url, json=params, timeout=10)
            
            if response.status_code == 200:
                print("텔레그램 메시지 전송 성공")
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
        
    def analyze_market_state(self, df):
        """시장 상태 분석"""
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
        elif abs(price_trend) > 2:  # 거래량 조건 제거
            self.market_state = 'trend'
        else:
            self.market_state = 'normal'
            
        return {
            'volatility': volatility,
            'avg_volatility': avg_volatility,
            'price_trend': price_trend,
            'bb_trend': bb_trend
        }

    def update_trading_conditions(self, market_status):
        """시장 상태에 따른 매매 조건 업데이트"""
        old_state = self.market_state
        old_conditions = self.trading_conditions.copy()
        
        # 시장 상태에 따른 조건 업데이트
        if self.market_state == 'volatile':
            self.trading_conditions.update({
                'rsi_oversold': 25,
                'rsi_overbought': 75,
                'bb_squeeze': 0.3,
                'bb_expansion': 2.5
            })
        elif self.market_state == 'trend':
            self.trading_conditions.update({
                'rsi_oversold': 35,
                'rsi_overbought': 65,
                'bb_squeeze': 0.7,
                'bb_expansion': 1.8
            })
        else:
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
            
            return message
        
        return None

    def get_ohlcv(self, ticker, interval="minute1", count=300):
        """업비트 방식으로 OHLCV 데이터 조회"""
        try:
            df = pyupbit.get_ohlcv(ticker, interval=interval, count=count)
            if df is None:
                return None
            
            df.columns = ['시가', '고가', '저가', '종가', '거래량', '거래금액']
            return df
        except Exception as e:
            print(f"{ticker} OHLCV 데이터 조회 실패: {e}")
            return None

    def calculate_rsi(self, df, period=14):
        """RSI 계산"""
        df['변화량'] = df['종가'].diff()
        df['상승폭'] = df['변화량'].apply(lambda x: x if x > 0 else 0)
        df['하락폭'] = df['변화량'].apply(lambda x: -x if x < 0 else 0)
        
        # 지수이동평균 방식으로 RSI 계산
        df['AU'] = df['상승폭'].ewm(alpha=1/period, min_periods=period).mean()
        df['AD'] = df['하락폭'].ewm(alpha=1/period, min_periods=period).mean()
        
        df['RSI'] = df['AU'] / (df['AU'] + df['AD']) * 100
        return df

    def calculate_bollinger_bands(self, df, n=20, k=2):
        """볼린저 밴드 계산"""
        try:
            if len(df) < n:
                return None

            # 컬럼명 통일
            df['종가'] = df['close'] if 'close' in df.columns else df['종가']
            
            df['중심선'] = df['종가'].rolling(window=n).mean()
            df['표준편차'] = df['종가'].rolling(window=n).std()
            
            df['상단밴드'] = df['중심선'] + (df['표준편차'] * k)
            df['하단밴드'] = df['중심선'] - (df['표준편차'] * k)
            df['밴드폭'] = (df['상단밴드'] - df['하단밴드']) / df['중심선'] * 100
            
            return df
        except Exception as e:
            print(f"볼린저 밴드 계산 중 오류: {e}")
            return None

    def analyze_volume(self, df):
        """거래량 분석"""
        # 이동평균 거래량 계산
        df['거래량MA5'] = df['거래량'].rolling(window=5).mean()
        df['거래량MA20'] = df['거래량'].rolling(window=20).mean()
        
        # 거래량 증가율
        df['거래량증가율'] = (df['거래량'] / df['거래량MA5'] - 1) * 100
        return df

    def analyze_market(self, ticker):
        """시장 분석"""
        try:
            analysis_results = {}
            
            for timeframe, config in self.timeframes.items():
                try:
                    df = self.get_ohlcv(ticker, interval=config['interval'], count=config['count'])
                    if df is None or df.empty:
                        continue

                    # 지표 계산
                    df = self.calculate_rsi(df)
                    df = self.calculate_bollinger_bands(df)
                    df = self.analyze_volume(df)
                    
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
        """매매 신호 생성"""
        signals = []
        
        timeframe = 'minute1'
        if timeframe not in analysis['timeframes']:
            return signals
        
        data = analysis['timeframes'][timeframe]
        
        # RSI + 볼린저밴드 복합 신호
        if data['rsi'] >= self.trading_conditions['rsi_overbought']:  # 70으로 수정
            if data['percent_b'] >= 0.9:  # 상단밴드 근접
                print(f"[DEBUG] {analysis['ticker']} 매도 신호 감지: RSI={data['rsi']:.2f}, %B={data['percent_b']:.2f}")
                signals.append(('매도', f'RSI 과매수({data["rsi"]:.1f}) + 상단밴드 근접(%B:{data["percent_b"]:.2f})', analysis['ticker']))
        
        elif data['rsi'] <= self.trading_conditions['rsi_oversold']:  # 30
            if data['percent_b'] <= 0.1:  # 하단밴드 근접
                print(f"[DEBUG] {analysis['ticker']} 매수 신호 감지: RSI={data['rsi']:.2f}, %B={data['percent_b']:.2f}")
                signals.append(('매수', f'RSI 과매도({data["rsi"]:.1f}) + 하단밴드 근접(%B:{data["percent_b"]:.2f})', analysis['ticker']))

        if signals:
            print(f"[DEBUG] {analysis['ticker']} 매매 신호 생성됨: {signals}")
        
        return signals

    def format_analysis_message(self, analysis):
        """분석 결과 메시지 포맷팅"""
        message = f"🔍 {analysis['ticker']} 분석 과\n\n"
        message += f"💰 현재가: {analysis['current_price']:,.0f}원\n"
        message += f"📊 RSI: {analysis['rsi']:.2f}\n\n"
        
        message += f"📈 볼린저 밴드\n"
        message += f"상단: {analysis['bb_upper']:,.0f}원\n"
        message += f"중심: {analysis['bb_middle']:,.0f}원\n"
        message += f"하단: {analysis['bb_lower']:,.0f}원\n"
        message += f"밴드폭: {analysis['bb_bandwidth']:.2f}%\n"
        message += f"%B: {analysis['percent_b']:.2f}\n\n"
        
        message += f"📊 거래량\n"
        message += f"현재: {analysis['volume']:,.0f}\n"
        message += f"5일평균: {analysis['volume_ma5']:,.0f}\n"
        message += f"증가율: {analysis['volume_increase']:.2f}%\n"
        
        return message

    def check_trading_alerts(self, analysis):
        """여러 시간대의 매매 조건 접근 알림 체크"""
        ticker = analysis['ticker']
        current_time = datetime.now()
        
        if ticker in self.alert_sent:
            if current_time - self.alert_sent[ticker] < self.alert_cooldown:
                return None

        alerts = []
        
        # 여러 시간대의 지표 확인
        for timeframe, data in analysis['timeframes'].items():
            # RSI 접근 알림
            if 32 <= data['rsi'] <= 35:
                alerts.append(f"{timeframe} RSI 과매도 구간 접근 중 ({data['rsi']:.2f})")
            elif 65 <= data['rsi'] <= 68:
                alerts.append(f"{timeframe} RSI 과매수 구간 접근 중 ({data['rsi']:.2f})")
            
            # 볼린저 밴드 접근 알림
            if 0.05 <= data['percent_b'] <= 0.1:
                alerts.append(f"{timeframe} 하단 밴드 접근 중")
            elif 0.9 <= data['percent_b'] <= 0.95:
                alerts.append(f"{timeframe} 상단 밴드 접근 중")
            
        if alerts:
            self.alert_sent[ticker] = current_time
            message = f"⚠️ {ticker} 매매 시그널 접근 알림\n\n"
            message += f"현재가: {format(int(analysis['current_price']), ',')}원\n"
            message += "감지된 신호:\n"
            for alert in alerts:
                message += f"- {alert}\n"
            return message
        
        return None

    def get_top_volume_tickers(self, limit=40):
        """거래량 상위 코인 목록 조회"""
        try:
            all_tickers = pyupbit.get_tickers(fiat="KRW")
            volume_data = []
            
            for ticker in all_tickers:
                try:
                    # 일봉 기준으로 거래량 조회
                    df = pyupbit.get_ohlcv(ticker, interval="day", count=1)
                    if df is not None and not df.empty:
                        # 거래대금 = 거래량 * 종가
                        trade_price = df['volume'].iloc[-1] * df['close'].iloc[-1]
                        volume_data.append((ticker, trade_price))
                    
                    time.sleep(0.1)  # API 호출 제한 방지
                    
                except Exception as e:
                    print(f"[ERROR] {ticker} 거래량 회 실패: {e}")
                    continue
            
            # 거래대금 기준 정렬
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
            '/sell_all': self.sell_all_positions,
            '/market': self.show_market_analysis,
            '/coins': self.show_trading_coins,
            '/help': self.show_help
        }
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
        self.command_thread = None  # 명령어 처리 스레드 추가
        self.last_tickers_update = None  # 마지막 티커 업데이트 시간 추가
        
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
                                threading.Thread(target=self.process_command, args=(command,)).start()
                                
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
        """매매 신호 처리"""
        try:
            print(f"\n[DEBUG] ====== 매매 신호 처리 시작: {ticker} {signal_type} ======")
            
            # 매도 신호 처리
            if signal_type == '매도':
                # 1. 포지션 확인
                if ticker not in self.position_manager.positions:
                    print(f"[DEBUG] {ticker} 보유하지 않은 코인")
                    return False, "보유하지 않은 코인"
                
                position = self.position_manager.positions[ticker]
                total_quantity = position.total_quantity
                
                # 2. 실제 보유 수량 확인 (추가된 부분)
                balances = self.upbit.get_balances()
                if balances:
                    currency = ticker.replace('KRW-', '')
                    actual_balance = next((float(b['balance']) for b in balances 
                                        if b['currency'] == currency), 0)
                    if actual_balance > 0:
                        total_quantity = actual_balance  # 실제 보유 수량으로 업데이트
                    else:
                        return False, "실제 보유 수량이 없습니다"
                
                # 3. 매도 주문 실행
                print(f"[DEBUG] {ticker} 매도 시도:")
                print(f"- 매도 수량: {total_quantity}")
                
                try:
                    order = self.upbit.upbit.sell_market_order(ticker, total_quantity)
                    print(f"[DEBUG] {ticker} 매도 주문 결과: {order}")
                    
                    if order and 'error' not in order:
                        success, message = self.position_manager.close_position(ticker)
                        if success:
                            self.telegram.send_message(
                                f"💰 매도 완료: {ticker}\n"
                                f"수량: {total_quantity:.8f}"
                            )
                        return success, message
                    
                    return False, f"매도 주문 실패: {order}"
                except Exception as e:
                    print(f"[ERROR] {ticker} 매도 주문 중 오류: {str(e)}")
                    return False, f"매도 주문 오류: {str(e)}"
            
            # 매수 신호 처리 (기존 코드 유지)
            elif signal_type == '매수':
                # 현재가 조회 전 유효성 검사 추가
                if not ticker or not signal_type:
                    return False, "잘못된 매매 신호"
                
                current_price = pyupbit.get_current_price(ticker)
                if not current_price or current_price <= 0:  # 현재가 유효성 검사 강화
                    return False, "현재가 조회 실패 또는 잘못된 현재가"
                
                if signal_type == '매수':
                    balance = self.upbit.get_balances()
                    if not balance:  # 잔고 조회 실패 시
                        return False, "잔고 조회 실패"
                        
                    krw_balance = next((float(item['balance']) for item in balance if item['currency'] == 'KRW'), 0)
                    print(f"[DEBUG] 현재 KRW 잔고: {krw_balance}")
                    
                    if krw_balance < 5000:
                        return False, "잔고 부족"
                        
                    # 기존 포지션 확인 (추가매수 로직)
                    if ticker in self.position_manager.positions:
                        position = self.position_manager.positions[ticker]
                        
                        if position.buy_count >= 3:
                            return False, "최대 매수 횟수 도달"
                        
                        # RSI + 볼린저 밴드 분석
                        analysis = self.analyzer.analyze_market(ticker)
                        if not analysis or 'minute1' not in analysis['timeframes']:
                            return False, "시장 분석 실패"
                        
                        data = analysis['timeframes']['minute1']
                        
                        # 추가매수 조건
                        if position.buy_count == 1:  # 1차 추가매수
                            if (data['rsi'] <= 35 and data['percent_b'] <= 0.2):
                                split_amounts = self.calculate_split_orders(self.analyzer.market_state)
                                order_amount = split_amounts[1]
                                print(f"[DEBUG] 1차 추가매수 조건 충족 - RSI: {data['rsi']:.2f}, %B: {data['percent_b']:.2f}")
                            else:
                                return False, "1차 추가매수 조건 미충족"
                            
                        elif position.buy_count == 2:  # 2차 추가매수
                            if (data['rsi'] <= 30 and data['percent_b'] <= 0.1):
                                split_amounts = self.calculate_split_orders(self.analyzer.market_state)
                                order_amount = split_amounts[2]
                                print(f"[DEBUG] 2차 추가매수 조건 충족 - RSI: {data['rsi']:.2f}, %B: {data['percent_b']:.2f}")
                            else:
                                return False, "2차 추가매수 조건 미충족"
                        
                        # 추가매수 주문 실행
                        # 주문 금액이 최소 주문금액보다 큰지 확인
                        if order_amount < 5000:
                            return False, "주문 금액이 최소 주문금액보다 작습니다"

                        # 주문 금액을 정수로 변환
                        order_amount = int(order_amount)
                        order = self.upbit.upbit.buy_market_order(ticker, order_amount)
                        print(f"[DEBUG] 추가매수 주문 결과: {order}")
                        
                        if order and 'error' not in order:
                            max_wait = 5  # 최대 5초 대기
                            wait_count = 0
                            
                            while wait_count < max_wait:
                                time.sleep(1)
                                executed_order = self.upbit.upbit.get_order(order['uuid'])
                                
                                if executed_order:
                                    # 주문이 체결된 경우
                                    if executed_order['state'] == 'done':
                                        quantity = float(executed_order['executed_volume'])
                                        if quantity > 0:
                                            success, message = self.position_manager.open_position(ticker, current_price, quantity)
                                            if success:
                                                self.send_position_update(ticker, "신규 매수 (1/3)")
                                                time.sleep(3)
                                            return success, message
                                        return False, "체결 수량이 0입니다"
                                    
                                    # 주문이 취소된 경우
                                    elif executed_order['state'] == 'cancel':
                                        return False, "주문이 취소되었습니다"
                                    
                                    # 아직 대기 중인 경우
                                    elif executed_order['state'] == 'wait':
                                        wait_count += 1
                                        continue
                                        
                                else:
                                    return False, "주문 상태 조회 실패"
                            
                            # 최대 대기 시간 초과
                            return False, "주문 체결 시간 초과"
                            
                        return False, f"매수 주문 실패: {order}"
                        
                    # 신규 매수
                    else:
                        split_amounts = self.calculate_split_orders(self.analyzer.market_state)
                        if split_amounts[0] > krw_balance:
                            return False, "주문 금액이 잔고보다 큽니다"
                        
                        # 주문 금액을 정수로 변환
                        order_amount = int(split_amounts[0])
                        if order_amount < 5000:
                            return False, "주문 금액이 최소 주문금액보다 작습니다"

                        order = self.upbit.upbit.buy_market_order(ticker, order_amount)
                        print(f"[DEBUG] 신규 매수 주문 결과: {order}")
                        
                        if order and 'error' not in order:
                            max_wait = 5  # 최대 5초 대기
                            wait_count = 0
                            
                            while wait_count < max_wait:
                                time.sleep(1)
                                executed_order = self.upbit.upbit.get_order(order['uuid'])
                                
                                if executed_order:
                                    # 주문이 체결된 경우
                                    if executed_order['state'] == 'done':
                                        quantity = float(executed_order['executed_volume'])
                                        if quantity > 0:
                                            success, message = self.position_manager.open_position(ticker, current_price, quantity)
                                            if success:
                                                self.send_position_update(ticker, "신규 매수 (1/3)")
                                                time.sleep(3)
                                            return success, message
                                        return False, "체결 수량이 0입니다"
                                    
                                    # 주문이 취소된 경우
                                    elif executed_order['state'] == 'cancel':
                                        return False, "주문이 취소되었습니다"
                                    
                                    # 아직 대기 중인 경우
                                    elif executed_order['state'] == 'wait':
                                        wait_count += 1
                                        continue
                                        
                                else:
                                    return False, "주문 상태 조회 실패"
                            
                            # 최대 대기 시간 초과
                            return False, "주문 체결 시간 초과"
                            
                        return False, f"매수 주문 실패: {order}"
                        
            try:
                # 1. 포지션 확인
                if ticker not in self.position_manager.positions:
                    print(f"[DEBUG] {ticker} 보유하지 않은 코인")
                    return False, "보유하지 않은 코인"
                
                position = self.position_manager.positions[ticker]
                total_quantity = position.total_quantity
                
                print(f"[DEBUG] {ticker} 포지션 정보:")
                print(f"- 총 수량: {total_quantity}")
                print(f"- 매수 횟수: {position.buy_count}")
                
                # 2. 수량 유효성 검사
                if total_quantity <= 0:
                    print(f"[ERROR] {ticker} 잘못된 수량: {total_quantity}")
                    return False, "잘못된 수량"

                try:
                    # 3. 현재가 조회 (여러 번 시도)
                    max_retries = 3
                    current_price = None
                    
                    for attempt in range(max_retries):
                        try:
                            current_price = pyupbit.get_current_price(ticker)
                            if current_price and current_price > 0:
                                print(f"[DEBUG] {ticker} 현재가 조회 성공: {current_price}")
                                break
                            print(f"[DEBUG] {ticker} 현재가 재시도 {attempt + 1}/{max_retries}")
                            time.sleep(0.5)
                        except Exception as e:
                            print(f"[ERROR] {ticker} 현재가 조회 시도 {attempt + 1} 실패: {str(e)}")
                            if attempt == max_retries - 1:
                                raise
                            time.sleep(0.5)
                    
                    if not current_price or current_price <= 0:
                        print(f"[ERROR] {ticker} 현재가 조회 최종 실패")
                        return False, "현재가 조회 실패"
                        
                except Exception as e:
                    print(f"[ERROR] {ticker} 현재가 조회 중 오류: {str(e)}")
                    return False, f"현재가 조회 오류: {str(e)}"

                # 4. 시장 분석
                try:
                    analysis = self.analyzer.analyze_market(ticker)
                    if not analysis:
                        print(f"[ERROR] {ticker} 시장 분석 결과 없음")
                        return False, "시장 분석 실패"
                    
                    # minute1 데이터가 없으면 minute5 데이터 사용
                    if 'minute1' not in analysis['timeframes']:
                        print(f"[INFO] {ticker} minute1 데이터 없음, minute5 데이터로 대체")
                        if 'minute5' not in analysis['timeframes']:
                            print(f"[ERROR] {ticker} minute5 데이터도 없음")
                            return False, "시장 분석 실패"
                        data = analysis['timeframes']['minute5']
                    else:
                        data = analysis['timeframes']['minute1']
                        
                    print(f"[DEBUG] {ticker} 분석 데이터:")
                    print(f"- RSI: {data['rsi']:.2f}")
                    print(f"- %B: {data['percent_b']:.2f}")
                    
                    # 5. 매도 수량 계산
                    if data['rsi'] >= 75 and data['percent_b'] >= 0.95:
                        quantity = total_quantity
                        sell_reason = "전량 매도 (강력 매도신호)"
                    elif data['rsi'] >= 70 and data['percent_b'] >= 0.9:
                        quantity = total_quantity * 0.5
                        sell_reason = "부분 매도 (50%)"
                    elif data['rsi'] >= 65 and data['percent_b'] >= 0.85:
                        quantity = total_quantity * 0.3
                        sell_reason = "부분 매도 (30%)"
                    else:
                        print(f"[DEBUG] {ticker} 매도 조건 미충족")
                        return False, "매도 조건 미충족"

                    # 6. 매도 주문 실행
                    print(f"[DEBUG] {ticker} 매도 시도:")
                    print(f"- 매도 수량: {quantity}")
                    print(f"- 매도 이유: {sell_reason}")
                    
                    order = self.upbit.upbit.sell_market_order(ticker, quantity)
                    print(f"[DEBUG] {ticker} 매도 주문 결과: {order}")
                    
                    if order and 'error' not in order:
                        if quantity == total_quantity:
                            success, message = self.position_manager.close_position(ticker)
                        else:
                            success, message = self.position_manager.update_position_quantity(ticker, total_quantity - quantity)
                        
                        if success:
                            self.telegram.send_message(
                                f"💰 {sell_reason}: {ticker}\n"
                                f"수량: {quantity:.8f}\n"
                                f"현재가: {current_price:,.0f}원\n"
                                f"RSI: {data['rsi']:.2f}, %B: {data['percent_b']:.2f}"
                            )
                        return success, message
                        
                    return False, f"매도 주문 실패: {order}"
                    
                except Exception as e:
                    print(f"[ERROR] {ticker} 매도 처리 중 오류: {str(e)}")
                    return False, f"매도 처리 중 오류: {str(e)}"
                    
            except Exception as e:
                print(f"[ERROR] {ticker} 매도 처리 중 상세 오류:")
                print(f"- 오류 타입: {type(e)}")
                print(f"- 오류 내용: {str(e)}")
                print(f"- 오류 발생 위치:\n{traceback.format_exc()}")
                return False, f"매도 처리 중 오류: {str(e)}"
            
        except Exception as e:
            error_msg = f"매매 처리 중 오류 발생: {str(e)}"
            print(f"[ERROR] {error_msg}")
            self.telegram.send_message(f"⚠️ {error_msg}")
            return False, error_msg
    
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
        """시장 모니터링"""
        try:
            current_time = datetime.now()
            
            # 티커 목록 일일 업데이트 (자정 기준)
            if not self.last_tickers_update or current_time.date() > self.last_tickers_update.date():
                self.analyzer.tickers = self.analyzer.get_top_volume_tickers(40)
                self.last_tickers_update = current_time
                print(f"[INFO] 거래량 상위 40개 코인 목록 갱신됨")

            # 각 코인별 개별 분석 실행
            for ticker in self.analyzer.tickers:
                if not self.is_running:
                    break
                
                try:
                    # 개별 코인 분석 및 신호 처리
                    analysis = self.analyzer.analyze_market(ticker)
                    if analysis:
                        signals = self.analyzer.get_trading_signals(analysis)
                        if signals:
                            for signal in signals:
                                if signal:
                                    action, reason, ticker = signal
                                    print(f"[DEBUG] {ticker} 신호 감지: {action}, 사유: {reason}")
                                    success, message = self.process_buy_signal(ticker, action)
                                    if success:
                                        self.telegram.send_message(f"✅ {ticker} {action} 성공: {reason}")
                                    else:
                                        print(f"[DEBUG] {ticker} {action} 실패: {message}")
                    
                    # 코인별 API 호출 제한 방지를 위한 짧은 대기
                    time.sleep(0.2)
                    
                except Exception as e:
                    print(f"[ERROR] {ticker} 처리 중 오류: {str(e)}")
                    self.log_error(f"{ticker} 처리 중 오류", e)
                    continue
                
        except Exception as e:
            error_msg = f"모니터링 중 심각한 오류 발생: {str(e)}"
            print(f"[CRITICAL ERROR] {error_msg}")
            self.log_error("모니터링 중 심각한 오류", e)

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
        """재 시장 상황 분석 결과 전송"""
        message = "🔍 현 시장 상황 분석\n\n"
        
        # 주요 코인 목록 확장
        major_coins = [
            'KRW-BTC', 'KRW-ETH', 'KRW-XRP', 'KRW-SOL', 
            'KRW-ADA', 'KRW-DOGE', 'KRW-MATIC', 'KRW-DOT',
            'KRW-TRX', 'KRW-LINK'
        ]
        
        # 상위 거래량 코인 추가
        volume_leaders = []
        for ticker in self.analyzer.tickers:
            if ticker not in major_coins:  # 중복 제
                try:
                    current_volume = pyupbit.get_current_price(ticker) * \
                                   pyupbit.get_ohlcv(ticker, interval="day", count=1)['volume'].iloc[-1]
                    volume_leaders.append((ticker, current_volume))
                except:
                    continue
        
        # 거래량 기준 상위 5개 코인 택
        volume_leaders.sort(key=lambda x: x[1], reverse=True)
        top_volume_coins = [coin[0] for coin in volume_leaders[:5]]
        
        # 모든 분석 대상 코인
        analysis_targets = major_coins + top_volume_coins
        
        for ticker in analysis_targets:
            analysis = self.analyzer.analyze_market(ticker)
            if analysis:
                message += f"📊 {ticker}\n"
                message += f"현재가: {format(int(analysis['current_price']), ',')}원\n"
                message += f"RSI: {analysis['rsi']:.2f}\n"
                message += f"거래량 증가율: {analysis['volume_increase']:.2f}%\n"
                message += f"밴드폭: {analysis['bb_bandwidth']:.2f}%\n\n"
        
        # 전체 시장 상태 및 추가 정보
        message += f"🌍 전체 시장 상태: {self.analyzer.market_state}\n"
        message += f"📊 현재 매매 조건:\n"
        message += f"- RSI 과매도: {self.analyzer.trading_conditions['rsi_oversold']}\n"
        message += f"- RSI 과매수: {self.analyzer.trading_conditions['rsi_overbought']}\n"
        message += f"- 밴드 수축: {self.analyzer.trading_conditions['bb_squeeze']}\n"
        message += f"- 밴드 확장: {self.analyzer.trading_conditions['bb_expansion']}\n"
        
        # 메시지가 너무 길 경우 분할 전송
        max_length = 4096
        if len(message) > max_length:
            messages = [message[i:i+max_length] for i in range(0, len(message), max_length)]
            for msg in messages:
                self.telegram.send_message(msg)
        else:
            self.telegram.send_message(message)
        
        self.telegram.send_message(message)

    def show_trading_coins(self):
        """업비트의 모든 KRW 마켓 코인 목록 및 상세 분석 결과 전송"""
        try:
            message = "🔍 전체 거래소 코인 상세 분석\n\n"
            
            # 모든 KRW 마 코인 가져오기
            all_tickers = pyupbit.get_tickers(fiat="KRW")
            price_data = []
            
            for ticker in all_tickers:
                try:
                    current_price = pyupbit.get_current_price(ticker)
                    if current_price:
                        price_data.append((ticker, current_price))
                except:
                    continue
            
            # 가격 기준으로 정렬 (고가 코인 선)
            price_data.sort(key=lambda x: x[1], reverse=True)
            
            # 상위 20개 코인 상세 분석
            for ticker, current_price in price_data[:20]:
                try:
                    analysis = self.analyzer.analyze_market(ticker)
                    if not analysis:
                        continue
                    
                    # 매매 신호 강도 평가
                    buy_signals = 0
                    sell_signals = 0
                    total_signals = 0
                    
                    message += f"🪙 {ticker}\n"
                    message += f"💰 현재가: {format(int(current_price), ',')}원\n\n"
                    
                    # 시간대별 분석 결과
                    for timeframe, data in analysis['timeframes'].items():
                        if not data:
                            continue
                        
                        message += f"⏰ {timeframe} 분석:\n"
                        total_signals += 1
                        
                        # RSI 확인
                        if data.get('rsi'):
                            message += f"RSI: {data['rsi']:.2f}"
                            if data['rsi'] <= self.analyzer.trading_conditions['rsi_oversold']:
                                message += " (과도⤴️)"
                                buy_signals += 1
                            elif data['rsi'] >= self.analyzer.trading_conditions['rsi_overbought']:
                                message += " (과매수⤵️)"
                                sell_signals += 1
                            message += "\n"
                        
                        # 밴드폭 확인
                        if data.get('bb_bandwidth'):
                            message += f"밴드폭: {data['bb_bandwidth']:.2f}%"
                            if data['bb_bandwidth'] < self.analyzer.trading_conditions['bb_squeeze']:
                                message += " (수축💫)"
                            elif data['bb_bandwidth'] > self.analyzer.trading_conditions['bb_expansion']:
                                message += " (확장↔️)"
                            message += "\n"
                        
                        # %B 확인
                        if data.get('percent_b') is not None:
                            message += f"%B: {data['percent_b']:.2f}"
                            if data['percent_b'] <= 0.05:
                                message += " (하단돌파⚠️)"
                                buy_signals += 1
                            elif data['percent_b'] >= 0.95:
                                message += " (상단돌파⚠️)"
                                sell_signals += 1
                            message += "\n"
                        
                        message += "\n"
                    
                    # 매매 신호 강도에 따른 상태 평가 (시장 상태 반영)
                    if total_signals > 0:
                        buy_strength = (buy_signals / total_signals) * 100
                        sell_strength = (sell_signals / total_signals) * 100
                        
                        message += "📊 매매 상태: "
                        if self.analyzer.market_state == 'volatile':
                            # 변동성 장에서는 더 보수적으로 판단
                            if buy_strength >= 70:
                                message += "🟢 매수 임박 (강도: {:.1f}%)\n".format(buy_strength)
                            elif sell_strength >= 70:
                                message += "🔴 매도 임박 (강도: {:.1f}%)\n".format(sell_strength)
                            else:
                                message += " 관망\n"
                        elif self.analyzer.market_state == 'trend':
                            # 추세장에서는 더 민감하게 반응
                            if buy_strength >= 50:
                                message += "🟢 매수 임박 (강도: {:.1f}%)\n".format(buy_strength)
                            elif sell_strength >= 50:
                                message += "🔴 매도 임박 (강도: {:.1f}%)\n".format(sell_strength)
                            elif buy_strength >= 30:
                                message += "🟡 매수 관망 (강도: {:.1f}%)\n".format(buy_strength)
                            elif sell_strength >= 30:
                                message += "🟡 매도 관망 (강도: {:.1f}%)\n".format(sell_strength)
                            else:
                                message += "⚪ 관망\n"
                    
                    # 보 상태 확인
                    if ticker in self.position_manager.positions:
                        position = self.position_manager.get_position_status(ticker)
                        message += f"\n💼 보유 중:\n"
                        message += f"평균단가: {format(int(position['average_price']), ',')}원\n"
                        message += f"수익률: {position['profit']:.2f}%\n"
                        message += f"매: {position['buy_count']}/3\n"
                    
                    message += "\n" + "─" * 30 + "\n\n"
                    
                except Exception as e:
                    print(f"{ticker} 분석 중 오류: {e}")
                    continue
            
            # 시장 전체 상태 추가
            message += f"\n🌍 전체 시장 상태: {self.analyzer.market_state}\n"
            message += "📊 현재 매매 조건:\n"
            message += f"- RSI 과매도: {self.analyzer.trading_conditions['rsi_oversold']}\n"
            message += f"- RSI 과매수: {self.analyzer.trading_conditions['rsi_overbought']}\n"
            message += f"- 밴드 수축: {self.analyzer.trading_conditions['bb_squeeze']}\n"
            message += f"- 밴드 확장: {self.analyzer.trading_conditions['bb_expansion']}\n"
            
            message += f"\n총 {len(all_tickers)}개 중 가격 상위 20개 표시"
            
            # 메시지가 너무 길 경우 분할 전송
            max_length = 4096
            if len(message) > max_length:
                messages = [message[i:i+max_length] for i in range(0, len(message), max_length)]
                for msg in messages:
                    self.telegram.send_message(msg)
            else:
                self.telegram.send_message(message)
            
        except Exception as e:
            print(f"전체 코인 분석 중 오류: {e}")
            self.telegram.send_message(f"⚠️ 코인 분석 중 오류가 발생했습니다: {e}")

    def show_help(self):
        """봇 사용법 안내"""
        message = "🤖 자동매매 봇 사용법\n\n"
        message += "/start - 봇 시작\n"
        message += "/stop - 봇 중지\n"
        message += "/status - 포지션 상태 확인\n"
        message += "/profit - 수익률 확인\n"
        message += "/market - 시장 상황 분석\n"
        message += "/coins - 거래중인 코인 목록\n"
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
        self.last_update = datetime.now()
        self.entry_time = datetime.now()  # 첫 진입 시간 추가
        self.stop_loss = -5.0
        self.max_hold_time = timedelta(hours=6)  # 최대 보유 시간 설정
        
    def should_force_sell(self):
        """강제 매도 조건 확인"""
        current_time = datetime.now()
        hold_time = current_time - self.entry_time
        return hold_time >= self.max_hold_time
    
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
    
    def add_position(self, price, quantity):
        """추가 매수"""
        if self.buy_count >= 3:
            return False, "최대 매수 횟수 초과"
        
        self.entries.append((price, quantity))
        self.buy_count += 1
        self.last_update = datetime.now()
        return True, "추가 매수 성공"
    
    def calculate_profit(self, current_price):
        """수익률 계산"""
        try:
            if not current_price or current_price <= 0 or not self.average_price:
                return 0.0
            return ((current_price - self.average_price) / self.average_price) * 100
        except Exception as e:
            print(f"수익률 계산 중 오류: {e}")
            return 0.0

class PositionManager:
    def __init__(self, upbit_api):
        self.upbit = upbit_api
        self.positions = {}  # ticker: Position
        self.max_positions = 10
        
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
    
    def get_position_status(self, ticker):
        """포지션 상태 조회"""
        if ticker not in self.positions:
            return None
            
        position = self.positions[ticker]
        current_price = pyupbit.get_current_price(ticker)
        
        return {
            'ticker': ticker,
            'average_price': position.average_price,
            'quantity': position.total_quantity,
            'buy_count': position.buy_count,
            'profit': position.calculate_profit(current_price),
            'status': position.status,
            'last_update': position.last_update
        }

    def get_positions(self):
        """모든 포지션 상태 조회"""
        positions = {}
        for ticker in self.positions:
            positions[ticker] = self.get_position_status(ticker)
        return positions

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
                telegram.send_message(f"⚠️ 오류 발생: {str(e)}\n재시작을 시도합니다.")
                time.sleep(5)
                continue
                
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