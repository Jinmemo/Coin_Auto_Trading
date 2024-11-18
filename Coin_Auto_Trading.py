from dotenv import load_dotenv
import os
import pyupbit
import jwt
import uuid
import requests
from datetime import datetime
import time
from datetime import datetime, timedelta

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

    def send_message(self, message, parse_mode=None):
        """텔레그램으로 메시지를 보내는 함수"""
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            params = {
                'chat_id': self.chat_id,
                'text': message
            }
            # parse_mode가 지정된 경우에만 추가
            if parse_mode:
                params['parse_mode'] = parse_mode
                
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                print("텔레그램 메시지 전송 성공")
                return True
            else:
                print(f"텔레그램 메시지 전송 실패: {response.text}")
                return False
        except Exception as e:
            print(f"텔레그램 메시지 전송 중 오류 발생: {e}")
            return False

    def test_connection(self):
        """텔레그램 봇 연결 테스트"""
        test_message = "🤖 텔레그램 봇 연결 테스트"
        return self.send_message(test_message)

class MarketAnalyzer:
    def __init__(self, upbit_api):
        self.upbit = upbit_api
        self.tickers = pyupbit.get_tickers(fiat="KRW")
        # 분석할 시간대 설정
        self.timeframes = {
            'minute1': {'interval': 'minute1', 'count': 200},
            'minute5': {'interval': 'minute5', 'count': 200},
            'minute15': {'interval': 'minute15', 'count': 200},
            'minute60': {'interval': 'minute60', 'count': 200},
            'day': {'interval': 'day', 'count': 20}
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
                df = self.get_ohlcv(ticker, interval=config['interval'], count=config['count'])
                if df is None:
                    continue

                # 지표 계산
                df = self.calculate_rsi(df)
                df = self.calculate_bollinger_bands(df)
                if df is None:
                    continue
                    
                df = self.analyze_volume(df)
                
                if df.empty or df.iloc[-1].isnull().any():
                    continue

                current = df.iloc[-1]
                
                try:
                    percent_b = (current['종가'] - current['하단밴드']) / (current['상단밴드'] - current['하단밴드'])
                except:
                    percent_b = 0

                analysis_results[timeframe] = {
                    'rsi': current.get('RSI', 0),
                    'bb_bandwidth': current.get('밴드폭', 0),
                    'percent_b': percent_b,
                    'volume_increase': current.get('거래량증가율', 0)
                }

            if not analysis_results:
                return None

            current_price = pyupbit.get_current_price(ticker)
            if not current_price:
                return None
            
            return {
                'ticker': ticker,
                'current_price': current_price,
                'timeframes': analysis_results,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            print(f"{ticker} 분석 중 오류: {e}")
            return None

    def get_trading_signals(self, analysis):
        """향상된 매매 신호 생성"""
        signals = []
        
        # 15분봉 이상의 데이터만 사용
        long_timeframes = ['minute15', 'minute60', 'day']
        valid_timeframes = {k: v for k, v in analysis['timeframes'].items() if k in long_timeframes}
        
        if not valid_timeframes:
            return signals

        # 각 시간대별 신호 확인
        for timeframe, data in valid_timeframes.items():
            # RSI + 볼린저밴드 복합 신호 (동적 조건 사용)
            if data['rsi'] <= self.trading_conditions['rsi_oversold']:
                if data['percent_b'] <= (0.1 if self.market_state == 'volatile' else 0.05):
                    signals.append(('매수', f'{timeframe} RSI 과매도 + 하단밴드 접근', analysis['ticker']))
            
            elif data['rsi'] >= self.trading_conditions['rsi_overbought']:
                if data['percent_b'] >= (0.9 if self.market_state == 'volatile' else 0.95):
                    signals.append(('매도', f'{timeframe} RSI 과매수 + 상단밴드 접근', analysis['ticker']))

            # 볼린저 밴드 패턴 분석 (동적 조건 사용)
            if data['bb_bandwidth'] < self.trading_conditions['bb_squeeze']:
                signals.append(('관찰', f'{timeframe} 밴드 수축 - 브레이크아웃 대기', analysis['ticker']))
            elif data['bb_bandwidth'] > self.trading_conditions['bb_expansion']:
                if self.market_state == 'trend':
                    # 추세장에서는 더 민감하게 반응
                    if data['percent_b'] > 0.7:
                        signals.append(('매수', f'{timeframe} 추세장 상승 브레이크아웃', analysis['ticker']))
                    elif data['percent_b'] < 0.3:
                        signals.append(('매도', f'{timeframe} 추세장 하락 브레이크아웃', analysis['ticker']))
                else:
                    # 일반/변동성 장에서는 더 보수적으로
                    if data['percent_b'] > 0.8:
                        signals.append(('매수', f'{timeframe} 밴드 확장 + 상승 브레이크아웃', analysis['ticker']))
                    elif data['percent_b'] < 0.2:
                        signals.append(('매도', f'{timeframe} 밴드 확장 + 하락 브레이크아웃', analysis['ticker']))

            # 시장 상태별 추가 조건
            if self.market_state == 'volatile':
                # 변동성 장에서는 반등 기회를 더 적극적으로 포착
                if data['rsi'] < self.trading_conditions['rsi_oversold'] + 5:  # 과매도 근처
                    signals.append(('관찰', f'{timeframe} 변동성 장 매수 기회 관찰', analysis['ticker']))
            elif self.market_state == 'trend':
                # 추세장에서는 추세 추종 전략
                if data['rsi'] > 60 and data['percent_b'] > 0.6:  # 상승 추세
                    signals.append(('관찰', f'{timeframe} 상승 추세 매수 기회 관찰', analysis['ticker']))
                elif data['rsi'] < 40 and data['percent_b'] < 0.4:  # 하락 추세
                    signals.append(('관찰', f'{timeframe} 하락 추세 매도 기회 관찰', analysis['ticker']))

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
            
            # 거래량 급증 사전 알림
            if data['volume_increase'] >= self.trading_conditions['volume_surge'] * 0.8:
                alerts.append(f"{timeframe} 거래량 급증 중 ({data['volume_increase']:.2f}%)")

        if alerts:
            self.alert_sent[ticker] = current_time
            message = f"⚠️ {ticker} 매매 시그널 접근 알림\n\n"
            message += f"현재가: {format(int(analysis['current_price']), ',')}원\n"
            message += "감지된 신호:\n"
            for alert in alerts:
                message += f"- {alert}\n"
            return message
        
        return None

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

    def load_existing_positions(self):
        """기존 보유 코인을 포지션에 추가"""
        try:
            balances = self.upbit.get_balances()
            if not balances:
                return

            loaded_positions = 0
            for balance in balances:
                ticker = balance['currency']
                if ticker == 'KRW':  # KRW는 건너뛰기
                    continue

                # KRW 마켓 티커로 변환
                market_ticker = f"KRW-{ticker}"
                
                # 수량과 평균단가 확인
                quantity = float(balance['balance'])
                avg_price = float(balance['avg_buy_price'])
                
                # 1000원 이상인 포지션만 불러오기
                current_value = quantity * avg_price
                if current_value < 1000:
                    continue

                # 포지션 추가
                success, message = self.position_manager.open_position(market_ticker, avg_price, quantity)
                if success:
                    loaded_positions += 1
                    self.telegram.send_message(
                        f"💼 기존 포지션 불옴: {market_ticker}\n"
                        f"평균단가: {avg_price:,.0f}원\n"
                        f"수량: {quantity:.8f}"
                    )

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
                'offset': self.last_processed_update_id + 1,  # 마지막으로 처리한 업데이트 이후의 시지만 가져오기
                'timeout': 30
            }
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                updates = response.json()
                if 'result' in updates and updates['result']:
                    for update in updates['result']:
                        # 업데이트 ID 저장
                        self.last_processed_update_id = update['update_id']
                        
                        if 'message' in update and 'text' in update['message']:
                            command = update['message']['text']
                            if command.startswith('/'):
                                self.process_command(command)
                    
        except Exception as e:
            print(f"텔레그램 명령어 확인 중 오류: {e}")

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
        """향상된 매매 로직 구현"""
        try:
            current_price = pyupbit.get_current_price(ticker)
            print(f"매매 신호 처리 시작: {ticker}, {signal_type}, 현재가: {current_price}")  # 디버깅 로그
            
            if signal_type == '매수':
                # 새 포지션 오픈
                if ticker not in self.position_manager.positions:
                    split_amounts = self.calculate_split_orders(self.analyzer.market_state)
                    quantity = (split_amounts[0] * 0.9995) / current_price
                    print(f"매수 시도: {ticker}, 금액: {split_amounts[0]}, 수량: {quantity}")  # 디버깅 로그
                    
                    order = self.upbit.upbit.buy_market_order(ticker, split_amounts[0])
                    print(f"매수 주문 결과: {order}")  # 디버깅 로그
                    
                    if order and 'error' not in order:
                        success, message = self.position_manager.open_position(ticker, current_price, quantity)
                        if success:
                            self.send_position_update(ticker, "신규 매수 (1/3)")
                            self.position_manager.positions[ticker].remaining_orders = split_amounts[1:]
                            print(f"매수 성공: {ticker}, {message}")  # 디버깅 로그
                        return success, message
                    return False, f"매수 주문 실패: {order}"
                
                # 추가 매수
                position = self.position_manager.positions[ticker]
                if position.buy_count < 3 and hasattr(position, 'remaining_orders'):
                    next_amount = position.remaining_orders[0]
                    quantity = (next_amount * 0.9995) / current_price
                    order = self.upbit.upbit.buy_market_order(ticker, next_amount)
                    
                    if order and 'error' not in order:
                        success, message = self.position_manager.add_to_position(ticker, current_price, quantity)
                        if success:
                            self.send_position_update(ticker, f"추가 매수 ({position.buy_count}/3)")
                            position.remaining_orders = position.remaining_orders[1:]
                        return success, message
                    return False, "추가 매수 주문 실패"
                
                return False, "최대 매수 횟수 초과"
            
            elif signal_type == '매도':
                if ticker in self.position_manager.positions:
                    position = self.position_manager.positions[ticker]
                    print(f"매도 시도: {ticker}, 수량: {position.total_quantity}")  # 디버깅 로그
                    
                    order = self.upbit.upbit.sell_market_order(ticker, position.total_quantity)
                    print(f"매도 주문 결과: {order}")  # 디버깅 로그
                    
                    if order and 'error' not in order:
                        success, message = self.position_manager.close_position(ticker)
                        if success:
                            self.send_position_update(ticker, "매도")
                            print(f"매도 성공: {ticker}, {message}")  # 디버깅 로그
                        return success, message
                    return False, f"매도 주문 실패: {order}"
                
                return False, "보유하지 않은 코인"
            
        except Exception as e:
            print(f"주문 처리 중 오류 발생: {str(e)}")  # 디버깅 로그
            return False, f"주문 처리 중 오류 발생: {str(e)}"
    
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

    def monitor_market(self):
        """시장 모니터링 실행"""
        print("시장 모니터링 시작...")
        self.telegram.send_message("🤖 자동매매 봇이 시작 모니터링을 시작합니다.")
        self.is_running = True
        
        while self.is_running:
            try:
                for ticker in self.analyzer.tickers:
                    analysis = self.analyzer.analyze_market(ticker)
                    if analysis:
                        # 매매 조건 접근 알림 체크
                        alert_message = self.analyzer.check_trading_alerts(analysis)
                        if alert_message:
                            self.telegram.send_message(alert_message)
                        
                        # 기존 매매 신호 처리
                        signals = self.analyzer.get_trading_signals(analysis)
                        if signals:
                            print(f"매매 신호 발생: {ticker}, {signals}")  # 디버깅 로그
                            
                        for signal in signals:
                            action, reason, ticker = signal
                            if action in ['매수', '매도']:
                                print(f"매매 시도: {ticker}, {action}, {reason}")  # 디버깅 로그
                                success, message = self.process_buy_signal(ticker, action)
                                print(f"매매 결과: {success}, {message}")  # 디버깅 로그
                                
                                if success:
                                    self.telegram.send_message(f"✅ {ticker} {action} 성공: {reason}")
                                else:
                                    self.telegram.send_message(f"❌ {ticker} {action} 실패: {message}")
                    
                    time.sleep(0.1)
                
                time.sleep(15)
                
            except Exception as e:
                error_message = f"모니터링 중 오류 발생: {e}"
                print(error_message)
                self.telegram.send_message(f"⚠️ {error_message}")

    def show_market_analysis(self):
        """현재 시장 상황 분석 결과 전송"""
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
            if ticker not in major_coins:  # 중복 제외
                try:
                    current_volume = pyupbit.get_current_price(ticker) * \
                                   pyupbit.get_ohlcv(ticker, interval="day", count=1)['volume'].iloc[-1]
                    volume_leaders.append((ticker, current_volume))
                except:
                    continue
        
        # 거래량 기준 상위 5개 코인 선택
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
        message += f"📈 거래량 상위: {', '.join(top_volume_coins)}\n"
        
        self.telegram.send_message(message)

    def show_trading_coins(self):
        """업비트의 모든 KRW 마켓 코인 목록 및 상세 분석 결과 전송"""
        try:
            message = "🔍 전체 거래소 코인 상세 분석\n\n"
            
            # 모든 KRW 마켓 코인 가져오기
            all_tickers = pyupbit.get_tickers(fiat="KRW")
            price_data = []
            
            for ticker in all_tickers:
                try:
                    current_price = pyupbit.get_current_price(ticker)
                    if current_price:
                        price_data.append((ticker, current_price))
                except:
                    continue
            
            # 가격 기준으로 정렬 (고가 코인 우선)
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
                                message += " (과매도⤴️)"
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
                                message += "⚪ 관망\n"
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
                        else:
                            # 일반장에서는 기본 기준 적용
                            if buy_strength >= 60:
                                message += "🟢 매수 임박 (강도: {:.1f}%)\n".format(buy_strength)
                            elif sell_strength >= 60:
                                message += "🔴 매도 임박 (강도: {:.1f}%)\n".format(sell_strength)
                            elif buy_strength >= 30:
                                message += "🟡 매수 관망 (강도: {:.1f}%)\n".format(buy_strength)
                            elif sell_strength >= 30:
                                message += "🟡 매도 관망 (강도: {:.1f}%)\n".format(sell_strength)
                            else:
                                message += "⚪ 관망\n"
                    
                    # 보유 상태 확인
                    if ticker in self.position_manager.positions:
                        position = self.position_manager.get_position_status(ticker)
                        message += f"\n💼 보유 중:\n"
                        message += f"평균단가: {format(int(position['average_price']), ',')}원\n"
                        message += f"수익률: {position['profit']:.2f}%\n"
                        message += f"매수횟수: {position['buy_count']}/3\n"
                    
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

class Position:
    def __init__(self, ticker, entry_price, quantity):
        self.ticker = ticker
        self.entries = [(entry_price, quantity)]
        self.buy_count = 1
        self.status = 'active'
        self.last_update = datetime.now()
        self.stop_loss = -5.0  # 손 기준 (5%)
        self.remaining_orders = []  # 남은 분할 매수 금액
        self.sell_count = 0  # 매도 횟수
        self.profit_targets = {  # 분할 매도 목표가
            'volatile': [2.0, 3.0, 4.0],  # 변동성 장
            'trend': [3.0, 4.0, 5.0],     # 추세장
            'normal': [2.5, 3.5, 4.5]     # 일반장
        }
        
    def check_stop_loss(self, current_price):
        """손절 조건 확인"""
        profit = self.calculate_profit(current_price)
        return profit <= self.stop_loss
    
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
        return ((current_price - self.average_price) / self.average_price) * 100

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
            
            # 매도 주문
            self.upbit.upbit.sell_market_order(ticker, position.total_quantity)
            
            # 포지션 제거
            del self.positions[ticker]
            return True, "포지션 종료 성공"
        except Exception as e:
            return False, f"매도 실패: {str(e)}"

if __name__ == "__main__":
    upbit = UpbitAPI()
    telegram = TelegramBot()
    analyzer = MarketAnalyzer(upbit)
    monitor = MarketMonitor(upbit, telegram, analyzer)
    
    try:
        # 시작 전 연결 테스트
        if telegram.test_connection():
            telegram.send_message("🤖 자동매매 봇이 시작되었습니다.\n명령어 목록을 보려면 /help를 입력하세요.")
            
            while True:
                try:
                    # 텔레그램 명령어 확인
                    monitor.check_telegram_commands()
                    
                    # 봇이 실행 중일 때만 시장 모니터링
                    if monitor.is_running:
                        for ticker in analyzer.tickers:
                            analysis = analyzer.analyze_market(ticker)
                            if analysis:
                                # 매매 조건 접근 알림 체크
                                alert_message = analyzer.check_trading_alerts(analysis)
                                if alert_message:
                                    telegram.send_message(alert_message)
                                
                                # 기존 매매 신호 처리
                                signals = analyzer.get_trading_signals(analysis)
                                for signal in signals:
                                    action, reason, ticker = signal
                                    if action in ['매수', '매도']:
                                        success, message = monitor.process_buy_signal(ticker, action)
                                        if success:
                                            telegram.send_message(f"✅ {ticker} {action} 성공: {reason}")
                                        else:
                                            telegram.send_message(f"❌ {ticker} {action} 실패: {message}")
                            time.sleep(0.1)  # API 호출 제한 방지
                    
                    time.sleep(1)  # CPU 사용량 감소
                    
                except Exception as e:
                    error_message = f"모니터링 중 오류 발생: {e}"
                    print(error_message)
                    telegram.send_message(f"⚠️ {error_message}")
                    time.sleep(5)
                    
    except KeyboardInterrupt:
        print("\n프로그램 종료...")
        telegram.send_message("🔴 자동매매 봇이 종료되었니다.")
    except Exception as e:
        error_message = f"프로그램 실행 중 오류 발생: {e}"
        print(error_message)
        telegram.send_message(f"⚠️ {error_message}")