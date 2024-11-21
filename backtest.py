import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyupbit
import sqlite3
import os
from concurrent.futures import ThreadPoolExecutor
import traceback
import logging
from tqdm import tqdm

class BackTester:
    def __init__(self, start_date, end_date, initial_balance=10000000):
        """백테스터 초기화"""
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.positions = {}
        self.trades = []
        self.max_positions = 10
        
        # 매매 조건 설정 (실제 봇과 동일)
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
        
        # 로깅 설정 먼저 초기화
        self.setup_logging()
        
        # 결과 저장용 DB 설정
        self.db_path = 'backtest_results.db'
        self.init_database()

    def setup_logging(self):
        """로깅 설정"""
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        self.logger = logging.getLogger('backtest')
        self.logger.setLevel(logging.INFO)
        
        # 파일 핸들러
        fh = logging.FileHandler(f'logs/backtest_{datetime.now().strftime("%Y%m%d")}.log')
        fh.setLevel(logging.INFO)
        
        # 콘솔 핸들러
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # 포맷터
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        # 기존 핸들러 제거
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def init_database(self):
        """백테스트 결과 저장용 데이터베이스 초기화"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 백테스트 거래 내역 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS backtest_trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ticker TEXT NOT NULL,
                        entry_time TIMESTAMP NOT NULL,
                        exit_time TIMESTAMP NOT NULL,
                        entry_price REAL NOT NULL,
                        exit_price REAL NOT NULL,
                        quantity REAL NOT NULL,
                        profit_rate REAL NOT NULL,
                        profit_amount REAL NOT NULL,
                        trade_type TEXT NOT NULL,
                        buy_count INTEGER NOT NULL
                    )
                ''')
                
                # 백테스트 요약 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS backtest_summary (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        start_date TIMESTAMP NOT NULL,
                        end_date TIMESTAMP NOT NULL,
                        initial_balance REAL NOT NULL,
                        final_balance REAL NOT NULL,
                        total_trades INTEGER NOT NULL,
                        win_rate REAL NOT NULL,
                        max_drawdown REAL NOT NULL,
                        profit_factor REAL NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                conn.commit()
                self.logger.info("데이터베이스 초기화 완료")
                
        except Exception as e:
            self.logger.error(f"데이터베이스 초기화 실패: {str(e)}")
            raise

    def calculate_indicators(self, df):
        """기술적 지표 계산"""
        try:
            if df is None or len(df) < 20:
                return None
                
            # 데이터 복사 및 전처리
            df = df.copy()
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['high'] = pd.to_numeric(df['high'], errors='coerce')
            df['low'] = pd.to_numeric(df['low'], errors='coerce')
            df = df.dropna()
            
            if len(df) < 20:
                return None

            # RSI 계산 (14일)
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))

            # 볼린저 밴드 (20일, 2표준편차)
            df['MA20'] = df['close'].rolling(window=20).mean()
            std = df['close'].rolling(window=20).std()
            df['Upper'] = df['MA20'] + (std * 2)
            df['Lower'] = df['MA20'] - (std * 2)
            
            # %B 계산
            df['%B'] = (df['close'] - df['Lower']) / (df['Upper'] - df['Lower'])
            
            # 밴드폭 계산
            df['Bandwidth'] = ((df['Upper'] - df['Lower']) / df['MA20']) * 100

            return df

        except Exception as e:
            self.logger.error(f"지표 계산 중 오류: {str(e)}")
            return None

    def check_buy_signal(self, row):
        """매수 신호 확인 (실제 봇과 동일한 조건)"""
        try:
            rsi = row['RSI']
            percent_b = row['%B']
            bandwidth = row['Bandwidth']
            
            # 강한 매수 신호
            if rsi <= 20:  # RSI 20 이하
                if percent_b < 0.05 and bandwidth > 1.0:  # 밴드 하단 크게 이탈 + 높은 변동성
                    return True, 1.5  # 강한 신호 (1.5배 포지션)
                elif percent_b < 0.2 and bandwidth > 1.0:  # 밴드 하단 + 높은 변동성
                    return True, 1.2  # 중강도 신호 (1.2배 포지션)
                    
            # 일반 매수 신호
            elif rsi <= 25:  # RSI 25 이하
                if percent_b < 0.1 and bandwidth > 1.0:  # 밴드 하단 + 높은 변동성
                    return True, 1.0  # 일반 신호 (기본 포지션)
            
            return False, 0

        except Exception as e:
            self.logger.error(f"매수 신호 확인 중 오류: {str(e)}")
            return False, 0

    def check_sell_signal(self, row, position):
        """매도 신호 확인"""
        try:
            rsi = row['RSI']
            percent_b = row['%B']
            bandwidth = row['Bandwidth']
            
            # 강제 매도 조건 (손절/익절)
            entry_price = position['entry_price']
            current_price = row['close']
            profit_rate = ((current_price - entry_price) / entry_price) * 100
            hold_time = pd.Timestamp(row.name) - position['entry_time']
            
            # 손절: -2.5%
            if profit_rate <= -2.5:
                return True, "손절"
                
            # 익절: 5.0%
            if profit_rate >= 5.0:
                return True, "익절"
                
            # 시간 조건: 6시간 초과 & 수익 중
            if hold_time.total_seconds() / 3600 >= 6 and profit_rate > 0:
                return True, "시간 만료"
            
            # RSI 기반 매도 신호
            if rsi >= 80:  # RSI 80 이상
                if percent_b > 0.95 and bandwidth > 1.0:
                    return True, "RSI 과매수"
                elif percent_b > 0.8 and bandwidth > 1.0:
                    return True, "RSI 과매수"
            elif rsi >= 75:  # RSI 75 이상
                if percent_b > 0.9 and bandwidth > 1.0:
                    return True, "RSI 과매수"
            
            return False, ""

        except Exception as e:
            self.logger.error(f"매도 신호 확인 중 오류: {str(e)}")
            return False, ""
        
    def run_backtest(self, tickers):
        try:
            self.logger.info(f"백테스트 시작: {self.start_date} ~ {self.end_date}")
            
            # 전체 데이터 수집
            all_data = {}
            for ticker in tqdm(tickers, desc="데이터 수집"):
                try:
                    # 1분봉 데이터 가져오기
                    df = pyupbit.get_ohlcv(ticker, interval="minute1", 
                                        to=self.end_date, 
                                        count=7200)
                    
                    if df is not None and len(df) > 0:
                        # 지표 계산
                        df = self.calculate_indicators(df)
                        if df is not None:
                            # 백테스트 기간에 해당하는 데이터만 필터링
                            mask = (df.index >= self.start_date) & (df.index <= self.end_date)
                            df = df.loc[mask]
                            
                            if not df.empty:
                                self.logger.info(f"\n{ticker} 데이터 샘플:")
                                self.logger.info(f"데이터 기간: {df.index[0]} ~ {df.index[-1]}")
                                self.logger.info(f"데이터 개수: {len(df)}")
                                self.logger.info(f"RSI 범위: {df['RSI'].min():.2f} ~ {df['RSI'].max():.2f}")
                                self.logger.info(f"%B 범위: {df['%B'].min():.2f} ~ {df['%B'].max():.2f}")
                                self.logger.info(f"밴드폭 범위: {df['Bandwidth'].min():.2f} ~ {df['Bandwidth'].max():.2f}")
                                
                                all_data[ticker] = df
                            else:
                                self.logger.warning(f"{ticker} 해당 기간 데이터 없음")
                                
                except Exception as e:
                    self.logger.error(f"{ticker} 데이터 수집 실패: {str(e)}")
                    continue

            # 백테스트 실행
            for current_time in tqdm(pd.date_range(self.start_date, self.end_date, freq='1min'),
                                desc="백테스트 진행"):
                
                # 각 티커별로 현재 시점의 데이터가 있는지 확인
                for ticker, df in all_data.items():
                    if current_time in df.index:
                        current_data = df.loc[current_time]
                        
                        # 포지션이 있는 경우 매도 신호 확인
                        if ticker in self.positions:
                            sell_signal, reason = self.check_sell_signal(current_data, self.positions[ticker])
                            if sell_signal:
                                self.close_position(ticker, current_data['close'], current_time, reason)
                        
                        # 포지션이 없고 여유 공간이 있는 경우 매수 신호 확인
                        elif len(self.positions) < self.max_positions:
                            buy_signal, strength = self.check_buy_signal(current_data)
                            if buy_signal:
                                self.open_position(ticker, current_data['close'], current_time, strength)

            self.logger.info(f"총 거래 횟수: {len(self.trades)}")
            self.save_results()
            return self.generate_report()

        except Exception as e:
            self.logger.error(f"백테스트 실행 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            return None

    def open_position(self, ticker, price, time, strength):
        """포지션 진입"""
        try:
            # 투자 금액 계산 (전체 자산의 10%)
            investment = self.balance * 0.1 * strength
            
            if investment <= 0 or investment > self.balance:
                return False
            
            # 수량 계산
            quantity = investment / price
            
            # 포지션 기록
            self.positions[ticker] = {
                'entry_price': price,
                'quantity': quantity,
                'entry_time': time,
                'buy_count': 1,
                'investment': investment
            }
            
            # 잔고 차감
            self.balance -= investment
            
            self.logger.info(f"매수: {ticker}, 가격: {price:,.0f}, 수량: {quantity:.8f}")
            return True
            
        except Exception as e:
            self.logger.error(f"포지션 진입 중 오류: {str(e)}")
            return False

    def close_position(self, ticker, price, time, reason):
        """포지션 청산"""
        try:
            position = self.positions[ticker]
            
            # 수익률 계산
            profit_rate = ((price - position['entry_price']) / position['entry_price']) * 100
            profit_amount = (price * position['quantity']) - position['investment']
            
            # 거래 기록
            self.trades.append({
                'ticker': ticker,
                'entry_time': position['entry_time'],
                'exit_time': time,
                'entry_price': position['entry_price'],
                'exit_price': price,
                'quantity': position['quantity'],
                'profit_rate': profit_rate,
                'profit_amount': profit_amount,
                'reason': reason,
                'buy_count': position['buy_count']
            })
            
            # 잔고 업데이트
            self.balance += (price * position['quantity'])
            
            # 포지션 제거
            del self.positions[ticker]
            
            self.logger.info(f"매도: {ticker}, 가격: {price:,.0f}, 수익률: {profit_rate:.2f}%, 사유: {reason}")
            return True
            
        except Exception as e:
            self.logger.error(f"포지션 청산 중 오류: {str(e)}")
            return False        
        
    def save_results(self):
        """백테스트 결과 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 거래 내역 저장
                for trade in self.trades:
                    cursor.execute('''
                        INSERT INTO backtest_trades (
                            ticker, entry_time, exit_time, entry_price, exit_price,
                            quantity, profit_rate, profit_amount, trade_type, buy_count
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        trade['ticker'],
                        trade['entry_time'].isoformat(),
                        trade['exit_time'].isoformat(),
                        trade['entry_price'],
                        trade['exit_price'],
                        trade['quantity'],
                        trade['profit_rate'],
                        trade['profit_amount'],
                        trade['reason'],
                        trade['buy_count']
                    ))
                
                # 백테스트 요약 저장
                total_trades = len(self.trades)
                winning_trades = len([t for t in self.trades if t['profit_rate'] > 0])
                win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                
                # 최대 낙폭 계산
                balance_history = []
                current_balance = self.initial_balance
                for trade in self.trades:
                    current_balance += trade['profit_amount']
                    balance_history.append(current_balance)
                
                max_balance = self.initial_balance
                max_drawdown = 0
                for balance in balance_history:
                    max_balance = max(max_balance, balance)
                    drawdown = (max_balance - balance) / max_balance * 100
                    max_drawdown = max(max_drawdown, drawdown)
                
                # 수익 요인 계산
                total_profit = sum([t['profit_amount'] for t in self.trades if t['profit_rate'] > 0])
                total_loss = abs(sum([t['profit_amount'] for t in self.trades if t['profit_rate'] <= 0]))
                profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
                
                cursor.execute('''
                    INSERT INTO backtest_summary (
                        start_date, end_date, initial_balance, final_balance,
                        total_trades, win_rate, max_drawdown, profit_factor
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    self.start_date.isoformat(),
                    self.end_date.isoformat(),
                    self.initial_balance,
                    self.balance,
                    total_trades,
                    win_rate,
                    max_drawdown,
                    profit_factor
                ))
                
                conn.commit()
                self.logger.info("백테스트 결과 저장 완료")
                
        except Exception as e:
            self.logger.error(f"결과 저장 중 오류: {str(e)}")

    def generate_report(self):
        """백테스트 결과 보고서 생성"""
        try:
            total_trades = len(self.trades)
            if total_trades == 0:
                return "거래 내역이 없습니다."

            winning_trades = len([t for t in self.trades if t['profit_rate'] > 0])
            win_rate = (winning_trades / total_trades) * 100
            
            profit_rates = [t['profit_rate'] for t in self.trades]
            avg_profit = sum(profit_rates) / len(profit_rates)
            max_profit = max(profit_rates)
            max_loss = min(profit_rates)
            
            total_return = ((self.balance - self.initial_balance) / self.initial_balance) * 100
            
            report = f"""
📊 백테스트 결과 보고서

📅 테스트 기간: {self.start_date.strftime('%Y-%m-%d')} ~ {self.end_date.strftime('%Y-%m-%d')}

💰 자산 현황
시작 자산: {self.initial_balance:,.0f}원
최종 자산: {self.balance:,.0f}원
총 수익률: {total_return:.2f}%

📈 거래 통계
총 거래 횟수: {total_trades}회
승률: {win_rate:.2f}%
평균 수익률: {avg_profit:.2f}%
최대 수익: {max_profit:.2f}%
최대 손실: {max_loss:.2f}%

🔍 상위 수익 거래
"""
            # 상위 5개 수익 거래
            top_trades = sorted(self.trades, key=lambda x: x['profit_rate'], reverse=True)[:5]
            for i, trade in enumerate(top_trades, 1):
                report += f"{i}. {trade['ticker']}: {trade['profit_rate']:.2f}% "
                report += f"({trade['entry_time'].strftime('%m-%d %H:%M')} ~ "
                report += f"{trade['exit_time'].strftime('%m-%d %H:%M')})\n"

            return report
            
        except Exception as e:
            self.logger.error(f"보고서 생성 중 오류: {str(e)}")
            return f"보고서 생성 실패: {str(e)}"

if __name__ == "__main__":
    # 테스트 기간 설정
    start_date = "2023-10-01"
    end_date = "2023-10-31"
    initial_balance = 100000  # 1천만원
    
    print(f"[INFO] 테스트 기간: {start_date} ~ {end_date}")
    
    # 테스트할 티커 목록 (거래량 상위 20개)
    tickers = pyupbit.get_tickers(fiat="KRW")[:20]
    print(f"[INFO] 테스트할 코인: {', '.join(tickers)}")
    
    try:
        # 백테스터 인스턴스 생성
        backtest = BackTester(start_date, end_date, initial_balance)
        
        # 백테스트 실행
        print("\n[INFO] 백테스트 시작...")
        report = backtest.run_backtest(tickers)
        
        # 결과 출력
        if report:
            print("\n" + "="*50)
            print(report)
            print("="*50)
        else:
            print("\n[ERROR] 백테스트 실행 실패")
            
    except Exception as e:
        print(f"\n[ERROR] 프로그램 실행 중 오류 발생: {str(e)}")
        traceback.print_exc()