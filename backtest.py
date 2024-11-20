import pyupbit
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import traceback

class BackTester:
    def __init__(self):
        self.initial_capital = 1000000
        self.capital = self.initial_capital
        self.available_capital = self.initial_capital
        self.invested_capital = 0
        self.positions = {}
        self.max_positions = 10
        self.results = {}
        
    def calculate_indicators(self, df):
        try:
            if df is None or len(df) < 20:
                return None
            
            df = df.copy()
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['high'] = pd.to_numeric(df['high'], errors='coerce')
            df['low'] = pd.to_numeric(df['low'], errors='coerce')
            df = df.dropna()
            
            if len(df) < 20:
                return None

            # RSI 계산 (EMA 방식)
            delta = df['close'].diff()
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
            
            df['MA20'] = df['close'].rolling(window=window).mean()
            band = unit * df['close'].rolling(window=window).std(ddof=0)
            
            df['upper_band'] = df['MA20'] + band
            df['lower_band'] = df['MA20'] - band
            
            # %B 계산
            df['%B'] = (df['close'] - df['lower_band']) / (df['upper_band'] - df['lower_band'])
            
            # 밴드폭 계산
            df['bb_bandwidth'] = (df['upper_band'] - df['lower_band']) / df['MA20'] * 100

            df = df.dropna()
            return df

        except Exception as e:
            return None

    def get_trading_signals(self, row):
        """매매 신호 생성"""
        try:
            signals = []
            
            rsi = row['RSI']
            bb_bandwidth = row['bb_bandwidth']
            percent_b = row['%B']
            
            # ��수 신호 (더 엄격한 조건)
            if rsi <= 20:  # RSI가 20 이하로 매우 낮을 때 (25 → 20)
                if percent_b < 0.05:  # 볼린저 밴드 하단을 크게 벗어남 (0.1 → 0.05)
                    if bb_bandwidth > 1.0:  # 변동성이 충분히 높을 때
                        signals.append(('매수', 1.5))
                elif percent_b < 0.2:  # 볼린저 밴드 하단 영역 (0.3 → 0.2)
                    if bb_bandwidth > 1.0:
                        signals.append(('매수', 1.2))
                        
            elif rsi <= 25:  # RSI가 25 이하일 때 (30 → 25)
                if percent_b < 0.1 and bb_bandwidth > 1.0:  # 밴드 하단 + 높은 변동성
                    signals.append(('매수', 1.0))
            
            # 매도 신호 (더 엄격한 조건)
            elif rsi >= 80:  # RSI가 80 이상으로 매우 높을 때 (75 → 80)
                if percent_b > 0.95:  # 볼린저 밴드 상단을 크게 벗어남 (0.9 → 0.95)
                    if bb_bandwidth > 1.0:
                        signals.append(('매도', 1.5))
                elif percent_b > 0.8:  # 볼린저 밴드 상단 영역 (0.7 → 0.8)
                    if bb_bandwidth > 1.0:
                        signals.append(('매도', 1.2))
                        
            elif rsi >= 75:  # RSI가 75 이상일 때 (70 → 75)
                if percent_b > 0.9 and bb_bandwidth > 1.0:  # 밴드 상단 + 높은 변동성
                    signals.append(('매도', 1.0))

            return signals
                
        except Exception as e:
            print(f"매매 신호 생성 중 오류: {str(e)}")
            return []

    def execute_trade(self, ticker, signal, price, time):
        """매매 실행"""
        try:
            action, strength = signal
            
            if action == '매수' and len(self.positions) < self.max_positions:
                # 사용 가능한 자본 계산
                max_position_size = self.available_capital * 0.1 * strength
                
                if ticker in self.positions:
                    # 추가매수 전략 수정
                    position = self.positions[ticker]
                    if position['buy_count'] >= 3:  # 최대 3회
                        return False
                    
                    # 기존 포지션 분석
                    entries = position['entries']
                    total_quantity = sum(qty for _, qty in entries)
                    total_investment = sum(p * q for p, q in entries)
                    avg_price = total_investment / total_quantity
                    
                    # 추가매수 가격 조건
                    price_drop = ((avg_price - price) / avg_price) * 100
                    
                    # 단계별 추가매수 전략
                    if position['buy_count'] == 1 and price_drop >= 1.2:
                        # 첫 번째 추가매수: 1.2% 하락 시 100% 추가
                        quantity = (total_quantity * 1.0)
                    elif position['buy_count'] == 2 and price_drop >= 2.0:
                        # 두 번째 추가매수: 2.0% 하락 시 120% 추가
                        quantity = (total_quantity * 1.2)
                    else:
                        return False
                    
                    total_cost = price * quantity
                    if total_cost > self.available_capital:
                        return False
                    
                    position['entries'].append((price, quantity))
                    position['buy_count'] += 1
                    
                    # 자본금 업데이트
                    self.available_capital -= total_cost
                    self.invested_capital += total_cost
                    
                    return True
                    
                else:
                    # 신규 매수 (기존과 동일)
                    quantity = max_position_size / price
                    total_cost = price * quantity
                    
                    if total_cost > self.available_capital:
                        return False
                    
                    self.positions[ticker] = {
                        'entries': [(price, quantity)],
                        'entry_time': time,
                        'buy_count': 1
                    }
                    
                    self.available_capital -= total_cost
                    self.invested_capital += total_cost
                    
                    return True
                
            elif action == '매도' and ticker in self.positions:
                position = self.positions[ticker]
                total_quantity = sum(qty for _, qty in position['entries'])
                total_investment = sum(p * q for p, q in position['entries'])
                
                # 매도 금액 계산
                sell_amount = price * total_quantity
                fee = sell_amount * 0.0015
                net_amount = sell_amount - fee
                
                # 순손익 계산
                profit = net_amount - total_investment
                
                # 자본금 업데이트
                self.available_capital += net_amount
                self.invested_capital -= total_investment
                self.capital = self.available_capital + self.invested_capital
                
                # 거래 결과 저장
                if ticker not in self.results:
                    self.results[ticker] = []
                
                self.results[ticker].append({
                    'entry_time': position['entry_time'],
                    'exit_time': time,
                    'entry_price': total_investment/total_quantity,
                    'exit_price': price,
                    'profit_rate': ((price/(total_investment/total_quantity)) - 1) * 100,
                    'profit': profit,
                    'quantity': total_quantity,
                    'hold_time': time - position['entry_time']
                })
                
                del self.positions[ticker]
                return True
                
            return False
            
        except Exception as e:
            return False

    def run_backtest(self, data):
        """백테스팅 실행"""
        try:
            for ticker, df in data.items():
                # 지표 계산
                df = self.calculate_indicators(df)
                if df is None:
                    continue
                    
                # 매매 신호 처리
                for index, row in df.iterrows():
                    signals = self.get_trading_signals(row)
                    
                    for signal in signals:
                        self.execute_trade(ticker, signal, row['close'], index)
                        
                    # 보유 포지션 관리
                    self.manage_positions(ticker, row, index)
                    
            return self.analyze_results()
            
        except Exception as e:
            return None

    def manage_positions(self, ticker, row, time):
        """포지션 관리 (손절/익절)"""
        if ticker in self.positions:
            position = self.positions[ticker]
            entries = position['entries']
            
            # 총 수량과 평균단가 계산
            total_quantity = sum(qty for _, qty in entries)
            total_investment = sum(p * q for p, q in entries)
            avg_price = total_investment / total_quantity
            
            current_price = row['close']
            profit_rate = ((current_price/avg_price) - 1) * 100
            hold_time = time - position['entry_time']
            
            # 손절/익절 조건 수정
            if (profit_rate <= -2.5 or                    # 손절: -2.5%
                profit_rate >= 5.0 or                     # 익절: 5.0%
                (hold_time.total_seconds() >= 21600 and   # 6시간 초과 & 수익 중
                 profit_rate > 0)):
                
                # 매도 실행
                success = self.execute_trade(ticker, ('매도', 1.0), current_price, time)
                if success:
                    print(f"[INFO] {ticker} 청산 완료 (수익률: {profit_rate:.2f}%)")
                else:
                    print(f"[ERROR] {ticker} 청산 실패")

    def analyze_results(self):
        """백테스팅 결과 분석"""
        total_trades = 0
        winning_trades = 0
        total_profit = 0
        
        for ticker, trades in self.results.items():
            for trade in trades:
                total_trades += 1
                if trade['profit'] > 0:
                    winning_trades += 1
                total_profit += trade['profit']
        
        return {
            'initial_capital': self.initial_capital,
            'final_capital': self.capital,
            'total_return': ((self.capital/self.initial_capital) - 1) * 100,
            'total_trades': total_trades,
            'win_rate': (winning_trades/total_trades*100) if total_trades > 0 else 0,
            'profit': total_profit
        }

def get_backtest_data():
    """백테스팅용 데이터 준비"""
    tickers = pyupbit.get_tickers(fiat="KRW")[:20]  # 상위 5개 코인
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)  # 1주일 데이터
    
    data = {}
    
    for ticker in tickers:
        try:
            # 1분봉 데이터 조회
            df = pyupbit.get_ohlcv(ticker, interval="minute1", to=end_date, count=10080)  # 7일 * 24시간 * 60분
            
            if df is None or len(df) < 100:  # 최소 100개 데이터 필요
                continue
                
            # 컬럼명 변경
            df.columns = ['open', 'high', 'low', 'close', 'volume', 'value']
            
            # 거래량 0인 구간 제거
            df = df[df['volume'] > 0].copy()
            
            # 결측치 처리
            df = df.dropna()
            
            if len(df) > 100:  # 유효 데이터 최종 확인
                data[ticker] = df
            
        except Exception as e:
            print(f"{ticker} 데이터 수집 실패: {str(e)}")
            continue
            
    return data

def plot_results(ticker, df, trades):
    """백테스팅 결과 시각화"""
    plt.figure(figsize=(15,7))
    
    # 가격 차트
    plt.plot(df.index, df['close'], label='Price', alpha=0.7)
    
    # 매수/매도 지점 표시
    for trade in trades:
        try:
            # 수익 거래는 진한 색으로
            alpha = 1.0 if trade['profit'] > 0 else 0.5
            
            # 매수 지점
            plt.scatter(trade['entry_time'], trade['entry_price'], 
                       color='green', marker='^', s=100, alpha=alpha,
                       label='Buy' if trade['profit'] > 0 else None)
                       
            # 매도 지점
            plt.scatter(trade['exit_time'], trade['exit_price'], 
                       color='red', marker='v', s=100, alpha=alpha,
                       label='Sell' if trade['profit'] > 0 else None)
                       
            # 수익률 표시
            plt.annotate(f"{trade['profit_rate']:.1f}%", 
                        (trade['exit_time'], trade['exit_price']),
                        xytext=(10, 10), textcoords='offset points')
                        
        except KeyError as e:
            print(f"거래 데이터 누락: {e}")
            continue
    
    plt.title(f'{ticker} Backtest Results')
    plt.legend()
    plt.grid(True)
    plt.show()

# 데이터 수집 및 백테스팅 실행
if __name__ == "__main__":
    print("백테스팅 데이터 수집 중...")
    data = get_backtest_data()
    
    if not data:
        print("데이터 수집 실패")
        exit()
        
    print(f"\n총 {len(data)}개 코인에 대해 백테스팅 시작\n")
    
    backtest = BackTester()
    results = backtest.run_backtest(data)
    
    if results:
        print("\n백테스팅 결과:")
        print(f"초기자본: {results['initial_capital']:,.0f}원")
        print(f"최종자본: {results['final_capital']:,.0f}원")
        print(f"총수익률: {results['total_return']:.2f}%")
        print(f"총거래수: {results['total_trades']}회")
        print(f"승률: {results['win_rate']:.2f}%")
        print(f"순수익: {results['profit']:,.0f}원")