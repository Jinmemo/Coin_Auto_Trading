import pyupbit
import ta
import pandas as pd
import numpy as np

# 매도/매수 설정
access = "본인의 고유 Access Key"
secret = "본인의 고유 Secret Key"

# 업비트 API 요청
upbit = pyupbit.Upbit(access, secret)

# 기본 자금 설정
money = 10000  # 매수 시 투자 금액
rsi_buy_threshold = 55  # 매수 RSI 기준
rsi_sell_threshold = 70  # 매도 RSI 기준

# 2. 데이터 준비 및 지표 계산
df = pyupbit.get_ohlcv("KRW-BTC", interval="day", count=365)  # 1년치 일간 데이터

# RSI 계산
df['RSI'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()

# 이동평균선 (단타용 MA5, 장타용 MA20)
df['MA5'] = df['close'].rolling(window=5).mean()    # 5일 이동평균 (단타)
df['MA20'] = df['close'].rolling(window=20).mean()   # 20일 이동평균 (장타)

# 3. 시그널 초기화
df['Signal'] = 0
df['current_flow'] = 0

# 4. 매수/매도 조건
for i in range(20, len(df)):
    if pd.notna(df['RSI'].iloc[i]):
        # 단타 매수 조건 (RSI 55 이하, MA5가 MA20을 상향 돌파)
        buy_short_term = (
            df['RSI'].iloc[i] < rsi_buy_threshold and
            df['MA5'].iloc[i] > df['MA20'].iloc[i] and
            df['MA5'].iloc[i-1] < df['MA20'].iloc[i-1]
        )
        
        # 장타 매수 조건 (RSI 55 이하, 장기 상승 추세)
        buy_long_term = (
            df['RSI'].iloc[i] < rsi_buy_threshold and
            df['close'].iloc[i] > df['MA20'].iloc[i]
        )
        
        # 매도 조건 (RSI 70 이상, MA5가 MA20을 하향 돌파)
        sell_condition = (
            df['RSI'].iloc[i] > rsi_sell_threshold or
            (df['MA5'].iloc[i] < df['MA20'].iloc[i] and df['MA5'].iloc[i-1] > df['MA20'].iloc[i-1])
        )
        
        # 매수 및 매도 시그널 기록
        if buy_short_term or buy_long_term:
            df.iloc[i, df.columns.get_loc('Signal')] = 1  # 매수 시그널
            df.iloc[i, df.columns.get_loc('current_flow')] = -money
        elif sell_condition:
            df.iloc[i, df.columns.get_loc('Signal')] = -1  # 매도 시그널

# 5. 매수량 계산
df['buy_cnt'] = 0
df.loc[df['Signal'] == 1, 'buy_cnt'] = money / df['open']

# 6. 누적 계산
df['accumulated_buy_cnt'] = 0
df['return'] = 0
accumulated_sum = 0
accumulated_money = 0

for index, row in df.iterrows():
    if row['Signal'] == -1 and accumulated_money != 0:
        df.at[index, 'accumulated_buy_cnt'] = accumulated_sum
        df.at[index, 'return'] = accumulated_sum * row['open'] / accumulated_money
        df.at[index, 'current_flow'] = accumulated_sum * row['open']
        accumulated_sum = 0
        accumulated_money = 0
    
    if row['Signal'] == 1:
        accumulated_sum += row['buy_cnt']
        accumulated_money += money

# 결과 출력
total_buy_amount = -df.loc[df['current_flow'] < 0, 'current_flow'].sum()
total_sell_amount = df.loc[df['current_flow'] > 0, 'current_flow'].sum()
win_trades = len(df[df['return'] > 1])
total_trades = len(df[df['return'] != 0])
win_rate = win_trades / total_trades if total_trades > 0 else 0

print("=== 백테스팅 결과 ===")
print(f"누적 수익률 : {(total_sell_amount/total_buy_amount-1)*100:.2f}%" if total_buy_amount > 0 else "거래 없음")
print(f"누적 매수 금액 : {round(total_buy_amount):,}원")
print(f"누적 매도 금액 : {round(total_sell_amount):,}원")
print(f"누적 수익 금액 : {round(total_sell_amount-total_buy_amount):,}원")
print(f"승률 : {win_rate*100:.2f}%")
print(f"총 거래 횟수 : {total_trades}회")
