import random
import numpy as np
import matplotlib.pyplot as plt
import requests

# 1. 마틴게일 전략
def martingale_strategy(initial_investment, max_rounds, win_prob=0.5, win_amount=2):
    """
    마틴게일 전략을 적용하여 투자 진행.
    매번 손실을 볼 때마다 배수로 투자금 늘려서 손실 회복을 목표로 함.
    """
    balance = initial_investment
    round_num = 1
    bet = initial_investment
    
    while round_num <= max_rounds and balance > 0:
        print(f"라운드 {round_num}: 베팅금액 = {bet}, 현재 잔액 = {balance}")
        
        # 승패를 랜덤하게 시뮬레이션
        if random.random() < win_prob:
            balance += bet * win_amount  # 이기면 배당금 추가
            print(f"이겼습니다! 새로운 잔액: {balance}")
            bet = initial_investment  # 베팅금을 초기화
        else:
            balance -= bet  # 지면 베팅금만큼 잔액 차감
            print(f"졌습니다! 새로운 잔액: {balance}")
            bet *= 2  # 베팅금을 두 배로 늘림
        
        round_num += 1
        
        if balance <= 0:
            print("잔액이 부족합니다!")
            break
    
    return balance

# 2. 이동평균선 전략
def moving_average_strategy(prices, short_window=5, long_window=20):
    """
    이동평균선 전략을 적용하여 매수/매도 신호를 생성.
    short_window: 단기 이동평균선, long_window: 장기 이동평균선
    """
    short_ma = np.convolve(prices, np.ones(short_window)/short_window, mode='valid')
    long_ma = np.convolve(prices, np.ones(long_window)/long_window, mode='valid')

    # 매수/매도 신호 생성
    signals = []
    for i in range(len(short_ma)):
        if short_ma[i] > long_ma[i] and (i == 0 or short_ma[i-1] <= long_ma[i-1]):
            signals.append("Buy")
        elif short_ma[i] < long_ma[i] and (i == 0 or short_ma[i-1] >= long_ma[i-1]):
            signals.append("Sell")
        else:
            signals.append("Hold")

    return signals, short_ma, long_ma

# 3. 실시간 비트코인 가격 받아오기 (Upbit API 사용)
def get_bitcoin_price():
    """
    Upbit API를 사용하여 실시간 비트코인 가격을 받아오는 함수.
    """
    url = 'https://api.upbit.com/v1/ticker?markets=KRW-BTC'
    response = requests.get(url)
    data = response.json()
    return float(data[0]['trade_price'])

# 4. 종합 전략 실행
def combined_trading_strategy(initial_investment, max_rounds=10):
    """
    마틴게일 전략 + 이동평균선 전략 + 실시간 비트코인 가격을 결합한 종합 트레이딩 전략.
    """
    # 비트코인 가격 데이터를 시뮬레이션으로 생성 (실제 사용시 upbit API로 교체 가능)
    np.random.seed(0)
    prices = np.cumsum(np.random.randn(100) + 0.5) + 50  # 비트코인 가격 시뮬레이션
    
    # 이동평균선 전략 실행
    signals, short_ma, long_ma = moving_average_strategy(prices)
    
    # 시뮬레이션으로 사용할 잔액
    balance = initial_investment
    bet = initial_investment
    round_num = 1

    # 비트코인 가격과 신호를 기준으로 거래 실행
    for signal in signals[:max_rounds]:
        current_price = get_bitcoin_price()  # 실제 비트코인 가격 가져오기
        print(f"현재 비트코인 가격: {current_price}")
        
        if signal == "Buy":
            balance -= bet  # 매수시 돈 차감
            print(f"매수! 잔액: {balance}")
        elif signal == "Sell":
            balance += bet  # 매도시 돈 추가
            print(f"매도! 잔액: {balance}")
        
        # 마틴게일 전략 적용
        if signal == "Sell":
            balance = martingale_strategy(balance, max_rounds)  # 손실 회복을 위한 마틴게일 전략
        
        round_num += 1
        
        if balance <= 0:
            print("잔액이 부족하여 거래를 종료합니다!")
            break

    print(f"최종 잔액: {balance}")
    return balance

# 테스트 실행
initial_investment = 1000  # 초기 투자금
final_balance = combined_trading_strategy(initial_investment)
print(f"종합 전략 종료 후 최종 잔액: {final_balance}")
