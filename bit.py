import os
from dotenv import load_dotenv
import pyupbit
import telegram  
from telegram import Bot
import asyncio
import pandas as pd
import numpy as np
import time

def get_bollinger_bands(df, window=20, num_std=2):
    ma = df['close'].rolling(window=window).mean()  # 20일 동안의 평균값을 계산하여 중심선(이동 평균)을 만든다.
    std = df['close'].rolling(window=window).std()  # 표준편차는 데이터의 분산 정도를 나타낸다. 값이 클수록 변동성이 크다.
    upper = ma + (std * num_std) # 상한선 계산 : ma(이동 평균값) + 표준편차 * 2 
    lower = ma - (std * num_std) # 하한선 계산 : ma(이동 평균값) - 표준편차 * 2
    return upper, ma, lower

def get_rsi(df, period=14):
    delta = df['close'].diff()   # df['close'] : 증가데이터 , diff() 현재 값과 이전 값의 차이를 계산하여 가격 변화량을 생성
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean() # 상승폭 : 상승한 경우 값 그대로 유지, 그렇지 않으면 0처리
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean() # 하락폭 : 하락한 경우 값을 음수에서 양수로 변환, 그렇지 않으면 0처리
    rs = gain / loss # 상승폭이 크면 rs가 높아지고, 하락폭이 크면 rs가 낮아진다.
    rsi = 100 - (100 / (1 + rs))  # rsi는 0 ~ 100 사이의 값을 가진다.
                                  # 70 이상 : 과매수 상태 -> 가격 하락 가능성
                                  # 30 이하 : 과매도 상태 -> 가격 상승 가능성 
                                  # 과매수 일때는 매도 , 과매도 일때는 매수
    return rsi

async def analyze_market():
    print("시장 분석 시작...")
    tickers = pyupbit.get_tickers(fiat="KRW") # 거래 가능한 암호화폐 목록 반환
    potential_coins = []
    
    for ticker in tickers[:30]:
        try:
            print(f"분석 중: {ticker}")
            df = pyupbit.get_ohlcv(ticker, interval="minute1") # 1분 간격으로 ohlcv(시가,고가,저가,종가,거래량) 데이터 가져옴.
            if df is None:
                continue
                
            # 볼린저 밴드 계산(상한선 , 평균 , 하한선)
            upper_band, middle_band, lower_band = get_bollinger_bands(df)
            
            # RSI 계산
            rsi = get_rsi(df)
            
            # 이동평균선 계산
            ma5 = df['close'].rolling(window=5).mean()
            ma10 = df['close'].rolling(window=10).mean()
            
            current_price = df['close'].iloc[-1]    # 데이터프레임 가장 최근 종가
            current_rsi = rsi.iloc[-1]              # rsi 값중 가장 최근 값
            
            # 거래량 분석
            volume_ma = df['volume'].rolling(window=20).mean()  # 20일간 거래량
            current_volume = df['volume'].iloc[-1]              # 가장 최근 거래량
            
            print(f"{ticker} - 현재가: {current_price:,.0f}원, RSI: {current_rsi:.2f}")
            
            # 매수 조건 완화
            if (
                # RSI 조건 완화
                current_rsi < 35 and  # 30 -> 35로 완화
                
                # 볼린저 밴드 조건 완화
                current_price <= lower_band.iloc[-1] * 1.02 and  # 1.01 -> 1.02로 완화
                
                # 기본 상승 추세 확인
                (df['close'].iloc[-1] > df['close'].iloc[-2] or  # 직전 봉보다 상승
                 current_price > ma5.iloc[-1]) and  # 또는 5분선 위
                
                # 거래량 조건 완화
                current_volume > volume_ma.iloc[-1] * 1.5  # 2배 -> 1.5배로 완화
            ):
                print(f"매수 신호 감지: {ticker}")
                potential_coins.append({
                    'ticker': ticker,
                    'rsi': current_rsi,
                    'price': current_price,
                    'action': 'buy',
                    'buy_price': current_price
                })
            
            # 매도 조건은 그대로 유지
            elif (
                current_price <= df['close'].iloc[-2] * 0.985 or  # 1.5% 손절
                (current_rsi > 75 and current_price >= upper_band.iloc[-1]) or  # RSI 과매수 + 볼린저 상단
                current_price >= df['close'].iloc[-2] * 1.02  # 2% 이상 수익
            ):
                print(f"매도 신호 감지: {ticker}")
                potential_coins.append({
                    'ticker': ticker,
                    'rsi': current_rsi,
                    'price': current_price,
                    'action': 'sell'
                })
                
        except Exception as e:
            print(f"{ticker} 분석 중 에러 발생: {str(e)}")
            continue
    
    return potential_coins

async def execute_trade(upbit, coin_info, bot, chat_id):
    ticker = coin_info['ticker']
    action = coin_info['action']
    
    try:
        if action == 'buy':
            # 보유 현금 확인
            balance = upbit.get_balance("KRW")
            print(f"현재 보유 현금: {balance}원")
            
            if balance > 5000:  # 최소 주문금액 5000원 이상
                invest_amount = balance * 0.1   # 자본의 10프로만 매수
                print(f"{ticker} 매수 시도: {invest_amount}원")
                
                # 매수 주문
                result = upbit.buy_market_order(ticker, invest_amount)
                print(f"매수 주문 결과: {result}")
                
                if result and 'error' not in result:
                    message = f"매수 성공!\n코인: {ticker}\n투자금액: {invest_amount:,.0f}원\nRSI: {coin_info['rsi']:.2f}"
                    await bot.send_message(chat_id=chat_id, text=message)
                else:
                    print(f"매수 실패: {result}")
            else:
                print(f"잔액 부족. 현재 잔액: {balance}원")
        
        elif action == 'sell':
            # 보유 수량 확인
            balance = upbit.get_balance(ticker)
            print(f"{ticker} 현재 보유수량: {balance}")
            
            if balance > 0:
                print(f"{ticker} 매도 시도: {balance}개")
                
                # 매도 주문
                result = upbit.sell_market_order(ticker, balance)
                print(f"매도 주문 결과: {result}")
                
                if result and 'error' not in result:
                    message = f"매도 성공!\n코인: {ticker}\n매도수량: {balance}\nRSI: {coin_info['rsi']:.2f}"
                    await bot.send_message(chat_id=chat_id, text=message)
                else:
                    print(f"매도 실패: {result}")
            else:
                print(f"{ticker} 보유수량 없음")
                
    except Exception as e:
        error_message = f"거래 실행 중 에러 발생: {str(e)}"
        print(error_message)
        await bot.send_message(chat_id=chat_id, text=error_message)

async def send_telegram_message(bot, chat_id, message, max_retries=3): # max_retries=3은 api 호출 시 최대 3번만 재호출하라
    for attempt in range(max_retries):
        try:
            await bot.send_message(chat_id=chat_id, text=message)
            return True
        except Exception as e:
            print(f"텔레그램 메시지 전송 실패 (시도 {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5)  # 5초 대기 후 재시도
    return False

async def main():
    print("프로그램 시작")
    load_dotenv()  # env 파일 road 라이브러리
    print("환경변수 로드 완료")
    
    UPBIT_ACCESS_KEY = os.getenv('UPBIT_ACCESS_KEY')
    UPBIT_SECRET_KEY = os.getenv('UPBIT_SECRET_KEY')
    TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

    upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
    bot = Bot(token=TELEGRAM_TOKEN)

    print("업비트, 텔레그램 연동 완료")
    
    # 텔레그램 시작 메시지 전송 (실패해도 프로그램은 계속 실행)
    await send_telegram_message(bot, TELEGRAM_CHAT_ID, "자동매매 봇이 시작되었습니다.")

    while True:
        try:
            print("\n새로운 분석 사이클 시작")
            potential_trades = await analyze_market() # analyze_market() 메서드는 rsi차트와 볼린저밴드 차트에서 적합한 코인을 리턴함.
            print(f"발견된 매매 신호: {len(potential_trades)}개")
            
            for trade in potential_trades:
                await execute_trade(upbit, trade, bot, TELEGRAM_CHAT_ID)
            
            print("1분 대기 시작...")
            await asyncio.sleep(60)
            
        except Exception as e:
            error_message = f"에러 발생: {str(e)}"
            print(error_message)
            # 텔레그램 에러 메시지 전송 (실패해도 계속 실행)
            await send_telegram_message(bot, TELEGRAM_CHAT_ID, error_message)
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())