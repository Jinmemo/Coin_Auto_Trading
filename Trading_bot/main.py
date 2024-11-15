import asyncio
import logging
from datetime import datetime
import sys
import traceback
from Trading_bot.core.trader import Trader
from Trading_bot.utils.telegram import TelegramNotifier

# 루트 로거 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 모듈별 로거 레벨 설정
logging.getLogger('Trading_bot.utils.telegram').setLevel(logging.ERROR)
logging.getLogger('Trading_bot.core.upbit_api').setLevel(logging.INFO)
logging.getLogger('Trading_bot.core.trader').setLevel(logging.INFO)
logging.getLogger('asyncio').setLevel(logging.ERROR)
logging.getLogger('aiohttp').setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

trader = None
notifier = None

async def init_bot():
    """봇 초기화"""
    global trader, notifier
    try:
        logger.info("트레이딩 봇 초기화 중...")
        
        # 텔레그램 노티파이어 초기화
        notifier = TelegramNotifier()
        
        # 트레이더 초기화
        trader = Trader()
        trader.notifier = notifier
        notifier.set_trader(trader)
        
        logger.info("트레이딩 봇 초기화 완료")
        return True
        
    except Exception as e:
        error_msg = f"봇 초기화 중 오류 발생: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)
        if notifier:
            await notifier.send_message(f"⚠️ {error_msg}")
        return False

async def shutdown():
    """안전한 종료 처리"""
    logger.info("트레이딩 봇 종료 중...")
    
    if trader:
        await trader.stop()
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    logger.info(f"남은 태스크 개수: {len(tasks)}")
    
    for task in tasks:
        task.cancel()
    
    logger.info("모든 태스크 취소 완료")
    await asyncio.gather(*tasks, return_exceptions=True)

async def main():
    """메인 함수"""
    try:
        # 봇 초기화
        if not await init_bot():
            logger.error("봇 초기화 실패")
            return
        
        # 트레이딩 시작
        logger.info("트레이딩 시작")
        await trader.start()
        
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 종료됩니다.")
        await shutdown()
    except Exception as e:
        error_msg = f"프로그램 실행 중 오류 발생: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)
        if notifier:
            await notifier.send_message(f"⚠️ {error_msg}")
        await shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("프로그램이 사용자에 의해 종료되었습니다.")
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {str(e)}\n{traceback.format_exc()}")
    finally:
        # 프로그램 종료 시간 출력
        end_time = datetime.now()
        if trader and trader.start_time:
            running_time = end_time - trader.start_time
            minutes = running_time.total_seconds() / 60
            logger.info(f"실행 시간: {int(minutes)}분")
        logger.info(f"종료 시간: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 강제 종료
        sys.exit(0) 