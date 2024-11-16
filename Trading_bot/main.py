import sys
from pathlib import Path
import platform
import signal

# 프로젝트 루트 디렉토리를 Python 경로에 추가
current_dir = Path(__file__).parent  # Trading_bot 디렉토리
project_root = current_dir.parent  # Coin_Auto_Trading 디렉토리
sys.path.append(str(project_root))

# 상대 경로로 임포트
from Trading_bot.core.trader import Trader
from Trading_bot.utils.telegram import TelegramNotifier

import asyncio
import logging
from datetime import datetime
import traceback

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
        await notifier.initialize()
        
        # 트레이더 초기화
        trader = Trader()
        
        # 상호 참조 설정
        trader.set_notifier(notifier)
        notifier.set_trader(trader)
        
        # 트레이더 초기화
        await trader.initialize()
        
        logger.info("트레이딩 봇 초기화 완료")
        return True
        
    except Exception as e:
        error_msg = f"봇 초기화 중 오류 발생: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)
        if notifier:
            await notifier.send_message(f"⚠️ {error_msg}")
        return False

async def cleanup():
    """프로그램 종료 처리"""
    try:
        logger.info("프로그램 종료 시작")
        
        # 트레이더 종료
        if trader and trader.is_running:
            try:
                await trader.stop()
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"트레이더 종료 중 오류: {str(e)}")
        
        # 노티파이어 종료
        if notifier and notifier._is_running:
            try:
                await notifier.stop()
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"노티파이어 종료 중 오류: {str(e)}")
        
        logger.info("프로그램 종료 완료")
        
    except Exception as e:
        logger.error(f"종료 처리 중 오류: {str(e)}")

async def main():
    """메인 함수"""
    try:
        logger.info("트레이딩 봇 시작")
        
        # 봇 초기화
        if not await init_bot():
            logger.error("봇 초기화 실패")
            return

        # Windows와 Unix 플랫폼에 따른 시그널 처리
        if platform.system() != 'Windows':
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(handle_shutdown(s)))
        else:
            # Windows에서는 KeyboardInterrupt로 처리
            signal.signal(signal.SIGINT, lambda sig, frame: asyncio.create_task(handle_shutdown(sig)))
            signal.signal(signal.SIGTERM, lambda sig, frame: asyncio.create_task(handle_shutdown(sig)))

        logger.info("메인 루프 시작")
        
        # 메인 루프
        while True:
            try:
                # 트레이더 상태 확인
                if not trader or not trader.is_running:
                    logger.error("트레이더가 실행 중이 아닙니다")
                    break
                
                # 노티파이어 상태 확인
                if not notifier or not notifier._is_running:
                    logger.error("노티파이어가 실행 중이 아닙니다")
                    break
                
                # 주기적인 상태 체크
                await trader.check_status()
                
                # CPU 부하 방지를 위한 대기
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                logger.info("메인 루프가 취소되었습니다")
                break
            except Exception as e:
                logger.error(f"메인 루프 실행 중 오류: {str(e)}\n{traceback.format_exc()}")
                await asyncio.sleep(5)

    except asyncio.CancelledError:
        logger.info("프로그램이 취소되었습니다")
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {str(e)}\n{traceback.format_exc()}")
    finally:
        await cleanup()

async def handle_shutdown(sig):
    """종료 시그널 처리"""
    try:
        logger.info("프로그램 종료 신호를 받았습니다")
        
        # 종료 메시지 전송
        if notifier and notifier._is_running:
            try:
                await notifier.send_message("🛑 프로그램 종료 신호를 받았습니다. 안전하게 종료합니다...")
            except:
                pass
        
        # 정리 작업 수행
        await cleanup()
        
    except Exception as e:
        logger.error(f"종료 처리 중 오류: {str(e)}")

if __name__ == "__main__":
    loop = None
    try:
        start_time = datetime.now()
        logger.info(f"시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if platform.system() == 'Windows':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        def shutdown():
            """종료 처리"""
            if loop and loop.is_running():
                loop.create_task(handle_shutdown(None))
                loop.stop()
        
        # 시그널 핸들러 등록
        if platform.system() == 'Windows':
            signal.signal(signal.SIGINT, lambda s, f: shutdown())
            signal.signal(signal.SIGTERM, lambda s, f: shutdown())
        else:
            loop.add_signal_handler(signal.SIGINT, shutdown)
            loop.add_signal_handler(signal.SIGTERM, shutdown)
        
        try:
            loop.run_until_complete(main())
        except KeyboardInterrupt:
            logger.info("키보드 인터럽트 감지")
            loop.run_until_complete(handle_shutdown(None))
        except asyncio.CancelledError:
            pass
        finally:
            try:
                # 남은 태스크 정리
                pending = asyncio.all_tasks(loop)
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                
                # 이벤트 루프 종료
                if not loop.is_closed():
                    loop.close()
                
            except Exception as e:
                logger.error(f"종료 처리 중 오류: {str(e)}")
        
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {str(e)}\n{traceback.format_exc()}")
    finally:
        end_time = datetime.now()
        if trader and trader.start_time:
            running_time = end_time - trader.start_time
            minutes = running_time.total_seconds() / 60
            logger.info(f"실행 시간: {int(minutes)}분")
        logger.info(f"종료 시간: {end_time.strftime('%Y-%m-%d %H:%M:%S')}") 