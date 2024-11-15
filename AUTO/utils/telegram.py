import logging
import aiohttp
from typing import Optional
from Trading_bot.config.settings import settings
import asyncio

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

class TelegramNotifier:
    def __init__(self):
        self.bot_token = settings.TELEGRAM_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.trader = None
        self.last_update_id = 0
        self._polling_task = None
        self._polling_lock = asyncio.Lock()

    def set_trader(self, trader):
        """트레이더 객체 설정"""
        self.trader = trader
        # 폴링 시작
        if not self._polling_task:
            self._polling_task = asyncio.create_task(self.start_polling())
            logger.info("텔레그램 메시지 폴링 시작")

    async def start_polling(self):
        """텔레그램 메시지 폴링"""
        logger.info("텔레그램 봇 폴링 시작")
        while True:
            try:
                async with self._polling_lock:
                    updates = await self._get_updates()
                    if updates:
                        for update in updates:
                            if 'message' in update and 'text' in update['message']:
                                command = update['message']['text']
                                logger.info(f"텔레그램 명령어 수신: {command}")
                                
                                if self.trader:
                                    response = await self.trader.handle_command(command)
                                    if response:
                                        await self.send_message(response)
                            
                            self.last_update_id = update['update_id'] + 1
                
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                logger.info("텔레그램 폴링 종료")
                break
            except Exception as e:
                logger.error(f"텔레그램 폴링 중 오류 발생: {str(e)}")
                await asyncio.sleep(5)

    async def _get_updates(self, timeout=30):
        """텔레그램 업데이트 조회"""
        try:
            url = f"{self.base_url}/getUpdates"
            params = {
                'offset': self.last_update_id,
                'timeout': timeout,
                'allowed_updates': ['message']
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=35) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('result', [])
                    elif response.status == 409:
                        logger.debug("다른 getUpdates 요청이 실행 중입니다.")
                        await asyncio.sleep(1)
                        return []
                    else:
                        logger.error(f"텔레그램 업데이트 조회 실패: {response.status}")
                        return []
                    
        except asyncio.TimeoutError:
            logger.warning("텔레그램 업데이트 조회 시간 초과")
            return []
        except Exception as e:
            logger.error(f"텔레그램 업데이트 조회 실패: {str(e)}")
            return []

    async def send_message(self, message: str) -> bool:
        """텔레그램으로 메시지 전송"""
        try:
            url = f"{self.base_url}/sendMessage"
            data = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML"
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, timeout=10) as response:
                    if response.status == 200:
                        return True
                    logger.error(f"텔레그램 메시지 전송 실패: {response.status}")
                    return False

        except Exception as e:
            logger.error(f"텔레그램 메시지 전송 중 오류 발생: {str(e)}")
            return False

    async def stop(self):
        """폴링 종료"""
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
            self._polling_task = None
