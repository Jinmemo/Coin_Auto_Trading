import logging
import aiohttp
from typing import Optional, Dict
import sys
import os

# 상대 경로로 import
from ..config.settings import settings
import asyncio
from ..strategies.base import Position
from datetime import datetime, timedelta
import hashlib

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class TelegramNotifier:
    def __init__(self):
        self.bot_token = settings.TELEGRAM_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.trader = None
        self.last_update_id = 0
        self._polling_task = None
        self._polling_lock = asyncio.Lock()
        self.session = None
        self._is_running = False
        logger.info("TelegramNotifier 초기화 완료")

    async def initialize(self):
        """초기화"""
        self.session = aiohttp.ClientSession()
        self._is_running = True
        self._polling_task = asyncio.create_task(self.start_polling())
        logger.info("TelegramNotifier 시작")

    async def start_polling(self):
        """텔레그램 메시지 폴링"""
        logger.info("텔레그램 봇 폴링 시작")
        
        while self._is_running:
            try:
                updates = await self._get_updates()
                if updates:
                    for update in updates:
                        await self._process_update(update)
                        self.last_update_id = update['update_id'] + 1
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"폴링 중 오류: {str(e)}")
                await asyncio.sleep(5)

    async def _process_update(self, update: dict):
        """업데이트 처리"""
        try:
            if 'message' in update and 'text' in update['message']:
                command = update['message']['text']
                logger.info(f"명령어 수신: {command}")
                
                if self.trader:
                    response = await self.handle_command(command)
                    if response:
                        await self.send_message(response)
                        
        except Exception as e:
            logger.error(f"업데이트 처리 중 오류: {str(e)}")

    async def handle_command(self, command: str) -> str:
        """명령어 처리"""
        try:
            if not self.trader:
                return "⚠️ 트레이더가 초기화되지 않았습니다."

            commands = {
                '/status': self._get_status_message,
                '/balance': self._get_balance_message,
                '/positions': self._get_positions_message,
                '/analysis': self._get_analysis_message,
                '/profit': self._get_profit_message,
                '/coins': self._get_coins_message,
                '/stop': self._handle_stop_command,
                '/help': lambda: self._get_help_message()
            }

            if command in commands:
                if command == '/help':
                    return commands[command]()
                return await commands[command]()
            else:
                return "❌ 알 수 없는 명령어입니다. /help를 입력하여 사용 가능한 명령어를 확인하세요."

        except Exception as e:
            logger.error(f"명령어 처리 중 오류: {str(e)}")
            return f"⚠️ 명령어 처리 중 오류가 발생했습니다: {str(e)}"

    async def _get_status_message(self) -> str:
        """상세 상태 메시지 생성"""
        try:
            await self.trader.update_balance()
            active_positions = len(self.trader.positions)
            
            # 실행 시간 계산
            uptime = datetime.now() - self.trader.start_time
            hours = int(uptime.total_seconds() // 3600)
            minutes = int((uptime.total_seconds() % 3600) // 60)
            
            message = (
                f"🤖 트레이딩 봇 상태 보고\n"
                f"━━━━━━━━━━━━━━━━\n\n"
                f"⏰ 실행 정보\n"
                f"• 시작 시간: {self.trader.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"• 실행 시간: {hours}시간 {minutes}분\n\n"
                f"💰 자산 현황\n"
                f"• 보유 잔고: {self.trader.available_balance:,.0f}원\n"
                f"• 총 포지션: {active_positions}개\n\n"
                f"📊 거래 현황\n"
                f"• 감시 코인: {len(self.trader.trading_coins)}개\n"
                f"• 거래 횟수: {self.trader.trade_stats.total_trades}회\n"
                f"• 승률: {self.trader.trade_stats.win_rate:.1f}%\n\n"
                f"🔄 마지막 업데이트: {datetime.now().strftime('%H:%M:%S')}"
            )
            return message
        except Exception as e:
            logger.error(f"상태 메시지 생성 실패: {str(e)}")
            return "⚠️ 상태 ��회 중 오류가 발생했습니다."

    async def _get_balance_message(self) -> str:
        """상세 잔고 메시지 생성"""
        try:
            await self.trader.update_balance()
            
            # 포지션별 투자금액 계산
            total_invested = sum(
                float(position.entry_price) * float(position.amount)
                for position in self.trader.positions.values()
            )
            
            message = (
                f"💰 자산 상세 현황\n"
                f"━━━━━━━━━━━━━━━━\n\n"
                f"📈 계좌 정보\n"
                f"• 보유 현금: {self.trader.available_balance:,.0f}원\n"
                f"• 투자 금액: {total_invested:,.0f}원\n"
                f"• 총 자산: {(self.trader.available_balance + total_invested):,.0f}원\n\n"
                f"📊 투자 비율\n"
                f"• 현금 비중: {(self.trader.available_balance / (self.trader.available_balance + total_invested) * 100):.1f}%\n"
                f"• 투자 비중: {(total_invested / (self.trader.available_balance + total_invested) * 100):.1f}%\n\n"
                f"🔄 마지막 업데이트: {datetime.now().strftime('%H:%M:%S')}"
            )
            return message
        except Exception as e:
            logger.error(f"잔고 메시지 생성 실패: {str(e)}")
            return "⚠️ 잔고 조회 중 오류가 발생했습니다."

    async def _get_positions_message(self) -> str:
        """상세 포지션 메시지 생성"""
        try:
            if not self.trader.positions:
                return "📍 현재 보유 중인 포지션이 없습니다."
            
            message = "📊 포지션 상세 정보\n━━━━━━━━━━━━━━━━\n\n"
            
            for market, position in self.trader.positions.items():
                coin = market.split('-')[1]
                entry_amount = float(position.entry_price) * float(position.amount)
                current_price = float(position.entry_price) * (1 + float(position.unrealized_pnl))
                pnl_percent = float(position.unrealized_pnl) * 100
                
                # 이모지 선택
                emoji = "🟢" if pnl_percent >= 0 else "🔴"
                
                message += (
                    f"{emoji} {coin}\n"
                    f"• 진입가: {float(position.entry_price):,.0f}원\n"
                    f"• 현재가: {current_price:,.0f}원\n"
                    f"• 수량: {float(position.amount):.8f}\n"
                    f"• 투자금: {entry_amount:,.0f}원\n"
                    f"• 평가금: {(entry_amount * (1 + float(position.unrealized_pnl))):,.0f}원\n"
                    f"• 수익률: {pnl_percent:+.2f}%\n"
                    f"• 보유기간: {(datetime.now() - position.entry_time).total_seconds() / 3600:.1f}시간\n\n"
                )
            
            message += f"🔄 마지막 업데이트: {datetime.now().strftime('%H:%M:%S')}"
            return message
        except Exception as e:
            logger.error(f"포지션 메시지 생성 실패: {str(e)}")
            return "⚠️ 포지션 조회 중 오류가 발생했습니다."

    async def _get_profit_message(self) -> str:
        """상세 수익 메시지 생성"""
        try:
            # 실현 손익
            realized_profit = sum(position.realized_pnl for position in self.trader.position_history)
            
            # 미실현 손익
            unrealized_profit = sum(
                float(position.unrealized_pnl) * float(position.entry_price) * float(position.amount)
                for position in self.trader.positions.values()
            )
            
            message = (
                f"💰 수익 상세 보고\n"
                f"━━━━━━━━━━━━━━━━\n\n"
                f"📈 손익 현황\n"
                f"• 실현 손익: {realized_profit:+,.0f}원\n"
                f"• 미실현 손익: {unrealized_profit:+,.0f}원\n"
                f"• 총 손익: {(realized_profit + unrealized_profit):+,.0f}원\n\n"
                f"📊 거래 통계\n"
                f"• 총 거래: {self.trader.trade_stats.total_trades}회\n"
                f"• 승리: {self.trader.trade_stats.winning_trades}회\n"
                f"• 패배: {self.trader.trade_stats.losing_trades}회\n"
                f"• 승률: {self.trader.trade_stats.win_rate:.1f}%\n"
                f"• 평균 수익: {self.trader.trade_stats.average_profit:+.2f}%\n\n"
                f"🔄 마지막 업데이트: {datetime.now().strftime('%H:%M:%S')}"
            )
            return message
        except Exception as e:
            logger.error(f"수익 메시지 생성 실패: {str(e)}")
            return "⚠️ 수익 조회 중 오류가 발생했습니다."

    async def _get_coins_message(self) -> str:
        """상세 코인 목록 메시지 생성"""
        try:
            coins = [coin.split('-')[1] for coin in self.trader.trading_coins]
            
            message = (
                f"👀 감시 중인 코인 상세 정보\n"
                f"━━━━━━━━━━━━━━━━\n\n"
                f"📊 코인 목록 ({len(coins)}개)\n"
                f"{', '.join(coins)}\n\n"
                f"💡 거래 조건\n"
                f"• RSI 기준: {self.trader.signal_generator.rsi_oversold} 이하\n"
                f"• 변동성 기준: 2% 이상\n"
                f"• 거래량 상위: {len(coins)}개\n\n"
                f"🔄 마지막 업데이트: {datetime.now().strftime('%H:%M:%S')}\n"
                f"(매 15분마다 자동 업데이트)"
            )
            return message
        except Exception as e:
            logger.error(f"코인 목록 메시지 생성 실패: {str(e)}")
            return "⚠️ 코인 목록 조회 중 오류가 발생했습니다."

    async def _get_analysis_message(self) -> str:
        """시장 분석 메시지 생성"""
        try:
            message = (
                f"📊 시장 분석 리포트\n"
                f"━━━━━━━━━━━━━━━━\n\n"
            )

            # RSI 분석
            for market in self.trader.trading_coins:
                coin = market.split('-')[1]
                
                # 기본 지표 데이터 수집
                rsi = await self.trader.signal_generator.get_rsi(market)
                current_price = await self.trader.upbit.get_current_price(market)
                candles = await self.trader.upbit.get_candles(market, count=24)  # 24시간 데이터
                
                # 24시간 변동성 계산
                high = max(candle['high_price'] for candle in candles)
                low = min(candle['low_price'] for candle in candles)
                volatility = ((high - low) / low) * 100
                
                # 24시간 거래량
                volume_24h = sum(candle['candle_acc_trade_volume'] for candle in candles)
                volume_price_24h = sum(candle['candle_acc_trade_price'] for candle in candles)
                
                # 이동평균선 계산
                ma5 = sum(candle['trade_price'] for candle in candles[:5]) / 5
                ma20 = sum(candle['trade_price'] for candle in candles[:20]) / 20
                
                # 추세 판단
                trend = "↗️ 상승" if ma5 > ma20 else "↘️ 하락" if ma5 < ma20 else "➡️ 횡보"
                
                # RSI 상태에 따른 이모지와 매매 신호
                if rsi <= 30:
                    status = "💚 과매도 구간"
                    signal = "⚡ 매수 신호"
                elif rsi >= 70:
                    status = "❤️ 과매수 구간"
                    signal = "⚡ 매도 신호"
                else:
                    status = "💛 중립 구간"
                    signal = "✋ 관망"
                
                # 가격 변동 그래프 (간단한 ASCII 차트)
                prices = [candle['trade_price'] for candle in reversed(candles[:12])]
                max_price = max(prices)
                min_price = min(prices)
                price_range = max_price - min_price
                chart = ""
                if price_range > 0:
                    for price in prices:
                        normalized = int((price - min_price) / price_range * 8)
                        chart += "█" * normalized + "▁" * (8 - normalized) + " "
                
                message += (
                    f"🪙 {coin}\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"💰 가격 정보\n"
                    f"• 현재가: {current_price:,.0f}원\n"
                    f"• 고가: {high:,.0f}원\n"
                    f"• 저가: {low:,.0f}원\n"
                    f"• 변동성: {volatility:.1f}%\n\n"
                    f"📈 12시간 가격 추이\n"
                    f"{chart}\n\n"
                    f"📊 기술적 지표\n"
                    f"• RSI: {rsi:.1f} ({status})\n"
                    f"• 5시간 이평선: {ma5:,.0f}원\n"
                    f"• 20시간 이평선: {ma20:,.0f}원\n"
                    f"• 추세: {trend}\n\n"
                    f"💎 거래��� 정보\n"
                    f"• 24시간 거래량: {volume_24h:.1f} {coin}\n"
                    f"• 24시간 거래대금: {volume_price_24h:,.0f}원\n\n"
                    f"📱 매매 신호\n"
                    f"• 현재 상태: {signal}\n"
                    f"• 투자 전략: {'적극 매수 고려' if rsi <= 25 else '매수 고려' if rsi <= 30 else '매도 고려' if rsi >= 70 else '관망'}\n\n"
                )

            message += (
                f"💡 참고사항\n"
                f"• RSI: 30 이하(과매도), 70 이상(과매수)\n"
                f"• 이동평균: 단기>장기(상승추세), 단기<장기(하락추세)\n"
                f"• 변동성: 일일 고가와 저가의 변동폭\n\n"
                f"🔄 마지막 업데이트: {datetime.now().strftime('%H:%M:%S')}"
            )
            return message

        except Exception as e:
            logger.error(f"시장 분석 메시지 생성 실패: {str(e)}")
            return "⚠️ 시장 분석 중 오류가 발생했습니다."

    async def _handle_stop_command(self) -> str:
        """봇 종료 처리"""
        try:
            message = (
                f"🛑 트레이딩 봇 종료\n"
                f"━━━━━━━━━━━━━━━━\n\n"
                f"📈 최종 실행 결과\n"
            )

            # 최종 잔고 업데이트
            await self.trader.update_balance()
            
            # 실현 손익
            realized_profit = sum(position.realized_pnl for position in self.trader.position_history)
            
            # 미실현 손익
            unrealized_profit = sum(
                float(position.unrealized_pnl) * float(position.entry_price) * float(position.amount)
                for position in self.trader.positions.values()
            )
            
            # 실행 시간 계산
            uptime = datetime.now() - self.trader.start_time
            hours = int(uptime.total_seconds() // 3600)
            minutes = int((uptime.total_seconds() % 3600) // 60)

            message += (
                f"• 실행 시간: {hours}시간 {minutes}분\n"
                f"• 최종 잔고: {self.trader.available_balance:,.0f}원\n"
                f"• 실현 손익: {realized_profit:+,.0f}원\n"
                f"• 미실현 손익: {unrealized_profit:+,.0f}원\n"
                f"• 총 거래: {self.trader.trade_stats.total_trades}회\n"
                f"• 승률: {self.trader.trade_stats.win_rate:.1f}%\n\n"
                f"🙏 이용해 주셔서 감사합니다.\n"
                f"봇을 안전하게 종료합니다..."
            )

            # 실제 종료 처리
            await self.trader.stop()
            return message

        except Exception as e:
            logger.error(f"종료 처리 실패: {str(e)}")
            return "⚠️ 봇 종료 중 오류가 발생했습니다."

    async def stop(self):
        """종료"""
        try:
            logger.info("TelegramNotifier 종료 시작")
            self._is_running = False
            
            # 폴링 태스크 취소
            if self._polling_task and not self._polling_task.done():
                self._polling_task.cancel()
                try:
                    await self._polling_task
                except asyncio.CancelledError:
                    pass
            
            # 세션 종료
            if self.session:
                if not self.session.closed:
                    await self.session.close()
                
                # 모든 커넥션이 정리될 때까지 대기
                await asyncio.sleep(0.25)
            
            logger.info("TelegramNotifier 종료 완료")
        except Exception as e:
            logger.error(f"TelegramNotifier 종료 중 오류: {str(e)}")

    def set_trader(self, trader):
        """트레이더 설정"""
        self.trader = trader
        logger.info("트레이더 설정 완료")

    async def _get_updates(self, timeout: int = 30) -> list:
        """텔레그램 업데이트 조회"""
        try:
            url = f"{self.base_url}/getUpdates"
            params = {
                'offset': self.last_update_id,
                'timeout': timeout,
                'allowed_updates': ['message']
            }
            
            if not self.session or self.session.closed:
                self.session = aiohttp.ClientSession()
                
            async with self.session.get(url, params=params, timeout=timeout) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('ok'):
                        return data.get('result', [])
                    else:
                        logger.error(f"텔레그램 API 오류: {data.get('description')}")
                        return []
                else:
                    logger.error(f"텔레그램 API 응답 오류: {response.status}")
                    return []
                
        except asyncio.TimeoutError:
            logger.debug("텔레그램 업데이트 타임아웃 (정상)")
            return []
        except aiohttp.ClientError as e:
            logger.error(f"텔레그램 API 연결 오류: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"텔레그램 업데이트 조회 중 오류: {str(e)}")
            return []

    async def send_message(self, text: str) -> bool:
        """텔레그램 메시지 전송"""
        try:
            url = f"{self.base_url}/sendMessage"
            data = {
                'chat_id': self.chat_id,
                'text': text,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            
            if not self.session or self.session.closed:
                self.session = aiohttp.ClientSession()
                
            async with self.session.post(url, json=data) as response:
                if response.status == 200:
                    response_data = await response.json()
                    if response_data.get('ok'):
                        logger.info("메시지 전송 성공")
                        return True
                    else:
                        logger.error(f"메시지 전송 실패: {response_data.get('description')}")
                        return False
                else:
                    logger.error(f"메시지 전송 실패: {response.status}")
                    return False
                
        except Exception as e:
            logger.error(f"메시지 전송 중 오류: {str(e)}")
            return False

    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        await self.stop()

    def __del__(self):
        """소멸자"""
        if self.session and not self.session.closed:
            if asyncio.get_event_loop().is_running():
                asyncio.create_task(self.session.close())
            else:
                logger.warning("세션이 제대로 종료되지 않았을 수 있습니다")
