import logging
import aiohttp
from typing import Optional, Dict
import sys
import os
import ssl

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
        self.session = None
        self.bot_token = settings.TELEGRAM_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.trader = None
        self.last_update_id = 0
        self._polling_task = None
        self._polling_lock = asyncio.Lock()
        self._is_running = False
        self._is_initialized = False
        self._ssl_context = ssl.create_default_context()
        logger.info("TelegramNotifier 초기화 완료")

    async def initialize(self):
        """초기화"""
        try:
            if not self._is_initialized:
                connector = aiohttp.TCPConnector(ssl=self._ssl_context)
                self.session = aiohttp.ClientSession(connector=connector)
                self._is_running = True
                self._is_initialized = True
                self._polling_task = asyncio.create_task(self.start_polling())
                logger.info("TelegramNotifier 시작")
            return True
        except Exception as e:
            logger.error(f"TelegramNotifier 초기화 실패: {str(e)}")
            if self.session and not self.session.closed:
                await self.session.close()
            raise e

    async def close(self):
        """리소스 정리"""
        try:
            self._is_running = False
            self._is_initialized = False
            if self._polling_task:
                self._polling_task.cancel()
                try:
                    await self._polling_task
                except asyncio.CancelledError:
                    pass
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
            logger.info("TelegramNotifier 종료")
        except Exception as e:
            logger.error(f"TelegramNotifier 종료 중 오류: {str(e)}")

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
                '/signals': self._get_signals_message,
                '/settings': self._get_settings_message,
                '/risk': self._get_risk_message,
                '/start': self._handle_start_command,
                '/stop': self._handle_stop_command,
                '/restart': self._handle_restart_command,
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
            return "⚠️ 상태 조회 중 오류가 발생했습니다."

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
                f"• 현금 ��중: {(self.trader.available_balance / (self.trader.available_balance + total_invested) * 100):.1f}%\n"
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
            
            current_time = datetime.now()
            
            for market, position in self.trader.positions.items():
                coin = market.split('-')[1]
                entry_amount = float(position.entry_price) * float(position.amount)
                current_price = float(position.entry_price) * (1 + float(position.unrealized_pnl))
                pnl_percent = float(position.unrealized_pnl) * 100
                
                # 보유 시간 계산
                holding_hours = (current_time - position.entry_time).total_seconds() / 3600
                
                # 보유 기간 표시 형식 개선
                if holding_hours < 24:
                    holding_time = f"{holding_hours:.1f}시간"
                else:
                    holding_days = holding_hours / 24
                    holding_time = f"{holding_days:.1f}일 ({holding_hours:.1f}시간)"
                
                # 이모지 선택 (수익률에 따라)
                emoji = "🟢" if pnl_percent >= 0 else "🔴"
                
                message += (
                    f"{emoji} {coin}\n"
                    f"• 진입가: {float(position.entry_price):,.0f}원\n"
                    f"• 현재가: {current_price:,.0f}원\n"
                    f"• 수량: {float(position.amount):.8f}\n"
                    f"• 투자금: {entry_amount:,.0f}원\n"
                    f"• 평가금: {(entry_amount * (1 + float(position.unrealized_pnl))):,.0f}원\n"
                    f"• 수익률: {pnl_percent:+.2f}%\n"
                    f"• 보유기간: {holding_time}\n\n"
                )
            
            message += f"🔄 마지막 업데이트: {current_time.strftime('%H:%M:%S')}"
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

            for market in self.trader.trading_coins:
                try:
                    market_state = await self.trader.analyzer.analyze_market(market)
                    if market_state is None:
                        continue

                    coin = market.split('-')[1]
                    
                    # RSI 상태 판단
                    if market_state.rsi <= 30:
                        rsi_status = "💚 과매도 구간"
                    elif market_state.rsi >= 70:
                        rsi_status = "❤️ 과매수 구간"
                    elif 30 < market_state.rsi <= 45:
                        rsi_status = "💛 매수 관심 구간"
                    elif 65 <= market_state.rsi < 70:
                        rsi_status = "🧡 매도 관심 구간"
                    else:
                        rsi_status = "💛 중립 구간"

                    # 이동평균선 추세 판단
                    if market_state.ma5 > market_state.ma20 and market_state.current_price > market_state.ma5:
                        ma_trend = "↗️ 단기 상승세"
                    elif market_state.ma5 < market_state.ma20 and market_state.current_price < market_state.ma5:
                        ma_trend = "↘️ 단기 하락세"
                    else:
                        ma_trend = "➡️ 횡보세"

                    # 볼린저 밴드 위치
                    if market_state.current_price >= market_state.bb_upper * 0.99:
                        bb_position = "⚠️ 상단 밴드 근접"
                    elif market_state.current_price <= market_state.bb_lower * 1.01:
                        bb_position = "✅ 하단 밴드 근접"
                    else:
                        bb_position = "중심선 부근"

                    # 거래량 상태
                    volume_status = "🔥 거래량 급증" if market_state.volume_ratio >= 2.0 else (
                        "📈 거래량 증가" if market_state.volume_ratio >= 1.5 else "보통"
                    )
                    
                    message += (
                        f"🪙 {coin}\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"💰 가격 정보\n"
                        f"• 현재가: {market_state.current_price:,.0f}원\n"
                        f"• 등락률: {market_state.price_change:+.1f}%\n\n"
                        f"📊 기술적 지표\n"
                        f"• RSI: {market_state.rsi:.1f} ({rsi_status})\n"
                        f"• 이동평균선: {ma_trend}\n"
                        f"  - MA5: {market_state.ma5:,.0f}원\n"
                        f"  - MA20: {market_state.ma20:,.0f}원\n"
                        f"  - MA50: {market_state.ma50:,.0f}원\n"
                        f"• 볼린저 밴드: {bb_position}\n"
                        f"  - 상단: {market_state.bb_upper:,.0f}원\n"
                        f"  - 중심: {market_state.bb_middle:,.0f}원\n"
                        f"  - 하단: {market_state.bb_lower:,.0f}원\n"
                        f"• 거래량: {volume_status}\n\n"
                        f"📱 매매 신호\n"
                    )

                    # 매매 신호 판단
                    is_rsi_buy = market_state.rsi <= 30
                    is_bb_buy = market_state.current_price <= market_state.bb_lower * 1.01
                    is_buy_signal = is_rsi_buy and is_bb_buy

                    if is_buy_signal:
                        message += (
                            "• 현재 상태: ⚡ 매수 신호\n"
                            "• 투자 전략: 💪 적극 매수 고려\n"
                            f"  - RSI 과매도: {market_state.rsi:.1f}\n"
                            f"  - BB 하단 근접: {((market_state.current_price / market_state.bb_lower - 1) * 100):+.1f}%\n"
                        )
                    elif market_state.rsi >= 70 and market_state.current_price >= market_state.bb_upper * 0.99:
                        message += (
                            "• 현재 상태: 🔴 매도 신호\n"
                            "• 투자 전략: 매도 고려\n"
                            f"  - RSI 과매수: {market_state.rsi:.1f}\n"
                            f"  - BB 상단 근접: {((market_state.current_price / market_state.bb_upper - 1) * 100):+.1f}%\n"
                        )
                    else:
                        message += "• 현재 상태: ✋ 관망\n• 투자 전략: 추가 시그널 대기\n"

                    message += "\n"

                except Exception as e:
                    logger.error(f"{market} 분석 실패: {str(e)}")
                    continue

            message += (
                f"💡 참고사항\n"
                f"• RSI: 30↓(과매도), 45↓(매수관심), 65↑(매도관심), 70↑(과매수)\n"
                f"• 볼린저 밴드: 하단(매수신호), 상단(매도신호)\n"
                f"• 거래량: 1.5배↑(증가), 2.0배↑(급증)\n\n"
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
        """노티파이어 종료"""
        try:
            self._is_running = False
            self._is_initialized = False
            
            # 폴링 태스크 취소
            if self._polling_task:
                self._polling_task.cancel()
                try:
                    await self._polling_task
                except asyncio.CancelledError:
                    pass
            
            # 세션 종료
            if self.session and not self.session.closed:
                await self.session.close()
                await asyncio.sleep(0.1)  # 세션 종료 대기
            
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
                connector = aiohttp.TCPConnector(ssl=self._ssl_context)
                self.session = aiohttp.ClientSession(connector=connector)
                
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

    async def send_message(self, message: str) -> bool:
        """텔레그램 메시지 전송"""
        try:
            # 세션이 없거나 닫혀있으면 새로 생성
            if not self.session or self.session.closed:
                connector = aiohttp.TCPConnector(ssl=self._ssl_context)
                self.session = aiohttp.ClientSession(connector=connector)
                self._is_initialized = True

            # 메시지 길이 체크 및 분할
            if len(message) > 4096:
                chunks = [message[i:i+4096] for i in range(0, len(message), 4096)]
                success = True
                for chunk in chunks:
                    success &= await self._send_single_message(chunk)
                return success
            else:
                return await self._send_single_message(message)

        except Exception as e:
            logger.error(f"메시지 전송 중 오류: {str(e)}")
            return False

    async def _send_single_message(self, message: str) -> bool:
        """단일 메시지 전송"""
        try:
            url = f"{self.base_url}/sendMessage"
            
            # HTML 특수문자 이스케이프
            message = (message.replace('<', '&lt;')
                             .replace('>', '&gt;')
                             .replace('&', '&amp;'))
            
            data = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }

            async with self.session.post(url, json=data) as response:
                if response.status == 200:
                    return True
                else:
                    error_msg = await response.text()
                    logger.error(f"메시지 전송 실패: {error_msg}")
                    return False

        except aiohttp.ClientError as e:
            logger.error(f"메시지 전송 중 네트워크 오류: {str(e)}")
            # 세션 재생성
            if not self.session or self.session.closed:
                connector = aiohttp.TCPConnector(ssl=self._ssl_context)
                self.session = aiohttp.ClientSession(connector=connector)
                self._is_initialized = True
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

    def _get_help_message(self) -> str:
        """도움말 메시지 생성"""
        return (
            "🤖 트레이딩 봇 도움말\n\n"
            "📌 기본 명령어:\n"
            "/status - 봇 상태 확인\n"
            "/balance - 계좌 잔고 확인\n"
            "/positions - 보유 포지션 확인\n"
            "/profit - 수익 현황 확인\n\n"
            
            "📊 분석 명령어:\n"
            "/analysis - 실시간 매매 신호 분석\n"
            "/coins - 감시 중인 코인 목록\n"
            "/signals - 최근 매매 신호\n\n"
            
            "⚙️ 설정 명령어:\n"
            "/settings - 현재 설정 확인\n"
            "/risk - 리스크 설정 확인\n\n"
            
            "🛠️ 시스템 명령어:\n"
            "/start - 트레이딩 시작\n"
            "/stop - 트레이너 중지\n"
            "/restart - 봇 재시작\n"
            "/help - 도움말 보기\n\n"
            
            "ℹ️ 참고사항:\n"
            "• 모든 명령어는 1초 간격으로 사용 가능\n"
            "• 실시간 시장 상황에 따라 응답이 지연될  있음\n"
            "• 문제 발생 시 자동으로 알림이 전송됨"
        )

    async def _get_signals_message(self) -> str:
        """최근 매매 신호 메시지 생성"""
        try:
            if not hasattr(self.trader, 'recent_signals'):
                return "📊 최근 매매 신호가 없습니다."

            message = "📊 최근 매매 신호\n━━━━━━━━━━━━━━━━\n\n"
            
            for signal in self.trader.recent_signals[-10:]:  # 최근 10개만 표시
                signal_type = "🔵 매수" if signal['type'] == 'buy' else "🔴 매도"
                message += (
                    f"{signal_type} - {signal['market']}\n"
                    f"• 시간: {signal['timestamp'].strftime('%H:%M:%S')}\n"
                    f"• 가격: {signal['price']:,}원\n"
                    f"• RSI: {signal['rsi']:.1f}\n"
                    f"• 변동률: {signal['change_rate']:+.2f}%\n\n"
                )

            message += f"🔄 마지막 업데이트: {datetime.now().strftime('%H:%M:%S')}"
            return message

        except Exception as e:
            logger.error(f"신호 메시지 생성 실패: {str(e)}")
            return "⚠️ 신호 조회 중 오류가 발생했습니다."

    async def _get_settings_message(self) -> str:
        """현재 설정 메시지 생성"""
        try:
            message = (
                f"⚙️ 현재 트레이딩 설정\n"
                f"━━━━━━━━━━━━━━━━\n\n"
                f"📈 매매 조건\n"
                f"• RSI 기간: {settings.RSI_PERIOD}\n"
                f"• RSI 과매도: {settings.RSI_OVERSOLD}\n"
                f"• RSI 과매수: {settings.RSI_OVERBOUGHT}\n"
                f"• 볼린저 기간: {settings.BOLLINGER_PERIOD}\n"
                f"• 볼린저 표준편차: {settings.BOLLINGER_STD}\n\n"
                f"💰 자금 관리\n"
                f"• 최대 포지션: {settings.MAX_POSITIONS}개\n"
                f"• 포지션 크기 비율: {settings.POSITION_SIZE_RATIO * 100}%\n"
                f"• 최소 거래금액: {settings.MIN_TRADE_AMOUNT:,}원\n\n"
                f"⏰ 시간 설정\n"
                f"• 거래 간격: {settings.TRADING_INTERVAL}초\n\n"
                f"🔄 마지막 업데이트: {datetime.now().strftime('%H:%M:%S')}"
            )
            return message

        except Exception as e:
            logger.error(f"설정 메시지 생성 실���: {str(e)}")
            return "⚠️ 설정 조회 중 오류가 발생했습니다."

    async def _get_risk_message(self) -> str:
        """리스크 설정 메시지 생성"""
        try:
            message = (
                f"⚠️ 리스크 관리 설정\n"
                f"━━━━━━━━━━━━━━━━\n\n"
                f"💰 손익 관리\n"
                f"• 익절 목표: {settings.TAKE_PROFIT_RATIO * 100:.1f}%\n"
                f"• 손절 기준: {settings.STOP_LOSS_RATIO * 100:.1f}%\n"
                f"• 최대 손실률: {settings.MAX_LOSS_RATE:.1f}%\n"
                f"• 일일 손실한도: {settings.DAILY_LOSS_LIMIT:,}원\n\n"
                f"📊 포지션 관리\n"
                f"• 최대 미실현손실: {settings.MAX_DRAWDOWN:.1f}%\n"
                f"• 최대 변동성: {settings.MAX_VOLATILITY:.1f}%\n"
                f"• 거래량 임계값: {settings.VOLUME_THRESHOLD}배\n\n"
                f"🔄 마지막 업데이트: {datetime.now().strftime('%H:%M:%S')}"
            )
            return message

        except Exception as e:
            logger.error(f"리스크 메시지 생성 실패: {str(e)}")
            return "⚠️ 리스크 설정 조회 중 오류가 발생했습니다."

    async def _handle_start_command(self) -> str:
        """트레이딩 시작 명령어 처리"""
        try:
            if self.trader.is_running:
                return "⚠️ 트레이딩이 이미 실행 중입니다."
            
            await self.trader.start()
            return "✅ 트레이딩을 시작합니다."

        except Exception as e:
            logger.error(f"트레이딩 시작 실패: {str(e)}")
            return f"⚠️ 트레이딩 시작 중 오류가 발생했습니다: {str(e)}"

    async def _handle_restart_command(self) -> str:
        """봇 재시작 명령어 처리"""
        try:
            await self.trader.stop()
            await asyncio.sleep(1)
            await self.trader.start()
            return "✅ 트레이딩 봇을 재시작했습니다."

        except Exception as e:
            logger.error(f"재시작 실패: {str(e)}")
            return f"⚠️ 재시작 중 오류가 발생했습니다: {str(e)}"
