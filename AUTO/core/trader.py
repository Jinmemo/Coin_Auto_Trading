import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional, List, Tuple
from decimal import Decimal
import time

from Trading_bot.config.settings import settings
from Trading_bot.core.analyzer import MarketAnalyzer, MarketState
from Trading_bot.core.signal_generator import SignalGenerator
from Trading_bot.strategies.strategy_manager import StrategyManager
from Trading_bot.strategies.base import Position, PositionType
from Trading_bot.utils.telegram import TelegramNotifier
from Trading_bot.core.upbit_api import UpbitAPI
from Trading_bot.core.types import TraderInterface

logger = logging.getLogger(__name__)

class Position:
    def __init__(self, market: str, entry_price: float, amount: float, position_type: str):
        self.market = market
        self.entry_price = entry_price
        self.amount = amount
        self.position_type = position_type  # 'long' or 'short'
        self.entry_time = datetime.now()
        self.unrealized_pnl = 0.0
        self.realized_pnl = 0.0

class Trader(TraderInterface):
    def __init__(self):
        self.notifier = TelegramNotifier()
        self.upbit = UpbitAPI(self.notifier)
        self.analyzer = MarketAnalyzer()
        self.signal_generator = SignalGenerator(self.notifier)
        self.strategy_manager = StrategyManager()
        self.is_running = False
        self.trading_coins = []
        self.positions: Dict[str, Position] = {}  # 현재 보유 포지션
        self.position_history: List[Position] = []  # 종료된 포지션 이력
        self.available_balance = 0.0
        self.start_time = None
        self.last_status_report = None
        self.status_report_interval = 300  # 5분마다 상태 보고
        self.notifier.set_trader(self)
        self._command_lock = asyncio.Lock()  # 명령어 처리 락 추가
        self.last_command = {'text': None, 'time': 0}  # 마지막 명령어 저장
        self.command_cooldown = 1  # 명령어 쿨다운 (초)

    async def update_trading_coins(self):
        """거래 대상 코인 목록 업데이트"""
        self.trading_coins = await self.upbit.update_trading_coins()
        return self.trading_coins

    async def start(self):
        """트레이딩 시작"""
        try:
            self.start_time = datetime.now()
            
            # 시작 메시지와 명령어 안내
            start_message = (
                f"🚀 트레이딩 봇 시작\n"
                f"시작 : {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"\n📌 사용 가능한 명령어:\n"
                f"/status - 현재 봇 상태 조회\n"
                f"/balance - 현재 잔고 조회\n"
                f"/positions - 보유 중인 포지션 조회\n"
                f"/analysis - 현재 시장 분석\n"
                f"/profit - 총 수익 조회\n"
                f"/coins - 감시 중인 코인 목록\n"
                f"/stop - 봇 종료\n"
                f"/help - 명령어 도움말"
            )
            
            logger.info("트레이딩 봇이 시작되었습니다.")
            await self.notifier.send_message(start_message)
            
            self.is_running = True
            self.last_status_report = time.time()
            
            while self.is_running:
                try:
                    # 거래 대상 코인 업데이트
                    self.trading_coins = await self.upbit.update_trading_coins()
                    logger.info(f"감시 중인 코인: {len(self.trading_coins)}개")
                    
                    # 잔고 업데이트
                    await self.update_balance()
                    
                    # 각 코인에 대해 전략 실행
                    for market in self.trading_coins:
                        await self._process_coin(market)
                        await asyncio.sleep(0.1)  # API 호출 간격 조절
                    
                    # 상태 보고
                    current_time = time.time()
                    if current_time - self.last_status_report >= self.status_report_interval:
                        await self._send_status_report()
                        self.last_status_report = current_time
                    
                    # 메인 루프 대기
                    await asyncio.sleep(settings.TRADING_INTERVAL)
                    
                except Exception as e:
                    error_message = f"메인 루프 실행 중 오류 발생: {str(e)}"
                    logger.error(error_message)
                    await self.notifier.send_message(f"⚠️ {error_message}")
                    await asyncio.sleep(5)  # 오류 발생 시 잠시 대기
                    
        except Exception as e:
            error_message = f"트레이딩 봇 실행 중 오류 발생: {str(e)}"
            logger.error(error_message)
            await self.notifier.send_message(f"⚠️ {error_message}")
        finally:
            await self.stop()

    async def _process_coin(self, market: str):
        """개별 코인 처리"""
        try:
            # 마켓 정보 조회
            market_data = await self.upbit.get_market_info(market)
            if market_data is None:
                logger.warning(f"{market} 마켓 정보 조회 실패")
                return

            # 전략 실행
            await self.execute_strategy(market, market_data)

        except Exception as e:
            error_message = f"코인 처리 실패 ({market}): {str(e)}"
            logger.error(error_message)
            await self.notifier.send_message(f"⚠️ {error_message}")

    async def stop(self):
        """트레이딩 종료"""
        try:
            self.is_running = False
            if self.upbit:
                await self.upbit.close()
            if self.notifier:
                await self.notifier.stop()
            logger.info("트레이딩 봇 종료")
        except Exception as e:
            logger.error(f"트레이딩 봇 종료 중 오류 발생: {str(e)}")

    def _get_running_time(self) -> str:
        """실행 시간 계산"""
        if not self.start_time:
            return "0분"
        
        running_time = datetime.now() - self.start_time
        hours = running_time.seconds // 3600
        minutes = (running_time.seconds % 3600) // 60
        
        if hours > 0:
            return f"{hours}시간 {minutes}분"
        return f"{minutes}분"

    async def _send_status_report(self):
        """상태 보고"""
        try:
            # 포지션 정보 수집
            active_positions = len(self.positions)
            total_profit = sum(position.unrealized_pnl for position in self.positions.values())
            
            # 상태 메시지 생성
            status_message = (
                f"📊 트레이딩 봇 상태 보고\n"
                f"실행 시간: {self._get_running_time()}\n"
                f"보유 잔고: {self.available_balance:,.0f}원\n"
                f"활성 포지션: {active_positions}개\n"
                f"미실현 손익: {total_profit:,.0f}원\n"
                f"감시 중인 코인: {len(self.trading_coins)}개"
            )
            
            # 활성 포지션 상세 정보
            if active_positions > 0:
                position_details = "\n\n📍 활성 포지션 상세:"
                for market, position in self.positions.items():
                    profit_rate = (position.unrealized_pnl / (position.entry_price * position.amount)) * 100
                    position_details += f"\n{market}: {profit_rate:.2f}% ({position.unrealized_pnl:,.0f}원)"
                status_message += position_details
            
            await self.notifier.send_message(status_message)
            
        except Exception as e:
            logger.error(f"상태 보고 실패: {str(e)}")

    async def _trading_loop(self):
        """트레이딩 사이클 실행"""
        try:
            for coin in self.trading_coins:
                # 시장 상태 분석
                market_state = await self.analyzer.analyze_market(coin)
                if not market_state or not market_state.is_valid:
                    continue

                # 전략 업데이트
                strategy_changed = await self.strategy_manager.update_strategy(market_state)
                if strategy_changed:
                    await self._handle_strategy_change(market_state)

                # 포지션 관리
                await self._manage_position(coin, market_state)

                # 새로운 진입 기회 분석
                if len(self.strategy_manager.active_strategy.positions) < settings.MAX_COINS:
                    await self._analyze_entry(coin, market_state)

        except Exception as e:
            logger.error(f"트레이딩 사이클 실패: {str(e)}")

    async def _manage_position(self, coin: str, market_state: MarketState):
        """포지션 관리"""
        try:
            position = self.strategy_manager.active_strategy.positions.get(coin)
            if not position:
                return

            # 포지션 업데이트
            update_info = await self.strategy_manager.active_strategy.update_position(position, market_state)
            if not update_info:
                return

            # 청산 조건 확인
            if await self._should_close_position(position, market_state, update_info):
                await self._close_position(position, market_state)
                return

            # 추가 진입 확인
            if await self.strategy_manager.active_strategy.should_add_position(position, market_state):
                await self._add_to_position(position, market_state)

            # 포지션 업데이트 알림
            if abs(update_info['profit_rate']) >= settings.PROFIT_NOTIFICATION_THRESHOLD:
                await self.notifier.send_position_update(update_info)

        except Exception as e:
            logger.error(f"포지션 관리 실패 ({coin}): {str(e)}")

    async def _analyze_entry(self, coin: str, market_state: MarketState):
        """진입 기회 분석"""
        try:
            strategy = self.strategy_manager.active_strategy
            
            # 진입 그널 확인
            if not await strategy.should_enter(market_state):
                return

            # 포지션 크기 계산
            position_size = await strategy.calculate_position_size(market_state)
            if position_size < settings.MIN_TRADE_AMOUNT:
                return

            # 진입점 계산
            entry_points = await strategy.calculate_entry_points(market_state)
            if not entry_points:
                return

            # 포지션 타입 결정
            position_type = await strategy.determine_position_type(market_state)

            # 주문 실행
            order_result = await self._execute_order(
                coin=coin,
                price=entry_points['entry_price'],
                amount=position_size,
                position_type=position_type
            )

            if order_result:
                # 포지션 생성
                position = Position(
                    coin=coin,
                    entry_price=entry_points['entry_price'],
                    amount=position_size,
                    position_type=position_type,
                    timestamp=datetime.now(),
                    take_profit=entry_points['take_profit'],
                    stop_loss=entry_points['stop_loss'],
                    trailing_stop=entry_points.get('trailing_stop')
                )
                strategy.positions[coin] = position

                # 진입 알림
                await self.notifier.send_trade_notification({
                    'type': '신규 진입',
                    'coin': coin,
                    'price': entry_points['entry_price'],
                    'amount': position_size,
                    'position_type': position_type.value,
                    'strategy': strategy.name
                })

        except Exception as e:
            logger.error(f"진입 분석 실패 ({coin}): {str(e)}")

    async def _should_close_position(self, position: Position, market_state: MarketState, update_info: Dict) -> bool:
        """청산 조건 확인"""
        try:
            strategy = self.strategy_manager.active_strategy
            
            # 기본 청산 조건
            if await strategy.should_exit(position, market_state):
                return True

            current_price = market_state.current_price
            profit_rate = update_info['profit_rate']

            # 손익 기준 청산
            if current_price >= position.take_profit:
                return True
            if current_price <= position.stop_loss:
                return True

            # 트레일링 스탑
            if position.trailing_stop and (
                (position.position_type == PositionType.LONG and current_price <= position.trailing_stop) or
                (position.position_type == PositionType.SHORT and current_price >= position.trailing_stop)
            ):
                return True

            # 보유 시간 기준
            min_time, max_time = PositionType.get_holding_time(position.position_type)
            holding_duration = position.get_holding_duration()
            
            if holding_duration > max_time and profit_rate > 0:
                return True

            return False

        except Exception as e:
            logger.error(f"청산 조건 확인 실패: {str(e)}")
            return False

    async def _execute_order(self, coin: str, price: float, amount: float, position_type: PositionType) -> bool:
        """주문 실행"""
        try:
            if settings.TEST_MODE:
                logger.info(f"테스 모드 문: {coin} {position_type.value} {amount}개 @ {price}원")
                return True

            order_result = self.upbit.buy_limit_order(coin, price, amount) if position_type == PositionType.LONG else \
                          self.upbit.sell_limit_order(coin, price, amount)

            if order_result:
                logger.info(f"주문 성공: {coin} {position_type.value} {amount}개 @ {price}원")
                return True

            return False

        except Exception as e:
            logger.error(f"주문 실패: {str(e)}")
            return False

    async def _handle_strategy_change(self, market_state: MarketState):
        """전략 변경 처리"""
        try:
            old_strategy = self.strategy_manager.active_strategy.name
            new_strategy = self.strategy_manager.active_strategy.name
            
            await self.notifier.send(
                f"🔄 전략 변경\n"
                f"이전: {old_strategy}\n"
                f"현재: {new_strategy}\n"
                f"사유: {market_state.trend} 추세, "
                f"RSI: {market_state.rsi:.1f}, "
                f"변동성: {market_state.volatility:.2%}"
            )

        except Exception as e:
            logger.error(f"전략 변경 처리 실패: {str(e)}")

    async def get_trading_status(self) -> Dict:
        """트레이딩 상태 조회"""
        try:
            active_positions = self.strategy_manager.active_strategy.positions
            total_profit = 0
            position_details = []

            for position in active_positions.values():
                market_state = await self.analyzer.analyze_market(position.coin)
                if market_state:
                    update_info = await self.strategy_manager.active_strategy.update_position(position, market_state)
                    if update_info:
                        total_profit += update_info['profit_rate']
                        position_details.append({
                            'coin': position.coin,
                            'profit_rate': update_info['profit_rate'],
                            'position_type': position.position_type.value,
                            'holding_time': position.get_holding_duration()
                        })

            return {
                'is_running': self.is_running,
                'active_strategy': self.strategy_manager.active_strategy.name,
                'total_positions': len(active_positions),
                'total_profit': total_profit,
                'position_details': position_details
            }

        except Exception as e:
            logger.error(f"상태 조회 실패: {str(e)}")
            return {}

    async def update_balance(self):
        """잔고 업데이트"""
        try:
            balance = await self.upbit.get_balance()
            if balance is not None:
                self.available_balance = float(balance)
                logger.debug(f"잔고 업데이트: {self.available_balance:,.0f}원")
        except Exception as e:
            logger.error(f"잔고 업데이트 실패: {str(e)}")

    async def open_position(self, market: str, position_type: str, amount: float) -> Optional[Position]:
        """새로운 포지션 생성"""
        try:
            current_price = await self.upbit.get_current_price(market)
            if not current_price:
                return None

            # 주문 실행
            if position_type == 'long':
                order_result = await self.upbit.place_order(market, 'bid', amount, current_price)
            else:
                order_result = await self.upbit.place_order(market, 'ask', amount, current_price)

            if order_result and order_result.get('state') == 'done':
                position = Position(market, current_price, amount, position_type)
                self.positions[market] = position
                
                # 텔레그램 알림
                await self.notifier.send_message(
                    f"🔔 새로운 포지션 생성\n"
                    f"코인: {market}\n"
                    f"타입: {position_type}\n"
                    f"진입가: {current_price:,}원\n"
                    f"수량: {amount}"
                )
                
                return position
            return None

        except Exception as e:
            logger.error(f"포지션 생성 실패 ({market}): {str(e)}")
            return None

    async def close_position(self, market: str) -> bool:
        """포지션 종료"""
        try:
            position = self.positions.get(market)
            if not position:
                return False

            current_price = await self.upbit.get_current_price(market)
            if not current_price:
                return False

            # 주문 실행
            order_type = 'ask' if position.position_type == 'long' else 'bid'
            order_result = await self.upbit.place_order(market, order_type, position.amount, current_price)

            if order_result and order_result.get('state') == 'done':
                # 수익 계산
                if position.position_type == 'long':
                    pnl = (current_price - position.entry_price) * position.amount
                else:
                    pnl = (position.entry_price - current_price) * position.amount

                position.realized_pnl = pnl
                self.position_history.append(position)
                del self.positions[market]

                # 텔레그램 알림
                await self.notifier.send_message(
                    f"🔔 포지션 종료\n"
                    f"코인: {market}\n"
                    f"타입: {position.position_type}\n"
                    f"진입가: {position.entry_price:,}원\n"
                    f"종료가: {current_price:,}원\n"
                    f"수익률: {(pnl / (position.entry_price * position.amount)) * 100:.2f}%\n"
                    f"수익금: {pnl:,}원"
                )

                return True
            return False

        except Exception as e:
            logger.error(f"포지션 종료 실패 ({market}): {str(e)}")
            return False

    async def update_positions(self):
        """포지션 업데이트"""
        try:
            for market, position in list(self.positions.items()):
                current_price = await self.upbit.get_current_price(market)
                if not current_price:
                    continue

                # 미현 손익 계산
                if position.position_type == 'long':
                    position.unrealized_pnl = (current_price - position.entry_price) * position.amount
                else:
                    position.unrealized_pnl = (position.entry_price - current_price) * position.amount

                # 손절 로직
                pnl_ratio = position.unrealized_pnl / (position.entry_price * position.amount)
                if pnl_ratio <= settings.STOP_LOSS_RATIO:
                    logger.info(f"손절 조건 도달: {market} ({pnl_ratio:.2f}%)")
                    await self.close_position(market)

                # 익절 로직
                elif pnl_ratio >= settings.TAKE_PROFIT_RATIO:
                    logger.info(f"익절 조건 도달: {market} ({pnl_ratio:.2f}%)")
                    await self.close_position(market)

        except Exception as e:
            logger.error(f"포지션 업데이트 실패: {str(e)}")

    async def execute_strategy(self, market: str, market_data: Dict):
        """전략 실행"""
        try:
            # 현재 포지션 확인
            current_position = self.positions.get(market)
            
            # 잔고 업데이트
            balance = await self.upbit.get_balance()
            if balance is None:
                logger.error(f"{market} 잔고 조회 실패")
                return
            
            self.available_balance = float(balance)
            
            # 전략 신호 확인
            signal = await self.signal_generator.generate_signal(market, market_data)
            
            if signal:
                if signal == 'buy' and not current_position:
                    # 매수 가능 금액 계산
                    available_amount = min(
                        self.available_balance * settings.POSITION_SIZE_RATIO,
                        self.available_balance
                    )
                    
                    if available_amount >= settings.MIN_TRADE_AMOUNT:
                        # 매수 주문 실행
                        position = await self.open_position(market, 'long', available_amount)
                        if position:
                            message = (
                                f"✅ 매수 주문 체결\n"
                                f"코인: {market}\n"
                                f"매수가: {position.entry_price:,}원\n"
                                f"수량: {position.amount:.8f}\n"
                                f"주문금액: {available_amount:,}원"
                            )
                            logger.info(message)
                            await self.notifier.send_message(message)
                
                elif signal == 'sell' and current_position:
                    # 매도 주문 실행
                    if await self.close_position(market):
                        profit = current_position.unrealized_pnl
                        profit_rate = (profit / (current_position.entry_price * current_position.amount)) * 100
                        
                        message = (
                            f" 매도 주문 체결\n"
                            f"코인: {market}\n"
                            f"매수가: {current_position.entry_price:,}원\n"
                            f"매도가: {market_data['trade_price']:,}원\n"
                            f"수익률: {profit_rate:.2f}%\n"
                            f"수익금: {profit:,}원"
                        )
                        logger.info(message)
                        await self.notifier.send_message(message)

        except Exception as e:
            error_message = f"전략 실행 실패 ({market}): {str(e)}"
            logger.error(error_message)
            await self.notifier.send_message(f"⚠️ {error_message}")

    async def handle_command(self, command: str) -> str:
        """텔레그램 명령어 처리"""
        try:
            async with self._command_lock:
                current_time = time.time()
                if (self.last_command['text'] == command and 
                    current_time - self.last_command['time'] < self.command_cooldown):
                    return None
                
                response = None
                if command == '/analysis':
                    response = await self._get_analysis_message()
                elif command == '/status':
                    response = await self._get_status_message()
                elif command == '/balance':
                    await self.update_balance()
                    response = f"💰 현재 잔고: {self.available_balance:,.0f}원"
                elif command == '/positions':
                    response = await self._get_positions_message()
                elif command == '/profit':
                    response = await self._get_profit_message()
                elif command == '/coins':
                    coins = [coin.split('-')[1] for coin in self.trading_coins]
                    response = f"📊 감시 중인 코인 목록 ({len(coins)}개):\n{', '.join(coins)}"
                elif command == '/stop':
                    await self.stop()
                    response = "🛑 트레이딩 봇을 종료합니다..."
                elif command == '/help':
                    response = (
                        "📌 사용 가능한 명령어:\n"
                        "/status - 현재 봇 상태 조회\n"
                        "/balance - 현재 잔고 조회\n"
                        "/positions - 보��� 중인 포지션 조회\n"
                        "/profit - 총 수익 조회\n"
                        "/coins - 감시 중인 코인 목록\n"
                        "/analysis - 코인 분석 결과 조회\n"
                        "/stop - 봇 종료\n"
                        "/help - 명령어 도움말"
                    )
                else:
                    response = "❌ 알 수 없는 명령어입니다. /help를 입력하여 사용 가능한 명령어를 확인하세요."

                self.last_command = {
                    'text': command,
                    'time': current_time
                }
                
                return response
                
        except Exception as e:
            error_message = f"명령어 처리 중 오류 발생: {str(e)}"
            logger.error(error_message)
            return f"⚠️ {error_message}"

    async def _get_status_message(self) -> str:
        """현재 봇 상태 메시지 ���성"""
        try:
            await self.update_balance()
            active_positions = len(self.positions)
            total_profit = sum(position.unrealized_pnl for position in self.positions.values())
            
            status_message = (
                f"📊 트레이딩 봇 상태\n"
                f"실행 시간: {self._get_running_time()}\n"
                f"보유 잔고: {self.available_balance:,.0f}원\n"
                f"활성 포지션: {active_positions}개\n"
                f"미실현 손익: {total_profit:,.0f}원\n"
                f"감시 중인 코인: {len(self.trading_coins)}개"
            )
            return status_message
        except Exception as e:
            logger.error(f"상태 메시지 생성 실패: {str(e)}")
            return "⚠️ 상태 조회 중 오류가 발생했습니다."

    async def _get_positions_message(self) -> str:
        """보유 포지션 메시지 생성"""
        try:
            if not self.positions:
                return "📍 현재 보유 중인 포지션이 없습니다."
            
            message = "📍 보유 포지션 목록:\n"
            for market, position in self.positions.items():
                profit_rate = (position.unrealized_pnl / (position.entry_price * position.amount)) * 100
                message += (
                    f"\n{market}\n"
                    f"진입가: {position.entry_price:,.0f}원\n"
                    f"수량: {position.amount:.8f}\n"
                    f"수익률: {profit_rate:.2f}%\n"
                    f"평가손익: {position.unrealized_pnl:,.0f}원\n"
                    f"---------------"
                )
            return message
        except Exception as e:
            logger.error(f"포지션 메시지 생성 실패: {str(e)}")
            return "⚠️ 포지션 조회 중 오류가 발생했습니다."

    async def _get_profit_message(self) -> str:
        """수익 정보 메시지 생성"""
        try:
            total_realized_profit = 0  # 실현 손익
            total_unrealized_profit = sum(p.unrealized_pnl for p in self.positions.values())  # 미실현 손익
            
            message = (
                f"💰 수익 현황\n"
                f"실현 손익: {total_realized_profit:,.0f}원\n"
                f"미실현 손익: {total_unrealized_profit:,.0f}원\n"
                f"총 손익: {(total_realized_profit + total_unrealized_profit):,.0f}원"
            )
            return message
        except Exception as e:
            logger.error(f"수익 메시지 생성 실패: {str(e)}")
            return "⚠️ 수익 조회 중 오류가 발생했습니다."

    def _get_running_time(self) -> str:
        """봇 실행 시간 계산"""
        try:
            if not hasattr(self, 'start_time'):
                return "알 수 없음"
            
            running_time = datetime.now() - self.start_time
            days = running_time.days
            hours = running_time.seconds // 3600
            minutes = (running_time.seconds % 3600) // 60
            
            if days > 0:
                return f"{days}일 {hours}시간 {minutes}분"
            elif hours > 0:
                return f"{hours}시간 {minutes}분"
            else:
                return f"{minutes}분"
        except Exception as e:
            logger.error(f"실행 시간 계산 실패: {str(e)}")
            return "알 수 없음"

    async def _get_analysis_message(self) -> str:
        """코인 분석 결과 메시지 생성"""
        try:
            buy_ready = []
            almost_ready = []
            watching = []
            
            logger.info(f"분석 시작: 총 {len(self.trading_coins)}개 코인")
            
            # 코인을 작은 그룹으로 나누어 처리
            chunk_size = 5  # 한 번에 5개씩 처리
            for i in range(0, len(self.trading_coins), chunk_size):
                chunk = self.trading_coins[i:i + chunk_size]
                
                # 동시에 여러 코인 처리
                tasks = [self._analyze_single_coin(market) for market in chunk]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, tuple):  # 정상적인 결과
                        status, category = result
                        if category == 'buy':
                            buy_ready.append(status)
                        elif category == 'almost':
                            almost_ready.append(status)
                        else:
                            watching.append(status)
                
                # 각 그룹 처리 후 잠시 대기
                await asyncio.sleep(0.5)
            
            # 메시지 생성
            message = "📊 실시간 매매 신호 분석\n\n"
            
            if buy_ready:
                message += "🔥 매수 신호:\n"
                message += "\n".join(buy_ready)
                message += "\n\n"
                
            if almost_ready:
                message += "⚡ 매수 임박:\n"
                message += "\n".join(almost_ready)
                message += "\n\n"
            
            if watching:
                message += "📈 감시 중인 코인:\n"
                # RSI 기준으로 정렬 (낮은 순)
                watching.sort(key=lambda x: float(x.split('RSI:')[1].split()[0]))
                # 상위 20개만 표시
                message += "\n".join(watching[:20])
                if len(watching) > 20:
                    message += f"\n...외 {len(watching)-20}개"
            else:
                message += "📈 감시 중인 코인: 데이터 수집 중..."
            
            message += f"\n\n💡 매수 조건:\n- RSI {self.signal_generator.rsi_oversold} 이하\n- 하락률 2% 이상"
            message += "\n\n⚠️ 이 분석은 참고용이며, 실제 투자는 신중하게 결정하세요."
            
            current_time = datetime.now().strftime("%H:%M:%S")
            message += f"\n\n🕒 마지막 업데이트: {current_time}"
            
            return message
            
        except Exception as e:
            logger.error(f"분석 메시지 생성 실패: {str(e)}")
            return f"⚠️ 분석 중 오류가 발생했습니다: {str(e)}"

    async def _analyze_single_coin(self, market: str) -> Optional[Tuple[str, str]]:
        """단일 코인 분석"""
        try:
            market_data = await self.upbit.get_market_info(market)
            if not market_data:
                return None

            coin = market.split('-')[1]
            rsi = await self.signal_generator._calculate_rsi(market, market_data)
            if rsi is None:
                return None
                
            current_price = market_data['trade_price']
            change_rate = market_data['signed_change_rate'] * 100
            
            # 상태 문자열 생성
            status_icon = "🟢" if change_rate > 0 else "🔴" if change_rate < -2 else "🟡"
            status = (
                f"{status_icon} {coin:<4} "
                f"RSI: {rsi:>5.1f} "
                f"변동률: {change_rate:>+6.1f}% "
                f"현재가: {current_price:,}원"
            )
            
            # 분류
            if rsi < self.signal_generator.rsi_oversold and change_rate < -2:
                return (status + " 🔥매수신호", 'buy')
            elif (rsi < self.signal_generator.rsi_oversold + 5 and change_rate < -1) or \
                 (rsi < self.signal_generator.rsi_oversold and change_rate < -1):
                return (status + " ⚡매수임박", 'almost')
            else:
                return (status, 'watch')
                
        except Exception as e:
            logger.error(f"코인 분석 실패 ({market}): {str(e)}")
            return None
