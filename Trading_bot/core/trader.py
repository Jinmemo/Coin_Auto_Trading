import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple
from decimal import Decimal, InvalidOperation
import time
from dataclasses import dataclass, field
import sys
import os

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from Trading_bot.config.settings import settings
from Trading_bot.core.analyzer import MarketAnalyzer, MarketState
from Trading_bot.core.signal_generator import SignalGenerator
from Trading_bot.strategies.strategy_manager import StrategyManager
from Trading_bot.strategies.base import Position, PositionType
from Trading_bot.utils.telegram import TelegramNotifier
from Trading_bot.core.upbit_api import UpbitAPI
from Trading_bot.core.types import TraderInterface

logger = logging.getLogger(__name__)

@dataclass
class TradeStats:
    """거래 통계"""
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_profit: float = 0.0
    max_profit: float = 0.0
    max_loss: float = 0.0
    daily_stats: Dict[str, Dict] = field(default_factory=dict)
    positions_history: List[Dict] = field(default_factory=list)

    @property
    def win_rate(self) -> float:
        """승률 계산"""
        return (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0

    @property
    def average_profit(self) -> float:
        """평균 수익률"""
        return (self.total_profit / self.total_trades) if self.total_trades > 0 else 0

    def update_daily_stats(self, profit: float):
        """일별 통계 업데이트"""
        today = datetime.now().strftime('%Y-%m-%d')
        if today not in self.daily_stats:
            self.daily_stats[today] = {
                'trades': 0,
                'wins': 0,
                'profit': 0.0
            }
        
        self.daily_stats[today]['trades'] += 1
        self.daily_stats[today]['profit'] += profit
        if profit > 0:
            self.daily_stats[today]['wins'] += 1

class Position:
    def __init__(self, market: str, entry_price: str, amount: str, position_type: str):
        self.market = market
        self.entry_price = Decimal(str(entry_price))
        self.amount = Decimal(str(amount))
        self.position_type = position_type
        self.entry_time = datetime.now()
        self.unrealized_pnl = Decimal('0')
        self.realized_pnl = Decimal('0')
        self.additional_entries = []

class Trader(TraderInterface):
    def __init__(self):
        self.upbit = None
        self.notifier = None
        self.analyzer = None
        self.signal_generator = None
        self.positions = {}
        self.position_history = []
        self.trading_coins = []
        self.available_balance = 0
        self.start_time = None
        self.trade_stats = TradeStats()
        self.is_running = False
        self._update_lock = asyncio.Lock()
        logger.info("트레이더 객체 생성")

    async def initialize(self):
        """트레이더 초기화"""
        try:
            logger.info("트레이더 초기화 시작")
            
            # Upbit API 초기화
            self.upbit = UpbitAPI()
            if not await self.upbit.initialize():
                raise Exception("UpbitAPI 초기화 실패")
            
            # 거래 코인 목록 업데이트
            if not await self.update_trading_coins():
                raise Exception("거래 코인 목록 업데이트 실패")
            
            # 거래 코인 목록을 UpbitAPI에 전달
            self.upbit.set_trading_coins(self.trading_coins)
            
            # 웹소켓 연결 초기화
            self.websocket = await self.upbit.init_websocket()
            if not self.websocket:
                raise Exception("웹소켓 연결 실패")
            
            # 웹소켓 핸들러 시작
            self.ws_task = asyncio.create_task(self._handle_websocket())
            
            # MarketAnalyzer 초기화
            self.analyzer = MarketAnalyzer()
            if not await self.analyzer.initialize(self.upbit):
                raise Exception("MarketAnalyzer 초기화 실패")
            
            # 시그널 생성기 초기화
            self.signal_generator = SignalGenerator(self.upbit)
            
            # 시작 시간 기록
            self.start_time = datetime.now()
            
            # 초기 상태 업데이트
            if not await self.update_balance():
                raise Exception("잔고 업데이트 실패")
            
            if not await self.update_positions():
                logger.warning("포지션 업데이트 실패")
            
            # 실행 상태 설정
            self.is_running = True
            
            logger.info("트레이더 초기화 완료")
            return True
            
        except Exception as e:
            logger.error(f"트레이더 초기화 실패: {str(e)}")
            await self.cleanup()
            return False

    async def cleanup(self):
        """리소스 정리"""
        try:
            # 실행 상태 변경
            self.is_running = False
            
            # 웹소켓 태스크 취소
            if hasattr(self, 'ws_task') and not self.ws_task.done():
                self.ws_task.cancel()
                try:
                    await self.ws_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"웹소켓 태스크 취소 중 오류: {str(e)}")
            
            # 웹소켓 연결 종료
            if hasattr(self, 'upbit') and self.upbit:
                await self.upbit.close_websocket()
                await self.upbit.close()  # UpbitAPI 세션 종료
            
            # 모든 실행 중인 태스크 취소
            for task in asyncio.all_tasks():
                if task is not asyncio.current_task():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        logger.error(f"태스크 취소 중 오류: {str(e)}")
            
            logger.info("리소스 정리 완료")
            
        except Exception as e:
            logger.error(f"리소스 정리 중 오류: {str(e)}")
        finally:
            # 이벤트 루프 종료
            loop = asyncio.get_event_loop()
            loop.stop()

    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        await self.cleanup()

    async def stop(self):
        """트레이딩 종료"""
        try:
            if self.is_running:
                logger.info("트레이딩 봇 종료 시작")
                self.is_running = False
                
                # 현재 실행 중인 모든 태스크 정리
                current_task = asyncio.current_task()
                for task in asyncio.all_tasks():
                    if task is not current_task and not task.done():
                        task.cancel()
                        try:
                            await asyncio.shield(task)
                        except asyncio.CancelledError:
                            pass
                        except Exception as e:
                            logger.error(f"태스크 종료 중 오류: {str(e)}")
                
                # 리소스 정리
                if hasattr(self, 'notifier') and self.notifier:
                    await self.notifier.send_message("🛑 트레이딩을 종료합니다...")
                    await self.notifier.close()
                
                # 기타 리소스 정리
                await self.cleanup()
                
                logger.info("트레이딩 봇 종료 완료")
                return True
                
        except Exception as e:
            logger.error(f"트레이딩 종료 실패: {str(e)}")
            return False

    async def check_status(self):
        """상태 체크"""
        try:
            if not self.is_running:
                return False
                
            # 주기적인 상태 업데이트
            await self.update_balance()
            await self.update_positions()
            await self.update_trading_coins()
            
            return True
        except Exception as e:
            logger.error(f"상태 체크 실패: {str(e)}")
            return False

    def set_notifier(self, notifier):
        """노티파이어 설정"""
        self.notifier = notifier

    async def update_trading_coins(self) -> bool:
        """거래량 상위 코인 업데이트 (30분 간격)"""
        try:
            if not self.upbit:
                logger.error("UpbitAPI가 초기화되지 않았습니다")
                return False

            current_time = time.time()
            
            # 30분(1800초) 간격으로 업데이트
            if not hasattr(self, '_last_coin_update') or \
               current_time - self._last_coin_update >= 1800:  
                
                # 거래량 상위 코인 조회
                coins = await self.upbit.get_top_volume_coins(limit=20)
                if not coins:
                    logger.error("거래량 상위 코인 조회 실패")
                    return False

                self.trading_coins = coins
                self._last_coin_update = current_time
                
                # 코인 목록 로깅
                coin_names = [coin.split('-')[1] for coin in self.trading_coins]
                logger.info(f"거래량 상위 코인 업데이트: {len(self.trading_coins)}개")
                logger.debug(f"감시 코인 목록: {', '.join(coin_names)}")
                
                # 텔레그램 알림 전송
                if self.notifier:
                    message = (
                        "📊 거래량 상위 코인 업데이트\n"
                        f"• 감시 코인: {len(coin_names)}개\n"
                        f"• 코인 목록: {', '.join(coin_names)}"
                    )
                    await self.notifier.send_message(message)

            return True

        except Exception as e:
            logger.error(f"거래량 상위 코인 업데이트 실패: {str(e)}")
            return False

    async def _process_coin(self, market: str):
        """개별 코인 처리"""
        try:
            market_state = await self.analyzer.analyze_market(market)
            if not market_state or not market_state.is_valid:
                logger.debug(f"{market} 분석 결과 무효")
                return

            position = self.positions.get(market)
            
            if position:  # 포지션이 있는 경우 매도 검토
                logger.debug(f"{market} 매도 조건 검사 중...")
                should_sell, sell_type = await self.should_sell(market_state, position)
                
                if should_sell:
                    # 매도 수량 계산
                    sell_amount = float(position.amount)
                    if sell_type == "PARTIAL":
                        sell_amount *= 0.5  # 50% 매도
                    
                    # 시장가 매도 실행
                    order_result = await self.upbit.place_order(
                        market=market,
                        side="ask",
                        volume=str(sell_amount),
                        price=str(market_state.current_price)
                    )
                    
                    if order_result:
                        order_status = await self.upbit.get_order(order_result['uuid'])
                        if order_status and order_status['state'] == 'done':
                            profit = (market_state.current_price - float(position.entry_price)) / float(position.entry_price) * 100
                            
                            if sell_type == "PARTIAL":
                                position.amount = float(position.amount) - sell_amount
                                logger.info(f"부분 매도 성공: {market} (수익률: {profit:.1f}%, 남은수량: {position.amount})")
                            else:
                                await self._close_position(market, profit)
                                logger.info(f"전량 매도 성공: {market} (수익률: {profit:.1f}%)")
                            
                            if self.notifier:
                                await self.notifier.send_message(
                                    f"{'🔸' if sell_type == 'PARTIAL' else '🔴'} {sell_type} 매도 체결\n"
                                    f"코인: {market}\n"
                                    f"가격: {market_state.current_price:,}원\n"
                                    f"수익률: {profit:.1f}%\n"
                                    f"{'남은수량: ' + str(position.amount) if sell_type == 'PARTIAL' else ''}"
                                )
                        else:
                            logger.error(f"매도 주문 미체결: {market}")
                            await self.upbit.cancel_order(order_result['uuid'])

            else:  # 포지션이 없는 경우 매수 검토
                logger.debug(f"{market} 매수 조건 검사 중...")
                should_buy, buy_type = await self.should_buy(market_state)
                
                if should_buy:
                    strategy = self.strategy_manager.get_active_strategy()
                    total_position_size = await self.calculate_position_size(market, strategy)
                    
                    if total_position_size > 0 and await self.can_place_order(market, total_position_size):
                        # 분할 매수 금액 계산
                        split_amount = total_position_size / 2 if buy_type == "FIRST" else total_position_size
                        
                        # 수량 계산 (금액 / 현재가)
                        volume = split_amount / market_state.current_price
                        volume = round(volume, 8)  # Upbit 최대 소수점 8자리
                        
                        order_result = await self.upbit.place_order(
                            market=market,
                            side="bid",
                            volume=str(volume),
                            price=str(market_state.current_price)
                        )
                        
                        if order_result:
                            order_status = await self.upbit.get_order(order_result['uuid'])
                            if order_status and order_status['state'] == 'done':
                                await self._create_position(
                                    market=market,
                                    entry_price=market_state.current_price,
                                    amount=volume
                                )
                                logger.info(
                                    f"{buy_type} 매수 성공: {market} "
                                    f"{split_amount:,.0f}원 @ {market_state.current_price:,}원"
                                )
                                
                                if self.notifier:
                                    await self.notifier.send_message(
                                        f"🔵 {buy_type} 매수 체결\n"
                                        f"코인: {market}\n"
                                        f"가격: {market_state.current_price:,}원\n"
                                        f"금액: {split_amount:,.0f}원\n"
                                        f"매수단계: {'1차' if buy_type == 'FIRST' else '2차'}"
                                    )
                            else:
                                logger.error(f"매수 주문 미체결: {market}")
                                await self.upbit.cancel_order(order_result['uuid'])

        except Exception as e:
            logger.exception(f"코인 처리 실패 ({market}): {str(e)}")

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
            
            # 성 포지션 상세 정보
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
            # 코인별 처리를 병렬로 실행
            tasks = [self._process_coin(coin) for coin in self.trading_coins]
            await asyncio.gather(*tasks)
            
        except Exception as e:
            logger.error(f"레이딩 사이클 실패: {str(e)}")

    async def _process_coin(self, coin: str):
        """개별 코인 처리"""
        try:
            # 시장 상태 분석
            market_state = await self.analyzer.analyze_market(coin)
            if not market_state or not market_state.is_valid:
                return

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
            logger.error(f"코인 처리 실패 ({coin}): {str(e)}")

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

            # 청산 조 확인
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

            # 포지션 타 결정
            position_type = await strategy.determine_position_type(market_state)

            # 주문 실
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
                logger.info(f"테스트 모드 문: {coin} {position_type.value} {amount}개 @ {price}원")
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
                f"사유: {market_state.trend} 세, "
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
            logger.error(f"태 조회 실패: {str(e)}")
            return {}

    async def update_balance(self):
        """잔고 업데이트"""
        try:
            if not self.upbit:
                logger.error("UpbitAPI가 초기화되지 않았습니다")
                return False

            balance = await self.upbit.get_balance()
            if balance is not None:
                self.available_balance = balance
                logger.debug(f"잔고 업데이트: {self.available_balance:,.0f}원")
                return True
            else:
                logger.error("잔고 조회 실패")
                return False

        except Exception as e:
            logger.error(f"잔고 업데이트 실패: {str(e)}")
            return False

    async def open_position(self, market: str, position_type: str, amount: float) -> Optional[Position]:
        """로운 포지션 생성"""
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

    async def close_position(self, market: str, position: Position, current_price: float, reason: str = None):
        """포지션 종료 및 통계 업데이트"""
        try:
            # 수익률 계산
            profit_rate = (current_price - position.entry_price) / position.entry_price
            
            # 통계 업데이트
            self.trade_stats.total_trades += 1
            self.trade_stats.total_profit += profit_rate
            
            if profit_rate > 0:
                self.trade_stats.winning_trades += 1
            else:
                self.trade_stats.losing_trades += 1
            
            self.trade_stats.max_profit = max(self.trade_stats.max_profit, profit_rate)
            self.trade_stats.max_loss = min(self.trade_stats.max_loss, profit_rate)
            
            # 일별 통계 업데이트
            self.trade_stats.update_daily_stats(profit_rate)
            
            # 거래 이력 저장
            trade_history = {
                'market': market,
                'entry_price': position.entry_price,
                'exit_price': current_price,
                'profit_rate': profit_rate,
                'holding_time': (datetime.now() - position.entry_time).total_seconds() / 3600,
                'additional_entries': len(position.additional_entries),
                'reason': reason,
                'timestamp': datetime.now()
            }
            self.trade_stats.positions_history.append(trade_history)
            
            # 텔레그램 알림 전송
            await self.send_trade_stats()
            
        except Exception as e:
            logger.error(f"포지션 종료 처리 실패: {str(e)}")

    async def send_trade_stats(self):
        """거래 통계 텔레그램 알림"""
        try:
            message = "📊 거래 통계 보고\n\n"
            
            # 전체 통계
            message += "🔸 전체 통계\n"
            message += f"총 거래: {self.trade_stats.total_trades}회\n"
            message += f"승률: {self.trade_stats.win_rate:.1f}%\n"
            message += f"평균 수익률: {self.trade_stats.average_profit:.2f}%\n"
            message += f"최대 수익: {self.trade_stats.max_profit:.2f}%\n"
            message += f"최대 손실: {self.trade_stats.max_loss:.2f}%\n\n"
            
            # 오늘의 통계
            today = datetime.now().strftime('%Y-%m-%d')
            if today in self.trade_stats.daily_stats:
                today_stats = self.trade_stats.daily_stats[today]
                message += "🔸 오늘의 거래\n"
                message += f"거래 횟수: {today_stats['trades']}회\n"
                win_rate = (today_stats['wins'] / today_stats['trades'] * 100) if today_stats['trades'] > 0 else 0
                message += f"승률: {win_rate:.1f}%\n"
                message += f"수익률: {today_stats['profit']:.2f}%\n\n"
            
            # 최근 5개 거래 이력
            message += "🔸 최근 거래 이력\n"
            recent_trades = sorted(self.trade_stats.positions_history[-5:], 
                                 key=lambda x: x['timestamp'], reverse=True)
            
            for trade in recent_trades:
                emoji = "🟢" if trade['profit_rate'] >= 0 else "🔴"
                message += f"{emoji} {trade['market']}: {trade['profit_rate']:.2f}% "
                message += f"({trade['holding_time']:.1f}시간)\n"
                if trade['reason']:
                    message += f"   사유: {trade['reason']}\n"
            
            await self.notifier.send_message(message)
            
        except Exception as e:
            logger.error(f"거래 통계 알림 전송 실패: {str(e)}")

    async def update_positions(self) -> bool:
        """포지션 업데이트"""
        try:
            if not self.upbit:
                logger.error("UpbitAPI가 초기화되지 않았습니다")
                return False

            # 보유 코인 조회
            holdings = await self.upbit.get_holdings()
            if holdings is None:
                logger.error("보유 코인 조회 실패")
                return False

            # 현재 포지션 목록
            current_positions = set(self.positions.keys())
            updated_positions = set()

            MIN_POSITION_VALUE = 1000  # 최소 포지션 가치 (1000원)

            for holding in holdings:
                try:
                    market = holding['market']
                    amount = float(holding['balance'])
                    avg_price = float(holding['avg_buy_price'])
                    
                    # 포지션 가치 계산
                    position_value = amount * avg_price
                    
                    # 1000원 미만 포지션 무시
                    if position_value < MIN_POSITION_VALUE:
                        logger.debug(f"최소 금액 미만 포지션 무시: {market} ({position_value:,.0f}원)")
                        continue
                    
                    updated_positions.add(market)
                    
                    if market not in self.positions:
                        # 새로운 포지션 생성
                        self.positions[market] = Position(
                            market=market,
                            entry_price=str(avg_price),
                            amount=str(amount),
                            position_type='long'
                        )
                    else:
                        # 기존 포지션 업데이트
                        position = self.positions[market]
                        position.amount = Decimal(str(amount))
                        position.entry_price = Decimal(str(avg_price))

                except (KeyError, ValueError) as e:
                    logger.error(f"포지션 데이터 처리 실패 ({market}): {str(e)}")
                    continue

            # 청산된 포지션 또는 최소 금액 미만 포지션 제거
            closed_positions = current_positions - updated_positions
            for market in closed_positions:
                del self.positions[market]

            if self.positions:
                logger.info(f"현재 보유 포지션: {len(self.positions)}개")
                # 포지션 상세 정보 로깅
                for market, pos in self.positions.items():
                    value = float(pos.amount) * float(pos.entry_price)
                    logger.info(f"- {market}: {value:,.0f}원")
            else:
                logger.info("보유 중인 포지션 없음")

            return True

        except Exception as e:
            logger.error(f"포지션 업데이트 실패: {str(e)}")
            return False

    async def check_balance(self, market: str) -> str:
        """특정 코인의 잔고 확인"""
        try:
            balance_info = await self.upbit.get_coin_balance(market)
            return balance_info['total']
            
        except Exception as e:
            logger.error(f"{market} 잔고 조회 실패")
            return '0'

    async def execute_strategy(self, market: str, market_data: Dict):
        """전략 실행"""
        try:
            balance_info = await self.upbit.get_coin_balance(market)
            if not balance_info or not isinstance(balance_info, dict):
                logger.warning(f"{market} 잔고 조회 실패")
                return

            # 안전한 데이터 변환
            try:
                total_balance = str(balance_info.get('total', '0'))
                avg_buy_price = str(balance_info.get('avg_buy_price', '0'))
                
                if float(total_balance) > 0:
                    position_value = await self.upbit.calculate_position_value(market)
                    if position_value and isinstance(position_value, dict):
                        profit_rate = str(position_value.get('profit_rate', '0'))
                        
                        if market not in self.positions:
                            self.positions[market] = Position(
                                market=market,
                                entry_price=avg_buy_price,
                                amount=total_balance,
                                position_type='long'
                            )
                        else:
                            position = self.positions[market]
                            position.amount = Decimal(total_balance)
                            position.entry_price = Decimal(avg_buy_price)
                            position.unrealized_pnl = Decimal(profit_rate)

                elif market in self.positions:
                    del self.positions[market]

            except (ValueError, TypeError, InvalidOperation) as e:
                logger.error(f"{market} 데이터 변환 실패: {str(e)}")
                return

        except Exception as e:
            logger.error(f"전략 실행 실패 ({market}): {str(e)}")

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
                    response = f"💰 현재 고: {self.available_balance:,.0f}원"
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
                        "/positions - 보유 포지션 회\n"
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
        """현재 봇 상태 메시지 생성"""
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
            return "⚠ 포지션 조회 중 오류가 발생했습니다."

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
                    message += f"\n... {len(watching)-20}개"
            else:
                message += "📈 감시 중인 코인: 이터 수집 중..."
            
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
            # OHLCV 데이터 조회
            ohlcv = await self.upbit.get_ohlcv(market)
            if ohlcv is None or len(ohlcv) < 120:
                return None

            # MarketAnalyzer를 통한 시장 상태 분석
            market_state = await self.analyzer.analyze_market(market, ohlcv)
            if market_state is None:
                return None

            coin = market.split('-')[1]
            change_rate = ((market_state.current_price - ohlcv['close'].iloc[-2]) / 
                          ohlcv['close'].iloc[-2] * 100)
            
            # 상태 문자열 생성
            status_icon = "🟢" if change_rate > 0 else "🔴" if change_rate < -2 else "🟡"
            status = (
                f"{status_icon} {coin:<4} "
                f"RSI: {market_state.rsi:>5.1f} "
                f"변동률: {change_rate:>+6.1f}% "
                f"현재가: {market_state.current_price:,}원"
            )
            
            # 분류
            if market_state.is_oversold and change_rate < -2:
                return (status + " 🔥매수신호", 'buy')
            elif (market_state.rsi < self.signal_generator.rsi_oversold + 5 and change_rate < -1) or \
                 (market_state.rsi < self.signal_generator.rsi_oversold and change_rate < -1):
                return (status + " ⚡매수임박", 'almost')
            else:
                return (status, 'watch')
                
        except Exception as e:
            logger.error(f"코인 분석 실패 ({market}): {str(e)}")
            return None

    async def update_market_states(self):
        """시장 상태 업데이트"""
        try:
            for market in self.trading_coins:
                market_state = await self.analyzer.get_market_state(market)
                if market_state:
                    self.market_states[market] = market_state
                    
                    # 포지션이 없는 경우 신규 진입 검토
                    if market not in self.positions:
                        await self.check_entry(market, market_state)
                    
                    # 포지션이 있는 경우 업데이트
                    else:
                        await self.update_position(market, market_state)
                        
        except Exception as e:
            logger.error(f"시장 상태 업데이트 실패: {str(e)}")

    async def update_position(self, market: str, market_state: MarketState):
        """포지션 업데이트"""
        try:
            position = self.positions[market]
            position.update_price_extremes(market_state.current_price)
            
            # 익률 업데이트
            position.unrealized_pnl = (market_state.current_price - position.entry_price) / position.entry_price
            position.last_rsi = market_state.rsi
            
            # 트레일링 스탑 체크
            if self.check_trailing_stop(position, market_state.current_price):
                await self.close_position(market, position, market_state.current_price, "트레일링 스탑")
                return
            
            # 전략 기반 청산 검토
            strategy = self.strategy_manager.get_strategy(position.position_type)
            if strategy:
                if await strategy.should_exit(position, market_state):
                    await self.close_position(market, position, market_state.current_price, "전략 청산")
                    return
                
                # 추가 매수 검토
                if await strategy.should_add_position(position, market_state):
                    await self.add_to_position(market, position, market_state)
                    
        except Exception as e:
            logger.error(f"포지션 업데이트 실패 ({market}): {str(e)}")

    async def add_to_position(self, market: str, position: Position, market_state: MarketState):
        """포지션 추가"""
        try:
            if len(position.additional_entries) >= 3:
                return
                
            strategy = self.strategy_manager.get_strategy(position.position_type)
            amount = await strategy.calculate_position_size(market_state)
            
            order = await self.upbit.place_order(
                market=market,
                side="bid",
                volume=amount / market_state.current_price
            )
            
            if order:
                entry = {
                    'price': market_state.current_price,
                    'amount': amount,
                    'timestamp': datetime.now()
                }
                position.additional_entries.append(entry)
                await self.notifier.send_trade_notification(
                    "추가매수", market, market_state.current_price, 
                    amount, f"{len(position.additional_entries)}차 추가매수"
                )
                
        except Exception as e:
            logger.error(f"추가 매수 실패 ({market}): {str(e)}")

    async def can_place_order(self, market: str, amount: float) -> bool:
        """주문 가능 여부 확인"""
        try:
            # 현재 잔고 조회
            await self.update_balance()
            
            # 최소 유지 잔고 설정 (예: 5000원)
            MIN_BALANCE = 5000
            
            # 필요한 금액 계산 (수수료 포함)
            required_amount = amount * 1.0005  # 0.05% 수수료 고려
            
            if self.available_balance < (required_amount + MIN_BALANCE):
                logger.warning(f"잔고 부족: 필요금액 {required_amount:,.0f}원, 현재잔고 {self.available_balance:,.0f}원")
                
                # 텔레그램 알림 전송
                if self.notifier:
                    message = (
                        f"⚠️ 잔고 부족으로 매수 제한\n"
                        f"코인: {market}\n"
                        f"필요금액: {required_amount:,.0f}원\n"
                        f"현재잔고: {self.available_balance:,.0f}원"
                    )
                    await self.notifier.send_message(message)
                return False
            
            return True
        except Exception as e:
            logger.error(f"주문 가능 여부 확인 실패: {str(e)}")
            return False

    async def should_buy(self, state: MarketState) -> bool:
        """매수 조건 검사"""
        try:
            if not state.is_valid or not self._can_open_position():
                return False

            # RSI 매수 조건
            is_rsi_buy = state.rsi <= 30  # RSI 30 이하일 때

            # 볼린저 밴드 매수 조건
            is_bb_buy = state.current_price <= state.bb_lower * 1.01  # 하단 밴드 근처

            # 매수 신호 (RSI와 볼린저 밴드 조건 모두 충족)
            is_buy_signal = is_rsi_buy and is_bb_buy

            # 매수 임박 알림 발송
            if is_buy_signal and self.notifier:
                await self.notifier.send_message(
                    f"⚡ 매수 신호 감지 ({state.market})\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"현재가격: {state.current_price:,}원\n"
                    f"RSI: {state.rsi:.1f}\n"
                    f"BB 하단: {state.bb_lower:,}원\n"
                    f"변동률: {state.price_change:+.1f}%"
                )

            return is_buy_signal

        except Exception as e:
            logger.error(f"매수 조건 검사 실패: {str(e)}")
            return False

    async def should_sell(self, state: MarketState, position: Position) -> bool:
        """매도 조건 검사"""
        try:
            if not state.is_valid:
                return False

            profit_rate = (state.current_price - position.entry_price) / position.entry_price * 100
            holding_time = (datetime.now() - position.entry_time).total_seconds() / 3600

            # 매도 조건 검사
            if profit_rate <= -3.0 or (profit_rate <= -2.0 and holding_time >= 24):  # ��절매
                sell_type = "FULL"
            elif state.rsi >= 70 and state.current_price >= state.bb_upper * 0.99:  # 부분 매도
                sell_type = "PARTIAL"
            elif ((state.rsi >= 70 and state.rsi_slope < 0) or    # RSI 하락 시작
                  (state.rsi >= 75 and state.current_price >= state.bb_upper * 0.99) or    # RSI 최대치
                  profit_rate >= 5.0):    # 목표 수익률
                sell_type = "FULL"
            else:
                return False  # 매도 조건 미충족

            # 매도 사유 결정
            sell_reason = (
                "손절매" if profit_rate <= -3.0 or (profit_rate <= -2.0 and holding_time >= 24)
                else "수익 실현" if profit_rate >= 5.0
                else "RSI 매도 신호"
            )
            
            # 매도 알림 발송
            if self.notifier:
                await self.notifier.send_message(
                    f"⚡ 매도 신호 감지 ({state.market})\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"현재가: {state.current_price:,}원\n"
                    f"RSI: {state.rsi:.1f}\n"
                    f"수익률: {profit_rate:+.1f}%\n"
                    f"보유시간: {holding_time:.1f}시간\n"
                    f"매도유형: {'전량' if sell_type == 'FULL' else '부분'} 매도\n"
                    f"매도사유: {sell_reason}"
                )

            return sell_type

        except Exception as e:
            logger.error(f"매도 조건 검사 실패: {str(e)}")
            return False

    def _can_open_position(self) -> bool:
        """새로운 포지션을 열 수 있는지 확인"""
        try:
            # 이미 보유한 포지션이 있는지 확인
            if self.positions:
                return False
                
            # 거래 가능한 잔고가 있는지 확인
            if not self.available_balance:
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"포지션 오픈 가능 여부 확인 중 오류: {str(e)}")
            return False

    async def calculate_position_size(self, market: str) -> float:
        """주문 금액 계산"""
        try:
            # 기본 주문 금액 설정 (예: 총 잔고의 30%)
            position_size = 5100
            
            # 최소 주문 금액 (예: 5,000원)
            MIN_ORDER_AMOUNT = 5000
            
            if position_size < MIN_ORDER_AMOUNT:
                return 0
            
            return position_size
            
        except Exception as e:
            logger.error(f"주문 금액 계산 실패: {str(e)}")
            return 0

    async def start_trading(self):
        """트레이딩 시작"""
        try:
            if not await self.initialize():
                raise Exception("트레이더 초기화 실패")

            self.is_running = True
            logger.info("트레이딩 시작")
            
            if self.notifier:
                await self.notifier.send_message(
                    "🚀 트레이딩 봇 시작\n"
                    f"• 감시 코인: {len(self.trading_coins)}개\n"
                    f"• 매수 금액: {await self.calculate_position_size(''):,}원\n"
                    "• 매수 조건: RSI 30↓ + BB 하단\n"
                    "• 매도 조건: RSI 70↑ + BB 상단 or +5% 익절/-3% 손절"
                )

            # 메인 업데이트 루프 (5초 간격)
            update_interval = 5  # 5초로 변경

            # 코인 목록 업데이트 루프 (5분 간격)
            last_coins_update = 0
            coins_update_interval = 300  # 5분

            while self.is_running:
                try:
                    current_time = time.time()

                    # 코인 목록 주기적 업데이트
                    if current_time - last_coins_update >= coins_update_interval:
                        await self.update_trading_coins()
                        last_coins_update = current_time

                    # 상태 업데이트
                    await self.update_balance()
                    await self.update_positions()

                    # 각 코인별 트레이딩 로직 실행 (병렬 처리)
                    tasks = []
                    for market in self.trading_coins:
                        tasks.append(self._process_coin(market))
                    
                    if tasks:
                        await asyncio.gather(*tasks)

                    # 지정된 간격만큼 대기
                    await asyncio.sleep(update_interval)

                except Exception as e:
                    logger.error(f"트레이딩 사이클 실행 중 오류: {str(e)}")
                    await asyncio.sleep(1)  # 에러 발생시 1초 대기

        except Exception as e:
            logger.error(f"트레이딩 시작 실패: {str(e)}")
            await self.stop()

    async def _handle_websocket(self):
        """웹소켓 메시지 ��리"""
        try:
            while self.is_running:
                message = await self.websocket.receive_json()
                
                if message['type'] == 'ticker':
                    market = message['code']
                    current_price = float(message['trade_price'])
                    
                    # 실시간 가격 업데이트 및 전략 실행
                    await self._process_realtime_update(market, current_price)
                    
        except Exception as e:
            logger.error(f"웹소켓 처리 중 오류: {str(e)}")
            if self.is_running:
                # 재연결 시도
                await asyncio.sleep(1)
                asyncio.create_task(self._handle_websocket())

    async def _process_realtime_update(self, market: str, current_price: float):
        """실시간 가격 업데이트 처리"""
        try:
            # 시장 상태 업데이트
            market_state = await self.analyzer.update_market_state(market, current_price)
            if not market_state:
                return

            # 매매 로직 실행
            await self._process_coin(market)
            
        except Exception as e:
            logger.error(f"실시간 업데이트 처리 실패 ({market}): {str(e)}")
