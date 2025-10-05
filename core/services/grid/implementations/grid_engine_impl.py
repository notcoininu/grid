"""
网格执行引擎实现

负责与交易所适配器交互，执行订单操作
复用现有的交易所适配器系统
"""

import asyncio
import time
from typing import List, Optional, Callable, Dict
from decimal import Decimal
from datetime import datetime

from ....logging import get_logger
from ....adapters.exchanges import ExchangeInterface, OrderSide as ExchangeOrderSide, OrderType
from ..interfaces.grid_engine import IGridEngine
from ..models import GridConfig, GridOrder, GridOrderSide, GridOrderStatus


class GridEngineImpl(IGridEngine):
    """
    网格执行引擎实现
    
    复用现有组件：
    - 交易所适配器（ExchangeInterface）
    - 订单管理
    - WebSocket订阅
    """
    
    def __init__(self, exchange_adapter: ExchangeInterface):
        """
        初始化执行引擎
        
        Args:
            exchange_adapter: 交易所适配器（通过DI注入）
        """
        self.logger = get_logger(__name__)
        self.exchange = exchange_adapter
        self.config: GridConfig = None
        
        # 订单回调
        self._order_callbacks: List[Callable] = []
        
        # 订单追踪
        self._pending_orders: Dict[str, GridOrder] = {}  # order_id -> GridOrder
        
        # 🔥 价格监控
        self._current_price: Optional[Decimal] = None
        self._last_price_update_time: float = 0
        self._price_ws_enabled = False  # WebSocket价格订阅是否启用
        
        # 🔥 订单健康检查
        self._expected_total_orders: int = 0  # 预期的总订单数（初始化时设定）
        self._health_check_task = None
        self._last_health_check_time: float = 0
        
        # 运行状态
        self._running = False
        
        # 获取交易所ID，避免直接打印整个对象（可能导致循环引用）
        exchange_id = getattr(exchange_adapter.config, 'exchange_id', 'unknown')
        self.logger.info(f"网格执行引擎初始化: {exchange_id}")
    
    async def initialize(self, config: GridConfig):
        """
        初始化执行引擎
        
        Args:
            config: 网格配置
        """
        self.config = config
        
        # 确保交易所连接
        if not self.exchange.is_connected():
            await self.exchange.connect()
            self.logger.info(f"连接到交易所: {config.exchange}")
        
        # 订阅用户数据流（接收订单更新）- 优先使用WebSocket
        self._ws_monitoring_enabled = False
        self._polling_task = None
        self._last_ws_check_time = 0  # 上次检查WebSocket的时间
        self._ws_check_interval = 30  # WebSocket检查间隔（秒）
        self._last_ws_message_time = time.time()  # 上次收到WebSocket消息的时间
        self._ws_timeout_threshold = 120  # WebSocket超时阈值（秒）
        
        try:
            self.logger.info("🔄 正在订阅WebSocket用户数据流...")
            await self.exchange.subscribe_user_data(self._on_order_update)
            self._ws_monitoring_enabled = True
            self.logger.info("✅ 订单更新流订阅成功 (WebSocket)")
            self.logger.info("📡 使用WebSocket实时监控订单成交")
        except Exception as e:
            self.logger.error(f"❌ 订单更新流订阅失败: {e}")
            self.logger.error(f"❌ 错误类型: {type(e).__name__}")
            import traceback
            self.logger.error(f"❌ 错误堆栈:\n{traceback.format_exc()}")
            self.logger.warning("⚠️ WebSocket暂时不可用，启用REST轮询作为临时备用")
        
        # 🔥 启动智能订单监控：WebSocket优先，REST备用
        self._start_smart_monitor()
        
        # 🔥 启动智能价格监控：WebSocket优先，REST备用
        await self._start_price_monitor()
        
        # 🔥 设置预期订单总数（网格数量）
        self._expected_total_orders = config.grid_count
        
        # 🔥 启动订单健康检查
        self._start_order_health_check()
        
        self.logger.info(
            f"✅ 执行引擎初始化完成: {config.exchange}/{config.symbol}"
        )
    
    async def place_order(self, order: GridOrder) -> GridOrder:
        """
        下单
        
        Args:
            order: 网格订单
        
        Returns:
            更新后的订单（包含交易所订单ID）
        """
        try:
            # 转换订单方向
            exchange_side = self._convert_order_side(order.side)
            
            # 使用交易所适配器下单（纯限价单）
            # 注意：不能在 params 中传递 Backpack API 不支持的参数（如 grid_id），
            # 否则会导致签名验证失败！Backpack 支持 clientId 参数
            exchange_order = await self.exchange.create_order(
                symbol=self.config.symbol,
                side=exchange_side,
                order_type=OrderType.LIMIT,  # 只使用限价单
                amount=order.amount,
                price=order.price,
                params=None  # 暂时不传递任何额外参数，避免签名问题
            )
            
            # 更新订单ID
            order.order_id = exchange_order.id or exchange_order.order_id
            order.status = GridOrderStatus.PENDING
            
            # 如果订单ID为临时ID（"pending"），尝试从符号查询获取实际ID
            if order.order_id == "pending" or not order.order_id:
                # Backpack API 有时只返回状态，需要查询获取实际订单ID
                # 暂时使用价格+数量作为唯一标识
                temp_id = f"grid_{order.grid_id}_{int(order.price)}_{int(order.amount*1000000)}"
                order.order_id = temp_id
                self.logger.warning(
                    f"订单ID为临时值，使用组合ID: {temp_id} "
                    f"(Grid {order.grid_id}, {order.side.value} {order.amount}@{order.price})"
                )
            
            # 添加到追踪列表
            self._pending_orders[order.order_id] = order
            
            self.logger.info(
                f"下单成功: {order.side.value} {order.amount}@{order.price} "
                f"(Grid {order.grid_id}, OrderID: {order.order_id})"
            )
            
            return order
            
        except Exception as e:
            self.logger.error(f"下单失败: {e}")
            order.mark_failed()
            raise
    
    async def place_batch_orders(self, orders: List[GridOrder], max_retries: int = 2) -> List[GridOrder]:
        """
        批量下单 - 优化版，支持大批量订单和失败重试
        
        Args:
            orders: 订单列表
            max_retries: 最大重试次数（默认2次）
        
        Returns:
            更新后的订单列表
        """
        total_orders = len(orders)
        self.logger.info(f"开始批量下单: {total_orders}个订单")
        
        # 分批下单，避免一次性并发过多（每批50个）
        batch_size = 50
        successful_orders = []
        failed_orders = []  # 记录失败的订单
        
        for i in range(0, total_orders, batch_size):
            batch = orders[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_orders + batch_size - 1) // batch_size
            
            self.logger.info(
                f"处理第{batch_num}/{total_batches}批订单 "
                f"({len(batch)}个订单)"
            )
            
            # 并发下单当前批次
            tasks = [self.place_order(order) for order in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 统计当前批次结果
            batch_success = 0
            for idx, result in enumerate(results):
                if isinstance(result, GridOrder):
                    successful_orders.append(result)
                    batch_success += 1
                else:
                    # 记录失败的订单
                    failed_orders.append((batch[idx], str(result)))
                    self.logger.error(f"订单下单失败: {result}")
            
            self.logger.info(
                f"第{batch_num}批完成: 成功{batch_success}/{len(batch)}个，"
                f"总进度: {len(successful_orders)}/{total_orders}"
            )
            
            # 短暂延迟，避免触发交易所限频
            if i + batch_size < total_orders:
                await asyncio.sleep(0.5)
        
        # ✅ 重试失败的订单
        if failed_orders and max_retries > 0:
            self.logger.warning(
                f"⚠️ 检测到{len(failed_orders)}个失败订单，开始重试..."
            )
            
            for retry_attempt in range(1, max_retries + 1):
                if not failed_orders:
                    break
                
                self.logger.info(
                    f"🔄 第{retry_attempt}次重试: {len(failed_orders)}个订单"
                )
                
                # 等待一段时间再重试，避免立即重试
                await asyncio.sleep(1.0)
                
                retry_orders = [order for order, _ in failed_orders]
                failed_orders = []  # 清空失败列表
                
                # 重试失败的订单
                tasks = [self.place_order(order) for order in retry_orders]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                retry_success = 0
                for idx, result in enumerate(results):
                    if isinstance(result, GridOrder):
                        successful_orders.append(result)
                        retry_success += 1
                    else:
                        # 仍然失败，记录下来
                        failed_orders.append((retry_orders[idx], str(result)))
                
                self.logger.info(
                    f"重试结果: 成功{retry_success}/{len(retry_orders)}个，"
                    f"剩余失败{len(failed_orders)}个"
                )
                
                # 如果还有失败的订单，短暂延迟后继续重试
                if failed_orders and retry_attempt < max_retries:
                    await asyncio.sleep(1.0)
        
        # 最终统计
        final_failed_count = len(failed_orders)
        success_rate = (len(successful_orders) / total_orders * 100) if total_orders > 0 else 0
        
        if final_failed_count > 0:
            self.logger.warning(
                f"⚠️ 批量下单完成: 成功{len(successful_orders)}/{total_orders}个 "
                f"({success_rate:.1f}%), 最终失败{final_failed_count}个"
            )
            
            # 记录失败订单的详细信息
            for order, error in failed_orders:
                self.logger.error(
                    f"订单最终失败: Grid {order.grid_id}, "
                    f"{order.side.value} {order.amount}@{order.price}, "
                    f"错误: {error}"
                )
        else:
            self.logger.info(
                f"✅ 批量下单完成: 成功{len(successful_orders)}/{total_orders}个 "
                f"({success_rate:.1f}%)"
            )
        
        # 🔥 批量下单完成后，主动查询一次所有订单状态
        # 目的：检测那些在提交时立即成交的订单
        self.logger.info("🔍 正在同步订单状态，检测立即成交的订单...")
        await asyncio.sleep(2)  # 等待2秒，让交易所处理完所有订单
        await self._sync_order_status_after_batch()
        
        return successful_orders
    
    async def cancel_order(self, order_id: str) -> bool:
        """
        取消订单
        
        Args:
            order_id: 订单ID
        
        Returns:
            是否成功
        """
        try:
            await self.exchange.cancel_order(order_id, self.config.symbol)
            
            # 从追踪列表移除
            if order_id in self._pending_orders:
                order = self._pending_orders[order_id]
                order.mark_cancelled()
                del self._pending_orders[order_id]
            
            self.logger.info(f"取消订单成功: {order_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"取消订单失败 {order_id}: {e}")
            return False
    
    async def cancel_all_orders(self) -> int:
        """
        取消所有订单
        
        Returns:
            取消的订单数量
        """
        try:
            cancelled_orders = await self.exchange.cancel_all_orders(self.config.symbol)
            count = len(cancelled_orders)
            
            # 清空追踪列表
            for order_id in list(self._pending_orders.keys()):
                order = self._pending_orders[order_id]
                order.mark_cancelled()
                del self._pending_orders[order_id]
            
            self.logger.info(f"取消所有订单: {count}个")
            return count
            
        except Exception as e:
            self.logger.error(f"取消所有订单失败: {e}")
            return 0
    
    async def get_order_status(self, order_id: str) -> Optional[GridOrder]:
        """
        查询订单状态
        
        Args:
            order_id: 订单ID
        
        Returns:
            订单信息
        """
        try:
            # 从交易所查询
            exchange_order = await self.exchange.get_order(order_id, self.config.symbol)
            
            # 更新本地订单信息
            if order_id in self._pending_orders:
                grid_order = self._pending_orders[order_id]
                
                # 如果已成交
                if exchange_order.status.value == "filled":
                    grid_order.mark_filled(
                        filled_price=exchange_order.price,
                        filled_amount=exchange_order.filled
                    )
                
                return grid_order
            
            return None
            
        except Exception as e:
            self.logger.error(f"查询订单状态失败 {order_id}: {e}")
            return None
    
    async def get_current_price(self) -> Decimal:
        """
        获取当前市场价格
        
        优先使用WebSocket缓存的价格，如果超时则使用REST API
        
        Returns:
            当前价格
        """
        try:
            # 🔥 优先使用WebSocket缓存的价格
            if self._current_price is not None:
                price_age = time.time() - self._last_price_update_time
                # 如果价格在5秒内更新过，直接返回缓存
                if price_age < 5:
                    return self._current_price
            
            # 🔥 WebSocket价格过期或不可用，使用REST API
            ticker = await self.exchange.get_ticker(self.config.symbol)
            
            # 优先使用last，其次bid/ask均价
            if ticker.last is not None:
                price = ticker.last
            elif ticker.bid is not None and ticker.ask is not None:
                price = (ticker.bid + ticker.ask) / Decimal('2')
            elif ticker.bid is not None:
                price = ticker.bid
            elif ticker.ask is not None:
                price = ticker.ask
            else:
                raise ValueError("Ticker数据不包含有效价格信息")
            
            # 更新缓存
            self._current_price = price
            self._last_price_update_time = time.time()
            
            return price
            
        except Exception as e:
            self.logger.error(f"获取当前价格失败: {e}")
            # 如果有缓存价格，即使过期也返回
            if self._current_price is not None:
                self.logger.warning(f"使用缓存价格（{time.time() - self._last_price_update_time:.0f}秒前）")
                return self._current_price
            raise
    
    def subscribe_order_updates(self, callback: Callable):
        """
        订阅订单更新
        
        Args:
            callback: 回调函数，接收订单更新
        """
        self._order_callbacks.append(callback)
        self.logger.debug(f"添加订单更新回调: {callback}")
    
    def get_monitoring_mode(self) -> str:
        """
        获取当前监控方式
        
        Returns:
            监控方式：'WebSocket' 或 'REST轮询'
        """
        if self._ws_monitoring_enabled:
            return "WebSocket"
        else:
            return "REST轮询"
    
    def _start_smart_monitor(self):
        """启动智能监控：WebSocket优先，REST临时备用"""
        if self._polling_task is None or self._polling_task.done():
            self._polling_task = asyncio.create_task(self._smart_monitor_loop())
            if self._ws_monitoring_enabled:
                self.logger.info("✅ 智能监控已启动：WebSocket (主)")
            else:
                self.logger.info("✅ 智能监控已启动：REST轮询 (临时备用)")
    
    async def _smart_monitor_loop(self):
        """智能监控循环：优先WebSocket，必要时使用REST"""
        self.logger.info("📡 智能监控循环已启动")
        
        while True:
            try:
                # 🔥 策略1：如果WebSocket正常，只做定期检查（不轮询订单）
                if self._ws_monitoring_enabled:
                    await asyncio.sleep(30)  # 30秒检查一次WebSocket状态
                    
                    current_time = time.time()
                    time_since_last_message = current_time - self._last_ws_message_time
                    
                    # 🔥 优先检查WebSocket连接状态（而不是消息时间）
                    ws_connected = True
                    if hasattr(self.exchange, '_ws_connected'):
                        ws_connected = self.exchange._ws_connected
                    
                    if not ws_connected:
                        self.logger.error("❌ WebSocket连接断开，切换到REST轮询模式")
                        self.logger.info(f"📊 最后收到消息时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self._last_ws_message_time))}")
                        self.logger.info(f"📊 当前挂单数量: {len(self._pending_orders)}")
                        self._ws_monitoring_enabled = False
                        self._last_ws_check_time = current_time
                        continue
                    
                    # 🔥 检查WebSocket心跳状态
                    heartbeat_age = 0
                    if hasattr(self.exchange, '_last_heartbeat'):
                        last_heartbeat = self.exchange._last_heartbeat
                        # 处理可能的datetime对象
                        if isinstance(last_heartbeat, datetime):
                            last_heartbeat = last_heartbeat.timestamp()
                        heartbeat_age = current_time - last_heartbeat
                        
                        if heartbeat_age > self._ws_timeout_threshold:
                            self.logger.error(
                                f"❌ WebSocket心跳超时（{heartbeat_age:.0f}秒未更新），"
                                f"切换到REST轮询模式"
                            )
                            self.logger.info(f"📊 最后心跳时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.exchange._last_heartbeat))}")
                            self.logger.info(f"📊 最后消息时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self._last_ws_message_time))}")
                            self.logger.info(f"📊 当前挂单数量: {len(self._pending_orders)}")
                            self._ws_monitoring_enabled = False
                            self._last_ws_check_time = current_time
                            continue
                    
                    # 🔥 如果连接和心跳都正常，打印健康状态
                    self.logger.info(
                        f"💓 WebSocket健康: 连接正常, 心跳 {heartbeat_age:.0f}秒前, "
                        f"消息 {time_since_last_message:.0f}秒前"
                    )
                    
                    # 💡 如果长时间没有消息，提示这是正常现象
                    if time_since_last_message > 300:  # 5分钟
                        self.logger.info(
                            f"💡 提示: {time_since_last_message:.0f}秒未收到订单更新 "
                            f"(无订单成交时的正常现象)"
                        )
                    
                    continue
                
                # 🔥 策略2：WebSocket不可用时，使用REST轮询
                await asyncio.sleep(3)  # 3秒轮询一次
                
                if self._pending_orders:
                    await self._check_pending_orders()
                
                # 🔥 策略3：定期尝试恢复WebSocket
                current_time = time.time()
                if current_time - self._last_ws_check_time >= self._ws_check_interval:
                    self._last_ws_check_time = current_time
                    await self._try_restore_websocket()
                
            except asyncio.CancelledError:
                self.logger.info("智能监控已停止")
                break
            except Exception as e:
                self.logger.error(f"智能监控出错: {e}")
                await asyncio.sleep(5)
    
    async def _try_restore_websocket(self):
        """尝试恢复WebSocket监控"""
        if self._ws_monitoring_enabled:
            return  # 已经在使用WebSocket
        
        try:
            self.logger.info("🔄 尝试恢复WebSocket监控...")
            
            # 尝试重新订阅用户数据流
            await self.exchange.subscribe_user_data(self._on_order_update)
            
            # 订阅成功，切换回WebSocket模式
            self._ws_monitoring_enabled = True
            # 重置WebSocket消息时间戳
            self._last_ws_message_time = time.time()
            self.logger.info("✅ WebSocket监控已恢复！切换回WebSocket模式")
            self.logger.info("📡 使用WebSocket实时监控订单成交")
            
        except Exception as e:
            self.logger.warning(f"⚠️ WebSocket恢复失败: {type(e).__name__}: {e}")
            self.logger.debug(f"详细错误: {e}，继续使用REST轮询")
            import traceback
            self.logger.debug(f"错误堆栈:\n{traceback.format_exc()}")
    
    async def _sync_order_status_after_batch(self):
        """
        批量下单后同步订单状态
        检测那些在提交时立即成交的订单
        """
        try:
            if not self._pending_orders:
                self.logger.debug("没有挂单需要同步")
                return
            
            # 获取所有挂单
            open_orders = await self.exchange.get_open_orders(self.config.symbol)
            
            if not open_orders:
                self.logger.warning("⚠️ 未获取到任何挂单，可能所有订单都已成交")
                # 所有订单都可能已成交，逐个检查
                pending_order_ids = list(self._pending_orders.keys())
                for order_id in pending_order_ids:
                    order = self._pending_orders.get(order_id)
                    if order:
                        self.logger.info(
                            f"🔍 订单 {order_id} (Grid {order.grid_id}) 不在挂单列表中，"
                            f"可能已成交，触发成交处理"
                        )
                        # 标记为已成交并触发回调
                        order.mark_filled(filled_price=order.price, filled_amount=order.amount)
                        del self._pending_orders[order_id]
                        
                        # 触发成交回调
                        for callback in self._order_callbacks:
                            try:
                                if asyncio.iscoroutinefunction(callback):
                                    await callback(order)
                                else:
                                    callback(order)
                            except Exception as e:
                                self.logger.error(f"订单回调执行失败: {e}")
                return
            
            # 创建挂单ID集合
            # OrderData使用'id'属性，不是'order_id'
            open_order_ids = {order.id for order in open_orders if order.id}
            
            # 检查哪些订单不在挂单列表中（可能已成交）
            filled_count = 0
            pending_order_ids = list(self._pending_orders.keys())
            
            for order_id in pending_order_ids:
                if order_id not in open_order_ids:
                    order = self._pending_orders.get(order_id)
                    if order:
                        filled_count += 1
                        self.logger.info(
                            f"✅ 检测到立即成交订单: {order.side.value} {order.amount}@{order.price} "
                            f"(Grid {order.grid_id}, OrderID: {order_id})"
                        )
                        
                        # 标记为已成交并触发回调
                        order.mark_filled(filled_price=order.price, filled_amount=order.amount)
                        del self._pending_orders[order_id]
                        
                        # 触发成交回调
                        for callback in self._order_callbacks:
                            try:
                                if asyncio.iscoroutinefunction(callback):
                                    await callback(order)
                                else:
                                    callback(order)
                            except Exception as e:
                                self.logger.error(f"订单回调执行失败: {e}")
            
            if filled_count > 0:
                self.logger.info(
                    f"🎯 同步完成: 检测到 {filled_count} 个立即成交订单，"
                    f"剩余挂单 {len(self._pending_orders)} 个"
                )
            else:
                self.logger.info(
                    f"✅ 同步完成: 所有 {len(self._pending_orders)} 个订单均在挂单列表中"
                )
                
        except Exception as e:
            self.logger.error(f"同步订单状态失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    async def _check_pending_orders(self):
        """检查挂单状态（通过REST API）"""
        try:
            # 获取当前所有挂单
            open_orders = await self.exchange.get_open_orders(self.config.symbol)
            
            # 创建订单ID集合（用于快速查找）
            open_order_ids = {order.id or order.order_id for order in open_orders if order.id or order.order_id}
            
            # 检查我们跟踪的订单
            filled_orders = []
            for order_id, grid_order in list(self._pending_orders.items()):
                # 如果订单不在挂单列表中，说明已成交或取消
                if order_id not in open_order_ids:
                    # 假设是成交了（网格系统不会主动取消订单）
                    filled_orders.append((order_id, grid_order))
            
            # 处理成交的订单
            for order_id, grid_order in filled_orders:
                self.logger.info(
                    f"📊 REST轮询检测到订单成交: {grid_order.side.value} "
                    f"{grid_order.amount}@{grid_order.price} (Grid {grid_order.grid_id})"
                )
                
                # 标记为已成交
                grid_order.mark_filled(grid_order.price, grid_order.amount)
                
                # 从挂单列表移除
                del self._pending_orders[order_id]
                
                # 通知回调
                for callback in self._order_callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(grid_order)
                        else:
                            callback(grid_order)
                    except Exception as e:
                        self.logger.error(f"订单回调执行失败: {e}")
            
            if filled_orders:
                self.logger.info(f"✅ REST轮询处理了 {len(filled_orders)} 个成交订单")
                
        except Exception as e:
            self.logger.error(f"检查挂单状态失败: {e}")
    
    async def _on_order_update(self, update_data: dict):
        """
        处理订单更新（来自WebSocket）
        
        Args:
            update_data: 交易所推送的订单更新数据
            
        Backpack格式:
        {
            "e": "orderFilled",     // 事件类型
            "i": "11815754679",     // 订单ID
            "X": "Filled",          // 订单状态
            "p": "215.10",          // 价格
            "z": "0.10"             // 已成交数量
        }
        """
        try:
            # 🔥 更新WebSocket消息时间戳（表示WebSocket正常工作）
            self._last_ws_message_time = time.time()
            
            # 添加调试日志
            self.logger.debug(f"📨 收到WebSocket订单更新: {update_data}")
            self.logger.debug(f"📊 WebSocket消息时间戳已更新: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self._last_ws_message_time))}")
            
            # ✅ 修复：使用Backpack的字段名
            order_id = update_data.get('i')  # Backpack使用'i'表示订单ID
            status = update_data.get('X')     # Backpack使用'X'表示状态
            event_type = update_data.get('e') # 事件类型
            
            if not order_id:
                self.logger.debug(f"订单更新缺少订单ID: {update_data}")
                return
            
            # 检查是否是我们的订单
            if order_id not in self._pending_orders:
                self.logger.debug(f"收到非监控订单的更新: {order_id}")
                return
            
            grid_order = self._pending_orders[order_id]
            
            self.logger.info(
                f"📨 订单更新: ID={order_id}, "
                f"事件={event_type}, 状态={status}, "
                f"Grid={grid_order.grid_id}"
            )
            
            # ✅ 修复：Backpack使用"Filled"表示已成交
            if status == 'Filled' or event_type == 'orderFilled':
                # 获取成交价格和数量
                filled_price = Decimal(str(update_data.get('p', grid_order.price)))
                filled_amount = Decimal(str(update_data.get('z', grid_order.amount)))  # 'z'是已成交数量
                
                grid_order.mark_filled(filled_price, filled_amount)
                
                # 从挂单列表移除
                del self._pending_orders[order_id]
                
                self.logger.info(
                    f"✅ 订单成交: {grid_order.side.value} {filled_amount}@{filled_price} "
                    f"(Grid {grid_order.grid_id})"
                )
                
                # 通知所有回调
                for callback in self._order_callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(grid_order)
                        else:
                            callback(grid_order)
                    except Exception as e:
                        self.logger.error(f"订单回调执行失败: {e}")
            
            # 🔥 处理订单取消事件
            elif status == 'Cancelled' or event_type == 'orderCancelled':
                # 从挂单列表移除
                del self._pending_orders[order_id]
                
                self.logger.warning(
                    f"⚠️ 订单被取消: {grid_order.side.value} {grid_order.amount}@{grid_order.price} "
                    f"(Grid {grid_order.grid_id}, OrderID: {order_id})"
                )
                
                # 🔥 重新挂单（恢复网格）
                self.logger.info(f"🔄 正在重新挂单以恢复网格 (Grid {grid_order.grid_id})...")
                
                # 创建新订单（使用相同的网格参数）
                new_order = GridOrder(
                    order_id="",  # 新订单ID将在提交后获得
                    grid_id=grid_order.grid_id,
                    side=grid_order.side,
                    price=grid_order.price,
                    amount=grid_order.amount,
                    status=GridOrderStatus.PENDING,
                    created_at=datetime.now()  # 添加创建时间
                )
                
                try:
                    # 提交新订单
                    placed_order = await self.place_order(new_order)
                    if placed_order:
                        self.logger.info(
                            f"✅ 网格恢复成功: {placed_order.side.value} {placed_order.amount}@{placed_order.price} "
                            f"(Grid {placed_order.grid_id}, 新OrderID: {placed_order.order_id})"
                        )
                    else:
                        self.logger.error(
                            f"❌ 网格恢复失败: Grid {grid_order.grid_id}, "
                            f"{grid_order.side.value} {grid_order.amount}@{grid_order.price}"
                        )
                except Exception as e:
                    self.logger.error(
                        f"❌ 重新挂单失败: Grid {grid_order.grid_id}, 错误: {e}"
                    )
                
        except Exception as e:
            self.logger.error(f"处理订单更新失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    def _convert_order_side(self, grid_side: GridOrderSide) -> ExchangeOrderSide:
        """
        转换订单方向
        
        Args:
            grid_side: 网格订单方向
        
        Returns:
            交易所订单方向
        """
        if grid_side == GridOrderSide.BUY:
            return ExchangeOrderSide.BUY
        else:
            return ExchangeOrderSide.SELL
    
    async def start(self):
        """启动执行引擎"""
        self._running = True
        self.logger.info("网格执行引擎已启动")
    
    async def stop(self):
        """停止执行引擎"""
        self._running = False
        
        # 取消所有挂单
        await self.cancel_all_orders()
        
        self.logger.info("网格执行引擎已停止")
    
    def is_running(self) -> bool:
        """是否运行中"""
        return self._running
    
    def __repr__(self) -> str:
        return f"GridEngine({self.exchange}, running={self._running})"
    
    # ==================== 价格监控相关方法 ====================
    
    async def _start_price_monitor(self):
        """启动智能价格监控：WebSocket优先，REST备用"""
        try:
            self.logger.info("🔄 正在订阅WebSocket价格数据流...")
            
            # 订阅WebSocket ticker
            await self.exchange.subscribe_ticker(self.config.symbol, self._on_price_update)
            self._price_ws_enabled = True
            
            self.logger.info("✅ 价格数据流订阅成功 (WebSocket)")
            self.logger.info("📡 使用WebSocket实时监控价格")
            
        except Exception as e:
            self.logger.error(f"❌ 价格数据流订阅失败: {e}")
            self.logger.error(f"❌ 错误类型: {type(e).__name__}")
            import traceback
            self.logger.error(f"❌ 错误堆栈:\n{traceback.format_exc()}")
            self.logger.warning("⚠️ WebSocket价格订阅失败，将使用REST API获取价格")
            self._price_ws_enabled = False
    
    def _on_price_update(self, ticker_data) -> None:
        """
        处理WebSocket价格更新
        
        Args:
            ticker_data: Ticker数据
        """
        try:
            # 提取价格
            if ticker_data.last is not None:
                price = ticker_data.last
            elif ticker_data.bid is not None and ticker_data.ask is not None:
                price = (ticker_data.bid + ticker_data.ask) / Decimal('2')
            elif ticker_data.bid is not None:
                price = ticker_data.bid
            elif ticker_data.ask is not None:
                price = ticker_data.ask
            else:
                return
            
            # 更新缓存
            self._current_price = price
            self._last_price_update_time = time.time()
            
            # 可选：记录价格更新（调试用）
            # self.logger.debug(f"💹 价格更新: {price}")
            
        except Exception as e:
            self.logger.error(f"处理价格更新失败: {e}")
    
    def get_price_monitor_mode(self) -> str:
        """
        获取当前价格监控方式
        
        Returns:
            监控方式：'WebSocket' 或 'REST'
        """
        if self._price_ws_enabled and self._current_price is not None:
            price_age = time.time() - self._last_price_update_time
            # 如果价格在10秒内更新过，认为WebSocket正常
            if price_age < 10:
                return "WebSocket"
        return "REST"
    
    # ==================== 订单健康检查相关方法 ====================
    
    def _start_order_health_check(self):
        """启动订单健康检查任务"""
        if self._health_check_task is None or self._health_check_task.done():
            self._health_check_task = asyncio.create_task(self._order_health_check_loop())
            self.logger.info(
                f"✅ 订单健康检查已启动：间隔={self.config.order_health_check_interval}秒"
            )
    
    async def _order_health_check_loop(self):
        """订单健康检查循环"""
        self.logger.info("📊 订单健康检查循环已启动")
        
        # 初始延迟，等待系统稳定
        await asyncio.sleep(60)  # 启动后1分钟开始第一次检查
        
        while self._running:
            try:
                current_time = time.time()
                time_since_last_check = current_time - self._last_health_check_time
                
                # 检查是否到达检查间隔
                if time_since_last_check >= self.config.order_health_check_interval:
                    await self._perform_order_health_check()
                    self._last_health_check_time = current_time
                
                # 休眠一段时间再检查（避免频繁循环）
                await asyncio.sleep(60)  # 每分钟检查一次是否到达间隔时间
                
            except asyncio.CancelledError:
                self.logger.info("订单健康检查已停止")
                break
            except Exception as e:
                self.logger.error(f"订单健康检查出错: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(60)  # 出错后等待1分钟再继续
    
    async def _perform_order_health_check(self):
        """执行订单健康检查"""
        try:
            self.logger.info("🔍 开始执行订单健康检查...")
            
            # 使用REST API获取所有挂单
            open_orders = await self.exchange.get_open_orders(self.config.symbol)
            
            # 统计实际订单数量
            actual_order_count = len(open_orders)
            
            # 对比预期订单数量
            if actual_order_count == self._expected_total_orders:
                self.logger.info(
                    f"✅ 订单健康检查正常：预期={self._expected_total_orders}个，"
                    f"实际={actual_order_count}个"
                )
            else:
                self.logger.warning(
                    f"⚠️ 订单健康检查异常：预期={self._expected_total_orders}个，"
                    f"实际={actual_order_count}个，差异={actual_order_count - self._expected_total_orders}个"
                )
                
                # 详细分析订单类型
                buy_orders = [o for o in open_orders if o.side.value.lower() == 'buy']
                sell_orders = [o for o in open_orders if o.side.value.lower() == 'sell']
                
                self.logger.info(
                    f"📊 订单详情：买单={len(buy_orders)}个，卖单={len(sell_orders)}个"
                )
                
                # 对比本地追踪的订单数量
                local_pending_count = len(self._pending_orders)
                self.logger.info(
                    f"📊 本地追踪：挂单={local_pending_count}个"
                )
                
                # 如果差异较大，记录更详细的信息
                if abs(actual_order_count - self._expected_total_orders) > 5:
                    self.logger.error(
                        f"❌ 订单数量差异过大！可能存在订单丢失或重复下单问题"
                    )
            
        except Exception as e:
            self.logger.error(f"执行订单健康检查失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

