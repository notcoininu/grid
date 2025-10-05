"""
网格交易系统协调器

核心协调逻辑：
1. 初始化网格系统
2. 处理订单成交事件
3. 自动挂反向订单
4. 异常处理和暂停恢复
"""

import asyncio
from typing import List, Optional
from decimal import Decimal
from datetime import datetime

from ....logging import get_logger
from ..interfaces import IGridStrategy, IGridEngine, IPositionTracker
from ..models import (
    GridConfig, GridState, GridOrder, GridOrderSide,
    GridOrderStatus, GridStatus, GridStatistics
)


class GridCoordinator:
    """
    网格交易系统协调器
    
    职责：
    1. 整合策略、引擎、跟踪器
    2. 订单成交后的反向挂单逻辑
    3. 批量成交处理
    4. 系统状态管理
    5. 异常处理
    """
    
    def __init__(
        self,
        config: GridConfig,
        strategy: IGridStrategy,
        engine: IGridEngine,
        tracker: IPositionTracker,
        grid_state: GridState
    ):
        """
        初始化协调器
        
        Args:
            config: 网格配置
            strategy: 网格策略
            engine: 执行引擎
            tracker: 持仓跟踪器
            grid_state: 网格状态（共享实例）
        """
        self.logger = get_logger(__name__)
        self.config = config
        self.strategy = strategy
        self.engine = engine
        self.tracker = tracker
        
        # 网格状态（使用传入的共享实例）
        self.state = grid_state
        
        # 运行控制
        self._running = False
        self._paused = False
        
        # 异常计数
        self._error_count = 0
        self._max_errors = 5  # 最大错误次数，超过则暂停
        
        # 🔥 价格移动网格专用
        self._price_escape_start_time: Optional[float] = None  # 价格脱离开始时间
        self._last_escape_check_time: float = 0  # 上次检查时间
        self._escape_check_interval: int = 10  # 检查间隔（秒）
        self._is_resetting: bool = False  # 是否正在重置网格
        
        self.logger.info(f"网格协调器初始化: {config}")
    
    async def initialize(self):
        """初始化网格系统"""
        try:
            self.logger.info("开始初始化网格系统...")
            
            # 1. 先初始化执行引擎（设置 engine.config）
            await self.engine.initialize(self.config)
            self.logger.info("执行引擎初始化完成")
            
            # 🔥 价格移动网格：获取当前价格并设置价格区间
            if self.config.is_follow_mode():
                current_price = await self.engine.get_current_price()
                self.config.update_price_range_for_follow_mode(current_price)
                self.logger.info(
                    f"价格移动网格：根据当前价格 ${current_price:,.2f} 设置价格区间 "
                    f"[${self.config.lower_price:,.2f}, ${self.config.upper_price:,.2f}]"
                )
            
            # 2. 初始化网格状态
            self.state.initialize_grid_levels(
                self.config.grid_count,
                self.config.get_grid_price
            )
            self.logger.info(f"网格状态初始化完成，共{self.config.grid_count}个网格层级")
            
            # 3. 初始化策略，生成所有初始订单
            initial_orders = self.strategy.initialize(self.config)
            
            # 🔥 价格移动网格：价格区间在初始化后才设置
            if self.config.is_follow_mode():
                self.logger.info(
                    f"策略初始化完成，生成{len(initial_orders)}个初始订单，"
                    f"覆盖价格区间 [${self.config.lower_price:,.2f}, ${self.config.upper_price:,.2f}]"
                )
            else:
                self.logger.info(
                    f"策略初始化完成，生成{len(initial_orders)}个初始订单，"
                    f"覆盖价格区间 ${self.config.lower_price:,.2f} - ${self.config.upper_price:,.2f}"
                )
            
            # 4. 订阅订单更新
            self.engine.subscribe_order_updates(self._on_order_filled)
            self.logger.info("订单更新订阅完成")
            
            # 5. 批量下所有初始订单（关键修改）
            self.logger.info(f"开始批量挂单，共{len(initial_orders)}个订单...")
            placed_orders = await self.engine.place_batch_orders(initial_orders)
            
            # 6. 批量添加到状态追踪（只添加未成交的订单）
            self.logger.info(f"开始添加{len(placed_orders)}个订单到状态追踪...")
            added_count = 0
            skipped_count = 0
            for order in placed_orders:
                # 🔥 检查订单是否已经在状态中（可能已经通过WebSocket成交回调处理）
                if order.order_id in self.state.active_orders:
                    skipped_count += 1
                    self.logger.debug(
                        f"⏭️ 跳过已存在订单: {order.order_id} (Grid {order.grid_id}, {order.side.value})"
                    )
                    continue
                
                # 🔥 检查订单是否已经成交（状态为FILLED）
                if order.status == GridOrderStatus.FILLED:
                    skipped_count += 1
                    self.logger.debug(
                        f"⏭️ 跳过已成交订单: {order.order_id} (Grid {order.grid_id}, {order.side.value})"
                    )
                    continue
                
                self.state.add_order(order)
                added_count += 1
                self.logger.debug(f"✅ 已添加订单到状态: {order.order_id} (Grid {order.grid_id}, {order.side.value})")
            
            self.logger.info(
                f"✅ 成功挂出{len(placed_orders)}/{len(initial_orders)}个订单，"
                f"覆盖整个价格区间"
            )
            self.logger.info(
                f"📊 订单添加统计: 新增={added_count}, 跳过={skipped_count} "
                f"(已存在或已成交)"
            )
            self.logger.info(
                f"📊 状态统计: "
                f"买单={self.state.pending_buy_orders}, "
                f"卖单={self.state.pending_sell_orders}, "
                f"活跃订单={len(self.state.active_orders)}"
            )
            
            # 7. 启动系统
            self.state.start()
            self._running = True
            
            self.logger.info("✅ 网格系统初始化完成，所有订单已就位，等待成交")
            
        except Exception as e:
            self.logger.error(f"❌ 网格系统初始化失败: {e}")
            self.state.set_error()
            raise
    
    async def _on_order_filled(self, filled_order: GridOrder):
        """
        订单成交回调 - 核心逻辑
        
        当订单成交时：
        1. 记录成交信息
        2. 计算反向订单参数
        3. 立即挂反向订单
        
        Args:
            filled_order: 已成交订单
        """
        try:
            if self._paused:
                self.logger.warning("系统已暂停，跳过订单处理")
                return
            
            self.logger.info(
                f"📢 订单成交: {filled_order.side.value} "
                f"{filled_order.filled_amount}@{filled_order.filled_price} "
                f"(Grid {filled_order.grid_id})"
            )
            
            # 1. 更新状态
            self.state.mark_order_filled(
                filled_order.order_id,
                filled_order.filled_price,
                filled_order.filled_amount or filled_order.amount
            )
            
            # 2. 记录到持仓跟踪器
            self.tracker.record_filled_order(filled_order)
            
            # 3. 计算反向订单参数
            new_side, new_price, new_grid_id = self.strategy.calculate_reverse_order(
                filled_order,
                self.config.grid_interval
            )
            
            # 4. 创建反向订单
            reverse_order = GridOrder(
                order_id="",  # 等待执行引擎填充
                grid_id=new_grid_id,
                side=new_side,
                price=new_price,
                amount=filled_order.filled_amount or filled_order.amount,  # 数量完全一致
                status=GridOrderStatus.PENDING,
                created_at=datetime.now(),
                parent_order_id=filled_order.order_id
            )
            
            # 5. 下反向订单
            placed_order = await self.engine.place_order(reverse_order)
            self.state.add_order(placed_order)
            
            # 6. 记录关联关系
            filled_order.reverse_order_id = placed_order.order_id
            
            self.logger.info(
                f"✅ 反向订单已挂: {new_side.value} "
                f"{reverse_order.amount}@{new_price} "
                f"(Grid {new_grid_id})"
            )
            
            # 7. 更新当前价格
            current_price = await self.engine.get_current_price()
            current_grid_id = self.config.get_grid_index_by_price(current_price)
            self.state.update_current_price(current_price, current_grid_id)
            
            # 重置错误计数
            self._error_count = 0
            
        except Exception as e:
            self.logger.error(f"❌ 处理订单成交失败: {e}")
            self._handle_error(e)
    
    async def _on_batch_orders_filled(self, filled_orders: List[GridOrder]):
        """
        批量订单成交处理
        
        处理价格剧烈波动导致的多订单同时成交
        
        Args:
            filled_orders: 已成交订单列表
        """
        try:
            if self._paused:
                self.logger.warning("系统已暂停，跳过批量订单处理")
                return
            
            self.logger.info(
                f"⚡ 批量成交: {len(filled_orders)}个订单"
            )
            
            # 1. 批量更新状态和记录
            for order in filled_orders:
                self.state.mark_order_filled(
                    order.order_id,
                    order.filled_price,
                    order.filled_amount or order.amount
                )
                self.tracker.record_filled_order(order)
            
            # 2. 批量计算反向订单
            reverse_params = self.strategy.calculate_batch_reverse_orders(
                filled_orders,
                self.config.grid_interval
            )
            
            # 3. 创建反向订单列表
            reverse_orders = []
            for side, price, grid_id, amount in reverse_params:
                order = GridOrder(
                    order_id="",
                    grid_id=grid_id,
                    side=side,
                    price=price,
                    amount=amount,
                    status=GridOrderStatus.PENDING,
                    created_at=datetime.now()
                )
                reverse_orders.append(order)
            
            # 4. 批量下单
            placed_orders = await self.engine.place_batch_orders(reverse_orders)
            
            # 5. 批量更新状态
            for order in placed_orders:
                self.state.add_order(order)
            
            self.logger.info(
                f"✅ 批量反向订单已挂: {len(placed_orders)}个"
            )
            
            # 6. 更新当前价格
            current_price = await self.engine.get_current_price()
            current_grid_id = self.config.get_grid_index_by_price(current_price)
            self.state.update_current_price(current_price, current_grid_id)
            
            # 重置错误计数
            self._error_count = 0
            
        except Exception as e:
            self.logger.error(f"❌ 批量处理订单成交失败: {e}")
            self._handle_error(e)
    
    def _handle_error(self, error: Exception):
        """
        处理异常
        
        策略：
        1. 记录错误
        2. 增加错误计数
        3. 超过阈值则暂停系统
        
        Args:
            error: 异常对象
        """
        self._error_count += 1
        
        self.logger.error(
            f"异常发生 ({self._error_count}/{self._max_errors}): {error}"
        )
        
        # 如果错误次数过多，暂停系统
        if self._error_count >= self._max_errors:
            self.logger.error(
                f"❌ 错误次数达到上限({self._max_errors})，暂停系统"
            )
            asyncio.create_task(self.pause())
    
    async def start(self):
        """启动网格系统"""
        if self._running:
            self.logger.warning("网格系统已经在运行")
            return
        
        await self.initialize()
        await self.engine.start()
        
        # 🔥 价格移动网格：启动价格脱离监控
        if self.config.is_follow_mode():
            asyncio.create_task(self._price_escape_monitor())
            self.logger.info("✅ 价格脱离监控已启动")
        
        self.logger.info("🚀 网格系统已启动")
    
    async def pause(self):
        """暂停网格系统（保留挂单）"""
        self._paused = True
        self.state.pause()
        
        self.logger.info("⏸️ 网格系统已暂停")
    
    async def resume(self):
        """恢复网格系统"""
        self._paused = False
        self._error_count = 0  # 重置错误计数
        self.state.resume()
        
        self.logger.info("▶️ 网格系统已恢复")
    
    async def stop(self):
        """停止网格系统（取消所有挂单）"""
        self._running = False
        self._paused = False
        
        # 取消所有挂单
        cancelled_count = await self.engine.cancel_all_orders()
        self.logger.info(f"取消了{cancelled_count}个挂单")
        
        # 停止引擎
        await self.engine.stop()
        
        # 更新状态
        self.state.stop()
        
        self.logger.info("⏹️ 网格系统已停止")
    
    async def get_statistics(self) -> GridStatistics:
        """
        获取统计数据
        
        Returns:
            网格统计数据
        """
        # 更新当前价格
        try:
            current_price = await self.engine.get_current_price()
            current_grid_id = self.config.get_grid_index_by_price(current_price)
            self.state.update_current_price(current_price, current_grid_id)
        except Exception as e:
            self.logger.warning(f"获取当前价格失败: {e}")
        
        # 获取统计数据
        stats = self.tracker.get_statistics()
        
        # 🔥 添加监控方式信息
        stats.monitoring_mode = self.engine.get_monitoring_mode()
        
        return stats
    
    def get_state(self) -> GridState:
        """获取网格状态"""
        return self.state
    
    def is_running(self) -> bool:
        """是否运行中"""
        return self._running and not self._paused
    
    def is_paused(self) -> bool:
        """是否暂停"""
        return self._paused
    
    def is_stopped(self) -> bool:
        """是否已停止"""
        return not self._running
    
    def get_status_text(self) -> str:
        """获取状态文本"""
        if self._paused:
            return "⏸️ 已暂停"
        elif self._running:
            return "🟢 运行中"
        else:
            return "⏹️ 已停止"
    
    def __repr__(self) -> str:
        return (
            f"GridCoordinator("
            f"status={self.get_status_text()}, "
            f"position={self.tracker.get_current_position()}, "
            f"errors={self._error_count})"
        )
    
    # ==================== 价格移动网格专用方法 ====================
    
    async def _price_escape_monitor(self):
        """
        价格脱离监控（价格移动网格专用）
        
        定期检查价格是否脱离网格范围，如果脱离时间超过阈值则重置网格
        """
        import time
        
        self.logger.info("🔍 价格脱离监控循环已启动")
        
        while self._running and not self._paused:
            try:
                current_time = time.time()
                
                # 检查间隔
                if current_time - self._last_escape_check_time < self._escape_check_interval:
                    await asyncio.sleep(1)
                    continue
                
                self._last_escape_check_time = current_time
                
                # 获取当前价格
                current_price = await self.engine.get_current_price()
                
                # 检查是否脱离
                should_reset, direction = self.config.check_price_escape(current_price)
                
                if should_reset:
                    # 记录脱离开始时间
                    if self._price_escape_start_time is None:
                        self._price_escape_start_time = current_time
                        self.logger.warning(
                            f"⚠️ 价格脱离网格范围（{direction}方向）: "
                            f"当前价格=${current_price:,.2f}, "
                            f"网格区间=[${self.config.lower_price:,.2f}, ${self.config.upper_price:,.2f}]"
                        )
                    
                    # 检查脱离时间是否超过阈值
                    escape_duration = current_time - self._price_escape_start_time
                    
                    if escape_duration >= self.config.follow_timeout:
                        self.logger.warning(
                            f"🔄 价格脱离超时（{escape_duration:.0f}秒 >= {self.config.follow_timeout}秒），"
                            f"准备重置网格..."
                        )
                        await self._reset_grid_for_price_follow(current_price, direction)
                        self._price_escape_start_time = None
                    else:
                        self.logger.info(
                            f"⏳ 价格脱离中（{direction}方向），"
                            f"已持续 {escape_duration:.0f}/{self.config.follow_timeout}秒"
                        )
                else:
                    # 价格回到范围内，重置脱离计时
                    if self._price_escape_start_time is not None:
                        self.logger.info(
                            f"✅ 价格已回到网格范围内: ${current_price:,.2f}"
                        )
                        self._price_escape_start_time = None
                
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                self.logger.info("价格脱离监控已停止")
                break
            except Exception as e:
                self.logger.error(f"价格脱离监控出错: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(10)  # 出错后等待10秒再继续
    
    async def _reset_grid_for_price_follow(self, current_price: Decimal, direction: str):
        """
        重置网格（价格移动网格专用）
        
        Args:
            current_price: 当前价格
            direction: 脱离方向 ("up" 或 "down")
        """
        if self._is_resetting:
            self.logger.warning("网格正在重置中，跳过本次重置")
            return
        
        try:
            self._is_resetting = True
            
            self.logger.info(
                f"🔄 开始重置网格: 当前价格=${current_price:,.2f}, 脱离方向={direction}"
            )
            
            # 1. 取消所有挂单
            self.logger.info("📋 步骤 1/6: 取消所有挂单...")
            cancelled_count = await self.engine.cancel_all_orders()
            self.logger.info(f"批量取消API返回: {cancelled_count} 个订单")
            
            # 2. 验证所有订单是否真的被取消（带重试机制）
            self.logger.info("📋 步骤 2/6: 验证订单取消状态...")
            
            max_retries = 3  # 最多重试3次
            retry_delay = 2  # 每次重试间隔2秒
            
            for retry in range(max_retries):
                # 等待让交易所处理取消请求
                if retry == 0:
                    await asyncio.sleep(1)  # 首次验证等待1秒
                else:
                    await asyncio.sleep(retry_delay)  # 重试时等待2秒
                
                # 获取当前未成交订单数量
                open_orders = await self.engine.exchange.get_open_orders(self.config.symbol)
                open_count = len(open_orders)
                
                if open_count == 0:
                    # 验证成功
                    self.logger.info(f"✅ 订单取消验证通过: 当前未成交订单 {open_count} 个")
                    break
                else:
                    # 验证失败
                    if retry < max_retries - 1:
                        # 还有重试机会，尝试再次取消
                        self.logger.warning(
                            f"⚠️ 第 {retry + 1} 次验证失败: 仍有 {open_count} 个未成交订单"
                        )
                        self.logger.info(f"🔄 尝试再次取消这些订单...")
                        
                        # 再次调用取消订单
                        retry_cancelled = await self.engine.cancel_all_orders()
                        self.logger.info(f"重试取消返回: {retry_cancelled} 个订单")
                    else:
                        # 已达到最大重试次数，放弃
                        self.logger.error(
                            f"❌ 订单取消验证最终失败！已重试 {max_retries} 次，仍有 {open_count} 个未成交订单"
                        )
                        self.logger.error(f"预期: 0 个订单, 实际: {open_count} 个订单")
                        self.logger.error("⚠️ 网格重置已暂停，不会挂出新订单，避免超出订单限制")
                        self.logger.error("💡 建议: 请手动检查交易所订单，或等待下次价格脱离时自动重试")
                        
                        # 不继续后续步骤，直接返回
                        return
            
            # 3. 清空状态
            self.logger.info("📋 步骤 3/6: 清空网格状态...")
            self.state.active_orders.clear()
            self.state.pending_buy_orders = 0
            self.state.pending_sell_orders = 0
            self.logger.info("✅ 网格状态已清空")
            
            # 4. 更新价格区间
            self.logger.info("📋 步骤 4/6: 更新价格区间...")
            old_range = (self.config.lower_price, self.config.upper_price)
            self.config.update_price_range_for_follow_mode(current_price)
            self.logger.info(
                f"✅ 价格区间已更新: "
                f"[${old_range[0]:,.2f}, ${old_range[1]:,.2f}] → "
                f"[${self.config.lower_price:,.2f}, ${self.config.upper_price:,.2f}]"
            )
            
            # 5. 重新初始化网格层级
            self.logger.info("📋 步骤 5/6: 重新初始化网格层级...")
            self.state.initialize_grid_levels(
                self.config.grid_count,
                self.config.get_grid_price
            )
            self.logger.info(f"✅ 网格层级已重新初始化，共{self.config.grid_count}个")
            
            # 6. 生成并挂出新订单
            self.logger.info("📋 步骤 6/6: 生成并挂出新订单...")
            initial_orders = self.strategy.initialize(self.config)
            placed_orders = await self.engine.place_batch_orders(initial_orders)
            
            # 添加到状态
            for order in placed_orders:
                if order.order_id in self.state.active_orders:
                    continue
                if order.status == GridOrderStatus.FILLED:
                    continue
                self.state.add_order(order)
            
            self.logger.info(
                f"✅ 网格重置完成！成功挂出 {len(placed_orders)} 个订单，"
                f"新价格区间: [${self.config.lower_price:,.2f}, ${self.config.upper_price:,.2f}]"
            )
            
        except Exception as e:
            self.logger.error(f"❌ 网格重置失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            self._handle_error(e)
        finally:
            self._is_resetting = False

