"""
持仓跟踪器实现

跟踪网格系统的持仓、盈亏、交易历史等
"""

from typing import Dict, List, Deque
from decimal import Decimal
from datetime import datetime, timedelta
from collections import deque

from ....logging import get_logger
from ..interfaces.position_tracker import IPositionTracker
from ..models import (
    GridOrder, GridStatistics, GridMetrics,
    GridConfig, GridState
)


class PositionTrackerImpl(IPositionTracker):
    """
    持仓跟踪器实现
    
    功能：
    1. 跟踪当前持仓和成本
    2. 计算已实现和未实现盈亏
    3. 记录交易历史
    4. 生成统计数据
    """
    
    def __init__(self, config: GridConfig, grid_state: GridState):
        """
        初始化持仓跟踪器
        
        Args:
            config: 网格配置
            grid_state: 网格状态
        """
        self.logger = get_logger(__name__)
        self.config = config
        self.state = grid_state
        
        # 持仓信息
        self.current_position = Decimal('0')      # 当前持仓数量
        self.position_cost = Decimal('0')         # 持仓总成本
        self.average_cost = Decimal('0')          # 平均成本
        
        # 盈亏统计
        self.realized_pnl = Decimal('0')          # 已实现盈亏
        self.total_fees = Decimal('0')            # 总手续费
        
        # 交易历史（最近1000条）
        self.trade_history: Deque[Dict] = deque(maxlen=1000)
        
        # 统计信息
        self.buy_count = 0
        self.sell_count = 0
        self.completed_cycles = 0
        
        # 资金信息（需要从交易所获取）
        self.available_balance = Decimal('0')
        self.frozen_balance = Decimal('0')
        
        # 时间信息
        self.start_time = datetime.now()
        self.last_trade_time = datetime.now()
        
        self.logger.info("持仓跟踪器初始化完成")
    
    def record_filled_order(self, order: GridOrder):
        """
        记录成交订单
        
        Args:
            order: 成交订单
        """
        if not order.is_filled():
            self.logger.warning(f"订单{order.order_id}未成交，跳过记录")
            return
        
        filled_price = order.filled_price or order.price
        filled_amount = order.filled_amount or order.amount
        
        # 更新持仓
        if order.is_buy_order():
            # 买单：增加持仓
            self.position_cost += filled_price * filled_amount
            self.current_position += filled_amount
            self.buy_count += 1
            
            self.logger.debug(
                f"买入: {filled_amount}@{filled_price}, "
                f"持仓: {self.current_position}"
            )
        else:
            # 卖单：减少持仓，计算已实现盈亏
            if self.current_position > 0:
                # 计算这笔卖出对应的成本
                avg_cost = self.position_cost / self.current_position if self.current_position > 0 else Decimal('0')
                sell_cost = avg_cost * filled_amount
                sell_value = filled_price * filled_amount
                
                # 已实现盈亏
                profit = sell_value - sell_cost
                self.realized_pnl += profit
                
                # 更新持仓成本
                self.position_cost -= sell_cost
                self.current_position -= filled_amount
                
                self.logger.debug(
                    f"卖出: {filled_amount}@{filled_price}, "
                    f"成本: {avg_cost}, 盈亏: {profit}, "
                    f"持仓: {self.current_position}"
                )
            else:
                # 如果是做空，持仓为负
                self.position_cost -= filled_price * filled_amount
                self.current_position -= filled_amount
            
            self.sell_count += 1
        
        # 更新平均成本
        if self.current_position != 0:
            self.average_cost = self.position_cost / abs(self.current_position)
        else:
            self.average_cost = Decimal('0')
        
        # 计算手续费（使用配置的手续费率）
        fee = filled_price * filled_amount * self.config.fee_rate
        self.total_fees += fee
        
        # 更新完成循环次数
        self.completed_cycles = min(self.buy_count, self.sell_count)
        
        # 记录交易历史
        self._record_trade(order, filled_price, filled_amount, profit if order.is_sell_order() else None)
        
        # 更新最后交易时间
        self.last_trade_time = datetime.now()
        
        self.logger.info(
            f"记录成交: {order.side.value} {filled_amount}@{filled_price}, "
            f"持仓: {self.current_position}, 已实现盈亏: {self.realized_pnl}"
        )
    
    def _record_trade(self, order: GridOrder, price: Decimal, amount: Decimal, profit: Decimal = None):
        """
        记录交易到历史
        
        Args:
            order: 订单
            price: 成交价格
            amount: 成交数量
            profit: 利润（卖单才有）
        """
        trade_record = {
            'time': order.filled_at or datetime.now(),
            'order_id': order.order_id,
            'grid_id': order.grid_id,
            'side': order.side.value,
            'price': float(price),
            'amount': float(amount),
            'value': float(price * amount),
            'profit': float(profit) if profit else None,
            'position_after': float(self.current_position),
            'realized_pnl': float(self.realized_pnl)
        }
        
        self.trade_history.append(trade_record)
    
    def get_current_position(self) -> Decimal:
        """
        获取当前持仓
        
        Returns:
            持仓数量（正数=多头，负数=空头）
        """
        return self.current_position
    
    def get_average_cost(self) -> Decimal:
        """
        获取平均持仓成本
        
        Returns:
            平均成本
        """
        return self.average_cost
    
    def calculate_unrealized_pnl(self, current_price: Decimal) -> Decimal:
        """
        计算未实现盈亏
        
        Args:
            current_price: 当前价格
        
        Returns:
            未实现盈亏
        """
        if self.current_position == 0:
            return Decimal('0')
        
        # 未实现盈亏 = (当前价格 - 平均成本) * 持仓数量
        unrealized_pnl = (current_price - self.average_cost) * self.current_position
        
        return unrealized_pnl
    
    def get_realized_pnl(self) -> Decimal:
        """
        获取已实现盈亏
        
        Returns:
            已实现盈亏
        """
        return self.realized_pnl
    
    def get_total_pnl(self, current_price: Decimal) -> Decimal:
        """
        获取总盈亏（已实现+未实现）
        
        Args:
            current_price: 当前价格
        
        Returns:
            总盈亏
        """
        unrealized = self.calculate_unrealized_pnl(current_price)
        return self.realized_pnl + unrealized
    
    def get_statistics(self) -> GridStatistics:
        """
        获取统计数据
        
        Returns:
            网格统计数据
        """
        # 获取当前价格
        current_price = self.state.current_price or self.config.get_first_order_price()
        
        # 计算未实现盈亏
        unrealized_pnl = self.calculate_unrealized_pnl(current_price)
        total_pnl = self.realized_pnl + unrealized_pnl
        net_profit = total_pnl - self.total_fees
        
        # 计算收益率
        initial_capital = self.config.order_amount * self.config.grid_count * current_price
        profit_rate = (net_profit / initial_capital * 100) if initial_capital > 0 else Decimal('0')
        
        # 计算资金利用率
        total_balance = self.available_balance + self.frozen_balance
        capital_utilization = (self.frozen_balance / total_balance * 100) if total_balance > 0 else 0.0
        
        # 运行时长
        running_time = datetime.now() - self.start_time
        
        statistics = GridStatistics(
            grid_count=self.config.grid_count,
            grid_interval=self.config.grid_interval,
            price_range=(self.config.lower_price, self.config.upper_price),
            current_price=current_price,
            current_grid_id=self.state.current_grid_id or 1,
            current_position=self.current_position,
            average_cost=self.average_cost,
            pending_buy_orders=self.state.pending_buy_orders,
            pending_sell_orders=self.state.pending_sell_orders,
            total_pending_orders=self.state.pending_buy_orders + self.state.pending_sell_orders,
            filled_buy_count=self.buy_count,
            filled_sell_count=self.sell_count,
            completed_cycles=self.completed_cycles,
            realized_profit=self.realized_pnl,
            unrealized_profit=unrealized_pnl,
            total_profit=total_pnl,
            total_fees=self.total_fees,
            net_profit=net_profit,
            profit_rate=profit_rate,
            grid_utilization=self.state.get_grid_utilization(),
            available_balance=self.available_balance,
            frozen_balance=self.frozen_balance,
            total_balance=total_balance,
            capital_utilization=capital_utilization,
            running_time=running_time,
            last_trade_time=self.last_trade_time
        )
        
        return statistics
    
    def get_metrics(self) -> GridMetrics:
        """
        获取性能指标
        
        Returns:
            网格性能指标
        """
        metrics = GridMetrics()
        
        # 获取当前价格
        current_price = self.state.current_price or self.config.get_first_order_price()
        
        # 计算总利润
        metrics.total_profit = self.get_total_pnl(current_price)
        
        # 计算收益率
        initial_capital = self.config.order_amount * self.config.grid_count * current_price
        if initial_capital > 0:
            metrics.profit_rate = (metrics.total_profit / initial_capital) * 100
        
        # 交易统计
        metrics.total_trades = self.buy_count + self.sell_count
        metrics.win_trades = self.completed_cycles  # 完整循环都算盈利
        metrics.loss_trades = 0  # 网格交易通常不会亏损（除非单边行情）
        
        if metrics.total_trades > 0:
            metrics.win_rate = (metrics.win_trades / (metrics.total_trades / 2)) * 100  # 一买一卖算一次
        
        # 计算日均收益
        running_days = (datetime.now() - self.start_time).days
        if running_days > 0:
            metrics.daily_profit = metrics.total_profit / Decimal(str(running_days))
            metrics.running_days = running_days
        
        # 计算平均每笔收益
        if self.completed_cycles > 0:
            metrics.avg_profit_per_trade = self.realized_pnl / Decimal(str(self.completed_cycles))
        
        # 手续费统计
        metrics.total_fees = self.total_fees
        if metrics.total_profit != 0:
            metrics.fee_rate = (self.total_fees / abs(metrics.total_profit)) * 100
        
        # 持仓统计
        metrics.max_position = abs(self.current_position)  # 简化处理
        metrics.avg_position = abs(self.current_position)
        
        return metrics
    
    def get_trade_history(self, limit: int = 10) -> List[Dict]:
        """
        获取交易历史
        
        Args:
            limit: 返回记录数
        
        Returns:
            交易记录列表
        """
        # 返回最新的N条记录
        history_list = list(self.trade_history)
        return history_list[-limit:] if len(history_list) > limit else history_list
    
    def update_balance(self, available: Decimal, frozen: Decimal):
        """
        更新资金信息
        
        Args:
            available: 可用资金
            frozen: 冻结资金
        """
        self.available_balance = available
        self.frozen_balance = frozen
    
    def reset(self):
        """重置跟踪器"""
        self.current_position = Decimal('0')
        self.position_cost = Decimal('0')
        self.average_cost = Decimal('0')
        self.realized_pnl = Decimal('0')
        self.total_fees = Decimal('0')
        self.trade_history.clear()
        self.buy_count = 0
        self.sell_count = 0
        self.completed_cycles = 0
        self.start_time = datetime.now()
        self.last_trade_time = datetime.now()
        
        self.logger.info("持仓跟踪器已重置")
    
    def __repr__(self) -> str:
        return (
            f"PositionTracker(position={self.current_position}, "
            f"avg_cost={self.average_cost}, "
            f"realized_pnl={self.realized_pnl})"
        )

