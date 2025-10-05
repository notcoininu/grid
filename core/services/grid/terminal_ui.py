"""
网格交易系统终端界面

使用Rich库实现实时监控界面
"""

import asyncio
from typing import Optional
from datetime import timedelta
from decimal import Decimal

from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text

from ...logging import get_logger
from .models import GridStatistics, GridType
from .coordinator import GridCoordinator


class GridTerminalUI:
    """
    网格交易终端界面
    
    显示内容：
    1. 运行状态
    2. 订单统计
    3. 持仓信息
    4. 盈亏统计
    5. 最近成交订单
    """
    
    def __init__(self, coordinator: GridCoordinator):
        """
        初始化终端界面
        
        Args:
            coordinator: 网格协调器
        """
        self.logger = get_logger(__name__)
        self.coordinator = coordinator
        self.console = Console()
        
        # 界面配置
        self.refresh_rate = 1  # 刷新频率（次/秒）- 降低刷新率减少闪烁
        self.history_limit = 10  # 显示历史记录数
        
        # 运行控制
        self._running = False
    
    def create_header(self, stats: GridStatistics) -> Panel:
        """创建标题栏"""
        # 判断网格类型（做多/做空）
        is_long = self.coordinator.config.grid_type in [GridType.LONG, GridType.MARTINGALE_LONG, GridType.FOLLOW_LONG]
        grid_type_text = "做多网格" if is_long else "做空网格"
        
        title = Text()
        title.append("🎯 网格交易系统实时监控 - ", style="bold cyan")
        title.append(f"{self.coordinator.config.exchange.upper()}/", style="bold yellow")
        title.append(f"{self.coordinator.config.symbol}", style="bold green")
        
        return Panel(title, style="bold white on blue")
    
    def create_status_panel(self, stats: GridStatistics) -> Panel:
        """创建运行状态面板"""
        # 判断网格类型（做多/做空）和模式（普通/马丁/价格移动）
        grid_type = self.coordinator.config.grid_type
        
        if grid_type == GridType.LONG:
            grid_type_text = "做多网格（普通）"
        elif grid_type == GridType.SHORT:
            grid_type_text = "做空网格（普通）"
        elif grid_type == GridType.MARTINGALE_LONG:
            grid_type_text = "做多网格（马丁）"
        elif grid_type == GridType.MARTINGALE_SHORT:
            grid_type_text = "做空网格（马丁）"
        elif grid_type == GridType.FOLLOW_LONG:
            grid_type_text = "做多网格（价格移动）"
        elif grid_type == GridType.FOLLOW_SHORT:
            grid_type_text = "做空网格（价格移动）"
        else:
            grid_type_text = grid_type.value
        
        status_text = self.coordinator.get_status_text()
        
        # 格式化运行时长
        running_time = str(stats.running_time).split('.')[0]  # 移除微秒
        
        content = Text()
        content.append(f"├─ 网格策略: {grid_type_text} ({stats.grid_count}格)   ", style="white")
        content.append(f"状态: {status_text}\n", style="bold")
        
        content.append(f"├─ 价格区间: ${stats.price_range[0]:,.2f} - ${stats.price_range[1]:,.2f}  ", style="white")
        content.append(f"网格间隔: ${stats.grid_interval}\n", style="cyan")
        
        content.append(f"├─ 当前价格: ${stats.current_price:,.2f}             ", style="bold yellow")
        content.append(f"当前位置: Grid {stats.current_grid_id}/{stats.grid_count}\n", style="white")
        
        content.append(f"└─ 运行时长: {running_time}", style="white")
        
        return Panel(content, title="📊 运行状态", border_style="green")
    
    def create_orders_panel(self, stats: GridStatistics) -> Panel:
        """创建订单统计面板"""
        content = Text()
        
        # 🔥 显示监控方式
        monitoring_mode = getattr(stats, 'monitoring_mode', 'WebSocket')
        if monitoring_mode == "WebSocket":
            mode_icon = "📡"
            mode_style = "bold cyan"
        else:
            mode_icon = "📊"
            mode_style = "bold yellow"
        
        content.append(f"├─ 监控方式: ", style="white")
        content.append(f"{mode_icon} {monitoring_mode}", style=mode_style)
        content.append("\n")
        
        # 计算网格范围
        if stats.pending_buy_orders > 0:
            buy_range = f"Grid {stats.current_grid_id + 1}-{stats.grid_count}"
        else:
            buy_range = "无"
        
        if stats.pending_sell_orders > 0:
            sell_range = f"Grid 1-{stats.current_grid_id}"
        else:
            sell_range = "无"
        
        content.append(f"├─ 未成交买单: {stats.pending_buy_orders}个 ({buy_range}) ⏳\n", style="green")
        content.append(f"├─ 未成交卖单: {stats.pending_sell_orders}个 ({sell_range}) ⏳\n", style="red")
        content.append(f"└─ 总挂单数量: {stats.total_pending_orders}个", style="white")
        
        return Panel(content, title="📋 订单统计", border_style="blue")
    
    def create_position_panel(self, stats: GridStatistics) -> Panel:
        """创建持仓信息面板"""
        position_color = "green" if stats.current_position > 0 else "red" if stats.current_position < 0 else "white"
        position_type = "做多" if stats.current_position > 0 else "做空" if stats.current_position < 0 else "空仓"
        
        # 未实现盈亏颜色
        unrealized_color = "green" if stats.unrealized_profit > 0 else "red" if stats.unrealized_profit < 0 else "white"
        unrealized_sign = "+" if stats.unrealized_profit > 0 else ""
        
        content = Text()
        content.append(f"├─ 当前持仓: ", style="white")
        content.append(f"{stats.current_position:+.4f} BTC ({position_type})      ", style=f"bold {position_color}")
        content.append(f"平均成本: ${stats.average_cost:,.2f}\n", style="white")
        
        content.append(f"├─ 可用资金: ${stats.available_balance:,.2f} USDC      ", style="white")
        content.append(f"冻结资金: ${stats.frozen_balance:,.2f}\n", style="yellow")
        
        content.append(f"└─ 未实现盈亏: ", style="white")
        content.append(f"{unrealized_sign}${stats.unrealized_profit:,.2f} ", style=f"bold {unrealized_color}")
        content.append(f"({unrealized_sign}{stats.unrealized_profit/abs(stats.current_position * stats.current_price) * 100 if stats.current_position != 0 else 0:.2f}%)", 
                      style=unrealized_color)
        
        return Panel(content, title="💰 持仓信息", border_style="yellow")
    
    def create_pnl_panel(self, stats: GridStatistics) -> Panel:
        """创建盈亏统计面板"""
        # 总盈亏颜色
        total_color = "green" if stats.total_profit > 0 else "red" if stats.total_profit < 0 else "white"
        total_sign = "+" if stats.total_profit >= 0 else ""
        
        # 已实现盈亏颜色
        realized_color = "green" if stats.realized_profit > 0 else "red" if stats.realized_profit < 0 else "white"
        realized_sign = "+" if stats.realized_profit >= 0 else ""
        
        # 收益率颜色
        rate_color = "green" if stats.profit_rate > 0 else "red" if stats.profit_rate < 0 else "white"
        rate_sign = "+" if stats.profit_rate >= 0 else ""
        
        content = Text()
        content.append(f"├─ 已实现: ", style="white")
        content.append(f"{realized_sign}${stats.realized_profit:,.2f}             ", style=f"bold {realized_color}")
        content.append(f"网格收益: {realized_sign}${stats.realized_profit:,.2f}\n", style=realized_color)
        
        content.append(f"├─ 未实现: ", style="white")
        content.append(f"{'+' if stats.unrealized_profit >= 0 else ''}${stats.unrealized_profit:,.2f}             ", 
                      style="cyan" if stats.unrealized_profit >= 0 else "red")
        content.append(f"手续费: -${stats.total_fees:,.2f}\n", style="red")
        
        content.append(f"└─ 总盈亏: ", style="white")
        content.append(f"{total_sign}${stats.total_profit:,.2f} ", style=f"bold {total_color}")
        content.append(f"({rate_sign}{stats.profit_rate:.2f}%)  ", style=f"bold {rate_color}")
        content.append(f"净收益: {total_sign}${stats.net_profit:,.2f}", style=total_color)
        
        return Panel(content, title="🎯 盈亏统计", border_style="magenta")
    
    def create_trigger_panel(self, stats: GridStatistics) -> Panel:
        """创建触发统计面板"""
        content = Text()
        
        content.append(f"├─ 买单成交: {stats.filled_buy_count}次               ", style="green")
        content.append(f"卖单成交: {stats.filled_sell_count}次\n", style="red")
        
        content.append(f"├─ 完整循环: {stats.completed_cycles}次 (一买一卖)      ", style="yellow")
        content.append(f"网格利用率: {stats.grid_utilization:.1f}%\n", style="cyan")
        
        # 平均每次循环收益
        avg_cycle_profit = stats.realized_profit / stats.completed_cycles if stats.completed_cycles > 0 else Decimal('0')
        content.append(f"└─ 平均循环收益: ${avg_cycle_profit:,.2f}", 
                      style="green" if avg_cycle_profit > 0 else "white")
        
        return Panel(content, title="🎯 触发统计", border_style="cyan")
    
    def create_recent_trades_table(self, stats: GridStatistics) -> Panel:
        """创建最近成交订单表格"""
        table = Table(show_header=True, header_style="bold magenta", box=None)
        
        table.add_column("时间", style="cyan", width=10)
        table.add_column("类型", width=4)
        table.add_column("价格", style="yellow", width=12)
        table.add_column("数量", style="white", width=12)
        table.add_column("网格层级", style="blue", width=10)
        
        # 获取最近交易记录
        trades = self.coordinator.tracker.get_trade_history(self.history_limit)
        
        for trade in reversed(trades[-5:]):  # 只显示最新5条
            time_str = trade['time'].strftime("%H:%M:%S")
            side = trade['side']
            side_style = "green" if side == "buy" else "red"
            price = f"${trade['price']:,.2f}"
            amount = f"{trade['amount']:.4f} BTC"
            grid_text = f"Grid {trade['grid_id']}"
            
            table.add_row(
                time_str,
                f"[{side_style}]{side.upper()}[/{side_style}]",
                price,
                amount,
                grid_text
            )
        
        if not trades:
            table.add_row("--", "--", "--", "--", "--")
        
        return Panel(table, title="📈 最近成交订单 (最新5条)", border_style="green")
    
    def create_controls_panel(self) -> Panel:
        """创建控制命令面板"""
        content = Text()
        content.append("[P]", style="bold yellow")
        content.append("暂停  ", style="white")
        content.append("[R]", style="bold green")
        content.append("恢复  ", style="white")
        content.append("[S]", style="bold red")
        content.append("停止  ", style="white")
        content.append("[Q]", style="bold cyan")
        content.append("退出", style="white")
        
        return Panel(content, title="🔧 控制命令", border_style="white")
    
    def create_layout(self, stats: GridStatistics) -> Layout:
        """创建完整布局"""
        layout = Layout()
        
        layout.split_column(
            Layout(self.create_header(stats), size=3),
            Layout(name="main"),
            Layout(self.create_controls_panel(), size=3)
        )
        
        layout["main"].split_row(
            Layout(name="left"),
            Layout(name="right")
        )
        
        layout["left"].split_column(
            Layout(self.create_status_panel(stats)),
            Layout(self.create_orders_panel(stats)),
            Layout(self.create_trigger_panel(stats))
        )
        
        layout["right"].split_column(
            Layout(self.create_position_panel(stats)),
            Layout(self.create_pnl_panel(stats)),
            Layout(self.create_recent_trades_table(stats))
        )
        
        return layout
    
    async def run(self):
        """运行终端界面"""
        self._running = True
        
        # ✅ 在 Live 上下文之前打印启动信息
        self.console.print("\n[bold green]✅ 网格交易系统终端界面已启动[/bold green]")
        self.console.print("[cyan]提示: 使用 Ctrl+C 停止系统[/cyan]\n")
        
        # 短暂延迟，让启动信息显示
        await asyncio.sleep(1)
        
        # ✅ 清屏，避免之前的输出干扰
        self.console.clear()
        
        with Live(self.create_layout(await self.coordinator.get_statistics()), 
                  refresh_per_second=self.refresh_rate,
                  console=self.console,
                  screen=True) as live:  # ✅ 使用全屏模式
            
            try:
                while self._running:
                    # 获取最新统计数据
                    stats = await self.coordinator.get_statistics()
                    
                    # 更新界面
                    live.update(self.create_layout(stats))
                    
                    # 休眠
                    await asyncio.sleep(1 / self.refresh_rate)
                    
            except KeyboardInterrupt:
                self.console.print("\n[yellow]收到退出信号...[/yellow]")
            finally:
                self._running = False
    
    def stop(self):
        """停止终端界面"""
        self._running = False

