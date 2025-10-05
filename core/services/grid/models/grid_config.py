"""
网格配置模型

定义网格交易系统的配置参数
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from decimal import Decimal


class GridType(Enum):
    """网格类型"""
    LONG = "long"                          # 做多网格（普通）
    SHORT = "short"                        # 做空网格（普通）
    MARTINGALE_LONG = "martingale_long"    # 马丁做多网格
    MARTINGALE_SHORT = "martingale_short"  # 马丁做空网格
    FOLLOW_LONG = "follow_long"            # 价格移动做多网格
    FOLLOW_SHORT = "follow_short"          # 价格移动做空网格


class GridDirection(Enum):
    """网格方向（内部使用）"""
    UP = "up"      # 向上（价格上涨方向）
    DOWN = "down"  # 向下（价格下跌方向）


@dataclass
class GridConfig:
    """
    网格配置
    
    所有参数由用户在配置文件中设置
    """
    
    # 基础参数（必需参数）
    exchange: str                           # 交易所名称 (如 "backpack")
    symbol: str                             # 交易对符号 (如 "BTC_USDC_PERP")
    grid_type: GridType                     # 网格类型（做多/做空）
    grid_interval: Decimal                  # 网格间隔（等差）
    order_amount: Decimal                   # 每格订单数量（基础金额）
    
    # 价格区间参数（可选参数，价格移动网格时不需要）
    lower_price: Optional[Decimal] = None   # 价格下限（价格移动网格时可选）
    upper_price: Optional[Decimal] = None   # 价格上限（价格移动网格时可选）
    
    # 计算得出的参数
    grid_count: int = field(init=False)     # 网格数量（自动计算或用户指定）
    
    # 可选参数
    max_position: Optional[Decimal] = None  # 最大持仓限制
    enable_notifications: bool = True        # 是否启用通知
    order_health_check_interval: int = 600   # 订单健康检查间隔（秒，默认10分钟）
    fee_rate: Decimal = Decimal('0.0001')    # 手续费率（默认万分之1）
    
    # 马丁网格参数（可选）
    martingale_increment: Optional[Decimal] = None  # 马丁网格递增金额（None表示不启用马丁模式）
    
    # 价格移动网格参数（可选）
    follow_grid_count: Optional[int] = None         # 价格移动网格数量（用户指定）
    follow_timeout: int = 300                       # 脱离超时时间（秒，默认5分钟）
    follow_distance: int = 1                        # 脱离距离（网格数，默认1格）
    
    def __post_init__(self):
        """初始化后计算网格数量"""
        # 🔥 价格移动网格：使用用户指定的网格数量
        if self.is_follow_mode():
            if self.follow_grid_count is None:
                raise ValueError("价格移动网格必须指定 follow_grid_count")
            self.grid_count = self.follow_grid_count
            # 价格区间将在运行时根据当前价格动态计算
        else:
            # 普通网格和马丁网格：根据价格区间计算网格数量
            if self.upper_price is None or self.lower_price is None:
                raise ValueError("普通网格和马丁网格必须指定 upper_price 和 lower_price")
            price_range = abs(self.upper_price - self.lower_price)
            self.grid_count = int(price_range / self.grid_interval)
        
        # 验证参数
        self._validate()
    
    def _validate(self):
        """验证配置参数"""
        # 价格移动网格的价格区间在运行时动态设置，跳过验证
        if self.is_follow_mode():
            if self.follow_grid_count is None or self.follow_grid_count <= 0:
                raise ValueError("价格移动网格必须指定有效的 follow_grid_count")
            if self.grid_interval is None or self.grid_interval <= 0:
                raise ValueError("网格间隔必须大于0")
            return
        
        # 普通网格和马丁网格验证
        if self.lower_price >= self.upper_price:
            raise ValueError("下限价格必须小于上限价格")
        
        if self.grid_interval <= 0:
            raise ValueError("网格间隔必须大于0")
        
        if self.order_amount <= 0:
            raise ValueError("订单数量必须大于0")
        
        if self.grid_count <= 0:
            raise ValueError(f"网格数量必须大于0，当前计算结果: {self.grid_count}")
    
    def get_first_order_price(self) -> Decimal:
        """
        获取第一个订单的价格
        
        做多网格：上限 - 1个网格间隔
        做空网格：下限 + 1个网格间隔
        """
        if self.grid_type == GridType.LONG:
            return self.upper_price - self.grid_interval
        else:  # SHORT
            return self.lower_price + self.grid_interval
    
    def get_grid_price(self, grid_index: int) -> Decimal:
        """
        获取指定网格索引的价格
        
        Args:
            grid_index: 网格索引 (1-based)
        
        Returns:
            该网格的价格
        """
        if self.grid_type == GridType.LONG:
            # 做多网格：从上限开始递减
            return self.upper_price - (grid_index * self.grid_interval)
        else:  # SHORT
            # 做空网格：从下限开始递增
            return self.lower_price + (grid_index * self.grid_interval)
    
    def get_grid_index_by_price(self, price: Decimal) -> int:
        """
        根据价格获取网格索引
        
        Args:
            price: 价格
        
        Returns:
            网格索引 (1-based)
        """
        if self.grid_type == GridType.LONG:
            # 做多网格
            index = int((self.upper_price - price) / self.grid_interval)
        else:  # SHORT
            # 做空网格
            index = int((price - self.lower_price) / self.grid_interval)
        
        # 确保索引在有效范围内
        return max(0, min(index, self.grid_count))
    
    def is_price_in_range(self, price: Decimal) -> bool:
        """检查价格是否在网格区间内"""
        return self.lower_price <= price <= self.upper_price
    
    def is_martingale_mode(self) -> bool:
        """
        判断是否为马丁网格模式
        
        Returns:
            True: 马丁网格模式
            False: 普通网格模式
        """
        return (
            self.grid_type in [GridType.MARTINGALE_LONG, GridType.MARTINGALE_SHORT] or
            self.martingale_increment is not None
        )
    
    def is_follow_mode(self) -> bool:
        """
        判断是否为价格移动网格模式
        
        Returns:
            True: 价格移动网格模式
            False: 其他模式
        """
        return self.grid_type in [GridType.FOLLOW_LONG, GridType.FOLLOW_SHORT]
    
    def update_price_range_for_follow_mode(self, current_price: Decimal):
        """
        为价格移动网格动态更新价格区间
        
        Args:
            current_price: 当前市场价格
            
        逻辑：
            做多网格：以当前价格为上限，向下计算下限
            做空网格：以当前价格为下限，向上计算上限
        """
        if not self.is_follow_mode():
            return
        
        if self.grid_type == GridType.FOLLOW_LONG:
            # 做多网格：当前价格为上限
            self.upper_price = current_price
            self.lower_price = current_price - (self.grid_count * self.grid_interval)
        elif self.grid_type == GridType.FOLLOW_SHORT:
            # 做空网格：当前价格为下限
            self.lower_price = current_price
            self.upper_price = current_price + (self.grid_count * self.grid_interval)
    
    def check_price_escape(self, current_price: Decimal) -> tuple[bool, str]:
        """
        检查价格是否脱离网格范围
        
        Args:
            current_price: 当前市场价格
            
        Returns:
            (是否需要重置, 脱离方向)
            
        逻辑：
            做多网格：只在向上脱离时重置（盈利方向）
            做空网格：只在向下脱离时重置（盈利方向）
        """
        if not self.is_follow_mode():
            return False, ""
        
        escape_threshold = self.grid_interval * self.follow_distance
        
        if self.grid_type == GridType.FOLLOW_LONG:
            # 做多网格：检查向上脱离（盈利方向）
            if current_price > self.upper_price + escape_threshold:
                return True, "up"
            # 向下脱离（亏损方向）不重置
            return False, ""
            
        elif self.grid_type == GridType.FOLLOW_SHORT:
            # 做空网格：检查向下脱离（盈利方向）
            if current_price < self.lower_price - escape_threshold:
                return True, "down"
            # 向上脱离（亏损方向）不重置
            return False, ""
        
        return False, ""
    
    def get_grid_order_amount(self, grid_index: int) -> Decimal:
        """
        获取指定网格的订单金额
        
        Args:
            grid_index: 网格索引 (1-based)
        
        Returns:
            该网格的订单金额
            
        逻辑：
            普通网格：固定金额 = order_amount
            马丁网格：递增金额 = order_amount + (grid_index - 1) * martingale_increment
            价格移动网格：固定金额 = order_amount
        """
        if not self.is_martingale_mode():
            # 普通网格和价格移动网格：固定金额
            return self.order_amount
        
        # 马丁网格：递增金额
        return self.order_amount + (grid_index - 1) * self.martingale_increment
    
    def __repr__(self) -> str:
        mode = "Martingale" if self.is_martingale_mode() else "Normal"
        return (
            f"GridConfig(exchange={self.exchange}, symbol={self.symbol}, "
            f"type={self.grid_type.value}, mode={mode}, "
            f"range=[{self.lower_price}, {self.upper_price}], "
            f"interval={self.grid_interval}, grids={self.grid_count})"
        )

