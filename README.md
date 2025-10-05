# 网格交易系统 (Grid Trading System)

一个功能强大的加密货币网格交易系统，支持多种网格策略和多个交易所。

## ✨ 特性

- 🎯 **三种网格模式**
  - 普通网格：固定金额，适合震荡行情
  - 马丁网格：递增金额，适合回归行情（高风险）
  - 价格移动网格：自动跟随价格，适合趋势行情

- 🔄 **智能订单监控**
  - WebSocket实时监控（主要）
  - REST API轮询（备用）
  - 自动切换和恢复机制

- 📊 **实时终端界面**
  - 网格运行状态
  - 订单统计
  - 持仓信息
  - 盈亏统计
  - 触发统计
  - 最近成交订单

- 🏦 **支持多个交易所**
  - Backpack
  - EdgeX
  - Hyperliquid
  - Binance
  - OKX

## 📋 系统要求

- Python 3.8+
- 稳定的网络连接
- 交易所API密钥

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置API密钥

编辑 `config/exchanges/backpack_config.yaml`（或其他交易所配置文件）：

```yaml
authentication:
  api_key: "your_api_key_here"
  private_key: "your_private_key_here"
```

**⚠️ 安全提示**：
- 不要将包含真实API密钥的配置文件提交到Git
- 建议使用环境变量存储API密钥
- 确保API密钥只有必要的权限（交易权限）

### 3. 配置网格参数

编辑 `config/grid/` 目录下的配置文件，例如 `backpack_long_grid.yaml`：

```yaml
grid_system:
  exchange: "backpack"
  symbol: "BTC_USDC_PERP"
  grid_type: "long"           # 网格类型
  
  upper_price: 123500.00      # 价格上限
  lower_price: 103600.00      # 价格下限
  grid_interval: 100.00       # 网格间隔
  order_amount: 0.0001        # 每格订单数量
  fee_rate: 0.0001            # 手续费率
```

### 4. 运行网格系统

```bash
# 普通做多网格
python run_grid_trading.py --config config/grid/backpack_long_grid.yaml

# 马丁做多网格
python run_grid_trading.py --config config/grid/backpack_martingale_long_grid.yaml

# 价格移动做多网格
python run_grid_trading.py --config config/grid/backpack_follow_long_grid.yaml
```

## 📖 详细文档

- [网格系统运行指南](docs/网格系统运行指南.md) - 完整的使用指南
- [三种网格模式完整指南](docs/三种网格模式完整指南.md) - 三种模式的详细说明
- [网格交易系统快速入门](docs/网格交易系统快速入门.md) - 快速入门教程
- [网格重置订单验证机制](docs/网格重置订单验证机制.md) - 价格移动网格的重置机制

## 🎯 网格模式对比

| 特性 | 普通网格 | 马丁网格 | 价格移动网格 |
|------|----------|----------|------------|
| **订单金额** | 固定 | 递增 | 固定 |
| **价格区间** | 固定 | 固定 | 动态跟随 |
| **风险等级** | 低-中 | 高 | 中 |
| **资金要求** | 较少 | 较多 | 中等 |
| **适用场景** | 震荡行情 | 回归行情 | 趋势行情 |
| **网格重置** | 无 | 无 | 自动重置 |

## ⚙️ 配置文件说明

### 普通网格配置

```yaml
grid_system:
  grid_type: "long"           # long=做多, short=做空
  upper_price: 123500.00
  lower_price: 103600.00
  grid_interval: 100.00
  order_amount: 0.0001        # 固定金额
```

### 马丁网格配置

```yaml
grid_system:
  grid_type: "martingale_long"
  upper_price: 123500.00
  lower_price: 103600.00
  grid_interval: 100.00
  order_amount: 0.001         # 基础金额
  martingale_increment: 0.001 # 递增金额
  max_position: 20.0          # 必须设置最大持仓
```

### 价格移动网格配置

```yaml
grid_system:
  grid_type: "follow_long"
  # 不需要设置 upper_price 和 lower_price
  follow_grid_count: 200      # 网格数量
  grid_interval: 50.00
  follow_timeout: 300         # 脱离超时（秒）
  follow_distance: 2          # 脱离距离（格数）
  order_amount: 0.0001
```

## 🔧 运行控制

### 启动系统

```bash
python run_grid_trading.py --config config/grid/your_config.yaml
```

### 停止系统

- **快捷键**：按 `Ctrl+C` 优雅退出
- **终端命令**：
  - `[P]` 暂停
  - `[S]` 停止
  - `[Q]` 退出

### 查看日志

```bash
# 主日志
tail -f logs/core.services.grid.coordinator.grid_coordinator.log

# 引擎日志
tail -f logs/core.services.grid.implementations.grid_engine_impl.log
```

## ⚠️ 风险提示

- **普通网格**：单边行情可能持续亏损
- **马丁网格**：持仓增长极快（二次方增长），风险极高，必须设置 `max_position`
- **价格移动网格**：会自动重置网格，需注意资金管理

**强烈建议**：
1. ✅ 小额测试：先用最小金额测试
2. ✅ 设置限制：必须设置 `max_position`（马丁网格）
3. ✅ 密切监控：随时关注持仓变化
4. ✅ 止损准备：准备好止损方案

## 📊 终端界面示例

```
┌─────────────────────────────────────────────────┐
│  网格交易系统实时监控 - BACKPACK/BTC_USDC_PERP   │
├─────────────────────────────────────────────────┤
│  运行状态                                        │
│  ├─ 网格策略: 做多网格（普通） (199格)          │
│  ├─ 价格区间: $103,600.00 - $123,500.00         │
│  ├─ 当前价格: $122,029.30  当前位置: Grid 10/199│
│  └─ 运行时长: 0:08:17                           │
├─────────────────────────────────────────────────┤
│  订单统计                                        │
│  ├─ 监控方式: 📡 WebSocket                      │
│  ├─ 未成交买单: 147个 ⏳                        │
│  ├─ 未成交卖单: 10个 ⏳                         │
│  └─ 总挂单数量: 157个                           │
├─────────────────────────────────────────────────┤
│  盈亏统计                                        │
│  ├─ 已实现: +$0.00    网格收益: +$0.00         │
│  ├─ 未实现: $-0.74    手续费: -$0.06           │
│  └─ 总盈亏: $-0.74 (-0.04%)  净收益: $-0.80    │
└─────────────────────────────────────────────────┘
```

## 🏗️ 项目结构

```
trading_strategy_sys/
├── config/                    # 配置文件
│   ├── exchanges/            # 交易所配置
│   │   ├── backpack_config.yaml
│   │   ├── edgex_config.yaml
│   │   └── hyperliquid_config.yaml
│   ├── grid/                 # 网格配置
│   │   ├── backpack_long_grid.yaml
│   │   ├── backpack_short_grid.yaml
│   │   ├── backpack_martingale_long_grid.yaml
│   │   ├── backpack_martingale_short_grid.yaml
│   │   ├── backpack_follow_long_grid.yaml
│   │   └── backpack_follow_short_grid.yaml
│   └── logging.yaml          # 日志配置
├── core/                      # 核心代码
│   ├── adapters/             # 交易所适配器
│   ├── services/             # 业务服务
│   │   └── grid/            # 网格系统
│   ├── domain/              # 领域模型
│   ├── infrastructure/      # 基础设施
│   └── di/                  # 依赖注入
├── docs/                     # 文档
├── logs/                     # 日志文件
├── run_grid_trading.py      # 网格系统启动脚本
├── requirements.txt         # 依赖列表
└── README.md               # 本文件
```

## 🤝 贡献

欢迎提交Issue和Pull Request！

## 📄 许可证

MIT License

## 📧 联系方式

如有问题或建议，请提交Issue。

---

**⚠️ 免责声明**：本软件仅供学习和研究使用。使用本软件进行实际交易的风险由用户自行承担。作者不对任何交易损失负责。
