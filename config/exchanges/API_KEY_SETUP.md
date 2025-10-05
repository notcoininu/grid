# API密钥配置指南

## ⚠️ 重要安全提示

**请勿将包含真实API密钥的配置文件提交到Git仓库！**

## 📋 配置步骤

### 1. Backpack交易所

编辑 `config/exchanges/backpack_config.yaml`：

```yaml
authentication:
  method: "ed25519"
  api_key: "your_api_key_here"           # 替换为你的API Key
  private_key: "your_private_key_here"   # 替换为你的私钥
```

**获取API密钥**：
1. 登录 [Backpack Exchange](https://backpack.exchange)
2. 进入 API 管理页面
3. 创建新的API密钥
4. 确保只启用"交易"权限（不需要"提现"权限）
5. 保存API Key和私钥

### 2. EdgeX交易所

编辑 `config/exchanges/edgex_config.yaml`：

```yaml
authentication:
  method: "hmac_sha256"
  api_key: "your_api_key_here"     # 替换为你的API Key
  api_secret: "your_api_secret_here" # 替换为你的API Secret
```

### 3. Hyperliquid交易所

编辑 `config/exchanges/hyperliquid_config.yaml`：

```yaml
authentication:
  private_key: 'your_private_key_here'     # 替换为你的钱包私钥
  wallet_address: 'your_wallet_address_here' # 替换为你的钱包地址
```

**⚠️ Hyperliquid特别注意**：
- Hyperliquid使用钱包签名认证
- 私钥格式：`0x...`（以0x开头的64位十六进制字符串）
- 钱包地址格式：`0x...`（以0x开头的40位十六进制字符串）

## 🔐 安全最佳实践

### 方式一：使用环境变量（推荐）

在 `~/.bashrc` 或 `~/.zshrc` 中添加：

```bash
# Backpack
export BACKPACK_API_KEY="your_api_key"
export BACKPACK_API_SECRET="your_private_key"

# EdgeX
export EDGEX_API_KEY="your_api_key"
export EDGEX_API_SECRET="your_api_secret"

# Hyperliquid
export HYPERLIQUID_PRIVATE_KEY="your_private_key"
export HYPERLIQUID_WALLET_ADDRESS="your_wallet_address"
```

然后在配置文件中引用：

```yaml
authentication:
  api_key: "${BACKPACK_API_KEY}"
  private_key: "${BACKPACK_API_SECRET}"
```

### 方式二：使用单独的密钥文件

创建 `config/exchanges/.secrets.yaml`（已在 `.gitignore` 中）：

```yaml
backpack:
  api_key: "your_api_key"
  private_key: "your_private_key"

edgex:
  api_key: "your_api_key"
  api_secret: "your_api_secret"

hyperliquid:
  private_key: "your_private_key"
  wallet_address: "your_wallet_address"
```

**确保将 `.secrets.yaml` 添加到 `.gitignore`！**

## ✅ 验证配置

运行测试脚本验证API密钥是否正确：

```bash
# 测试Backpack连接
python -c "from core.adapters.exchanges.factory import create_adapter; adapter = create_adapter('backpack'); print('✅ Backpack连接成功')"
```

## 🆘 常见问题

### Q1: "Invalid signature" 错误

**原因**：API密钥或私钥不正确，或者签名算法有误。

**解决方案**：
1. 确认API密钥和私钥是否正确复制（没有多余空格）
2. 确认API密钥是否已激活
3. 确认API密钥权限是否正确

### Q2: "Unauthorized" 错误

**原因**：API密钥没有足够的权限。

**解决方案**：
1. 检查API密钥权限设置
2. 确保启用了"交易"权限
3. 重新创建API密钥

### Q3: Hyperliquid "Invalid private key" 错误

**原因**：私钥格式不正确。

**解决方案**：
1. 确认私钥以 `0x` 开头
2. 确认私钥是64位十六进制字符串
3. 不要包含任何空格或换行符

## 📞 获取帮助

如果遇到问题，请：
1. 检查日志文件：`logs/core.services.grid.coordinator.grid_coordinator.log`
2. 查看文档：`docs/网格系统运行指南.md`
3. 提交Issue到GitHub

---

**最后更新**：2025-10-05
