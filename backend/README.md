# 加密货币专业交易终端系统

支持币安和OKX现货/合约交易的实时行情监控和自动交易系统，采用Flutter + FastAPI技术栈。

## 🏗️ 项目结构

```
crypto-trading-terminal/
├── backend/                 # Python FastAPI 后端
│   ├── src/
│   │   ├── main.py         # FastAPI应用入口
│   │   ├── config.py       # 配置管理
│   │   ├── adapters/       # 交易所适配器
│   │   ├── api/           # API路由
│   │   ├── core/          # 核心业务逻辑
│   │   ├── storage/       # 数据存储
│   │   └── ...
│   └── requirements.txt   # Python依赖
│
├── frontend/               # Flutter 桌面应用
│   ├── lib/
│   │   ├── main.dart      # Flutter应用入口
│   │   └── src/           # 应用源码
│   │       ├── domain/    # 业务逻辑层
│   │       ├── data/      # 数据层
│   │       └── presentation/ # UI层
│   └── pubspec.yaml       # Flutter依赖
│
├── tests/                 # 测试文件
│   ├── unit/             # 单元测试
│   ├── integration/      # 集成测试
│   ├── contract/         # 契约测试
│   └── performance/      # 性能测试
│
└── docs/                 # 项目文档
```

## 🚀 快速开始

### 后端启动

1. 进入后端目录：
   ```bash
   cd crypto-trading-terminal/backend
   ```

2. 创建虚拟环境：
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   source venv/bin/activate  # Linux/macOS
   ```

3. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

4. 启动服务：
   ```bash
   cd src
   python main.py
   ```

### 前端启动

1. 进入前端目录：
   ```bash
   cd crypto-trading-terminal/frontend
   ```

2. 获取依赖：
   ```bash
   flutter pub get
   ```

3. 运行应用：
   ```bash
   flutter run -d windows
   ```

## 📋 功能特性

### 核心功能 (P1)
- ✅ 现货交易实时行情监控与展示
- ✅ 合约交易实时行情监控与展示  
- ✅ 双交易所数据源管理与自动切换

### 高级功能 (P2/P3)
- 🔄 条件触发与多渠道通知系统
- 🔄 自动下单与风险控制
- 🔄 策略交易系统 (现货/合约)
- 🔄 账户管理与盈亏分析
- 🔄 AI智能分析与策略优化
- 🔄 Windows桌面界面体验优化

## 🛠️ 技术栈

### 后端
- **框架**: FastAPI + Uvicorn
- **数据库**: SQLAlchemy + SQLite/PostgreSQL + Redis
- **缓存**: Redis
- **WebSocket**: 支持实时数据推送
- **日志**: Structlog + Prometheus

### 前端
- **框架**: Flutter 3.16+
- **UI库**: Material 3 Design
- **状态管理**: Riverpod
- **图表**: FlChart + Syncfusion
- **桌面支持**: Window Manager

### 交易所集成
- **现货交易**: 币安 + OKX
- **合约交易**: 币安期货 + OKX衍生品
- **数据源**: REST API + WebSocket

## 📝 开发指南

### 环境要求
- Python 3.11+
- Flutter 3.16+
- Redis 6.0+ (可选)

### 代码规范
- **Python**: Black + isort + mypy
- **Flutter**: flutter_lints + very_good_analysis
- **Git**: 提交前运行格式化工具

### 测试
```bash
# 后端测试
cd backend
pytest tests/ -v

# 前端测试
cd frontend
flutter test
```

## 🔧 配置

### 环境变量
创建 `backend/.env` 文件：
```bash
# API配置
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=true

# 交易所配置 (可选，实盘交易需要)
BINANCE_API_KEY=your_api_key
BINANCE_SECRET_KEY=your_secret_key
OKX_API_KEY=your_api_key
OKX_SECRET_KEY=your_secret_key
OKX_PASSPHRASE=your_passphrase

# 测试环境
BINANCE_TESTNET=true
OKX_PAPER_TRADING=true
```

## 📊 项目进度

- [x] 项目结构创建 (Phase 1)
- [ ] 后端基础框架 (Phase 2)
- [ ] 前端基础框架 (Phase 2)
- [ ] 现货市场数据 (Phase 3)
- [ ] 合约市场数据 (Phase 3)
- [ ] 交易所切换 (Phase 3)
- [ ] 条件触发系统 (Phase 4)
- [ ] 自动交易功能 (Phase 5)
- [ ] 策略交易系统 (Phase 6-7)
- [ ] 账户管理 (Phase 8)
- [ ] AI分析功能 (Phase 9)
- [ ] 界面优化 (Phase 10)

## 🤝 贡献指南

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交修改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## ⚠️ 风险提示

1. **模拟交易**: 初期使用测试网和模拟交易功能
2. **资金安全**: 实盘交易前请充分测试
3. **风险管理**: 严格设置止损和资金管理
4. **投资有风险**: 加密货币交易存在重大风险

---

**版本**: v1.0.0  
**更新日期**: 2025-11-14