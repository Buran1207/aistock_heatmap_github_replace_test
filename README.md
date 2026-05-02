# AI Stock Heatmap

US / H / A 三市场 AI 产业链热力图。

## 本版重构重点

- 股票池只维护 `universe.json`。同一股票可以出现在多个细分环节，`fetch_prices.py` 会按 `市场:代码` 自动去重。
- 市场统一为 `US / H / A`，不再使用 `HK`。
- 细分环节按业务实质重排：
  - 基础大模型只保留自研通用/多模态模型或 MaaS 平台；
  - 金山办公、微盟等移入应用层；
  - ASML/AMAT/华大九天从 GPU/NPU 移入晶圆制造/设备/EDA/IP；
  - WDC/STX 从服务器整机移入存储/HBM/CXL；
  - 光模块、网络设备、散热液冷、IDC、云服务拆开。
- 个股点击打开 TradingView 图表页。浏览器已登录 TradingView 时，会直接使用用户自己的 TradingView 账户。

## 手动运行行情更新

GitHub 仓库页面 → Actions → `Update AI stock heatmap prices` → `Run workflow`。

若看不到 workflow，请确认文件路径是：

```text
.github/workflows/update-prices.yml
```

并确认 Settings → Actions → General → Workflow permissions 为 `Read and write permissions`。
