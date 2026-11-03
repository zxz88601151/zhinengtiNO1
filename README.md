# WAH — Web Agent Hub（V0 竖直切面）

本地优先的 Voice-First 智能体控制中心。设计基线见仓库上级 `01_交付文档/` 与 `03_项目意图与实施路线.md`。

## 运行

后端（模块根目录 `wah/`）：

```
go run ./apps/server
```

前端：

```
cd frontend
npm install
npm run dev
```

打开 http://127.0.0.1:5173 。后端 http://127.0.0.1:8080 。

Chrome / Edge 可用麦克风（Web Speech）。其它浏览器请用文字输入。

## 试这些句子

- `现在几点了` → L0 直答
- `记下明天买咖啡` → L1 确认后写笔记
- `每天早上八点检查邮箱` → L2 预约（只表示已安排）
- `帮我填报名表并提交` → L3，需输入「确认提交」

## 测试

```
go test ./...
```

覆盖：任务状态机合法迁移、L0 直答/L3 审批门控/拒绝取消、重复裁定与终态取消防护、审计日志 append-only 触发器。

## 本轮未做

云厂商 STT/TTS/LLM、真实 Playwright、Tauri 壳、预算限流、调度补跑终局（Q5/Q6 未裁）。
