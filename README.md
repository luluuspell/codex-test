# 炼丹炉 Native Companion Core — 0.5.0a9 Recovery Integrity

本包是炼丹炉原生后端内核仓库的完整源码快照，不是旧前端、影视后端、模型权重和用户素材的整合安装包。它在 a8 Desktop Bridge Protocol 上加固任务执行与恢复；未改用 LangGraph 或 Temporal。

## 立即验证

Python 3.11 或更新版本，无需模型 API、无需下载大模型：

```bash
python3 scripts/companion_smoke.py
```

这会在临时目录运行五个真实子进程崩溃场景：准备后退出、派发后副作用前退出、文件写入后回执前退出、结果落库后验证前退出、验证后事件分发前退出。输出包含实际退出码、恢复状态、副作用次数、原文件校验和 SQLite 完整性。

完整分组测试（创建本目录 .venv，仅安装测试依赖）：

```bash
bash verify.command
```

结果存于 `verification/verification.json`；每组独立超时和日志，结果区分 PASS / TEST_FAILURE / TIMEOUT / INFRASTRUCTURE_ERROR。

## 本轮实现

- `integrity.py`：短 SQLite BEGIN IMMEDIATE 事务；数据库内读取预算、扣减预算、保存操作和 Outbox；原子 PREPARED → RUNNING 竞争；操作不可变参数检查；持久结果的比较后更新。
- `owned_state.py`：Worker 写任务状态时重新核对租约，不能覆盖新 Worker 或用户已提交的暂停/取消。
- `runtime.py`：同一 Operation 不得二次派发；未知结果保持 UNKNOWN；reconcile 返回已执行仍要跑 Verifier；缺失 Provider 不终止其他恢复。
- `resources.py`：未解决操作绑定的资源额度不会因 TTL 到期或 Worker finally 退出而被重新分配；核对完成后才能释放。
- `agent.py`：模型调用前、返回后和状态写入时检查所有权；校验 Task 与 ContextManifest 的对应关系。
- `recovery.py`：逐操作隔离错误，返回 ready/degraded；有未核实操作时不假报取消完成。

## 保留的 a8 能力

Engine 侧 Unix Socket 客户端、长度帧、协议与能力握手、同用户 peer 校验、Bridge generation 和响应关联检查继续保留。World/ObjectRef、任务租约、记忆、事件消费游标仍使用同一 SQLite 存储。

## 验证边界

CI 包含 Linux Python 3.11/3.12/3.13 与 macOS Python 3.13。测试覆盖率描述执行过的语句比例，不代表产品完成度或无漏洞证明。包内 PACKAGE_META.json、MANIFEST.sha256 和 verification/ 给出本次构建的实际证据。

真实 macOS Finder/Accessibility/音乐控制、实时语音、真实云 LLM、网站/电商/视频 Provider、MaleCNS 神经仿真仍未作为本轮完成项。内置测试模型是明确标记的 fixture，不冒充真实大模型。

特别注意：本地原子派发不是任意第三方系统的 exactly-once；外部执行端仍需支持 fencing、幂等键或可查询回执。ResourceBroker 是额度准入，不是操作系统内存硬限制。UNKNOWN 保守等待需要后续 Provider 回执或人工核对，而不是无限自动重试。

详见 [本轮审计与来源](docs/A9_INTEGRITY_AUDIT.md)。
