# A9 原生执行与恢复审计

## 证据基线与工作范围

起始源码：`2757dd6c21f1b9126041ab66c5698939f9df0090`，0.5.0a8 Desktop Bridge Protocol。
本次在 `liandanlu-0.5.0a9-recovery-integrity` 分支工作，不把 a7 的历史描述当作当前代码，不修改主分支，不改用户本机配置或旧项目包。

仅添加复现测试的提交 `b1c4e4629b14d4debc44f985c7d92c6c01e7be77`：GitHub Actions Run 35625751002，在 Linux Python 3.13 日志中为 **10 failed, 62 passed**。十个失败都位于新复现用例中，揭示原先正常路径测试未覆盖的执行/恢复问题。

第一轮修复提交 `2b3f160292d1152e5421b87135fa2ab8725ff6e3`：Run 35626783080，四组 Linux/macOS CI 通过；Linux Python 3.13 日志为 **72 passed，81.17% statement coverage**。之后增加的进程死亡测试与最终结果，以本源码构建附带的 `verification/` 和 CI 日志为准，不把此前数字冒充最终测试总数。

## 复现问题 → 修复边界

1. 同一操作被重复 execute / 两个连接同时 execute：原子比较 PREPARED 状态后更新 RUNNING，只有事务赢家进入 Adapter；复用 Operation ID 不可再次执行。此保证仅覆盖通过该入口的本地派发。
2. Worker 租约过期后仍调用模型或改任务：模型调用前核对所有权，调用后再次核对；Worker 更新任务与租约核对在同一短事务里。过期 Worker 不得把新 Worker 的 Task 改成 WAITING。
3. 两份 Task Python 对象各自认为还有预算：预算从持久表读取，扣减、操作创建和 Outbox 进入同一事务。注入 Outbox 写失败时整组回滚。
4. reconcile(False, result) 被解释为绝对失败：保留 UNKNOWN，因为 False 没有证明外部动作未发生。reconcile(True, result) 必须继续验证，不能跳过 Verifier。
5. Provider 缺失使整个 Recovery 退出：单操作保持 UNKNOWN 和诊断，其他操作继续核对，报告 degraded。
6. UNKNOWN 操作被假取消：desired_state 记录请求，actual_state 保持 CANCELLING/PAUSING，直到未决操作有确定结果。
7. Worker 退出或 TTL 到期后重分配尚在使用的额度：匹配未决 Operation 的资源记录被保留。仅经核对完成才释放；不声称能强制限制进程实际 RSS。
8. 同工作区的另一 Task 上下文被套用：同时校验 ContextManifest.task_id 与 workspace_id。
9. 已准备参数、工作区权限发生变化：在持久派发边界重新核对不可变请求与当前对象权限，发现冲突不得执行。

## 新增真实崩溃验收

`scripts/companion_smoke.py` 运行真实 Python 子进程，通过 `os._exit(73)` 在五个事务/副作用边界直接退出，不执行 finally 清理。副作用使用 fsync 后的非幂等文件追加；重启若错误重放，会出现第二条相同操作标记。恢复必须只查询/验证，不得再次追加。还检查原文件 SHA-256、Outbox 重新派发无重复、SQLite integrity_check。

这是进程死亡/文件系统测试，不是断电耐久性测试，不等于真实云 API 或 macOS GUI 验证。模糊结果保持 UNKNOWN 是预期安全行为，不是用假成功凑齐测试。

## 查阅的外部资料与应用方式

- SQLite 官方事务：https://www.sqlite.org/lang_transaction.html
  BEGIN IMMEDIATE 适用于先读后写的短临界区；外部调用不得放在数据库锁内。本次用于派发竞争、预算和资源额度，未引入新数据库框架。
- SQLite 官方隔离：https://sqlite.org/isolation.html
  多连接使用持久数据库作为裁决点，而非各自 Python 缓存。
- Hazelcast 官方 FencedLock：https://docs.hazelcast.com/hazelcast/5.6/data-structures/fencedlock
  外部接收端也要识别 fencing token；本地校验不能撤回已发出的外部请求。本项目没有安装 Hazelcast，借鉴的是执行语义。
- Python subprocess 官方文档：https://docs.python.org/3.11/library/subprocess.html
  超时需正确清理子进程并回收返回状态。分组验证器保存 stdout/stderr，超时终止本组进程组，不无限轮询。

## 未关闭的边界与后续工作

- 外部副作用的 exactly-once 不作承诺。需实际 Provider 的幂等/查询/取消接口，以及 Desktop Bridge 服务端对 token 的拒绝协议。
- `GoalClaim.evidence_refs` 仍未通过完整 Evidence Registry 做真实性认证；不能称任务结论已经无法伪造。
- 所有 WorldState 投影的跨表原子更新、长期任务续租/恢复调度、完整资源状态探测、长时间稳定性测试仍需加固。
- SQLiteStore 的旧低层方法为兼容保留；对外服务只能暴露已授权的 Runtime/Broker 入口，不能把 store 直接暴露给 LLM 或不可信插件。
- 资源容量为显式配置的额度，并非真实物理 RAM/GPU 硬限制。未知本地重任务可能导致额度保守占用，需要核对旧进程而非自动清空。
- a9 源码 ZIP 仅涵盖当前 Native Core 仓库，不声称包含之前所有前端、Swift App、视频素材或神经连接组数据。
