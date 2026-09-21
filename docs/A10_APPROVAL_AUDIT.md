# A10 审批链审计

## 基线与范围

基于 a9 commit `312ff122d4da841fe861bb44160f80e9b468e2ff`，a9 PR #7 尚未合并时以堆叠分支开发。不修改 main；本包只覆盖 Native Core 仓库。

上一版本 Policy 可要求确认，但 Runner 只写 WAITING，未保存可恢复的待批准提案。这轮添加 durable ApprovalService，并将票据消耗与既有 Operation 事务连接。没有新增独立任务引擎。

## 数据流

ContextManifest → scripted/live model interface → ActionProposal → Policy → approval_requests(PENDING) → trusted Host human decision → APPROVED + QUEUED → new task lease → restore saved proposal (no model call) → reserve Operation + consume approval + debit budget + Outbox (one transaction) → final preflight → adapter → Verifier → EventLog.

## 必须成立的性质

1. 请求保存不占用操作预算，不执行工具。PENDING 状态下轮询不反复调用模型。
2. 用户批准绑定 Task、Workspace、参数、对象身份指纹、任务约束、能力规格、策略和有效期；不是全局放行开关。
3. 原始文件路径不出现在审批快照，存储的是对象引用与指纹。
4. 批准后不重新规划；重启后也恢复已批准的原动作。
5. 同一票据只能绑定一个 Operation。预算、操作、票据消耗和事件写入原子提交，异常整体回滚。
6. 消耗后派发前再次检查策略/对象/有效期/撤销；重复 approve 不重复排队。
7. 撤销会阻断排队和自动重新规划。已派发操作不可用 revoke 假装撤销副作用。
8. UNKNOWN 保留原票据已消耗状态；恢复只能核对、验证，不重放操作。
9. 人工拒绝、参数变化、权限变化、过期、跨工作区、错误身份、并发使用票据均有回归测试。
10. 真实子进程退出测试独立于布尔模拟成功，临时文件写入使用 fsync，恢复验证读取实际文件。

## 已知边界，不作扩大声明

- `human_principals` 校验依赖可信 Host 提供已认证的身份；不是用户登录、签名或抗恶意本地 Python 的隔离系统。工具注册表不得暴露 decide/revoke。
- digest 用于绑定已展示内容，不是数字签名。持有数据库写权限的攻击者不在本轮防护范围内。
- 对象指纹绑定已持久化的 Observation/Registry；未被观察的外部文件修改仍须适配器在执行时进行内容哈希/版本前置检查。
- 票据有效期随请求创建开始计时。过期或对象变化需撤销旧请求，再显式提出新请求；不静默更新用户批准内容。
- 票据已消耗但 PREPARED 后崩溃，恢复将取消未派发操作，不退回票据自动重试。资源/预算语义保持保守。
- 并发保证基于同一 SQLite 文件；不是跨主机外部 Provider exactly-once。
- 现有 GoalClaim 还没有完整 Evidence Registry 认证，本轮不把它标为完成。
- Native Core 的低层 Python API 属于可信内核接口；生产模型只能经过配置了 Policy/ApprovalService 的 NativeAgentRunner。

## 外部资料（原则对照，不照搬代码）

- OWASP Transaction Authorization Cheat Sheet：显著交易数据必须展示并绑定；授权在服务端执行；最后的控制检查与执行相连，防止批准内容被替换。
  https://cheatsheetseries.owasp.org/cheatsheets/Transaction_Authorization_Cheat_Sheet.html
- SQLite Transaction：BEGIN IMMEDIATE 将读取当前授权/预算并决定写入放入同一写事务；事务内不执行网络或操作系统副作用。
  https://www.sqlite.org/lang_transaction.html

验证报告由 CI 生成到 verification/，最终打包报告为 PACKAGE_REPORT.json。不要把代码存在、测试通过和用户 Mac 实际验收混同。
