# 样本链采录台

离线实验室扫码日志的保管链核验。原始事件与来源哈希留在 SQLite，结论关联证据 ID。样本和人员均为虚构代号，不连接外部服务。

## 运行

`python -m samplechain demo` 一条命令初始化、导入、判定并输出报告路径。`python -m samplechain --help` 查看独立命令；`python -m pytest -q` 测试。运行期只用 Python 3.11 标准库。

## 数据与规则

Batch → Sample，Source → Event，Event → Issue，Sample → Decision。采集、移交、接收、入柜、取出、检测需按顺序；封签开封和复封成对，离柜时间超过 30 分钟、温度超 2–8 度、过期或作废阻断检测。扫描设备序号断号及冲突保留问题。报告包含全部输入行、来源哈希、问题和规则版本。

当前单机实现按事件时间排序；无法在缺失外部锚点时判断设备时钟准确性。人工争议处理属于后续业务，不自动推断。

安装检查：在仓库目录运行 `python -m pip install . --no-build-isolation`；本地开发也可直接运行模块命令。报告同时生成 JSON 和 Markdown，`inspect` 子命令可追踪单个对象。

审计台账：Run 由规则版本和来源摘要确定，Evidence 关联原始对象与来源，Decision 引用 Run；重复核验保持同一输入的报告稳定。

原始来源字节保存在 SQLite BLOB 中，JSON 证据包以 Base64 表示字节；修改输入文件不会覆盖已采录证据。

## 双区冷藏控制（有限班次精确优化）

`fridge_control.py` 在既有保管链证据之上增加冷藏控制层，不回写旧事件或可检测结论。每个案例（`fridge-case.json` + 传感器 CSV）先经 `register-fridge` 核对真实 `fridge_in` 事件、入柜扫描来源与原始字节 SHA，再由 `fridge-plan` 求解。

物理模型（每区每时段）：期末温度 = 期初温度 + 外界增温 − 实际分配冷却量；共享压缩机每时段最多服务一区。各区累计热暴露为期末温度超过该区保护阈值的正温差之和，缺读数不得当零，已发生暴露不能用未来负值冲销。

`solver.py` 用毫度整数算术做精确状态合并动态规划（本地案例限 4 区 8 时段），按容量上限剪枝并保留可复核最优性证据（每层状态数、剪枝数、可行终态、最优终态键、确定性动作序），不是只看当前最热区的贪心。两种合同共用温度动力学与总暴露上限，按字典序求解：

- `min_energy`：先最小化用电整数分，再最小化各区累计暴露最大值。
- `min_max_zone_exposure`：先最小化各区累计暴露最大值，再最小化用电。

迟到传感器反馈（`fridge-feedback`）只修正剩余时段起点并重求后缀：已执行动作、已发生电费与暴露不可变。传感器身份冲突、时区不一致/无时区、缺项、非物理读数或无可行方案时，整份待执行命令置 `held`、计划置 `held`/`infeasible` 并写入 `fridge_hold`，不发布无法执行的片段；同 SHA 反馈重放幂等。

CLI：`register-fridge` → `fridge-plan --contract …` → `fridge-execute --period …` → `fridge-feedback` → `fridge-report`（同时输出 JSON 与 Markdown，含每时段读数/预测温度、命令与占用、各区暴露、电价、来源摘要、计划版本与最优性证据）。同输入重算的计划 ID 与报告稳定。
