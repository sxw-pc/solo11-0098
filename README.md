# 样本链采录台

离线实验室扫码日志的保管链核验。原始事件与来源哈希留在 SQLite，结论关联证据 ID。样本和人员均为虚构代号，不连接外部服务。

## 运行

`python -m samplechain demo` 一条命令初始化、导入、判定并输出报告路径。`python -m samplechain --help` 查看独立命令；`python -m pytest -q` 测试。运行期只用 Python 3.11 标准库。

## 数据与规则

Batch → Sample，Source → Event，Event → Issue，Sample → Decision。采集、移交、接收、入柜、取出、检测需按顺序；封签开封和复封成对，离柜时间超过 30 分钟、温度超 2–8 度、过期或作废阻断检测。扫描设备序号断号及冲突保留问题。报告包含全部输入行、来源哈希、问题和规则版本。

当前单机实现按事件时间排序；无法在缺失外部锚点时判断设备时钟准确性。人工争议处理属于后续业务，不自动推断。

## 换采接管

样本可经 `replacement_of` 关联换采后代，链允许多代；循环、同一样本出现两个有效替代者、跨批次链接都会阻断相关检测。样本可带自身 `expiry` 覆盖批次到期时间，父子样本各自保留完整封签、温度暴露、到期和测试链，子样本的合格证据不回写父样本。放行契约由 `policy` 选定（`init`/`evaluate` 的 `--policy`，默认 `acceptance`）：`collect` 采集即替代，recollect 事件时刻父样本立即停止检测资格；`acceptance` 验收后替代，子样本完成 receive、fridge_in 且封签/温度证据合格后才接管，等待期间父样本依自身有效期与证据独立判定。recollect 事件可声明 `replacement` 子样本，与登记链接不一致记 `recollect_mismatch`。每次判定把每条链的接管状态与生效时点写入 takeover 表，报告按代列出授权窗口、事件链与当前责任样本，历史判定不被覆盖。

安装检查：在仓库目录运行 `python -m pip install . --no-build-isolation`；本地开发也可直接运行模块命令。报告同时生成 JSON 和 Markdown，`inspect` 子命令可追踪单个对象。

审计台账：Run 由规则版本和来源摘要确定，Evidence 关联原始对象与来源，Decision 引用 Run；重复核验保持同一输入的报告稳定。

原始来源字节保存在 SQLite BLOB 中，JSON 证据包以 Base64 表示字节；修改输入文件不会覆盖已采录证据。
